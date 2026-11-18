package io.hanacdc.connect;

import io.hanacdc.control.CapabilityAssessment;
import io.hanacdc.control.HanaCapabilities;
import io.hanacdc.control.HanaContract;
import io.hanacdc.control.HanaControlPlane;
import io.hanacdc.control.JdbcHanaControlPlane;
import io.hanacdc.control.LogBackupEntry;
import io.hanacdc.control.ManifestPlanner;
import io.hanacdc.control.PathMapper;
import io.hanacdc.control.PersistenceStreamStatus;
import io.hanacdc.control.RetentionGuard;
import io.hanacdc.control.ScaleOutCatalogSnapshot;
import io.hanacdc.control.ScaleOutPathMapping;
import io.hanacdc.control.StreamingWatermarks;
import io.hanacdc.decoder.ParallelLogDecoder;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;
import io.hanacdc.decoder.ParallelLogDecoder.Cursor;
import io.hanacdc.decoder.ParallelLogDecoder.DecodedFile;
import io.hanacdc.decoder.ParallelLogDecoder.DecoderSession;
import io.hanacdc.decoder.ParallelLogDecoder.EventPointer;
import io.hanacdc.decoder.ParallelLogDecoder.ScanConfig;
import io.hanacdc.decoder.NativeContainerResolver;
import io.hanacdc.decoder.NativeBackupHeaderValidator;
import io.hanacdc.decoder.NativeRowLobPipeline;
import io.hanacdc.schema.TableDecodePlan;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.NoSuchFileException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/** Ordered source task with read-only HANA policy/catalog discovery. */
public final class HanaSourceTask extends SourceTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(HanaSourceTask.class);
    private static final long OBJECT_VISIBILITY_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(60);

    private final ParallelLogDecoder decoder = new ParallelLogDecoder();
    private final ReentrantLock pollOwnership = new ReentrantLock();
    private final AtomicReference<Thread> pollingThread = new AtomicReference<>();

    private volatile boolean stopped;
    private List<BackupFile> backups = new ArrayList<>();
    private int fileIndex;
    private DecodedFile decodedFile;
    private Cursor cursor;
    private DecoderSession decoderSession;
    private ScanConfig scanConfig;
    private DebeziumEventFactory eventFactory;
    private EventSizeGuard eventSizeGuard;
    private Map<String, Object> sourcePartition;
    private String runId;
    private int maxBatchSize;
    private long pollIntervalMs;
    private volatile long firstPosition;
    private volatile long lastPosition;
    private volatile long sourceId;
    private volatile long sourceVolume;
    private volatile long sourcePartitionId;
    private volatile long resumeBackupId = -1;
    private long resumeFileFirst = -1;
    private long resumeEventOffset = -1;
    private long resumeLogPosition = -1;
    private SourceOffset resumeOffset;
    private SourceEpoch sourceEpoch;
    private boolean hasStoredOffset;
    private boolean resumeCheckpointSeen;
    private boolean finiteCorpusFinished;

    private volatile boolean catalogMode;
    private HanaControlPlane controlPlane;
    private TableIncludeList tableIncludeList;
    private List<TableDecodePlan> tableDecodePlans = List.of();
    private TablePlanGuard tablePlanGuard;
    private SourceTopologyMode configuredTopologyMode;
    private SourceTopology sourceTopology;
    private HanaContract contract;
    private PathMapper pathMapper;
    private HanaCapabilities initialCapabilities;
    private long policyRefreshIntervalMs;
    private long policyMaximumStalenessMs;
    private AsyncPolicyRefresher<PolicyRefreshSnapshot> policyRefresher;
    private String policyViolationMode;
    private int idlePolls;
    private Path missingPath;
    private long missingSinceNanos;
    private volatile long objectVisibleHwm;
    private volatile long decodedHwm;
    private volatile long publishedHwm;
    private volatile long kafkaCommittedHwm;
    private volatile long oldestRequiredPosition;
    private long previousRatePosition;
    private long previousRateObservedNanos;
    private double observedRedoBytesPerSecond;
    private final NativeBackupHeaderValidator nativeHeaderValidator =
            new NativeBackupHeaderValidator();
    private String catalogDatabaseId;
    private volatile boolean prototypeMode;
    private NativeRowLobPipeline nativePipeline;
    private NativeRowLobPipeline.BackupCursor nativeCursor;
    private BoundedPrefetch<PrefetchedNativeTransaction> nativePrefetch;
    private NativeCheckpointCoordinator checkpointCoordinator;
    private NativeDebeziumEventFactory nativeEventFactory;
    private OrderedNativeEventBuilder nativeEventBuilder;
    private DurableReducerStateStore nativeStateStore;
    private final Deque<SourceRecord> nativePendingRecords = new ArrayDeque<>();
    private boolean nativeResumePending;
    private HeartbeatFactory heartbeatFactory;
    private Map<String, Object> latestHeartbeatOffset;
    private long heartbeatIntervalNanos;
    private long lastHeartbeatNanos;
    private volatile long retentionMinimumSeconds;
    private volatile long retentionSafetyMarginSeconds;
    private RetentionGuard.Status retentionStatus;
    private NativeRowLobPipeline.CompletedTransaction pendingProgressTransaction;
    private ReplayBoundary pendingProgressBoundary;
    private long nativeCheckpointNanos;
    private long nativeEventBuildNanos;
    private long nativePublications;
    private long nativePrefetchedTransactions;
    private long nativePrefetchWaitNanos;
    private long nativePolicyRefreshCount;
    private long nativePolicyRefreshNanos;
    private long nativePolicyApplyNanos;
    private long nativeCapabilityInspectNanos;
    private long nativeMetadataRefreshNanos;
    private long nativeTopologyRefreshNanos;
    private long nativeRetentionRefreshNanos;
    private boolean nativeMetricsReported;

    private record PrefetchedNativeTransaction(
            NativeRowLobPipeline.CompletedTransaction transaction,
            ReplayBoundary boundary) {}

    private record PolicyRefreshSnapshot(
            HanaCapabilities capabilities,
            List<TableDecodePlan> plans,
            SourceTopology topology,
            RetentionGuard.Status retention,
            long capabilityInspectNanos,
            long metadataRefreshNanos,
            long topologyRefreshNanos,
            long retentionRefreshNanos) {
        private PolicyRefreshSnapshot {
            plans = List.copyOf(plans);
        }
    }

    @Override
    public void start(Map<String, String> props) {
        stopped = false;
        fileIndex = 0;
        backups = new ArrayList<>();
        decoderSession = new DecoderSession();
        resumeBackupId = -1;
        resumeFileFirst = -1;
        resumeEventOffset = -1;
        resumeLogPosition = -1;
        resumeOffset = null;
        sourceEpoch = null;
        hasStoredOffset = false;
        resumeCheckpointSeen = false;
        finiteCorpusFinished = false;
        idlePolls = 0;
        missingPath = null;
        missingSinceNanos = 0;
        objectVisibleHwm = 0;
        decodedHwm = 0;
        publishedHwm = 0;
        kafkaCommittedHwm = 0;
        oldestRequiredPosition = 0;
        previousRatePosition = 0;
        previousRateObservedNanos = 0;
        observedRedoBytesPerSecond = 0;
        catalogDatabaseId = null;
        nativePipeline = null;
        nativeCursor = null;
        nativePrefetch = null;
        checkpointCoordinator = null;
        nativeEventFactory = null;
        nativeEventBuilder = null;
        eventSizeGuard = null;
        nativeStateStore = null;
        nativePendingRecords.clear();
        nativeResumePending = false;
        nativeCheckpointNanos = 0;
        nativeEventBuildNanos = 0;
        nativePublications = 0;
        nativePrefetchedTransactions = 0;
        nativePrefetchWaitNanos = 0;
        nativePolicyRefreshCount = 0;
        nativePolicyRefreshNanos = 0;
        nativePolicyApplyNanos = 0;
        nativeCapabilityInspectNanos = 0;
        nativeMetadataRefreshNanos = 0;
        nativeTopologyRefreshNanos = 0;
        nativeRetentionRefreshNanos = 0;
        nativeMetricsReported = false;
        heartbeatFactory = null;
        latestHeartbeatOffset = null;
        retentionStatus = null;
        policyRefresher = null;
        pendingProgressTransaction = null;
        pendingProgressBoundary = null;
        Map<String, String> normalized = HanaSourceConnector
                .normalizeParallelismProperties(props);
        AbstractConfig config = new AbstractConfig(
                HanaSourceConnector.CONFIG_DEF, normalized);
        tableIncludeList = TableIncludeList.compile(
                config.getList(HanaSourceConnector.TABLE_INCLUDE_LIST));
        tableDecodePlans = List.of();
        tablePlanGuard = null;
        configuredTopologyMode = SourceTopologyMode.parse(
                config.getString(HanaSourceConnector.SOURCE_TOPOLOGY_MODE));
        sourceTopology = null;
        prototypeMode = config.getBoolean(HanaSourceConnector.PROTOTYPE_MODE);
        runId = config.getString(HanaSourceConnector.RUN_ID);
        firstPosition = config.getLong(HanaSourceConnector.FIRST_POSITION);
        lastPosition = config.getLong(HanaSourceConnector.LAST_POSITION);
        if (lastPosition <= firstPosition) {
            throw new ConnectException("last.position must be greater than first.position");
        }
        objectVisibleHwm = firstPosition;
        decodedHwm = firstPosition;
        publishedHwm = firstPosition;
        kafkaCommittedHwm = firstPosition;
        oldestRequiredPosition = firstPosition;
        sourceId = config.getLong(HanaSourceConnector.SOURCE_ID);
        sourceVolume = config.getLong(HanaSourceConnector.SOURCE_VOLUME);
        sourcePartitionId = config.getLong(HanaSourceConnector.SOURCE_PARTITION);
        String threads = config.getString(HanaSourceConnector.SCAN_THREADS);
        int maxThreads = config.getInt(HanaSourceConnector.SCAN_MAX_THREADS);
        long targetChunkBytes = Math.multiplyExact(
                config.getInt(HanaSourceConnector.TARGET_CHUNK_MIB), 1024L * 1024L);
        int targetChunkMib = (int) Math.max(1L,
                Math.min(Integer.MAX_VALUE, targetChunkBytes / (1024L * 1024L)));
        scanConfig = new ScanConfig(
                threads,
                maxThreads,
                targetChunkBytes,
                targetChunkMib,
                config.getInt(HanaSourceConnector.LOG_PROCESSOR_CHUNKS_PER_THREAD),
                false);
        maxBatchSize = config.getInt(HanaSourceConnector.POLL_MAX_BATCH_SIZE);
        pollIntervalMs = config.getLong(HanaSourceConnector.POLL_INTERVAL_MS);
        eventSizeGuard = new EventSizeGuard(
                config.getLong(HanaSourceConnector.EVENT_MAX_BYTES));
        policyRefreshIntervalMs = config.getLong(HanaSourceConnector.POLICY_REFRESH_INTERVAL_MS);
        policyMaximumStalenessMs = config.getLong(
                HanaSourceConnector.POLICY_MAX_STALENESS_MS);
        if (policyMaximumStalenessMs < policyRefreshIntervalMs) {
            throw new ConnectException(HanaSourceConnector.POLICY_MAX_STALENESS_MS
                    + " must be greater than or equal to "
                    + HanaSourceConnector.POLICY_REFRESH_INTERVAL_MS);
        }
        policyViolationMode = config.getString(HanaSourceConnector.POLICY_VIOLATION_MODE);
        String serverName = config.getString(HanaSourceConnector.SERVER_NAME);
        heartbeatIntervalNanos = TimeUnit.MILLISECONDS.toNanos(
                config.getLong(HanaSourceConnector.HEARTBEAT_INTERVAL_MS));
        lastHeartbeatNanos = System.nanoTime();
        String heartbeatTopic = config.getString(HanaSourceConnector.HEARTBEAT_TOPIC_NAME).strip();
        if (heartbeatTopic.isEmpty()) {
            heartbeatTopic = config.getString(HanaSourceConnector.HEARTBEAT_TOPIC_PREFIX)
                    + "." + serverName;
        }
        heartbeatFactory = new HeartbeatFactory(serverName, heartbeatTopic);
        retentionMinimumSeconds = config.getLong(HanaSourceConnector.RETENTION_MIN_SECONDS);
        retentionSafetyMarginSeconds = config.getLong(
                HanaSourceConnector.RETENTION_SAFETY_MARGIN_SECONDS);
        eventFactory = prototypeMode ? new DebeziumEventFactory(
                config.getString(HanaSourceConnector.TOPIC),
                serverName,
                config.getString(HanaSourceConnector.DATABASE_NAME),
                config.getString(HanaSourceConnector.SCHEMA_NAME),
                config.getString(HanaSourceConnector.TABLE_NAME)) : null;

        catalogMode = "catalog".equals(config.getString(HanaSourceConnector.DISCOVERY_MODE));
        if (!prototypeMode && !catalogMode) {
            throw new ConnectException("native production path requires log.backup.discovery.mode=catalog");
        }
        if (catalogMode) {
            startCatalogMode(config, serverName);
        }
        else {
            startFilesystemMode(config, serverName);
        }

        Map<String, Object> stored = context.offsetStorageReader().offset(sourcePartition);
        long discoveryStart = firstPosition;
        if (stored != null) {
            hasStoredOffset = true;
            try {
                resumeOffset = SourceOffset.fromMap(stored);
            }
            catch (ConnectException error) {
                throw FailureClassifier.wrap("cannot restore source offset", error);
            }
            resumeBackupId = resumeOffset.backupId();
            resumeEventOffset = resumeOffset.eventOffset();
            resumeFileFirst = resumeOffset.fileFirst();
            resumeLogPosition = resumeOffset.logPosition();
            publishedHwm = resumeLogPosition;
            kafkaCommittedHwm = resumeLogPosition;
            oldestRequiredPosition = resumeFileFirst;
            discoveryStart = resumeFileFirst;
        }

        if (catalogMode) {
            prependCatalogPredecessor(discoveryStart);
            refreshManifest(discoveryStart);
        }
        else {
            discoverFilesystemCorpus(config);
        }
        sourceEpoch = catalogMode
                ? establishCatalogEpoch()
                : SourceEpoch.prototype(serverName, runId, sourceId,
                        sourceVolume, sourcePartitionId);
        if (resumeOffset != null) {
            try {
                resumeOffset.requireEpoch(sourceEpoch);
            }
            catch (ConnectException error) {
                throw FailureClassifier.wrap("stored offset belongs to another source epoch",
                        error);
            }
        }
        if (!prototypeMode) {
            startNativeDataPath(config);
            refreshRetentionGuard(Instant.now());
        }
        else if (stored != null) {
            locateResumeFile();
        }
        if (catalogMode) {
            policyRefresher = AsyncPolicyRefresher.create(
                    policyRefreshIntervalMs, policyMaximumStalenessMs);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        pollOwnership.lockInterruptibly();
        Thread owner = Thread.currentThread();
        pollingThread.set(owner);
        try {
            return pollOwned();
        }
        finally {
            pollingThread.compareAndSet(owner, null);
            if (stopped) {
                closeTaskResources();
            }
            pollOwnership.unlock();
        }
    }

    /** Runs only while the current thread owns all decoder/JDBC task resources. */
    private List<SourceRecord> pollOwned() throws InterruptedException {
        if (stopped) {
            return null;
        }
        if (catalogMode) {
            refreshPolicyIfDue();
        }
        if (!prototypeMode) {
            return pollNative();
        }
        List<SourceRecord> records = new ArrayList<>(maxBatchSize);
        try {
            while (!stopped && records.size() < maxBatchSize) {
                if (!ensureCursor()) {
                    break;
                }
                while (cursor.hasNext() && records.size() < maxBatchSize) {
                    EventPointer event = cursor.next();
                    if (skipReplayedEvent(event)) {
                        continue;
                    }
                    Map<String, Object> offset = sourceOffset(event);
                    SourceRecord record = eventFactory.create(event, sourcePartition, offset);
                    eventSizeGuard.requireWithinLimit(record);
                    records.add(record);
                    publishedHwm = Math.max(publishedHwm, event.logPosition());
                }
                if (!cursor.hasNext()) {
                    closeCurrentFile();
                    fileIndex++;
                }
            }
        }
        catch (IOException error) {
            throw FailureClassifier.wrap("cannot decode HANA log backup", error);
        }

        if (records.isEmpty() && !stopped) {
            boolean discovered = catalogMode && refreshManifest(nextExpectedPosition());
            if (discovered) {
                idlePolls = 0;
                return null;
            }
            finishFiniteCorpusIfRequired();
            long multiplier = 1L << Math.min(idlePolls++, 4);
            Thread.sleep(Math.min(5_000L, Math.multiplyExact(pollIntervalMs, multiplier)));
            return null;
        }
        idlePolls = 0;
        return records;
    }

    @Override
    public void stop() {
        stopped = true;
        Thread owner = pollingThread.getAndSet(null);
        if (owner != null) {
            owner.interrupt();
        }
        if (!pollOwnership.tryLock()) {
            return;
        }
        try {
            closeTaskResources();
        }
        finally {
            pollOwnership.unlock();
        }
    }

    private void closeTaskResources() {
        if (policyRefresher != null) {
            policyRefresher.close();
            policyRefresher = null;
        }
        closeNativePrefetch();
        if (nativeEventBuilder != null) {
            nativeEventBuilder.close();
            nativeEventBuilder = null;
        }
        if (nativePipeline != null) {
            reportNativeMetrics();
            nativePipeline.close();
            nativePipeline = null;
        }
        closeCurrentFile();
        if (controlPlane != null) {
            try {
                controlPlane.close();
            }
            catch (Exception closeFailure) {
                // The task is already stopping and no offset is advanced here,
                // but the operational failure must remain observable.
                LOGGER.warn("Failed to close HANA control plane while stopping source task",
                        closeFailure);
            }
            controlPlane = null;
        }
    }

    @Override
    public synchronized void commitRecord(SourceRecord record, RecordMetadata metadata) {
        if (record == null || record.sourceOffset() == null) {
            return;
        }
        long committed = number(record.sourceOffset(), "log_position", kafkaCommittedHwm);
        kafkaCommittedHwm = Math.max(kafkaCommittedHwm, committed);
        long fileFirst = number(record.sourceOffset(), "file_first", oldestRequiredPosition);
        oldestRequiredPosition = prototypeMode
                ? predecessorStart(fileFirst)
                : number(record.sourceOffset(), "state_replay_file_first", fileFirst);
    }

    @Override
    public void commit() {
        if (prototypeMode || checkpointCoordinator == null || nativeStateStore == null) {
            return;
        }
        try {
            Map<String, Object> stored = context.offsetStorageReader().offset(sourcePartition);
            if (stored == null) {
                return;
            }
            SourceOffset confirmed = SourceOffset.fromMap(stored);
            checkpointCoordinator.pruneConfirmed(confirmed);
        }
        catch (RuntimeException | IOException error) {
            throw FailureClassifier.wrap(
                    "cannot prune native state after confirmed offset commit", error);
        }
    }

    @Override
    public String version() {
        return HanaSourceConnector.VERSION;
    }

    private void startNativeDataPath(AbstractConfig config) {
        String stateDirectory = required(config.getString(HanaSourceConnector.STATE_DIR),
                HanaSourceConnector.STATE_DIR);
        nativeStateStore = new DurableReducerStateStore(Path.of(stateDirectory));
        nativeEventFactory = new NativeDebeziumEventFactory(
                config.getString(HanaSourceConnector.TOPIC),
                config.getString(HanaSourceConnector.SERVER_NAME),
                initialCapabilities.databaseName(), tableDecodePlans);
        nativeEventBuilder = new OrderedNativeEventBuilder(
                nativeEventFactory,
                config.getLong(HanaSourceConnector.EVENT_MAX_BYTES),
                nativeProcessingWorkerCount(config),
                config.getInt(HanaSourceConnector.LOG_PROCESSOR_CHUNKS_PER_THREAD));
        try {
            if (resumeOffset == null) {
                if (backups.isEmpty()) {
                    throw new ConnectException("SOURCE_EPOCH_UNAVAILABLE: no replay backup");
                }
                BackupFile first = backups.get(0);
                ReplayBoundary initial = new ReplayBoundary(first.backupId(), first.first(),
                        first.last(), first.first(), firstPosition);
                checkpointCoordinator = NativeCheckpointCoordinator.initialize(
                        sourceEpoch, nativeStateStore, initial,
                        config.getInt(HanaSourceConnector.STATE_MAX_UNCOMMITTED_GENERATIONS),
                        config.getLong(HanaSourceConnector.STATE_MAX_UNCOMMITTED_BYTES));
            }
            else {
                if (resumeOffset.stateGeneration() <= 0
                        || SourceOffset.NO_STATE_SHA256.equals(resumeOffset.stateSha256())) {
                    throw new ConnectException(
                            "NATIVE_STATE_REQUIRED: stored offset has no durable checkpoint");
                }
                checkpointCoordinator = NativeCheckpointCoordinator.restore(sourceEpoch,
                        nativeStateStore, resumeOffset.stateGeneration(),
                        resumeOffset.stateSha256(),
                        config.getInt(HanaSourceConnector.STATE_MAX_UNCOMMITTED_GENERATIONS),
                        config.getLong(HanaSourceConnector.STATE_MAX_UNCOMMITTED_BYTES));
                ensureReplayManifest(checkpointCoordinator.currentBoundary());
                nativeResumePending = checkpointCoordinator.currentBoundary()
                        .committedPosition() < resumeOffset.logPosition();
            }
            fileIndex = locateBackup(checkpointCoordinator.currentBoundary().backupId());
            oldestRequiredPosition = checkpointCoordinator.currentBoundary().fileFirst();
            nativePipeline = new NativeRowLobPipeline(
                    tableDecodePlans,
                    nativeProcessingWorkerCount(config),
                    config.getInt(HanaSourceConnector.LOG_PROCESSOR_CHUNKS_PER_THREAD),
                    config.getInt(HanaSourceConnector.TRANSACTION_MAX_OPEN),
                    config.getInt(HanaSourceConnector.TRANSACTION_MAX_ROWS),
                    config.getLong(HanaSourceConnector.TRANSACTION_MAX_BYTES),
                    new io.hanacdc.decoder.NativeLobReconstructor.Limits(
                            config.getLong(HanaSourceConnector.LOB_MAX_BUFFERED_BYTES),
                            config.getLong(HanaSourceConnector.LOB_MAX_BYTES),
                            config.getInt(HanaSourceConnector.LOB_MAX_OPEN_LOCATORS)));
            nativePrefetch = new BoundedPrefetch<>(
                    "hana-native-transaction-prefetch");
        }
        catch (IOException error) {
            throw FailureClassifier.wrap(
                    "cannot initialize native durable data path", error);
        }
    }

    private List<SourceRecord> pollNative() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>(maxBatchSize);
        int scannedTransactions = 0;
        try {
            while (!stopped && records.size() < maxBatchSize
                    && scannedTransactions < maxBatchSize) {
                if (!nativePendingRecords.isEmpty()) {
                    records.add(nativePendingRecords.removeFirst());
                    continue;
                }
                if (records.isEmpty()) {
                    SourceRecord heartbeat = heartbeatIfDue();
                    if (heartbeat != null) {
                        eventSizeGuard.requireWithinLimit(heartbeat);
                        records.add(heartbeat);
                        break;
                    }
                }
                PrefetchedNativeTransaction input = takeNativeTransaction();
                if (input == null) {
                    break;
                }
                startNativePrefetch();
                NativeRowLobPipeline.CompletedTransaction transaction = input.transaction();
                ReplayBoundary boundary = input.boundary();
                scannedTransactions++;
                if (transaction.terminalPosition()
                        <= checkpointCoordinator.currentBoundary().committedPosition()) {
                    continue;
                }
                if (transaction.rows().isEmpty()
                        && transaction.physicalDeletes().isEmpty()) {
                    pendingProgressTransaction = transaction;
                    pendingProgressBoundary = boundary;
                    continue;
                }
                long checkpointStarted = System.nanoTime();
                NativeCheckpointCoordinator.ProcessedTransaction processed =
                        checkpointCoordinator.process(transaction, boundary);
                nativeCheckpointNanos += System.nanoTime() - checkpointStarted;
                pendingProgressTransaction = null;
                pendingProgressBoundary = null;
                latestHeartbeatOffset = heartbeatOffset(
                        transaction, boundary, processed.postCheckpoint());
                List<NativeCheckpointCoordinator.Publication> selected = new ArrayList<>(
                        processed.publications().size());
                for (NativeCheckpointCoordinator.Publication publication
                        : processed.publications()) {
                    if (skipNativeReplayedPublication(publication)) {
                        continue;
                    }
                    selected.add(publication);
                }
                long eventStarted = System.nanoTime();
                List<SourceRecord> built = nativeEventBuilder.build(
                        selected, sourceEpoch, sourcePartition);
                nativeEventBuildNanos += System.nanoTime() - eventStarted;
                nativePublications += built.size();
                nativePendingRecords.addAll(built);
                if (!selected.isEmpty()) {
                    publishedHwm = Math.max(publishedHwm,
                            selected.get(selected.size() - 1).terminal().logPosition());
                }
            }
        }
        catch (IOException error) {
            throw FailureClassifier.wrap("cannot decode native HANA log backup", error);
        }

        if (records.isEmpty() && !stopped) {
            SourceRecord heartbeat;
            try {
                heartbeat = heartbeatIfDue();
            }
            catch (IOException error) {
                throw FailureClassifier.wrap(
                        "cannot persist native heartbeat progress", error);
            }
            if (heartbeat != null) {
                try {
                    eventSizeGuard.requireWithinLimit(heartbeat);
                }
                catch (IOException error) {
                    throw FailureClassifier.wrap(
                            "cannot serialize native heartbeat", error);
                }
                idlePolls = 0;
                return List.of(heartbeat);
            }
            boolean discovered = refreshManifest(nextExpectedPosition());
            if (discovered) {
                idlePolls = 0;
                return null;
            }
            finishNativeFiniteCorpusIfRequired();
            if (scannedTransactions > 0) {
                idlePolls = 0;
                return null;
            }
            long multiplier = 1L << Math.min(idlePolls++, 4);
            Thread.sleep(Math.min(5_000L, Math.multiplyExact(pollIntervalMs, multiplier)));
            return null;
        }
        idlePolls = 0;
        return records;
    }

    private PrefetchedNativeTransaction takeNativeTransaction()
            throws IOException, InterruptedException {
        return nativePrefetch.takeOrRead(this::readNativeTransaction);
    }

    private void startNativePrefetch() {
        if (stopped || nativePrefetch == null) return;
        nativePrefetch.start(this::readNativeTransaction);
    }

    private PrefetchedNativeTransaction readNativeTransaction() throws IOException {
        if (stopped || !ensureNativeCursor()) return null;
        NativeRowLobPipeline.CompletedTransaction transaction = nativeCursor.next();
        return new PrefetchedNativeTransaction(
                transaction, nativeReplayBoundary(transaction));
    }

    private boolean ensureNativeCursor() throws IOException {
        if (nativeCursor != null && nativeCursor.hasNext()) {
            return true;
        }
        while (fileIndex < backups.size()) {
            if (nativeCursor != null) {
                decodedHwm = Math.max(decodedHwm, backups.get(fileIndex).last());
                closeNativeCursor();
                fileIndex++;
                continue;
            }
            BackupFile backup = backups.get(fileIndex);
            if (!Files.isRegularFile(backup.path())) {
                long now = System.nanoTime();
                if (!backup.path().equals(missingPath)) {
                    missingPath = backup.path();
                    missingSinceNanos = now;
                }
                else if (now - missingSinceNanos >= OBJECT_VISIBILITY_TIMEOUT_NANOS) {
                    throw new IOException("FATAL_OBJECT_MISSING: successful catalog entry is not visible: "
                            + backup.path());
                }
                return false;
            }
            missingPath = null;
            objectVisibleHwm = Math.max(objectVisibleHwm, backup.last());
            nativeCursor = nativePipeline.open(backup);
            if (nativeCursor.hasNext()) {
                return true;
            }
            decodedHwm = Math.max(decodedHwm, backup.last());
            closeNativeCursor();
            fileIndex++;
        }
        return false;
    }

    private ReplayBoundary nativeReplayBoundary(
            NativeRowLobPipeline.CompletedTransaction transaction) throws IOException {
        long oldest = nativePipeline.oldestOpenPosition();
        BackupFile replay = oldest < 0
                ? transaction.terminal().backup()
                : backupContaining(oldest);
        long replayPosition = oldest < 0
                ? transaction.terminal().logPosition()
                : oldest;
        return new ReplayBoundary(replay.backupId(), replay.first(), replay.last(),
                replayPosition, transaction.terminalPosition());
    }

    private boolean skipNativeReplayedPublication(
            NativeCheckpointCoordinator.Publication publication) {
        if (!nativeResumePending) {
            return false;
        }
        long position = publication.terminal().logPosition();
        if (position < resumeOffset.logPosition()) {
            return true;
        }
        if (position > resumeOffset.logPosition()) {
            throw new ConnectException("OFFSET_NOT_FOUND: passed native transaction at "
                    + resumeOffset.logPosition());
        }
        if (publication.transactionId() != resumeOffset.transactionId()
                || publication.changeCount() != resumeOffset.changeCount()) {
            throw new ConnectException("OFFSET_IDENTITY_MISMATCH: native transaction ordinal");
        }
        if (publication.changeIndex() <= resumeOffset.changeIndex()) {
            if (publication.changeIndex() == publication.changeCount() - 1) {
                nativeResumePending = false;
            }
            return true;
        }
        nativeResumePending = false;
        return false;
    }

    private void finishNativeFiniteCorpusIfRequired() {
        if (finiteCorpusFinished || nextExpectedPosition() < lastPosition
                || nativeCursor != null || fileIndex < backups.size()) {
            return;
        }
        try {
            nativePipeline.finish();
            finiteCorpusFinished = true;
        }
        catch (IOException error) {
            throw FailureClassifier.wrap("cannot finish native finite corpus", error);
        }
    }

    private void ensureReplayManifest(ReplayBoundary boundary) {
        boolean present = backups.stream().anyMatch(value ->
                value.backupId() == boundary.backupId());
        if (present) {
            return;
        }
        backups.clear();
        if (!refreshManifest(boundary.fileFirst())) {
            throw new ConnectException("FATAL_STATE_REPLAY_BACKUP_MISSING: backup_id="
                    + boundary.backupId());
        }
        locateBackup(boundary.backupId());
    }

    private int locateBackup(long backupId) {
        for (int index = 0; index < backups.size(); index++) {
            if (backups.get(index).backupId() == backupId) {
                return index;
            }
        }
        throw new ConnectException("FATAL_STATE_REPLAY_BACKUP_MISSING: backup_id=" + backupId);
    }

    private BackupFile backupContaining(long position) throws IOException {
        for (BackupFile backup : backups) {
            if (position >= backup.first() && position < backup.last()) {
                return backup;
            }
        }
        throw new IOException("FATAL_STATE_REPLAY_BACKUP_MISSING: position=" + position);
    }

    private int nativeProcessingWorkerCount(AbstractConfig config) {
        return NativeWorkerSelector.select(
                config.getString(HanaSourceConnector.LOG_PROCESSOR_THREADS),
                config.getInt(HanaSourceConnector.LOG_PROCESSOR_MAX_THREADS),
                Runtime.getRuntime().availableProcessors(), Long.MAX_VALUE, 1L, 1,
                HanaSourceConnector.LOG_PROCESSOR_THREADS,
                HanaSourceConnector.LOG_PROCESSOR_MAX_THREADS, 6);
    }

    private void closeNativeCursor() {
        if (nativeCursor == null) {
            return;
        }
        try {
            nativeCursor.close();
        }
        catch (IOException ignored) {
            // A fatal read error is reported by poll; stop remains best-effort.
        }
        nativeCursor = null;
    }

    private void closeNativePrefetch() {
        if (nativePrefetch != null) {
            nativePrefetch.close();
            nativePrefetchedTransactions = nativePrefetch.completed();
            nativePrefetchWaitNanos = nativePrefetch.waitNanos();
            nativePrefetch = null;
        }
        closeNativeCursor();
    }

    private void reportNativeMetrics() {
        if (nativeMetricsReported || nativePipeline == null) {
            return;
        }
        nativeMetricsReported = true;
        System.out.println(nativePipeline.metrics().toJson());
        System.out.printf(java.util.Locale.ROOT,
                "{\"kind\":\"native_source_stage_metrics\","
                        + "\"publications\":%d,\"checkpoint_seconds\":%.9f,"
                        + "\"event_build_seconds\":%.9f,"
                        + "\"prefetched_transactions\":%d,"
                        + "\"prefetch_wait_seconds\":%.9f,"
                        + "\"policy_refresh_count\":%d,"
                        + "\"policy_refresh_seconds\":%.9f,"
                        + "\"policy_refresh_apply_seconds\":%.9f,"
                        + "\"capability_inspect_seconds\":%.9f,"
                        + "\"metadata_refresh_seconds\":%.9f,"
                        + "\"topology_refresh_seconds\":%.9f,"
                        + "\"retention_refresh_seconds\":%.9f}%n",
                nativePublications,
                nativeCheckpointNanos / 1_000_000_000.0,
                nativeEventBuildNanos / 1_000_000_000.0,
                nativePrefetchedTransactions,
                nativePrefetchWaitNanos / 1_000_000_000.0,
                nativePolicyRefreshCount,
                nativePolicyRefreshNanos / 1_000_000_000.0,
                nativePolicyApplyNanos / 1_000_000_000.0,
                nativeCapabilityInspectNanos / 1_000_000_000.0,
                nativeMetadataRefreshNanos / 1_000_000_000.0,
                nativeTopologyRefreshNanos / 1_000_000_000.0,
                nativeRetentionRefreshNanos / 1_000_000_000.0);
    }

    private void startCatalogMode(AbstractConfig config, String serverName) {
        String url = required(config.getString(HanaSourceConnector.DATABASE_URL),
                HanaSourceConnector.DATABASE_URL);
        String user = required(config.getString(HanaSourceConnector.DATABASE_USER),
                HanaSourceConnector.DATABASE_USER);
        Password configuredPassword = config.getPassword(HanaSourceConnector.DATABASE_PASSWORD);
        String password = configuredPassword == null ? "" : configuredPassword.value();
        if (password.isEmpty()) {
            throw new ConnectException(HanaSourceConnector.DATABASE_PASSWORD + " is required");
        }
        pathMapper = PathMapper.parse(config.getString(HanaSourceConnector.PATH_MAPPING));
        contract = new HanaContract(
                Set.of("immediate"),
                Set.of("file"),
                Set.of("none"),
                false,
                false,
                config.getLong(HanaSourceConnector.MAX_BACKUP_BYTES),
                config.getLong(HanaSourceConnector.MAX_PUBLICATION_DELAY_MS),
                config.getList(HanaSourceConnector.SUPPORTED_HANA_VERSIONS));
        try {
            controlPlane = JdbcHanaControlPlane.connect(
                    config.getString(HanaSourceConnector.DATABASE_DRIVER),
                    url,
                    user,
                    password,
                    sourceId,
                    config.getInt(HanaSourceConnector.CONTROL_QUERY_TIMEOUT_SECONDS));
            initialCapabilities = controlPlane.inspect();
            previousRatePosition = initialCapabilities.redoHead();
            previousRateObservedNanos = System.nanoTime();
            enforceAssessment(contract.assess(initialCapabilities));
            tableDecodePlans = TablePlanSelector.select(
                    tableIncludeList, controlPlane.columnTableMetadata(tableIncludeList::includes));
            tablePlanGuard = new TablePlanGuard(tableDecodePlans);
            sourceTopology = SourceTopology.discover(
                    configuredTopologyMode,
                    controlPlane.persistenceServices(),
                    tableDecodePlans,
                    sourceId);
            if (sourceTopology.actualMode() == SourceTopologyMode.SCALE_OUT) {
                validateScaleOutBootstrap(config);
            }
            System.out.println("HANA_CDC_SOURCE_TOPOLOGY " + sourceTopology.summary());
            for (TableDecodePlan plan : tableDecodePlans) {
                System.out.printf("HANA_CDC_TABLE_PLAN table=%s oid=%d partitions=%d "
                                + "schema_fingerprint=%s topology_fingerprint=%s%n",
                        plan.metadata().qualifiedName(), plan.metadata().objectId(),
                        plan.metadata().partitions().size(), plan.schemaFingerprint(),
                        plan.topologyFingerprint());
            }
        }
        catch (SQLException | IllegalArgumentException | IllegalStateException error) {
            throw FailureClassifier.wrap(
                    "cannot initialize read-only HANA control plane", error);
        }

        sourcePartition = SourcePartitionIdentity.catalog(serverName, sourceId,
                sourceVolume, sourcePartitionId, initialCapabilities.systemId(),
                initialCapabilities.databaseName());
    }

    private void validateScaleOutBootstrap(AbstractConfig config) throws SQLException {
        List<SourceStreamId> streams = sourceTopology.streams();
        Map<SourceStreamId, PersistenceStreamStatus> statuses = new LinkedHashMap<>();
        for (SourceStreamId stream : streams) {
            statuses.put(stream,
                    controlPlane.persistenceStreamStatus(stream.volumeId()));
        }
        Map<SourceStreamId, Long> starts = ScaleOutStartPositions.resolve(
                config.getString(HanaSourceConnector.SCALE_OUT_START_POSITIONS),
                streams, statuses);
        String perVolume = config.getString(
                HanaSourceConnector.SCALE_OUT_PATH_MAPPING).strip();
        Map<SourceStreamId, PathMapper> mappers;
        if (perVolume.isEmpty()) {
            mappers = new LinkedHashMap<>();
            streams.forEach(stream -> mappers.put(stream, pathMapper));
        }
        else {
            mappers = ScaleOutPathMapping.parse(perVolume).requireStreams(streams);
        }
        ScaleOutCatalogSnapshot.capture(controlPlane, sourceTopology.services(), starts,
                sourcePartitionId,
                config.getInt(HanaSourceConnector.SCALE_OUT_CATALOG_THREADS),
                contract, mappers);
        throw new IllegalArgumentException("FATAL_SCALE_OUT_COMMIT_GRAMMAR_UNCERTIFIED: "
                + "multi-volume catalog bootstrap succeeded for " + streams.size()
                + " streams, but native participant/decision grammar must pass the "
                + "real HANA scale-out qualification gate before publication is enabled");
    }

    private void startFilesystemMode(AbstractConfig config, String serverName) {
        required(config.getString(HanaSourceConnector.BACKUP_DIR), HanaSourceConnector.BACKUP_DIR);
        sourcePartition = SourcePartitionIdentity.filesystem(serverName, sourceId,
                sourceVolume, sourcePartitionId);
    }

    private void discoverFilesystemCorpus(AbstractConfig config) {
        try {
            backups = new ArrayList<>(ParallelLogDecoder.discoverCorpus(
                    Path.of(config.getString(HanaSourceConnector.BACKUP_DIR)),
                    firstPosition,
                    lastPosition));
        }
        catch (IOException error) {
            throw FailureClassifier.wrap("cannot discover log-backup corpus", error);
        }
    }

    private boolean refreshManifest(long expectedPosition) {
        if (!catalogMode || expectedPosition >= lastPosition) {
            return false;
        }
        try {
            List<LogBackupEntry> entries = controlPlane.successfulLogBackups(
                    sourceId, expectedPosition, lastPosition);
            List<BackupFile> discovered = ManifestPlanner.contiguousFiles(
                    entries,
                    expectedPosition,
                    lastPosition,
                    sourceId,
                    sourceVolume,
                    sourcePartitionId,
                    contract,
                    pathMapper);
            if (discovered.isEmpty()) {
                return false;
            }
            long current = backups.isEmpty() ? expectedPosition : nextExpectedPosition();
            for (BackupFile backup : discovered) {
                if (backup.last() <= current) {
                    continue;
                }
                if (backup.first() != current) {
                    throw new ConnectException("FATAL_GAP: expected " + current
                            + " but discovered " + backup.first());
                }
                backups.add(backup);
                current = backup.last();
            }
            return true;
        }
        catch (SQLException | IllegalStateException error) {
            throw FailureClassifier.wrap(
                    "cannot refresh authoritative HANA backup manifest", error);
        }
    }

    private void prependCatalogPredecessor(long boundary) {
        if (boundary == 0) {
            return;
        }
        try {
            List<LogBackupEntry> candidates = controlPlane.successfulLogBackups(
                    sourceId, boundary - 1, boundary).stream()
                    .filter(value -> value.lastRedoPosition() == boundary)
                    .toList();
            if (candidates.size() != 1) {
                throw new ConnectException("FATAL_PREDECESSOR_"
                        + (candidates.isEmpty() ? "MISSING" : "AMBIGUOUS")
                        + ": expected exactly one successful backup ending at " + boundary
                        + " but found " + candidates.size());
            }
            LogBackupEntry predecessor = candidates.get(0);
            List<BackupFile> replay = ManifestPlanner.contiguousFiles(
                    candidates,
                    predecessor.firstRedoPosition(),
                    boundary,
                    sourceId,
                    sourceVolume,
                    sourcePartitionId,
                    contract,
                    pathMapper);
            if (replay.size() != 1) {
                throw new ConnectException("FATAL_PREDECESSOR_INVALID: boundary=" + boundary);
            }
            backups.add(replay.get(0));
        }
        catch (SQLException | IllegalStateException error) {
            throw FailureClassifier.wrap(
                    "cannot discover predecessor for decoder-state replay", error);
        }
    }

    private void refreshPolicyIfDue() throws InterruptedException {
        try {
            var completed = policyRefresher.poll(() -> {
                BackupFile required = requiredReplayBackup();
                HanaControlPlane plane = controlPlane;
                return () -> loadPolicyRefresh(plane, required);
            });
            if (completed.isEmpty()) {
                return;
            }
            AsyncPolicyRefresher.Completion<PolicyRefreshSnapshot> completion =
                    completed.orElseThrow();
            PolicyRefreshSnapshot snapshot = completion.value();
            long applyStarted = System.nanoTime();
            applyPolicyRefresh(snapshot);
            nativePolicyApplyNanos += System.nanoTime() - applyStarted;
            nativePolicyRefreshCount++;
            nativePolicyRefreshNanos += completion.elapsedNanos();
            nativeCapabilityInspectNanos += snapshot.capabilityInspectNanos();
            nativeMetadataRefreshNanos += snapshot.metadataRefreshNanos();
            nativeTopologyRefreshNanos += snapshot.topologyRefreshNanos();
            nativeRetentionRefreshNanos += snapshot.retentionRefreshNanos();
        }
        catch (AsyncPolicyRefresher.StaleResultException error) {
            long ageMs = TimeUnit.NANOSECONDS.toMillis(error.ageNanos());
            long maximumMs = TimeUnit.NANOSECONDS.toMillis(error.maximumNanos());
            throw FailureClassifier.wrap("FATAL_CONTROL_PLANE_STALE: age_ms="
                    + ageMs + " max_staleness_ms=" + maximumMs, error);
        }
        catch (ExecutionException error) {
            throw policyRefreshFailure(error);
        }
    }

    static ConnectException policyRefreshFailure(ExecutionException failure) {
        Throwable cause = failure.getCause();
        if (cause instanceof Error fatal) {
            throw fatal;
        }
        return FailureClassifier.wrap("cannot refresh HANA policy", cause);
    }

    private PolicyRefreshSnapshot loadPolicyRefresh(
            HanaControlPlane plane, BackupFile required) throws SQLException {
        long phaseStarted = System.nanoTime();
        HanaCapabilities current = plane.inspect();
        long capabilityNanos = System.nanoTime() - phaseStarted;
        phaseStarted = System.nanoTime();
        List<TableDecodePlan> currentPlans = TablePlanSelector.select(
                tableIncludeList, plane.columnTableMetadata(tableIncludeList::includes));
        long metadataNanos = System.nanoTime() - phaseStarted;
        phaseStarted = System.nanoTime();
        SourceTopology currentTopology = SourceTopology.discover(
                configuredTopologyMode,
                plane.persistenceServices(),
                currentPlans,
                sourceId);
        long topologyNanos = System.nanoTime() - phaseStarted;
        phaseStarted = System.nanoTime();
        RetentionGuard.Status currentRetention = inspectRetentionGuard(
                plane, current.observedAt(), required);
        long retentionNanos = System.nanoTime() - phaseStarted;
        return new PolicyRefreshSnapshot(current, currentPlans, currentTopology,
                currentRetention, capabilityNanos, metadataNanos,
                topologyNanos, retentionNanos);
    }

    private void applyPolicyRefresh(PolicyRefreshSnapshot snapshot) {
        HanaCapabilities current = snapshot.capabilities();
        if (!initialCapabilities.systemId().equals(current.systemId())
                || !initialCapabilities.databaseName().equals(current.databaseName())) {
            throw new ConnectException("SOURCE_EPOCH_CHANGED: HANA database identity changed");
        }
        enforceAssessment(contract.assess(current));
        sourceTopology.requireUnchanged(snapshot.topology());
        tablePlanGuard.validate(snapshot.plans());
        updateRedoRate(current);
        applyRetentionStatus(snapshot.retention());
        StreamingWatermarks watermarks = watermarks(current);
        if (!watermarks.retentionSafe()) {
            throw new ConnectException("FATAL_RETENTION_GAP: earliest_available_position="
                    + current.earliestSuccessfulBackupPosition()
                    + " oldest_required_position=" + oldestRequiredPosition);
        }
        System.out.println(watermarks.toJson());
    }

    private void enforceAssessment(CapabilityAssessment assessment) {
        if (!assessment.canRead()) {
            throw new ConnectException("HANA capability gate failed: " + assessment.summary());
        }
        if (!assessment.ready()) {
            if ("fail".equals(policyViolationMode)) {
                throw new ConnectException("HANA policy gate failed: " + assessment.summary());
            }
            System.err.println("HANA_CDC_POLICY_VIOLATION " + assessment.summary());
        }
    }

    private boolean ensureCursor() throws IOException, InterruptedException {
        if (cursor != null && cursor.hasNext()) {
            return true;
        }
        while (fileIndex < backups.size()) {
            closeCurrentFile();
            BackupFile backup = backups.get(fileIndex);
            if (!Files.isRegularFile(backup.path())) {
                long now = System.nanoTime();
                if (!backup.path().equals(missingPath)) {
                    missingPath = backup.path();
                    missingSinceNanos = now;
                }
                else if (now - missingSinceNanos >= OBJECT_VISIBILITY_TIMEOUT_NANOS) {
                    throw new IOException("FATAL_OBJECT_MISSING: successful catalog entry is not visible: "
                            + backup.path());
                }
                return false;
            }
            missingPath = null;
            if (catalogMode) {
                String databaseId = nativeHeaderValidator.validate(backup).databaseId();
                if (!databaseId.equals(catalogDatabaseId)) {
                    throw new IOException("SOURCE_EPOCH_CHANGED: backup DBID changed from "
                            + catalogDatabaseId + " to " + databaseId);
                }
            }
            objectVisibleHwm = Math.max(objectVisibleHwm, backup.last());
            decodedFile = decoder.decode(backup, runId, scanConfig, null, decoderSession);
            decodedHwm = Math.max(decodedHwm, backup.last());
            if (decodedFile.metrics().scanWarnings() != 0) {
                long warnings = decodedFile.metrics().scanWarnings();
                closeCurrentFile();
                throw new IOException("FATAL_DECODE_WARNING: backup_id=" + backup.backupId()
                        + " warnings=" + warnings);
            }
            cursor = decodedFile.cursor();
            if (cursor.hasNext()) {
                return true;
            }
            closeCurrentFile();
            fileIndex++;
        }
        return false;
    }

    private void finishFiniteCorpusIfRequired() {
        if (finiteCorpusFinished || nextExpectedPosition() < lastPosition) {
            return;
        }
        try {
            decoderSession.finish();
            finiteCorpusFinished = true;
        }
        catch (IOException error) {
            throw FailureClassifier.wrap("cannot finish finite corpus", error);
        }
    }

    private long nextExpectedPosition() {
        if (backups.isEmpty()) {
            return firstPosition;
        }
        return backups.get(backups.size() - 1).last();
    }

    private Map<String, Object> sourceOffset(EventPointer event) {
        BackupFile origin = event.sourceBackup();
        return SourceOffset.withoutDurableState(sourceEpoch, origin.backupId(),
                origin.first(), origin.last(), event.logPosition(),
                event.sourceEventOffset(), event.sequence(),
                event.transactionId()).toMap();
    }

    private SourceEpoch establishCatalogEpoch() {
        if (backups.isEmpty()) {
            throw new ConnectException("SOURCE_EPOCH_UNAVAILABLE: no successful log backup "
                    + "is available at startup");
        }
        try {
            NativeBackupHeaderValidator.Metadata metadata =
                    nativeHeaderValidator.validate(backups.get(0));
            catalogDatabaseId = metadata.databaseId();
            return SourceEpoch.catalog(initialCapabilities, sourceId, sourceVolume,
                    sourcePartitionId, catalogDatabaseId, tableDecodePlans);
        }
        catch (IOException error) {
            throw catalogHeaderFailure(backups.get(0), error);
        }
    }

    static ConnectException catalogHeaderFailure(BackupFile backup, IOException error) {
        if (error instanceof NoSuchFileException) {
            IOException retentionGap = new IOException(
                    "FATAL_RETENTION_OBJECT_MISSING: backup_id=" + backup.backupId()
                            + " path=" + backup.path(), error);
            return FailureClassifier.wrap(
                    "cannot establish source epoch from native backup header", retentionGap);
        }
        return FailureClassifier.wrap(
                "cannot establish source epoch from native backup header", error);
    }

    private long predecessorStart(long fileFirst) {
        for (BackupFile backup : backups) {
            if (backup.last() == fileFirst) {
                return backup.first();
            }
        }
        return fileFirst;
    }

    private void updateRedoRate(HanaCapabilities current) {
        long now = System.nanoTime();
        if (previousRateObservedNanos != 0 && current.redoHead() >= previousRatePosition) {
            double seconds = (now - previousRateObservedNanos) / 1_000_000_000.0;
            if (seconds > 0) {
                observedRedoBytesPerSecond = (current.redoHead() - previousRatePosition)
                        * 64.0 / seconds;
            }
        }
        previousRatePosition = current.redoHead();
        previousRateObservedNanos = now;
    }

    private StreamingWatermarks watermarks(HanaCapabilities current) {
        return new StreamingWatermarks(
                firstPosition,
                current.redoHead(),
                current.successfulBackupHighWatermark(),
                objectVisibleHwm,
                decodedHwm,
                publishedHwm,
                kafkaCommittedHwm,
                current.earliestSuccessfulBackupPosition(),
                oldestRequiredPosition,
                current.retainedSuccessfulBackupBytes(),
                observedRedoBytesPerSecond,
                current.observedAt());
    }

    private Map<String, Object> heartbeatOffset(
            NativeRowLobPipeline.CompletedTransaction transaction,
            ReplayBoundary boundary,
            DurableReducerStateStore.Checkpoint checkpoint) {
        NativeRowLobPipeline.TerminalRecord terminal = transaction.terminal();
        return SourceOffset.withDurableState(sourceEpoch,
                terminal.backup().backupId(), terminal.backup().first(),
                terminal.backup().last(), terminal.logPosition(), terminal.fileOffset(),
                terminal.logPosition(), transaction.tid(), 0, 1,
                boundary, checkpoint).toMap();
    }

    private SourceRecord heartbeatIfDue() throws IOException {
        long now = System.nanoTime();
        if (now - lastHeartbeatNanos < heartbeatIntervalNanos) {
            return null;
        }
        if (pendingProgressTransaction != null) {
            DurableReducerStateStore.Checkpoint checkpoint =
                    checkpointCoordinator.advanceProgress(
                            pendingProgressTransaction, pendingProgressBoundary);
            latestHeartbeatOffset = heartbeatOffset(pendingProgressTransaction,
                    pendingProgressBoundary, checkpoint);
            pendingProgressTransaction = null;
            pendingProgressBoundary = null;
        }
        if (latestHeartbeatOffset == null) {
            return null;
        }
        lastHeartbeatNanos = now;
        return heartbeatFactory.create(sourcePartition, latestHeartbeatOffset,
                System.currentTimeMillis());
    }

    private void refreshRetentionGuard(Instant observedAt) {
        BackupFile required = requiredReplayBackup();
        try {
            applyRetentionStatus(inspectRetentionGuard(
                    controlPlane, observedAt, required));
        }
        catch (SQLException error) {
            throw FailureClassifier.wrap("cannot validate required replay backup", error);
        }
    }

    private RetentionGuard.Status inspectRetentionGuard(
            HanaControlPlane plane, Instant observedAt, BackupFile required) throws SQLException {
        List<LogBackupEntry> entries = plane.successfulLogBackups(
                sourceId, required.first(), required.last());
        return RetentionGuard.inspect(required, entries, pathMapper,
                contract, observedAt, retentionMinimumSeconds);
    }

    private void applyRetentionStatus(RetentionGuard.Status current) {
        retentionStatus = current;
        System.out.println(retentionStatus.toJson());
        if (lastPosition == Long.MAX_VALUE
                && retentionStatus.insideSafetyMargin(retentionSafetyMarginSeconds)) {
            String message = "RETENTION_HEADROOM_LOW: backup_id="
                    + retentionStatus.requiredBackupId() + " headroom_seconds="
                    + retentionStatus.contractHeadroomSeconds()
                    + " safety_margin_seconds=" + retentionSafetyMarginSeconds;
            if ("fail".equals(policyViolationMode)) {
                throw new ConnectException("HANA policy gate failed: " + message);
            }
            System.err.println("HANA_CDC_POLICY_VIOLATION " + message);
        }
    }

    private BackupFile requiredReplayBackup() {
        for (BackupFile backup : backups) {
            if (oldestRequiredPosition >= backup.first()
                    && oldestRequiredPosition < backup.last()) {
                return backup;
            }
        }
        throw new ConnectException("FATAL_STATE_REPLAY_BACKUP_MISSING: position="
                + oldestRequiredPosition);
    }

    private void locateResumeFile() {
        for (int index = 0; index < backups.size(); index++) {
            if (backups.get(index).backupId() == resumeBackupId) {
                // Rebuild any envelope that crosses into the offset's backup. Events
                // before the exact checkpoint are filtered by skipReplayedEvent().
                fileIndex = Math.max(0, index - 1);
                return;
            }
        }
        throw new ConnectException(
                "stored offset references unavailable backup_id=" + resumeBackupId
                        + "; refusing to skip a retention gap");
    }

    private boolean skipReplayedEvent(EventPointer event) {
        if (!hasStoredOffset) {
            return event.logPosition() < firstPosition;
        }
        if (resumeCheckpointSeen) {
            return false;
        }
        BackupFile origin = event.sourceBackup();
        if (origin.first() < resumeFileFirst) {
            return true;
        }
        if (origin.first() > resumeFileFirst) {
            throw new ConnectException("OFFSET_NOT_FOUND: passed stored file_first="
                    + resumeFileFirst + " before finding its event_offset=" + resumeEventOffset);
        }
        if (origin.backupId() != resumeBackupId) {
            throw new ConnectException("OFFSET_IDENTITY_MISMATCH: stored backup_id="
                    + resumeBackupId + " but manifest has " + origin.backupId());
        }
        long eventOffset = event.sourceEventOffset();
        if (eventOffset < resumeEventOffset) {
            return true;
        }
        if (eventOffset > resumeEventOffset) {
            throw new ConnectException("OFFSET_NOT_FOUND: passed stored event_offset="
                    + resumeEventOffset + " in backup_id=" + resumeBackupId);
        }
        resumeCheckpointSeen = true;
        return true;
    }

    private static long number(Map<String, ?> values, String key, long defaultValue) {
        Object value = values.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (!(value instanceof Number number)) {
            throw new ConnectException("stored offset " + key + " is not numeric: " + value);
        }
        return number.longValue();
    }

    private static String required(String value, String property) {
        if (value == null || value.isBlank()) {
            throw new ConnectException(property + " is required");
        }
        return value;
    }

    private void closeCurrentFile() {
        cursor = null;
        if (decodedFile != null) {
            decodedFile.close();
            decodedFile = null;
        }
    }
}

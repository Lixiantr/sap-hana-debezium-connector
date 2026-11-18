package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeBackupDecoder.NativeRecord;
import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;
import io.hanacdc.schema.TableDecodePlan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Production boundary between native backup framing and transactional CDC.
 *
 * <p>The pipeline validates every backup, keeps row/LOB state across backup
 * boundaries, classifies all selected-table records and releases complete row
 * images only at a global COMMIT. The following transactional stage coalesces
 * changes, persists restart state, and correlates physical DELETE with a
 * previously committed tombstone.</p>
 */
public final class NativeRowLobPipeline implements AutoCloseable {
    public static final int DEFAULT_MAX_OPEN_TRANSACTIONS = 4096;
    public static final int DEFAULT_MAX_BUFFERED_ROWS = 100_000;
    public static final long DEFAULT_MAX_BUFFERED_ROW_BYTES = 64L * 1024 * 1024;

    public enum Outcome { COMMIT, ROLLBACK }

    public record Metrics(
            long backups,
            long records,
            long payloadBytes,
            long completedTransactions,
            long backupOpenNanos,
            long recordScanWaitNanos,
            long payloadReadNanos,
            long recordBuildNanos,
            long transactionReduceNanos) {

        public String toJson() {
            return String.format(Locale.ROOT,
                    "{\"kind\":\"native_pipeline_metrics\","
                            + "\"backups\":%d,\"records\":%d,\"payload_bytes\":%d,"
                            + "\"completed_transactions\":%d,\"backup_open_seconds\":%.9f,"
                            + "\"record_scan_wait_seconds\":%.9f,"
                            + "\"payload_read_seconds\":%.9f,"
                            + "\"record_build_seconds\":%.9f,"
                            + "\"transaction_reduce_seconds\":%.9f}",
                    backups, records, payloadBytes, completedTransactions,
                    backupOpenNanos / 1_000_000_000.0,
                    recordScanWaitNanos / 1_000_000_000.0,
                    payloadReadNanos / 1_000_000_000.0,
                    recordBuildNanos / 1_000_000_000.0,
                    transactionReduceNanos / 1_000_000_000.0);
        }
    }

    public record PhysicalDelete(
            TableDecodePlan plan, long containerId, long rowId, long logPosition) {}

    public record TerminalRecord(
            BackupFile backup,
            long logPosition,
            long fileOffset,
            long nextPosition) {
        public TerminalRecord {
            Objects.requireNonNull(backup, "backup");
            if (logPosition < backup.first() || logPosition >= backup.last()
                    || fileOffset < NativeRecordReader.FILE_HEADER_BYTES
                    || nextPosition <= logPosition || nextPosition > backup.last()) {
                throw new IllegalArgumentException("invalid native terminal record");
            }
        }
    }

    private static final class DeleteEffect {
        private final PhysicalDelete value;
        private long pairedByRowPosition = -1;

        private DeleteEffect(PhysicalDelete value) {
            this.value = value;
        }
    }

    public record CompletedTransaction(
            long tid,
            TerminalRecord terminal,
            Outcome outcome,
            List<NativeRowDecoder.DecodedRow> rows,
            List<PhysicalDelete> physicalDeletes) {
        public CompletedTransaction {
            Objects.requireNonNull(terminal, "terminal");
            rows = List.copyOf(rows);
            physicalDeletes = List.copyOf(physicalDeletes);
            if (outcome == Outcome.ROLLBACK
                    && (!rows.isEmpty() || !physicalDeletes.isEmpty())) {
                throw new IllegalArgumentException("rollback result cannot publish changes");
            }
        }

        public long terminalPosition() {
            return terminal.logPosition();
        }
    }

    private static final class TransactionState implements AutoCloseable {
        private final NativeLobReconstructor.Limits lobLimits;
        private NativeLobReconstructor.Session lobs;
        private final List<NativeRowDecoder.DecodedRow> rows = new ArrayList<>();
        private final List<Integer> rowPayloadBytes = new ArrayList<>();
        private final List<DeleteEffect> deletes = new ArrayList<>();
        private final List<LobFragment> lobFragments = new ArrayList<>();
        private final Set<Long> requiredLobLocators = new LinkedHashSet<>();
        private long bufferedRowBytes;
        private long bufferedLobFragmentBytes;
        private boolean targetTouched;
        private DeleteEffect pendingReplacementDelete;
        private long preparedPosition = -1;
        private final long firstRecordPosition;

        private TransactionState(
                long firstRecordPosition, NativeLobReconstructor.Limits lobLimits) {
            this.firstRecordPosition = firstRecordPosition;
            this.lobLimits = lobLimits;
            lobs = new NativeLobReconstructor.Session(lobLimits);
        }

        @Override
        public void close() {
            lobs.close();
            rows.clear();
            rowPayloadBytes.clear();
            deletes.clear();
            lobFragments.clear();
            requiredLobLocators.clear();
            bufferedLobFragmentBytes = 0;
        }
    }

    private record LobFragment(NativeRecordHeader header, byte[] payload) {
        private LobFragment {
            payload = payload.clone();
        }

        @Override
        public byte[] payload() {
            return payload.clone();
        }
    }

    public final class BackupCursor implements AutoCloseable {
        private final BackupFile backup;
        private final NativeBackupDecoder.Cursor input;
        private CompletedTransaction pending;
        private boolean exhausted;
        private boolean closed;

        private BackupCursor(BackupFile backup, NativeBackupDecoder.Cursor input) {
            this.backup = backup;
            this.input = input;
        }

        public NativeBackupHeaderValidator.Metadata metadata() {
            return input.metadata();
        }

        public boolean hasNext() throws IOException {
            ensureCursorOpen();
            if (pending == null && !exhausted) {
                pending = findNext();
            }
            return pending != null;
        }

        public CompletedTransaction next() throws IOException {
            if (!hasNext()) {
                throw new NoSuchElementException("native row/LOB backup cursor is exhausted");
            }
            CompletedTransaction result = pending;
            pending = null;
            return result;
        }

        private CompletedTransaction findNext() throws IOException {
            while (input.hasNext()) {
                NativeRecord record = input.next();
                long reduceStarted = System.nanoTime();
                Optional<CompletedTransaction> result = accept(record);
                transactionReduceNanos += System.nanoTime() - reduceStarted;
                if (result.isPresent()) {
                    completedTransactions++;
                    return result.orElseThrow();
                }
            }
            exhausted = true;
            completeBackup(backup, input.metadata());
            return null;
        }

        private void ensureCursorOpen() throws IOException {
            if (closed) {
                throw new IOException("native row/LOB backup cursor is closed");
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            IOException failure = null;
            try {
                input.close();
            }
            catch (IOException error) {
                failure = error;
            }
            captureMetrics(input.metrics());
            activeCursor = false;
            if (failure != null) {
                throw failure;
            }
        }
    }

    private final NativeBackupDecoder backups;
    private final NativeContainerResolver resolver;
    private final NativeRowDecoder rows = new NativeRowDecoder();
    private final int rowWorkers;
    private final int processingChunksPerThread;
    private final ExecutorService rowExecutor;
    private final NativeMassRecordAssembler massRecords;
    private final Map<Long, TransactionState> transactions = new LinkedHashMap<>();
    private final int maxOpenTransactions;
    private final int maxBufferedRows;
    private final long maxBufferedRowBytes;
    private final NativeLobReconstructor.Limits lobLimits;
    private int bufferedRows;
    private long bufferedRowBytes;
    private long lastRecordPosition = -1;
    private long expectedBackupFirst = -1;
    private String databaseId;
    private boolean activeCursor;
    private boolean closed;
    private long backupCount;
    private long recordCount;
    private long payloadBytes;
    private long completedTransactions;
    private long backupOpenNanos;
    private long recordScanWaitNanos;
    private long payloadReadNanos;
    private long recordBuildNanos;
    private long transactionReduceNanos;
    public NativeRowLobPipeline(List<TableDecodePlan> plans, int processingWorkers) {
        this(plans, processingWorkers, 1,
                DEFAULT_MAX_OPEN_TRANSACTIONS, DEFAULT_MAX_BUFFERED_ROWS,
                DEFAULT_MAX_BUFFERED_ROW_BYTES, NativeLobReconstructor.DEFAULT_LIMITS);
    }

    public NativeRowLobPipeline(
            List<TableDecodePlan> plans,
            int processingWorkers,
            int maxOpenTransactions,
            int maxBufferedRows,
            long maxBufferedRowBytes) {
        this(plans, processingWorkers, 1, maxOpenTransactions,
                maxBufferedRows, maxBufferedRowBytes,
                NativeLobReconstructor.DEFAULT_LIMITS);
    }

    public NativeRowLobPipeline(
            List<TableDecodePlan> plans,
            int processingWorkers,
            int maxOpenTransactions,
            int maxBufferedRows,
            long maxBufferedRowBytes,
            NativeLobReconstructor.Limits lobLimits) {
        this(plans, processingWorkers, 1,
                maxOpenTransactions, maxBufferedRows, maxBufferedRowBytes, lobLimits);
    }

    public NativeRowLobPipeline(
            List<TableDecodePlan> plans,
            int processingWorkers,
            int processingChunksPerThread,
            int maxOpenTransactions,
            int maxBufferedRows,
            long maxBufferedRowBytes,
            NativeLobReconstructor.Limits lobLimits) {
        if (plans == null || plans.isEmpty()) {
            throw new IllegalArgumentException("at least one table decode plan is required");
        }
        if (processingWorkers <= 0
                || processingChunksPerThread <= 0 || maxOpenTransactions <= 0
                || maxBufferedRows <= 0 || maxBufferedRowBytes <= 0) {
            throw new IllegalArgumentException("invalid native row/LOB pipeline limits");
        }
        this.lobLimits = Objects.requireNonNull(lobLimits, "lobLimits");
        backups = new NativeBackupDecoder();
        resolver = new NativeContainerResolver(List.copyOf(plans));
        massRecords = new NativeMassRecordAssembler(maxBufferedRowBytes);
        rowWorkers = processingWorkers;
        this.processingChunksPerThread = processingChunksPerThread;
        rowExecutor = processingWorkers == 1 ? null
                : Executors.newFixedThreadPool(processingWorkers, rowThreadFactory());
        this.maxOpenTransactions = maxOpenTransactions;
        this.maxBufferedRows = maxBufferedRows;
        this.maxBufferedRowBytes = maxBufferedRowBytes;
    }

    public BackupCursor open(BackupFile backup) throws IOException {
        ensureOpen();
        Objects.requireNonNull(backup, "backup");
        if (activeCursor) {
            throw new IllegalStateException("only one native backup cursor may be active");
        }
        if (expectedBackupFirst >= 0 && backup.first() != expectedBackupFirst) {
            throw new IOException("FATAL_GAP: expected backup first=" + expectedBackupFirst
                    + " actual=" + backup.first());
        }
        long openStarted = System.nanoTime();
        NativeBackupDecoder.Cursor input = backups.open(backup);
        backupOpenNanos += System.nanoTime() - openStarted;
        backupCount++;
        if (databaseId != null && !databaseId.equals(input.metadata().databaseId())) {
            input.close();
            throw new IOException("SOURCE_EPOCH_CHANGED: native backup DBID changed");
        }
        activeCursor = true;
        return new BackupCursor(backup, input);
    }

    public Metrics metrics() {
        return new Metrics(backupCount, recordCount, payloadBytes,
                completedTransactions, backupOpenNanos, recordScanWaitNanos,
                payloadReadNanos, recordBuildNanos, transactionReduceNanos);
    }

    private void captureMetrics(NativeBackupDecoder.CursorMetrics value) {
        recordCount += value.records();
        payloadBytes += value.payloadBytes();
        recordScanWaitNanos += value.recordScanWaitNanos();
        payloadReadNanos += value.payloadReadNanos();
        recordBuildNanos += value.recordBuildNanos();
    }

    /** Accepts one strictly ordered native record; public for deterministic tests. */
    public Optional<CompletedTransaction> accept(NativeRecord record) throws IOException {
        ensureOpen();
        Objects.requireNonNull(record, "record");
        NativeRecordHeader outerHeader = record.header();
        NativeRecordHeader header = outerHeader;
        if (lastRecordPosition >= 0 && header.position() <= lastRecordPosition) {
            throw new IOException("FATAL_ORDERING: native record position did not advance");
        }
        lastRecordPosition = header.position();
        Optional<NativeRecord> reconstructed = massRecords.accept(record);
        if (reconstructed.isEmpty()) {
            return Optional.empty();
        }
        record = reconstructed.orElseThrow();
        header = record.header();
        byte[] payload = record.payloadView();

        TransactionState existing = transactions.get(header.tid());
        if (existing != null && existing.targetTouched && existing.preparedPosition >= 0
                && header.outerCode() != NativeRecordReader.OUTER_COMMIT
                && header.outerCode() != NativeRecordReader.OUTER_ROLLBACK) {
            throw transactionFailure("record after PREPARE_COMMIT: tid=" + header.tid()
                    + " prepare=" + existing.preparedPosition
                    + " record=" + header.position());
        }
        if (isLobRecord(header)) {
            TransactionState transaction = state(header.tid(), header.position());
            if (transaction.bufferedLobFragmentBytes
                    > transaction.lobLimits.maxBufferedBytes() - payload.length) {
                throw resource("LOB: deferred fragment budget exceeded");
            }
            transaction.lobFragments.add(new LobFragment(header, payload));
            transaction.bufferedLobFragmentBytes += payload.length;
        }

        Optional<NativeContainerResolver.Binding> binding = resolver.observe(header);
        if (binding.isPresent()) {
            acceptSelected(record, payload, binding.orElseThrow());
        }

        if (header.outerCode() == NativeRecordReader.OUTER_COMMIT) {
            if (existing != null && existing.targetTouched) {
                requireTerminalWidth(header, payload, 32, "COMMIT");
            }
            return complete(header, Outcome.COMMIT);
        }
        if (header.outerCode() == NativeRecordReader.OUTER_ROLLBACK) {
            if (existing != null && existing.targetTouched) {
                requireTerminalWidth(header, payload, 8, "ROLLBACK");
            }
            return complete(header, Outcome.ROLLBACK);
        }
        if (header.outerCode() == NativeRecordReader.OUTER_PREPARE_COMMIT) {
            TransactionState transaction = transactions.get(header.tid());
            if (transaction != null && transaction.targetTouched) {
                requireTerminalWidth(header, payload, 16, "PREPARE_COMMIT");
                if (transaction.preparedPosition >= 0) {
                    throw transactionFailure("duplicate PREPARE_COMMIT for tid="
                            + header.tid());
                }
                transaction.preparedPosition = header.position();
            }
        }
        if (header.outerCode() == NativeRecordReader.OUTER_PARTIAL_ROLLBACK) {
            applyPartialRollback(header, payload);
        }
        return Optional.empty();
    }

    public int openTransactions() {
        return transactions.size();
    }

    public long oldestOpenPosition() {
        return transactions.values().stream()
                .mapToLong(value -> value.firstRecordPosition)
                .min().orElse(-1);
    }

    public void finish() throws IOException {
        ensureOpen();
        if (activeCursor) {
            throw new IOException("cannot finish with an active backup cursor");
        }
        if (!transactions.isEmpty()) {
            throw new IOException("FATAL_TRANSACTION_INCOMPLETE: open TIDs="
                    + transactions.size());
        }
        massRecords.finish();
    }

    private void acceptSelected(
            NativeRecord record,
            byte[] payload,
            NativeContainerResolver.Binding binding) throws IOException {
        NativeRecordHeader header = record.header();
        TransactionState transaction;
        switch (header.operationCode()) {
            case NativeRecordReader.UT_INSERT_DATA, NativeRecordReader.UT_UPSERT,
                    NativeRecordReader.UT_UPDATE -> {
                transaction = state(header.tid(), header.position());
                List<NativeRowDecoder.DecodedRow> decoded = rows.decodeAll(
                        header, binding.plan(), payload, rowExecutor, rowWorkers,
                        processingChunksPerThread);
                for (NativeRowDecoder.DecodedRow row : decoded) {
                    validateDml(row);
                    if (!row.lobLocatorIds().isEmpty()) {
                        transaction.requiredLobLocators.addAll(row.lobLocatorIds());
                    }
                }
                reserveRows(decoded.size(), payload.length);
                transaction.bufferedRowBytes = Math.addExact(
                        transaction.bufferedRowBytes, payload.length);
                transaction.rows.addAll(decoded);
                for (int index = 0; index < decoded.size(); index++) {
                    transaction.rowPayloadBytes.add(
                            index == 0 ? payload.length : 0);
                }
                if (header.operationCode() == NativeRecordReader.UT_UPSERT
                        && transaction.pendingReplacementDelete != null
                        && transaction.pendingReplacementDelete.value.plan().metadata().objectId()
                        == binding.plan().metadata().objectId()) {
                    transaction.pendingReplacementDelete.pairedByRowPosition = header.position();
                }
                transaction.pendingReplacementDelete = null;
                transaction.targetTouched = true;
            }
            case NativeRecordReader.UT_INSERT_MVCC -> {
                // MVCC companion has no logical row body.
            }
            case NativeRecordReader.UT_DELETE -> {
                if (header.firstRowId() < 0) {
                    throw dmlViolation("physical DELETE has no row locator");
                }
                transaction = state(header.tid(), header.position());
                DeleteEffect delete = new DeleteEffect(new PhysicalDelete(
                        binding.plan(), header.containerId(),
                        header.firstRowId(), header.position()));
                transaction.deletes.add(delete);
                transaction.pendingReplacementDelete = delete;
                transaction.targetTouched = true;
            }
            default -> throw new IOException("FATAL_SCHEMA_CHANGE: selected table record "
                    + header.operationType() + " is not certified");
        }
    }

    private void validateDml(NativeRowDecoder.DecodedRow row) throws IOException {
        if (!row.fullImage()) {
            throw dmlViolation("row operation is not a full image");
        }
        int deletedIndex = row.plan().deletedFlagIndex();
        if (deletedIndex < 0) {
            throw dmlViolation("IS_DELETED mapping is unavailable");
        }
        Object value = row.values().get(deletedIndex).value();
        if (!(value instanceof Boolean deleted)) {
            throw dmlViolation("IS_DELETED is not a decoded BOOLEAN");
        }
        if (row.operationCode() == NativeRecordReader.UT_INSERT_DATA && deleted) {
            throw dmlViolation("INSERT with IS_DELETED=TRUE is prohibited");
        }
    }

    private Optional<CompletedTransaction> complete(
            NativeRecordHeader terminal, Outcome outcome) throws IOException {
        TransactionState transaction = transactions.get(terminal.tid());
        if (transaction == null) {
            return Optional.of(new CompletedTransaction(
                    terminal.tid(), terminal(terminal), outcome, List.of(), List.of()));
        }
        if (transaction.targetTouched && outcome == Outcome.COMMIT
                && (transaction.preparedPosition < 0
                || transaction.preparedPosition >= terminal.position())) {
            throw transactionFailure("COMMIT without ordered PREPARE_COMMIT: tid="
                    + terminal.tid());
        }
        transactions.remove(terminal.tid());
        bufferedRows -= transaction.rows.size();
        bufferedRowBytes -= transaction.bufferedRowBytes;
        try (transaction) {
            if (!transaction.targetTouched) {
                return Optional.empty();
            }
            if (outcome == Outcome.ROLLBACK) {
                return Optional.of(new CompletedTransaction(
                        terminal.tid(), terminal(terminal), outcome, List.of(), List.of()));
            }
            Set<Long> requiredLobs = transaction.requiredLobLocators;
            rebuildLobs(transaction, requiredLobs);
            Map<Long, NativeLobReconstructor.LobValue> lobs = transaction.lobs.finish();
            List<NativeRowDecoder.DecodedRow> resolved =
                    NativeCommittedRowResolver.resolve(transaction.rows, lobs);
            RelocationResult relocations = certifyInternalFullRowRewrites(
                    resolved, transaction.deletes);
            List<PhysicalDelete> unmatchedDeletes = relocations.deletes().stream()
                    .filter(delete -> delete.pairedByRowPosition < 0)
                    .map(delete -> delete.value)
                    .toList();
            return Optional.of(new CompletedTransaction(
                    terminal.tid(), terminal(terminal), outcome,
                    relocations.rows(), unmatchedDeletes));
        }
    }

    private record RelocationResult(
            List<NativeRowDecoder.DecodedRow> rows, List<DeleteEffect> deletes) {}

    /**
     * HANA may execute one full-row UPSERT/REPLACE as internal DELETEs followed
     * by full INSERT_DATA or UPDATE images. Under the application contract, a
     * transaction may mix these shapes only for a PK-preserving full-row rewrite.
     * Count and table equality are therefore the fail-closed structural
     * certificate; ambiguous mixtures are rejected.
     */
    private static RelocationResult certifyInternalFullRowRewrites(
            List<NativeRowDecoder.DecodedRow> rows,
            List<DeleteEffect> deletes) throws IOException {
        List<NativeRowDecoder.DecodedRow> result = new ArrayList<>(rows);
        List<DeleteEffect> remaining = new ArrayList<>();
        for (DeleteEffect delete : deletes) {
            if (delete.pairedByRowPosition < 0) {
                remaining.add(delete);
            }
        }
        if (remaining.isEmpty()) {
            return new RelocationResult(List.copyOf(result), List.of());
        }

        Map<Long, List<DeleteEffect>> deletesByObject = new LinkedHashMap<>();
        for (DeleteEffect delete : remaining) {
            deletesByObject.computeIfAbsent(
                    delete.value.plan().metadata().objectId(), ignored -> new ArrayList<>())
                    .add(delete);
        }
        Set<DeleteEffect> relocated = java.util.Collections.newSetFromMap(
                new java.util.IdentityHashMap<>());
        for (var entry : deletesByObject.entrySet()) {
            List<Integer> fullRowIndexes = new ArrayList<>();
            int otherRows = 0;
            for (int index = 0; index < result.size(); index++) {
                NativeRowDecoder.DecodedRow row = result.get(index);
                if (row.plan().metadata().objectId() != entry.getKey()) {
                    continue;
                }
                if (row.operationCode() == NativeRecordReader.UT_INSERT_DATA
                        || row.operationCode() == NativeRecordReader.UT_UPDATE) {
                    fullRowIndexes.add(index);
                }
                else {
                    otherRows++;
                }
            }
            if (fullRowIndexes.isEmpty()) {
                continue;
            }
            if (otherRows != 0 || fullRowIndexes.size() != entry.getValue().size()) {
                throw dmlViolation("ambiguous DELETE/full-row rewrite for object="
                        + entry.getKey() + " deletes=" + entry.getValue().size()
                        + " fullRows=" + fullRowIndexes.size()
                        + " otherRows=" + otherRows);
            }
            for (int index : fullRowIndexes) {
                NativeRowDecoder.DecodedRow row = result.get(index);
                if (row.operationCode() == NativeRecordReader.UT_INSERT_DATA) {
                    result.set(index, new NativeRowDecoder.DecodedRow(
                            row.plan(), row.tid(), row.logPosition(), row.rowId(),
                            row.containerId(), NativeRecordReader.UT_UPSERT,
                            row.fullImage(), row.values(), row.lobLocatorIds()));
                }
            }
            relocated.addAll(entry.getValue());
        }
        remaining.removeIf(relocated::contains);
        return new RelocationResult(List.copyOf(result), List.copyOf(remaining));
    }

    private void applyPartialRollback(NativeRecordHeader header, byte[] payload)
            throws IOException {
        if (payload.length != 48) {
            throw transactionFailure("PARTIAL_ROLLBACK payload width=" + payload.length);
        }
        ByteBuffer value = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        long markerA = value.getLong();
        long markerB = value.getLong();
        long reserved = value.getLong();
        long rollbackStart = value.getLong();
        long reason = value.getLong();
        long markerC = value.getLong();
        if (reserved != 0 || reason != 2 || rollbackStart < 0
                || rollbackStart >= header.position()) {
            throw transactionFailure("unknown PARTIAL_ROLLBACK grammar: start="
                    + rollbackStart + " reason=" + reason + " reserved=" + reserved);
        }
        if (rollbackStart == 0) {
            if (markerA != -1 || markerB != -1 || markerC != -1) {
                throw transactionFailure("full-rollback precursor has unknown markers");
            }
            TransactionState removed = transactions.remove(header.tid());
            if (removed != null) {
                releaseRows(removed);
                removed.close();
            }
            return;
        }
        TransactionState transaction = transactions.get(header.tid());
        if (transaction == null) {
            return;
        }
        int removedRows = 0;
        for (int index = transaction.rows.size() - 1; index >= 0; index--) {
            NativeRowDecoder.DecodedRow row = transaction.rows.get(index);
            if (row.logPosition() >= rollbackStart) {
                transaction.rows.remove(index);
                int payloadBytes = transaction.rowPayloadBytes.remove(index);
                removedRows++;
                transaction.bufferedRowBytes -= payloadBytes;
                bufferedRowBytes -= payloadBytes;
            }
        }
        bufferedRows -= removedRows;
        transaction.requiredLobLocators.clear();
        for (NativeRowDecoder.DecodedRow row : transaction.rows) {
            transaction.requiredLobLocators.addAll(row.lobLocatorIds());
        }
        transaction.deletes.removeIf(delete -> delete.value.logPosition() >= rollbackStart);
        for (DeleteEffect delete : transaction.deletes) {
            if (delete.pairedByRowPosition >= rollbackStart) {
                delete.pairedByRowPosition = -1;
            }
        }
        transaction.lobFragments.removeIf(fragment ->
                fragment.header().position() >= rollbackStart);
        transaction.bufferedLobFragmentBytes = transaction.lobFragments.stream()
                .mapToLong(fragment -> fragment.payload().length).sum();
        transaction.targetTouched = !transaction.rows.isEmpty()
                || !transaction.deletes.isEmpty();
        transaction.pendingReplacementDelete = lastUnpairedEffect(transaction);
    }

    private static DeleteEffect lastUnpairedEffect(TransactionState transaction) {
        long lastRowPosition = transaction.rows.stream()
                .mapToLong(NativeRowDecoder.DecodedRow::logPosition)
                .max().orElse(-1);
        DeleteEffect candidate = null;
        for (DeleteEffect delete : transaction.deletes) {
            if (delete.pairedByRowPosition < 0
                    && (candidate == null
                    || delete.value.logPosition() > candidate.value.logPosition())) {
                candidate = delete;
            }
        }
        return candidate != null && candidate.value.logPosition() > lastRowPosition
                ? candidate : null;
    }

    private static void rebuildLobs(
            TransactionState transaction,
            Set<Long> requiredLocators) throws IOException {
        transaction.lobs.close();
        transaction.lobs = new NativeLobReconstructor.Session(transaction.lobLimits);
        for (LobFragment fragment : transaction.lobFragments) {
            transaction.lobs.acceptRequired(
                    fragment.header(), fragment.payload(), requiredLocators);
        }
    }

    private void releaseRows(TransactionState transaction) {
        bufferedRows -= transaction.rows.size();
        bufferedRowBytes -= transaction.bufferedRowBytes;
        transaction.bufferedRowBytes = 0;
    }

    private TransactionState state(long tid, long firstRecordPosition) throws IOException {
        if (tid <= 0 || tid == Long.MAX_VALUE) {
            throw new IOException("FATAL_TRANSACTION_STATE: invalid application TID=" + tid);
        }
        TransactionState existing = transactions.get(tid);
        if (existing != null) {
            return existing;
        }
        if (transactions.size() >= maxOpenTransactions) {
            throw resource("TRANSACTION: too many open transactions");
        }
        TransactionState created = new TransactionState(firstRecordPosition, lobLimits);
        transactions.put(tid, created);
        return created;
    }

    private static TerminalRecord terminal(NativeRecordHeader header) {
        long units = header.allocationBytes() / NativeRecordReader.LOG_POSITION_BYTES;
        return new TerminalRecord(header.backup(), header.position(), header.fileOffset(),
                Math.addExact(header.position(), units));
    }

    private void reserveRows(int rowCount, int payloadBytes) throws IOException {
        if (rowCount <= 0 || bufferedRows > maxBufferedRows - rowCount
                || payloadBytes < 0
                || bufferedRowBytes > maxBufferedRowBytes - payloadBytes) {
            throw resource("TRANSACTION: buffered row budget exceeded");
        }
        bufferedRows += rowCount;
        bufferedRowBytes += payloadBytes;
    }

    private void completeBackup(
            BackupFile backup, NativeBackupHeaderValidator.Metadata metadata) {
        if (databaseId == null) {
            databaseId = metadata.databaseId();
        }
        expectedBackupFirst = backup.last();
    }

    private static boolean isLobRecord(NativeRecordHeader header) {
        return header.outerCode() == NativeRecordReader.OUTER_LOB_WRITE
                || (header.outerCode() == NativeRecordReader.OUTER_VIRTUAL_FILE
                && (header.operationCode() == NativeRecordReader.VF_TRUNCATE
                || header.operationCode() == NativeRecordReader.VF_OVERWRITE));
    }

    private static void requireTerminalWidth(
            NativeRecordHeader header, byte[] payload, int expected, String type)
            throws IOException {
        if (payload.length != expected) {
            throw transactionFailure(type + " payload width=" + payload.length
                    + " at " + header.position());
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("native row/LOB pipeline is closed");
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (TransactionState transaction : transactions.values()) {
            transaction.close();
        }
        transactions.clear();
        massRecords.clear();
        if (rowExecutor != null) {
            rowExecutor.shutdownNow();
            try {
                rowExecutor.awaitTermination(5, TimeUnit.SECONDS);
            }
            catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
        }
        activeCursor = false;
        bufferedRows = 0;
        bufferedRowBytes = 0;
    }

    private static IOException dmlViolation(String detail) {
        return new IOException("FATAL_DML_CONTRACT_VIOLATION: " + detail);
    }

    private static IOException transactionFailure(String detail) {
        return new IOException("FATAL_TRANSACTION_STATE: " + detail);
    }

    private static IOException resource(String detail) {
        return new IOException("FATAL_RESOURCE_LIMIT: " + detail);
    }

    private static ThreadFactory rowThreadFactory() {
        AtomicInteger sequence = new AtomicInteger();
        return task -> {
            Thread thread = new Thread(task,
                    "hana-native-row-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}

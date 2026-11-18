package io.hanacdc.decoder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Parallel, record-boundary-safe decoder for the immutable HANA benchmark log
 * backups. The HANA record framing is real; HCDC1 is a synthetic full-row image
 * used only to benchmark the decoder until native HANA row decoding is added.
 */
public final class ParallelLogDecoder {
    public static final int LOG_RECORD_FILE_BASE = 4096;
    public static final int MAX_RECORD_BYTES = 8 * 1024 * 1024;
    public static final int MAX_ENVELOPE_BYTES = 4096;
    public static final int MAX_RECONSTRUCTED_BYTES = MAX_ENVELOPE_BYTES * 2;
    public static final int CONTINUATION_HEADER_BYTES = 56;
    public static final int DEFAULT_TARGET_CHUNK_MIB = 256;
    public static final int DEFAULT_CHUNKS_PER_THREAD = 4;

    private static final byte[] MAGIC = "HCDC1|".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] END_MARKER = "|END".getBytes(StandardCharsets.US_ASCII);

    public record BackupFile(
            Path path,
            long volume,
            long partition,
            long first,
            long last,
            long backupId) {

        public BackupFile {
            Objects.requireNonNull(path, "path");
        }

        public String fileName() {
            Path name = path.getFileName();
            if (name == null) {
                throw new IllegalStateException("backup path does not name a file: " + path);
            }
            return name.toString();
        }

        public static Optional<BackupFile> parse(Path path) {
            Path fileName = path.getFileName();
            if (fileName == null) {
                return Optional.empty();
            }
            String name = fileName.toString();
            if (!name.startsWith("log_backup_")) {
                return Optional.empty();
            }
            String[] fields = name.substring("log_backup_".length()).split("_", -1);
            if (fields.length != 4) {
                return Optional.empty();
            }
            int dot = fields[3].indexOf('.');
            if (dot <= 0 || dot == fields[3].length() - 1) {
                return Optional.empty();
            }
            try {
                return Optional.of(new BackupFile(
                        path,
                        Long.parseLong(fields[0]),
                        Long.parseLong(fields[1]),
                        Long.parseLong(fields[2]),
                        Long.parseLong(fields[3].substring(0, dot)),
                        Long.parseLong(fields[3].substring(dot + 1))));
            }
            catch (NumberFormatException ignored) {
                return Optional.empty();
            }
        }
    }

    public record ScanConfig(
            String requestedThreads,
            int maxThreads,
            long targetChunkBytes,
            int targetChunkMib,
            int chunksPerThread,
            boolean calculateFingerprint) {

        public ScanConfig {
            Objects.requireNonNull(requestedThreads, "requestedThreads");
            if (!(requestedThreads.equals("auto") || isPositiveInteger(requestedThreads))) {
                throw new IllegalArgumentException("requestedThreads must be auto or positive");
            }
            if (maxThreads <= 0 || targetChunkBytes <= 0 || targetChunkMib <= 0 || chunksPerThread <= 0) {
                throw new IllegalArgumentException("scan limits must be positive");
            }
        }

        public static ScanConfig fixed(int threads, boolean fingerprint) {
            return new ScanConfig(
                    Integer.toString(threads),
                    Math.max(threads, Runtime.getRuntime().availableProcessors()),
                    (long) DEFAULT_TARGET_CHUNK_MIB * 1024 * 1024,
                    DEFAULT_TARGET_CHUNK_MIB,
                    DEFAULT_CHUNKS_PER_THREAD,
                    fingerprint);
        }

        public int activeThreads(long fileBytes) {
            int requested = requestedThreads.equals("auto")
                    ? maxThreads
                    : Integer.parseInt(requestedThreads);
            long useful = Math.max(1L, ceilDiv(fileBytes, targetChunkBytes));
            return (int) Math.max(1L, Math.min(Math.min(requested, maxThreads), useful));
        }

        private static boolean isPositiveInteger(String value) {
            try {
                return Integer.parseInt(value) > 0;
            }
            catch (NumberFormatException ignored) {
                return false;
            }
        }
    }

    public record ScanMetrics(
            String path,
            String file,
            long firstPosition,
            long lastPosition,
            long bytes,
            int records,
            int threads,
            int chunks,
            long candidates,
            int events,
            int uniqueEvents,
            int duplicates,
            long scanWarnings,
            int deferredFragments,
            long eventPayloadBytes,
            String firstFingerprintRecord,
            String lastFingerprintRecord,
            String fingerprint,
            double mmapSeconds,
            double indexSeconds,
            double parallelDecodeSeconds,
            double orderedMergeSeconds,
            double fingerprintSeconds,
            double scanSeconds,
            double totalSeconds,
            double megabytesPerSecond,
            double eventsPerSecond) {

        public String toJson() {
            StringBuilder json = new StringBuilder(1024);
            json.append('{');
            jsonField(json, "kind", "file").append(',');
            jsonField(json, "path", path).append(',');
            jsonField(json, "file", file).append(',');
            numberField(json, "first_position", firstPosition).append(',');
            numberField(json, "last_position", lastPosition).append(',');
            numberField(json, "bytes", bytes).append(',');
            numberField(json, "records", records).append(',');
            numberField(json, "threads", threads).append(',');
            numberField(json, "chunks", chunks).append(',');
            numberField(json, "candidates", candidates).append(',');
            numberField(json, "events", events).append(',');
            numberField(json, "unique_events", uniqueEvents).append(',');
            numberField(json, "duplicates", duplicates).append(',');
            numberField(json, "scan_warnings", scanWarnings).append(',');
            numberField(json, "deferred_fragments", deferredFragments).append(',');
            numberField(json, "event_payload_bytes", eventPayloadBytes).append(',');
            nullableJsonField(json, "first_fingerprint_record", firstFingerprintRecord).append(',');
            nullableJsonField(json, "last_fingerprint_record", lastFingerprintRecord).append(',');
            jsonField(json, "fingerprint", fingerprint).append(',');
            doubleField(json, "mmap_seconds", mmapSeconds).append(',');
            doubleField(json, "index_seconds", indexSeconds).append(',');
            doubleField(json, "parallel_decode_seconds", parallelDecodeSeconds).append(',');
            doubleField(json, "ordered_merge_seconds", orderedMergeSeconds).append(',');
            doubleField(json, "fingerprint_seconds", fingerprintSeconds).append(',');
            doubleField(json, "scan_seconds", scanSeconds).append(',');
            doubleField(json, "total_seconds", totalSeconds).append(',');
            doubleField(json, "megabytes_per_second", megabytesPerSecond).append(',');
            doubleField(json, "events_per_second", eventsPerSecond);
            return json.append('}').toString();
        }
    }

    /** Compact structure-of-arrays event batch; payload remains in bounded file windows. */
    public static final class EventBatch {
        private int size;
        private long[] start;
        private long[] workerStart;
        private long[] workerEnd;
        private long[] sequenceStart;
        private long[] sequenceEnd;
        private long[] eventTsStart;
        private long[] eventTsEnd;
        private long[] payloadStart;
        private long[] payloadEnd;
        private int[] recordSize;
        private long[] workerId;
        private long[] sequence;
        private long[] rowId;
        private long[] rowVersion;
        private long[] tid;
        private long[] logPosition;
        private byte[] deleted;
        private byte[][] ownedWorkerText;
        private byte[][] ownedSequenceText;
        private byte[][] ownedEventTs;
        private byte[][] ownedPayload;
        private BackupFile[] sourceBackup;

        EventBatch(int initialCapacity) {
            int capacity = Math.max(16, initialCapacity);
            start = new long[capacity];
            workerStart = new long[capacity];
            workerEnd = new long[capacity];
            sequenceStart = new long[capacity];
            sequenceEnd = new long[capacity];
            eventTsStart = new long[capacity];
            eventTsEnd = new long[capacity];
            payloadStart = new long[capacity];
            payloadEnd = new long[capacity];
            recordSize = new int[capacity];
            workerId = new long[capacity];
            sequence = new long[capacity];
            rowId = new long[capacity];
            rowVersion = new long[capacity];
            tid = new long[capacity];
            logPosition = new long[capacity];
            deleted = new byte[capacity];
            ownedWorkerText = new byte[capacity][];
            ownedSequenceText = new byte[capacity][];
            ownedEventTs = new byte[capacity][];
            ownedPayload = new byte[capacity][];
            sourceBackup = new BackupFile[capacity];
        }

        void add(
                long eventStart,
                long rawWorkerStart,
                long rawWorkerEnd,
                long rawSequenceStart,
                long rawSequenceEnd,
                long parsedWorker,
                long parsedSequence,
                long parsedRowId,
                long parsedVersion,
                boolean parsedDeleted,
                long timestampStart,
                long timestampEnd,
                long rawPayloadStart,
                long rawPayloadEnd,
                long sourceTid,
                long sourceLogPosition,
                int sourceRecordSize) {
            ensureCapacity(size + 1);
            int index = size++;
            start[index] = eventStart;
            workerStart[index] = rawWorkerStart;
            workerEnd[index] = rawWorkerEnd;
            sequenceStart[index] = rawSequenceStart;
            sequenceEnd[index] = rawSequenceEnd;
            workerId[index] = parsedWorker;
            sequence[index] = parsedSequence;
            rowId[index] = parsedRowId;
            rowVersion[index] = parsedVersion;
            deleted[index] = parsedDeleted ? (byte) 1 : (byte) 0;
            eventTsStart[index] = timestampStart;
            eventTsEnd[index] = timestampEnd;
            payloadStart[index] = rawPayloadStart;
            payloadEnd[index] = rawPayloadEnd;
            tid[index] = sourceTid;
            logPosition[index] = sourceLogPosition;
            recordSize[index] = sourceRecordSize;
        }

        void addOwned(
                long eventStart,
                byte[] workerText,
                byte[] sequenceText,
                long parsedWorker,
                long parsedSequence,
                long parsedRowId,
                long parsedVersion,
                boolean parsedDeleted,
                byte[] eventTimestamp,
                byte[] eventPayload,
                long sourceTid,
                long sourceLogPosition,
                int sourceRecordSize,
                BackupFile originBackup) {
            ensureCapacity(size + 1);
            int index = size++;
            start[index] = eventStart;
            workerId[index] = parsedWorker;
            sequence[index] = parsedSequence;
            rowId[index] = parsedRowId;
            rowVersion[index] = parsedVersion;
            deleted[index] = parsedDeleted ? (byte) 1 : (byte) 0;
            ownedWorkerText[index] = workerText;
            ownedSequenceText[index] = sequenceText;
            ownedEventTs[index] = eventTimestamp;
            ownedPayload[index] = eventPayload;
            workerEnd[index] = workerText.length;
            sequenceEnd[index] = sequenceText.length;
            eventTsEnd[index] = eventTimestamp.length;
            payloadEnd[index] = eventPayload.length;
            tid[index] = sourceTid;
            logPosition[index] = sourceLogPosition;
            recordSize[index] = sourceRecordSize;
            sourceBackup[index] = originBackup;
        }

        int compactUnique(LongPairSet seen) {
            int write = 0;
            for (int read = 0; read < size; read++) {
                if (!seen.add(workerId[read], sequence[read])) {
                    continue;
                }
                if (write != read) {
                    copy(read, write);
                }
                write++;
            }
            int removed = size - write;
            size = write;
            return removed;
        }

        private void copy(int from, int to) {
            start[to] = start[from];
            workerStart[to] = workerStart[from];
            workerEnd[to] = workerEnd[from];
            sequenceStart[to] = sequenceStart[from];
            sequenceEnd[to] = sequenceEnd[from];
            eventTsStart[to] = eventTsStart[from];
            eventTsEnd[to] = eventTsEnd[from];
            payloadStart[to] = payloadStart[from];
            payloadEnd[to] = payloadEnd[from];
            recordSize[to] = recordSize[from];
            workerId[to] = workerId[from];
            sequence[to] = sequence[from];
            rowId[to] = rowId[from];
            rowVersion[to] = rowVersion[from];
            tid[to] = tid[from];
            logPosition[to] = logPosition[from];
            deleted[to] = deleted[from];
            ownedWorkerText[to] = ownedWorkerText[from];
            ownedSequenceText[to] = ownedSequenceText[from];
            ownedEventTs[to] = ownedEventTs[from];
            ownedPayload[to] = ownedPayload[from];
            sourceBackup[to] = sourceBackup[from];
        }

        private void ensureCapacity(int wanted) {
            if (wanted <= start.length) {
                return;
            }
            int capacity = Math.max(wanted, start.length + (start.length >>> 1));
            start = java.util.Arrays.copyOf(start, capacity);
            workerStart = java.util.Arrays.copyOf(workerStart, capacity);
            workerEnd = java.util.Arrays.copyOf(workerEnd, capacity);
            sequenceStart = java.util.Arrays.copyOf(sequenceStart, capacity);
            sequenceEnd = java.util.Arrays.copyOf(sequenceEnd, capacity);
            eventTsStart = java.util.Arrays.copyOf(eventTsStart, capacity);
            eventTsEnd = java.util.Arrays.copyOf(eventTsEnd, capacity);
            payloadStart = java.util.Arrays.copyOf(payloadStart, capacity);
            payloadEnd = java.util.Arrays.copyOf(payloadEnd, capacity);
            recordSize = java.util.Arrays.copyOf(recordSize, capacity);
            workerId = java.util.Arrays.copyOf(workerId, capacity);
            sequence = java.util.Arrays.copyOf(sequence, capacity);
            rowId = java.util.Arrays.copyOf(rowId, capacity);
            rowVersion = java.util.Arrays.copyOf(rowVersion, capacity);
            tid = java.util.Arrays.copyOf(tid, capacity);
            logPosition = java.util.Arrays.copyOf(logPosition, capacity);
            deleted = java.util.Arrays.copyOf(deleted, capacity);
            ownedWorkerText = java.util.Arrays.copyOf(ownedWorkerText, capacity);
            ownedSequenceText = java.util.Arrays.copyOf(ownedSequenceText, capacity);
            ownedEventTs = java.util.Arrays.copyOf(ownedEventTs, capacity);
            ownedPayload = java.util.Arrays.copyOf(ownedPayload, capacity);
            sourceBackup = java.util.Arrays.copyOf(sourceBackup, capacity);
        }

        public int size() { return size; }
        public long start(int index) { return start[index]; }
        public long workerStart(int index) { return workerStart[index]; }
        public long workerEnd(int index) { return workerEnd[index]; }
        public long sequenceStart(int index) { return sequenceStart[index]; }
        public long sequenceEnd(int index) { return sequenceEnd[index]; }
        public long eventTsStart(int index) { return eventTsStart[index]; }
        public long eventTsEnd(int index) { return eventTsEnd[index]; }
        public long payloadStart(int index) { return payloadStart[index]; }
        public long payloadEnd(int index) { return payloadEnd[index]; }
        public int recordSize(int index) { return recordSize[index]; }
        public long workerId(int index) { return workerId[index]; }
        public long sequence(int index) { return sequence[index]; }
        public long rowId(int index) { return rowId[index]; }
        public long rowVersion(int index) { return rowVersion[index]; }
        public long tid(int index) { return tid[index]; }
        public long logPosition(int index) { return logPosition[index]; }
        public boolean deleted(int index) { return deleted[index] != 0; }
        public byte[] ownedWorkerText(int index) { return ownedWorkerText[index]; }
        public byte[] ownedSequenceText(int index) { return ownedSequenceText[index]; }
        public byte[] ownedEventTs(int index) { return ownedEventTs[index]; }
        public byte[] ownedPayload(int index) { return ownedPayload[index]; }
        public BackupFile sourceBackup(int index) { return sourceBackup[index]; }
        public int payloadLength(int index) {
            byte[] owned = ownedPayload[index];
            return owned == null
                    ? Math.toIntExact(payloadEnd[index] - payloadStart[index])
                    : owned.length;
        }
    }

    public static final class DecodedFile implements AutoCloseable {
        private final BackupFile backup;
        private BoundedFileData data;
        private final List<EventBatch> batches;
        private final List<EventBatch> batchView;
        private final ScanMetrics metrics;

        DecodedFile(
                BackupFile backup,
                BoundedFileData data,
                List<EventBatch> batches,
                ScanMetrics metrics) {
            this.backup = backup;
            this.data = data;
            this.batches = new ArrayList<>(batches);
            this.batchView = Collections.unmodifiableList(this.batches);
            this.metrics = metrics;
        }

        public BackupFile backup() { return backup; }
        public List<EventBatch> batches() { return batchView; }
        public ScanMetrics metrics() { return metrics; }

        public String ascii(long start, long end) {
            if (data == null) {
                throw new IllegalStateException("decoded file is closed");
            }
            try {
                return data.ascii(start, end);
            }
            catch (IOException error) {
                throw new IllegalStateException("cannot read decoded event bytes", error);
            }
        }

        public String eventTimestamp(EventBatch batch, int index) {
            byte[] owned = batch.ownedEventTs(index);
            return owned == null
                    ? ascii(batch.eventTsStart(index), batch.eventTsEnd(index))
                    : new String(owned, StandardCharsets.UTF_8);
        }

        public String payload(EventBatch batch, int index) {
            byte[] owned = batch.ownedPayload(index);
            return owned == null
                    ? ascii(batch.payloadStart(index), batch.payloadEnd(index))
                    : new String(owned, StandardCharsets.UTF_8);
        }

        public Cursor cursor() {
            return new Cursor(this);
        }

        BoundedFileData data() { return data; }

        @Override
        public void close() {
            batches.clear();
            if (data != null) {
                try {
                    data.close();
                }
                catch (IOException ignored) {
                    // Closing a read-only input cannot advance a source offset.
                }
            }
            data = null;
        }
    }

    public static final class Cursor {
        private final DecodedFile file;
        private int batchIndex;
        private int eventIndex;

        Cursor(DecodedFile file) {
            this.file = file;
        }

        public boolean hasNext() {
            normalize();
            return batchIndex < file.batches.size();
        }

        public EventPointer next() {
            normalize();
            if (batchIndex >= file.batches.size()) {
                throw new java.util.NoSuchElementException();
            }
            EventBatch batch = file.batches.get(batchIndex);
            return new EventPointer(file, batch, eventIndex++);
        }

        private void normalize() {
            while (batchIndex < file.batches.size()
                    && eventIndex >= file.batches.get(batchIndex).size()) {
                batchIndex++;
                eventIndex = 0;
            }
        }
    }

    public static final class EventPointer {
        private final DecodedFile file;
        private final EventBatch batch;
        private final int index;

        private EventPointer(DecodedFile file, EventBatch batch, int index) {
            this.file = Objects.requireNonNull(file, "file");
            this.batch = Objects.requireNonNull(batch, "batch");
            this.index = index;
        }

        public BackupFile sourceBackup() {
            BackupFile origin = batch.sourceBackup(index);
            return origin == null ? file.backup() : origin;
        }

        public long sourceEventOffset() {
            return batch.start(index);
        }

        public long rowId() { return batch.rowId(index); }
        public long rowVersion() { return batch.rowVersion(index); }
        public boolean deleted() { return batch.deleted(index); }
        public long logPosition() { return batch.logPosition(index); }
        public long sequence() { return batch.sequence(index); }
        public long transactionId() { return batch.tid(index); }
        public String timestamp() { return file.eventTimestamp(batch, index); }
        public String payload() { return file.payload(batch, index); }
        public boolean ownsPayload() { return batch.ownedPayload(index) != null; }
    }

    private static final class RecordIndex {
        private int count;
        private long[] offset = new long[4096];
        private int[] size = new int[4096];
        private long[] tid = new long[4096];
        private long[] logPosition = new long[4096];

        void add(long recordOffset, int recordSize, long recordTid, long position) {
            if (count == offset.length) {
                int capacity = offset.length + (offset.length >>> 1);
                offset = java.util.Arrays.copyOf(offset, capacity);
                size = java.util.Arrays.copyOf(size, capacity);
                tid = java.util.Arrays.copyOf(tid, capacity);
                logPosition = java.util.Arrays.copyOf(logPosition, capacity);
            }
            offset[count] = recordOffset;
            size[count] = recordSize;
            tid[count] = recordTid;
            logPosition[count] = position;
            count++;
        }
    }

    private record ChunkRange(int start, int end) {}

    private record PendingEnvelope(
            BackupFile originBackup,
            long eventStart,
            byte[] bytes,
            long tid,
            long logPosition,
            int recordSize) {}

    private record PendingResolution(EventBatch events, List<PendingEnvelope> remaining) {}

    private record ChunkResult(
            EventBatch events,
            long candidates,
            long warnings,
            long bytesOwned,
            List<PendingEnvelope> pending) {}

    /** Stateful decoder boundary that carries unresolved records across backups. */
    public static final class DecoderSession {
        private BackupFile previous;
        private List<PendingEnvelope> pending = List.of();

        private void verifyNext(BackupFile backup) throws IOException {
            if (previous == null) {
                return;
            }
            if (previous.volume() != backup.volume()
                    || previous.partition() != backup.partition()
                    || previous.last() != backup.first()) {
                throw new IOException("FATAL_GAP: decoder session expected position "
                        + previous.last() + " but received " + backup.first());
            }
        }

        private List<PendingEnvelope> pending() {
            return pending;
        }

        private void commit(BackupFile backup, List<PendingEnvelope> nextPending) {
            previous = backup;
            pending = List.copyOf(nextPending);
        }

        public boolean hasPendingFragments() {
            return !pending.isEmpty();
        }

        public int pendingFragmentCount() {
            return pending.size();
        }

        public void finish() throws IOException {
            if (!pending.isEmpty()) {
                PendingEnvelope first = pending.get(0);
                throw new IOException("FATAL_GAP: corpus ended with " + pending.size()
                        + " unresolved cross-backup fragment(s); first TID=" + first.tid()
                        + " at backup_id=" + first.originBackup().backupId());
            }
        }
    }

    public DecodedFile decode(
            BackupFile backup,
            String runId,
            ScanConfig config,
            MessageDigest corpusDigest) throws IOException, InterruptedException {
        return decode(backup, runId, config, corpusDigest, null);
    }

    public DecodedFile decode(
            BackupFile backup,
            String runId,
            ScanConfig config,
            MessageDigest corpusDigest,
            DecoderSession session) throws IOException, InterruptedException {
        Objects.requireNonNull(backup, "backup");
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(config, "config");
        if (session != null) {
            session.verifyNext(backup);
        }

        long totalStarted = System.nanoTime();
        long mmapStarted = System.nanoTime();
        long fileBytes = Files.size(backup.path());
        BoundedFileData data = new BoundedFileData(backup.path());
        double mmapSeconds = secondsSince(mmapStarted);
        try {
            long indexStarted = System.nanoTime();
            RecordIndex records = indexRecords(data, backup);
            double indexSeconds = secondsSince(indexStarted);
            if (records.count == 0) {
                throw new IOException("no strict HANA record headers in " + backup.path());
            }

            int threads = config.activeThreads(fileBytes);
            List<ChunkRange> chunks = makeChunks(records, threads * config.chunksPerThread());
            byte[] needle = ("HCDC1|" + runId + "|").getBytes(StandardCharsets.US_ASCII);
            PendingResolution incoming = session == null
                    ? new PendingResolution(new EventBatch(1), List.of())
                    : resolvePending(data, records, session.pending(), needle);

            long decodeStarted = System.nanoTime();
            List<ChunkResult> results = decodeChunks(
                    data, records, chunks, needle, threads, backup, session != null);
            double parallelDecodeSeconds = secondsSince(decodeStarted);

        long mergeStarted = System.nanoTime();
        long candidates = 0;
        long warnings = 0;
        int rawEvents = incoming.events().size();
        List<PendingEnvelope> outgoingPending = new ArrayList<>(incoming.remaining());
        for (ChunkResult result : results) {
            candidates += result.candidates;
            warnings += result.warnings;
            rawEvents += result.events.size();
            outgoingPending.addAll(result.pending());
        }
        LongPairSet seen = new LongPairSet(rawEvents);
        List<EventBatch> batches = new ArrayList<>(results.size() + 1);
        int duplicates = 0;
        if (incoming.events().size() != 0) {
            duplicates += incoming.events().compactUnique(seen);
            if (incoming.events().size() != 0) {
                batches.add(incoming.events());
            }
        }
        long previousStart = -1;
        for (ChunkResult result : results) {
            EventBatch batch = result.events;
            for (int index = 0; index < batch.size(); index++) {
                if (batch.start(index) < previousStart) {
                    throw new IOException("parallel chunks lost physical source order");
                }
                previousStart = batch.start(index);
            }
            duplicates += batch.compactUnique(seen);
            if (batch.size() != 0) {
                batches.add(batch);
            }
        }
        int eventCount = rawEvents - duplicates;
        double orderedMergeSeconds = secondsSince(mergeStarted);

        long fingerprintStarted = System.nanoTime();
        String fingerprint = "";
        String firstFingerprintRecord = null;
        String lastFingerprintRecord = null;
        long payloadBytes = 0;
        if (config.calculateFingerprint()) {
            MessageDigest fileDigest = sha256();
            FingerprintWriter writer = new FingerprintWriter(runId);
            boolean first = true;
            for (int batchIndex = 0; batchIndex < batches.size(); batchIndex++) {
                EventBatch batch = batches.get(batchIndex);
                for (int index = 0; index < batch.size(); index++) {
                    int length = writer.build(data, batch, index);
                    fileDigest.update(writer.line, 0, length);
                    if (corpusDigest != null) {
                        corpusDigest.update(writer.line, 0, length);
                    }
                    if (first) {
                        firstFingerprintRecord = writer.asString(length);
                        first = false;
                    }
                    if (batchIndex == batches.size() - 1 && index == batch.size() - 1) {
                        lastFingerprintRecord = writer.asString(length);
                    }
                    payloadBytes += batch.payloadLength(index);
                }
            }
            fingerprint = hex(fileDigest.digest());
        }
        else {
            for (EventBatch batch : batches) {
                for (int index = 0; index < batch.size(); index++) {
                    payloadBytes += batch.payloadLength(index);
                }
            }
        }
        double fingerprintSeconds = secondsSince(fingerprintStarted);
        if (!config.calculateFingerprint()) {
            fingerprintSeconds = 0.0;
        }

        double scanSeconds = indexSeconds + parallelDecodeSeconds + orderedMergeSeconds;
        double totalSeconds = secondsSince(totalStarted);
        ScanMetrics metrics = new ScanMetrics(
                backup.path().toString(),
                backup.fileName(),
                backup.first(),
                backup.last(),
                fileBytes,
                records.count,
                threads,
                chunks.size(),
                candidates,
                eventCount,
                eventCount,
                duplicates,
                warnings,
                outgoingPending.size(),
                payloadBytes,
                firstFingerprintRecord,
                lastFingerprintRecord,
                fingerprint,
                mmapSeconds,
                indexSeconds,
                parallelDecodeSeconds,
                orderedMergeSeconds,
                fingerprintSeconds,
                scanSeconds,
                totalSeconds,
                fileBytes / 1_000_000.0 / Math.max(scanSeconds, 1e-9),
                eventCount / Math.max(scanSeconds, 1e-9));
        if (session != null) {
            session.commit(backup, outgoingPending);
        }
            return new DecodedFile(backup, data, batches, metrics);
        }
        catch (IOException | InterruptedException | RuntimeException error) {
            data.close();
            throw error;
        }
    }

    public static List<BackupFile> discoverCorpus(Path directory, long first, long last)
            throws IOException {
        List<BackupFile> backups = new ArrayList<>();
        try (var paths = Files.list(directory)) {
            paths.forEach(path -> BackupFile.parse(path).ifPresent(backup -> {
                if (backup.volume() == 2
                        && backup.partition() == 0
                        && backup.first() >= first
                        && backup.last() <= last) {
                    backups.add(backup);
                }
            }));
        }
        backups.sort(Comparator.comparingLong(BackupFile::first)
                .thenComparingLong(BackupFile::last)
                .thenComparingLong(BackupFile::backupId));
        long expected = first;
        for (BackupFile backup : backups) {
            if (backup.first() != expected) {
                throw new IOException("corpus gap: expected " + expected + ", got " + backup.first());
            }
            expected = backup.last();
        }
        if (backups.isEmpty() || expected != last) {
            throw new IOException("corpus ends at " + expected + ", expected " + last);
        }
        return backups;
    }

    static RecordIndex indexRecords(BoundedFileData data, BackupFile backup) throws IOException {
        RecordIndex records = new RecordIndex();
        long candidate = LOG_RECORD_FILE_BASE;
        long limit = Math.max(0, data.limit() - 23);
        while (candidate < limit) {
            int size = data.getInt(candidate + 4);
            long unsignedSize = Integer.toUnsignedLong(size);
            if (unsignedSize < 32 || unsignedSize > MAX_RECORD_BYTES) {
                candidate += 64;
                continue;
            }
            long tid = data.getLong(candidate + 8);
            long position = data.getLong(candidate + 16);
            long aligned = align64(unsignedSize);
            long recordEnd = candidate + aligned;
            long expectedOffset;
            try {
                expectedOffset = Math.addExact(
                        LOG_RECORD_FILE_BASE,
                        Math.multiplyExact(Math.subtractExact(position, backup.first()), 64L));
            }
            catch (ArithmeticException ignored) {
                candidate += 64;
                continue;
            }
            if (tid <= 0
                    || tid >= Long.MAX_VALUE
                    || position < backup.first()
                    || position >= backup.last()
                    || recordEnd > data.limit()
                    || candidate != expectedOffset) {
                candidate += 64;
                continue;
            }
            records.add(candidate, (int) unsignedSize, tid, position);
            candidate += aligned;
        }
        return records;
    }

    private static List<ChunkRange> makeChunks(RecordIndex records, int wantedChunks) {
        int wanted = Math.max(1, Math.min(wantedChunks, records.count));
        long totalBytes = 0;
        for (int index = 0; index < records.count; index++) {
            totalBytes += align64(records.size[index]);
        }
        long target = Math.max(1, ceilDiv(totalBytes, wanted));
        List<ChunkRange> chunks = new ArrayList<>(wanted);
        int start = 0;
        long accumulated = 0;
        for (int index = 0; index < records.count; index++) {
            accumulated += align64(records.size[index]);
            int remainingRecords = records.count - index - 1;
            int remainingChunks = wanted - chunks.size() - 1;
            if (chunks.size() + 1 < wanted
                    && accumulated >= target
                    && remainingRecords >= remainingChunks) {
                chunks.add(new ChunkRange(start, index + 1));
                start = index + 1;
                accumulated = 0;
            }
        }
        if (start < records.count) {
            chunks.add(new ChunkRange(start, records.count));
        }
        return chunks;
    }

    private static List<ChunkResult> decodeChunks(
            BoundedFileData data,
            RecordIndex records,
            List<ChunkRange> chunks,
            byte[] needle,
            int threads,
            BackupFile backup,
            boolean deferAtEndOfFile) throws IOException, InterruptedException {
        AtomicInteger threadNumber = new AtomicInteger();
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "hana-redo-" + threadNumber.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
        ExecutorService executor = Executors.newFixedThreadPool(threads, factory);
        try {
            List<Callable<ChunkResult>> tasks = new ArrayList<>(chunks.size());
            for (ChunkRange chunk : chunks) {
                tasks.add(() -> scanChunk(
                        data, records, chunk, needle, backup, deferAtEndOfFile));
            }
            List<Future<ChunkResult>> futures = executor.invokeAll(tasks);
            List<ChunkResult> results = new ArrayList<>(futures.size());
            for (Future<ChunkResult> future : futures) {
                try {
                    results.add(future.get());
                }
                catch (ExecutionException error) {
                    Throwable cause = error.getCause();
                    if (cause instanceof IOException io) {
                        throw io;
                    }
                    throw new IOException("parallel decoder failed", cause);
                }
            }
            return results;
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static ChunkResult scanChunk(
            BoundedFileData data,
            RecordIndex records,
            ChunkRange chunk,
            byte[] needle,
            BackupFile backup,
            boolean deferAtEndOfFile) throws IOException {
        int estimatedEvents = Math.max(16, (chunk.end - chunk.start) * 5);
        EventBatch events = new EventBatch(estimatedEvents);
        long candidates = 0;
        long warnings = 0;
        long ownedBytes = 0;
        List<PendingEnvelope> pending = new ArrayList<>();
        for (int record = chunk.start; record < chunk.end; record++) {
            long recordStart = records.offset[record];
            long recordEnd = Math.min(data.limit(), recordStart + align64(records.size[record]));
            ownedBytes += recordEnd - recordStart;
            long searchEnd = Math.min(data.limit(), recordEnd + needle.length - 1);
            long cursor = recordStart;
            while (cursor < recordEnd) {
                long start = findPattern(data, needle, cursor, searchEnd);
                if (start < 0 || start >= recordEnd) {
                    break;
                }
                candidates++;
                cursor = start + needle.length;
                long envelopeLimit = Math.min(data.limit(), start + MAX_ENVELOPE_BYTES);
                long endMarker = findPattern(data, END_MARKER, start, envelopeLimit);
                boolean parsed = endMarker >= 0 && parseEnvelope(
                        data, start, endMarker, needle,
                        records.tid[record], records.logPosition[record],
                        records.size[record], events);
                if (!parsed) {
                    parsed = recoverFragmentedEnvelope(
                            data, records, record, start, needle, events);
                }
                if (!parsed) {
                    PendingEnvelope deferred = deferAtEndOfFile
                            ? pendingEnvelope(data, records, record, start, backup)
                            : null;
                    if (deferred == null) {
                        warnings++;
                    }
                    else {
                        pending.add(deferred);
                    }
                }
            }
        }
        return new ChunkResult(events, candidates, warnings, ownedBytes, List.copyOf(pending));
    }

    private static boolean parseEnvelope(
            BoundedFileData data,
            long start,
            long endMarker,
            byte[] needle,
            long tid,
            long logPosition,
            int recordSize,
            EventBatch events) throws IOException {
        if (!isValidUtf8(data, start, endMarker + END_MARKER.length)) {
            return false;
        }
        long delimiter1 = start + needle.length - 1;
        long delimiter2 = findByte(data, (byte) '|', delimiter1 + 1, endMarker);
        long delimiter3 = delimiter2 < 0 ? -1 : findByte(data, (byte) '|', delimiter2 + 1, endMarker);
        long delimiter4 = delimiter3 < 0 ? -1 : findByte(data, (byte) '|', delimiter3 + 1, endMarker);
        long delimiter5 = delimiter4 < 0 ? -1 : findByte(data, (byte) '|', delimiter4 + 1, endMarker);
        long delimiter6 = delimiter5 < 0 ? -1 : findByte(data, (byte) '|', delimiter5 + 1, endMarker);
        long delimiter7 = delimiter6 < 0 ? -1 : findByte(data, (byte) '|', delimiter6 + 1, endMarker);
        if (delimiter7 < 0 || findByte(data, (byte) '|', delimiter7 + 1, endMarker) >= 0) {
            return false;
        }
        try {
            long worker = parseLong(data, delimiter1 + 1, delimiter2);
            long sequence = parseLong(data, delimiter2 + 1, delimiter3);
            long rowId = parseLong(data, delimiter3 + 1, delimiter4);
            long version = parseLong(data, delimiter4 + 1, delimiter5);
            boolean deleted = parseLong(data, delimiter5 + 1, delimiter6) != 0;
            events.add(
                    start,
                    delimiter1 + 1,
                    delimiter2,
                    delimiter2 + 1,
                    delimiter3,
                    worker,
                    sequence,
                    rowId,
                    version,
                    deleted,
                    delimiter6 + 1,
                    delimiter7,
                    delimiter7 + 1,
                    endMarker,
                    tid,
                    logPosition,
                    recordSize);
            return true;
        }
        catch (NumberFormatException ignored) {
            return false;
        }
    }

    private static boolean isValidUtf8(BoundedFileData data, long start, long end)
            throws IOException {
        return data.isValidUtf8(start, end);
    }

    private static PendingEnvelope pendingEnvelope(
            BoundedFileData data,
            RecordIndex records,
            int record,
            long eventStart,
            BackupFile backup) throws IOException {
        if (records.count - record > 4096) {
            return null;
        }
        long firstEnd = Math.min(
                data.limit(), records.offset[record] + align64(records.size[record]));
        int initialLength = Math.toIntExact(firstEnd - eventStart);
        if (initialLength <= 0 || initialLength > MAX_RECONSTRUCTED_BYTES) {
            return null;
        }
        byte[] reconstructed = new byte[MAX_RECONSTRUCTED_BYTES];
        copy(data, eventStart, firstEnd, reconstructed, 0);
        int length = initialLength;
        for (int continuation = record + 1;
                continuation < records.count && length < MAX_RECONSTRUCTED_BYTES;
                continuation++) {
            if (records.tid[continuation] != records.tid[record]
                    || records.size[continuation] <= CONTINUATION_HEADER_BYTES) {
                continue;
            }
            long continuationStart = records.offset[continuation] + CONTINUATION_HEADER_BYTES;
            long continuationEnd = Math.min(
                    data.limit(), records.offset[continuation]
                            + align64(records.size[continuation]));
            int count = (int) Math.min(continuationEnd - continuationStart,
                    MAX_RECONSTRUCTED_BYTES - length);
            if (count <= 0) {
                continue;
            }
            copy(data, continuationStart, continuationStart + count, reconstructed, length);
            length += count;
        }
        if (findPattern(reconstructed, END_MARKER, 0, length) >= 0) {
            return null;
        }
        return new PendingEnvelope(
                backup,
                eventStart,
                java.util.Arrays.copyOf(reconstructed, length),
                records.tid[record],
                records.logPosition[record],
                records.size[record]);
    }

    private static PendingResolution resolvePending(
            BoundedFileData data,
            RecordIndex records,
            List<PendingEnvelope> pending,
            byte[] needle) throws IOException {
        EventBatch recovered = new EventBatch(Math.max(1, pending.size()));
        List<PendingEnvelope> remaining = new ArrayList<>();
        for (PendingEnvelope value : pending) {
            if (value.bytes().length >= MAX_RECONSTRUCTED_BYTES) {
                throw new IOException("cross-backup fragment exceeds decoder limit for TID="
                        + value.tid());
            }
            byte[] reconstructed = new byte[MAX_RECONSTRUCTED_BYTES];
            System.arraycopy(value.bytes(), 0, reconstructed, 0, value.bytes().length);
            int length = value.bytes().length;
            boolean resolved = false;
            int matchingRecords = 0;
            for (int record = 0;
                    record < records.count && matchingRecords < 4096
                            && length < MAX_RECONSTRUCTED_BYTES;
                    record++) {
                if (records.tid[record] != value.tid()
                        || records.size[record] <= CONTINUATION_HEADER_BYTES) {
                    continue;
                }
                matchingRecords++;
                long start = records.offset[record] + CONTINUATION_HEADER_BYTES;
                long end = Math.min(data.limit(), records.offset[record]
                        + align64(records.size[record]));
                int count = (int) Math.min(end - start, MAX_RECONSTRUCTED_BYTES - length);
                if (count <= 0) {
                    continue;
                }
                copy(data, start, start + count, reconstructed, length);
                length += count;
                int endMarker = findPattern(reconstructed, END_MARKER, 0, length);
                if (endMarker < 0) {
                    continue;
                }
                if (!parseOwnedEnvelope(
                        reconstructed,
                        endMarker,
                        value.eventStart(),
                        needle,
                        value.tid(),
                        value.logPosition(),
                        value.recordSize(),
                        recovered,
                        value.originBackup())) {
                    throw new IOException("invalid reconstructed cross-backup envelope for TID="
                            + value.tid());
                }
                resolved = true;
                break;
            }
            if (!resolved) {
                if (length >= MAX_RECONSTRUCTED_BYTES) {
                    throw new IOException("unresolved cross-backup fragment reached decoder limit for TID="
                            + value.tid());
                }
                remaining.add(new PendingEnvelope(
                        value.originBackup(),
                        value.eventStart(),
                        java.util.Arrays.copyOf(reconstructed, length),
                        value.tid(),
                        value.logPosition(),
                        value.recordSize()));
            }
        }
        return new PendingResolution(recovered, List.copyOf(remaining));
    }

    private static boolean recoverFragmentedEnvelope(
            BoundedFileData data,
            RecordIndex records,
            int record,
            long eventStart,
            byte[] needle,
            EventBatch events) throws IOException {
        long firstEnd = Math.min(
                data.limit(), records.offset[record] + align64(records.size[record]));
        int initialLength = Math.toIntExact(firstEnd - eventStart);
        if (initialLength <= 0 || initialLength > MAX_RECONSTRUCTED_BYTES) {
            return false;
        }
        byte[] reconstructed = new byte[MAX_RECONSTRUCTED_BYTES];
        copy(data, eventStart, firstEnd, reconstructed, 0);
        int length = initialLength;
        int endMarker = findPattern(reconstructed, END_MARKER, 0, length);
        if (endMarker >= 0) {
            return parseOwnedEnvelope(
                    reconstructed, endMarker, eventStart, needle,
                    records.tid[record], records.logPosition[record],
                    records.size[record], events, null);
        }

        int lastRecord = Math.min(records.count, record + 256);
        for (int continuation = record + 1; continuation < lastRecord; continuation++) {
            if (records.tid[continuation] != records.tid[record]
                    || records.size[continuation] <= CONTINUATION_HEADER_BYTES) {
                continue;
            }
            long continuationStart = records.offset[continuation] + CONTINUATION_HEADER_BYTES;
            long continuationEnd = Math.min(
                    data.limit(),
                    records.offset[continuation]
                            + align64(records.size[continuation]));
            int count = (int) Math.min(continuationEnd - continuationStart,
                    MAX_RECONSTRUCTED_BYTES - length);
            if (count <= 0) {
                return false;
            }
            copy(data, continuationStart, continuationStart + count, reconstructed, length);
            length += count;
            endMarker = findPattern(reconstructed, END_MARKER, 0, length);
            if (endMarker >= 0) {
                return parseOwnedEnvelope(
                        reconstructed, endMarker, eventStart, needle,
                        records.tid[record], records.logPosition[record],
                        records.size[record], events, null);
            }
        }
        return false;
    }

    private static boolean parseOwnedEnvelope(
            byte[] reconstructed,
            int endMarker,
            long eventStart,
            byte[] needle,
            long tid,
            long logPosition,
            int recordSize,
            EventBatch events,
            BackupFile originBackup) {
        int envelopeLength = endMarker + END_MARKER.length;
        if (envelopeLength > MAX_ENVELOPE_BYTES || endMarker < needle.length) {
            return false;
        }
        for (int index = 0; index < needle.length; index++) {
            if (reconstructed[index] != needle[index]) {
                return false;
            }
        }
        int delimiter1 = needle.length - 1;
        int delimiter2 = findByte(reconstructed, (byte) '|', delimiter1 + 1, endMarker);
        int delimiter3 = delimiter2 < 0 ? -1
                : findByte(reconstructed, (byte) '|', delimiter2 + 1, endMarker);
        int delimiter4 = delimiter3 < 0 ? -1
                : findByte(reconstructed, (byte) '|', delimiter3 + 1, endMarker);
        int delimiter5 = delimiter4 < 0 ? -1
                : findByte(reconstructed, (byte) '|', delimiter4 + 1, endMarker);
        int delimiter6 = delimiter5 < 0 ? -1
                : findByte(reconstructed, (byte) '|', delimiter5 + 1, endMarker);
        int delimiter7 = delimiter6 < 0 ? -1
                : findByte(reconstructed, (byte) '|', delimiter6 + 1, endMarker);
        if (delimiter7 < 0
                || findByte(reconstructed, (byte) '|', delimiter7 + 1, endMarker) >= 0) {
            return false;
        }
        try {
            long worker = parseLong(reconstructed, delimiter1 + 1, delimiter2);
            long sequence = parseLong(reconstructed, delimiter2 + 1, delimiter3);
            long rowId = parseLong(reconstructed, delimiter3 + 1, delimiter4);
            long version = parseLong(reconstructed, delimiter4 + 1, delimiter5);
            boolean deleted = parseLong(reconstructed, delimiter5 + 1, delimiter6) != 0;
            events.addOwned(
                    eventStart,
                    java.util.Arrays.copyOfRange(reconstructed, delimiter1 + 1, delimiter2),
                    java.util.Arrays.copyOfRange(reconstructed, delimiter2 + 1, delimiter3),
                    worker,
                    sequence,
                    rowId,
                    version,
                    deleted,
                    java.util.Arrays.copyOfRange(reconstructed, delimiter6 + 1, delimiter7),
                    java.util.Arrays.copyOfRange(reconstructed, delimiter7 + 1, endMarker),
                    tid,
                    logPosition,
                    recordSize,
                    originBackup);
            return true;
        }
        catch (NumberFormatException ignored) {
            return false;
        }
    }

    private static void copy(
            BoundedFileData source,
            long start,
            long end,
            byte[] target,
            int targetStart) throws IOException {
        source.copy(start, end, target, targetStart);
    }

    private static long parseLong(BoundedFileData data, long start, long end)
            throws IOException {
        return data.parseLong(start, end);
    }

    private static long parseLong(byte[] data, int start, int end) {
        if (start >= end) {
            throw new NumberFormatException("empty number");
        }
        int index = start;
        boolean negative = false;
        byte first = data[index];
        if (first == '-' || first == '+') {
            negative = first == '-';
            index++;
        }
        if (index >= end) {
            throw new NumberFormatException("sign only");
        }
        long result = 0;
        while (index < end) {
            int digit = data[index++] - '0';
            if (digit < 0 || digit > 9 || result > (Long.MAX_VALUE - digit) / 10) {
                throw new NumberFormatException("invalid or overflowing number");
            }
            result = result * 10 + digit;
        }
        return negative ? -result : result;
    }

    private static int findPattern(byte[] data, byte[] pattern, int from, int toExclusive) {
        int maximumStart = toExclusive - pattern.length;
        for (int index = from; index <= maximumStart; index++) {
            boolean matches = true;
            for (int offset = 0; offset < pattern.length; offset++) {
                if (data[index + offset] != pattern[offset]) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                return index;
            }
        }
        return -1;
    }

    private static int findByte(byte[] data, byte target, int from, int toExclusive) {
        for (int index = from; index < toExclusive; index++) {
            if (data[index] == target) {
                return index;
            }
        }
        return -1;
    }

    private static long findPattern(
            BoundedFileData data,
            byte[] pattern,
            long from,
            long toExclusive) throws IOException {
        long maximumStart = toExclusive - pattern.length;
        long cursor = from;
        while (cursor <= maximumStart) {
            long found = findByte(data, pattern[0], cursor, maximumStart + 1);
            if (found < 0) {
                return -1;
            }
            boolean matches = true;
            for (int offset = 1; offset < pattern.length; offset++) {
                if (data.get(found + offset) != pattern[offset]) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                return found;
            }
            cursor = found + 1;
        }
        return -1;
    }

    private static long findByte(
            BoundedFileData data,
            byte target,
            long from,
            long toExclusive) throws IOException {
        return data.findByte(target, from, toExclusive);
    }

    private static final class LongPairSet {
        private final long[] first;
        private final long[] second;
        private final byte[] used;
        private final int mask;

        LongPairSet(int expected) {
            int capacity = 16;
            long wanted = Math.max(16L, (long) Math.ceil(expected / 0.60));
            while (capacity < wanted && capacity > 0) {
                capacity <<= 1;
            }
            if (capacity <= 0) {
                throw new IllegalArgumentException("too many event identities");
            }
            first = new long[capacity];
            second = new long[capacity];
            used = new byte[capacity];
            mask = capacity - 1;
        }

        boolean add(long one, long two) {
            int slot = (int) mix64(one * 0x9e3779b97f4a7c15L ^ Long.rotateLeft(two, 29)) & mask;
            while (used[slot] != 0) {
                if (first[slot] == one && second[slot] == two) {
                    return false;
                }
                slot = (slot + 1) & mask;
            }
            used[slot] = 1;
            first[slot] = one;
            second[slot] = two;
            return true;
        }

        private static long mix64(long value) {
            value ^= value >>> 30;
            value *= 0xbf58476d1ce4e5b9L;
            value ^= value >>> 27;
            value *= 0x94d049bb133111ebL;
            return value ^ (value >>> 31);
        }
    }

    private static final class FingerprintWriter {
        private final byte[] runId;
        private final byte[] line = new byte[256];
        private final byte[] digits = new byte[32];

        FingerprintWriter(String runId) {
            this.runId = runId.getBytes(StandardCharsets.US_ASCII);
        }

        int build(BoundedFileData data, EventBatch batch, int index) throws IOException {
            int cursor = 0;
            cursor = copy(runId, cursor);
            line[cursor++] = ':';
            byte[] workerText = batch.ownedWorkerText(index);
            cursor = workerText == null
                    ? copy(data, batch.workerStart(index), batch.workerEnd(index), cursor)
                    : copy(workerText, cursor);
            line[cursor++] = ':';
            byte[] sequenceText = batch.ownedSequenceText(index);
            cursor = sequenceText == null
                    ? copy(data, batch.sequenceStart(index), batch.sequenceEnd(index), cursor)
                    : copy(sequenceText, cursor);
            line[cursor++] = 0;
            cursor = appendLong(batch.tid(index), cursor);
            line[cursor++] = 0;
            cursor = appendLong(batch.logPosition(index), cursor);
            line[cursor++] = 0;
            cursor = appendLong(batch.recordSize(index), cursor);
            line[cursor++] = '\n';
            return cursor;
        }

        String asString(int length) {
            return new String(line, 0, length, StandardCharsets.US_ASCII);
        }

        private int copy(byte[] source, int cursor) {
            System.arraycopy(source, 0, line, cursor, source.length);
            return cursor + source.length;
        }

        private int copy(BoundedFileData source, long start, long end, int cursor)
                throws IOException {
            int length = Math.toIntExact(end - start);
            source.copy(start, end, line, cursor);
            return cursor + length;
        }

        private int appendLong(long value, int cursor) {
            if (value == 0) {
                line[cursor++] = '0';
                return cursor;
            }
            boolean negative = value < 0;
            long current = negative ? value : -value;
            int count = 0;
            while (current != 0) {
                long quotient = current / 10;
                digits[count++] = (byte) ('0' - (current - quotient * 10));
                current = quotient;
            }
            if (negative) {
                line[cursor++] = '-';
            }
            while (count != 0) {
                line[cursor++] = digits[--count];
            }
            return cursor;
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    public static MessageDigest newCorpusDigest() {
        return sha256();
    }

    public static String hex(byte[] bytes) {
        char[] result = new char[bytes.length * 2];
        char[] alphabet = "0123456789abcdef".toCharArray();
        for (int index = 0; index < bytes.length; index++) {
            int value = bytes[index] & 0xff;
            result[index * 2] = alphabet[value >>> 4];
            result[index * 2 + 1] = alphabet[value & 0xf];
        }
        return new String(result);
    }

    private static long align64(long value) {
        return Math.addExact(value, 63L) & ~63L;
    }

    private static long ceilDiv(long value, long divisor) {
        return value / divisor + (value % divisor == 0 ? 0 : 1);
    }

    private static double secondsSince(long started) {
        return (System.nanoTime() - started) / 1_000_000_000.0;
    }

    private static StringBuilder jsonField(StringBuilder json, String name, String value) {
        return json.append('"').append(name).append("\":\"").append(escapeJson(value)).append('"');
    }

    private static StringBuilder nullableJsonField(StringBuilder json, String name, String value) {
        json.append('"').append(name).append("\":");
        return value == null ? json.append("null") : json.append('"').append(escapeJson(value)).append('"');
    }

    private static StringBuilder numberField(StringBuilder json, String name, long value) {
        return json.append('"').append(name).append("\":").append(value);
    }

    private static StringBuilder doubleField(StringBuilder json, String name, double value) {
        return json.append('"').append(name).append("\":").append(String.format(Locale.ROOT, "%.9f", value));
    }

    public static String escapeJson(String value) {
        StringBuilder escaped = new StringBuilder(value.length() + 16);
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (character < 0x20) {
                        escaped.append(String.format(Locale.ROOT, "\\u%04x", (int) character));
                    }
                    else {
                        escaped.append(character);
                    }
                }
            }
        }
        return escaped.toString();
    }
}

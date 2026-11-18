package io.hanacdc.decoder;

import io.hanacdc.decoder.ParallelLogDecoder.DecodedFile;
import io.hanacdc.decoder.ParallelLogDecoder.EventBatch;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Deterministic local-file sink matching the NDJSON event schema. It uses
 * a 1 MiB userspace buffer and forces data after every immutable backup file.
 */
public final class LocalNdjsonSink implements AutoCloseable {
    public static final int BUFFER_BYTES = 1024 * 1024;

    public record PublishMetrics(
            String file,
            long events,
            long bytes,
            double serializeAndWriteSeconds,
            double forceSeconds,
            double publishSeconds) {
    }

    public record Totals(
            Path path,
            long events,
            long bytes,
            double serializeAndWriteSeconds,
            double forceSeconds,
            double publishSeconds) {
    }

    private static final byte[] EVENT_ID = ascii("{\"event_id\":\"");
    private static final byte[] RUN_ID = ascii("\",\"run_id\":\"");
    private static final byte[] WORKER_ID = ascii("\",\"worker_id\":");
    private static final byte[] SEQUENCE = ascii(",\"sequence\":");
    private static final byte[] ROW_ID = ascii(",\"id\":");
    private static final byte[] ROW_VERSION = ascii(",\"row_version\":");
    private static final byte[] DELETED = ascii(",\"is_deleted\":");
    private static final byte[] EVENT_TS = ascii(",\"event_ts\":\"");
    private static final byte[] PAYLOAD = ascii("\",\"payload\":\"");
    private static final byte[] SOURCE_FILE = ascii("\",\"source\":{\"backup_file\":\"");
    private static final byte[] BACKUP_ID = ascii("\",\"backup_id\":");
    private static final byte[] BACKUP_FIRST = ascii(",\"backup_first_position\":");
    private static final byte[] BACKUP_LAST = ascii(",\"backup_last_position\":");
    private static final byte[] LOG_POSITION = ascii(",\"log_position\":");
    private static final byte[] TID = ascii(",\"tid\":");
    private static final byte[] RECORD_SIZE = ascii(",\"record_size\":");
    private static final byte[] CAPTURED = ascii("},\"captured_at_ns\":0}\n");
    private static final byte[] TRUE = ascii("true");
    private static final byte[] FALSE = ascii("false");
    private static final byte[] HEX = ascii("0123456789abcdef");

    private final Path path;
    private final FileOutputStream file;
    private final BufferedOutputStream output;
    private byte[] line = new byte[4096];
    private byte[] sourceField = new byte[4096];
    private final byte[] digits = new byte[32];
    private boolean closed;
    private long totalEvents;
    private long totalBytes;
    private double totalSerializeAndWriteSeconds;
    private double totalForceSeconds;

    public LocalNdjsonSink(Path path) throws IOException {
        this.path = Objects.requireNonNull(path, "path").toAbsolutePath();
        Path parent = this.path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        file = new FileOutputStream(this.path.toFile(), false);
        output = new BufferedOutputStream(file, BUFFER_BYTES);
    }

    public PublishMetrics publish(DecodedFile decoded, String runId) throws IOException {
        ensureOpen();
        Objects.requireNonNull(decoded, "decoded");
        Objects.requireNonNull(runId, "runId");
        BoundedFileData source = decoded.data();
        byte[] encodedRunId = runId.getBytes(StandardCharsets.UTF_8);
        String backupName = decoded.backup().fileName();
        byte[] encodedBackupName = backupName.getBytes(StandardCharsets.UTF_8);
        long eventsBefore = totalEvents;
        long bytesBefore = totalBytes;

        long writeStarted = System.nanoTime();
        for (EventBatch batch : decoded.batches()) {
            for (int index = 0; index < batch.size(); index++) {
                ParallelLogDecoder.BackupFile origin = batch.sourceBackup(index);
                byte[] eventBackupName = origin == null
                        ? encodedBackupName
                        : origin.fileName().getBytes(StandardCharsets.UTF_8);
                int length = buildLine(
                        source, batch, index, encodedRunId, eventBackupName,
                        origin == null ? decoded.backup().backupId() : origin.backupId(),
                        origin == null ? decoded.backup().first() : origin.first(),
                        origin == null ? decoded.backup().last() : origin.last());
                output.write(line, 0, length);
                totalEvents++;
                totalBytes += length;
            }
        }
        output.flush();
        double serializeAndWriteSeconds = elapsed(writeStarted);
        long forceStarted = System.nanoTime();
        file.getChannel().force(false);
        double forceSeconds = elapsed(forceStarted);
        totalSerializeAndWriteSeconds += serializeAndWriteSeconds;
        totalForceSeconds += forceSeconds;
        return new PublishMetrics(
                backupName,
                totalEvents - eventsBefore,
                totalBytes - bytesBefore,
                serializeAndWriteSeconds,
                forceSeconds,
                serializeAndWriteSeconds + forceSeconds);
    }

    public Totals totals() {
        return new Totals(
                path,
                totalEvents,
                totalBytes,
                totalSerializeAndWriteSeconds,
                totalForceSeconds,
                totalSerializeAndWriteSeconds + totalForceSeconds);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            output.close();
        }
    }

    private int buildLine(
            BoundedFileData source,
            EventBatch batch,
            int index,
            byte[] runId,
            byte[] backupName,
            long backupId,
            long backupFirst,
            long backupLast) throws IOException {
        int needed = 768 + runId.length * 2 + backupName.length
                + batch.payloadLength(index);
        ensureLineCapacity(needed);
        int cursor = 0;
        cursor = append(EVENT_ID, cursor);
        cursor = appendEscaped(runId, cursor);
        line[cursor++] = ':';
        cursor = appendEventField(
                source, batch.workerStart(index), batch.workerEnd(index),
                batch.ownedWorkerText(index), cursor);
        line[cursor++] = ':';
        cursor = appendEventField(
                source, batch.sequenceStart(index), batch.sequenceEnd(index),
                batch.ownedSequenceText(index), cursor);
        cursor = append(RUN_ID, cursor);
        cursor = appendEscaped(runId, cursor);
        cursor = append(WORKER_ID, cursor);
        cursor = appendLong(batch.workerId(index), cursor);
        cursor = append(SEQUENCE, cursor);
        cursor = appendLong(batch.sequence(index), cursor);
        cursor = append(ROW_ID, cursor);
        cursor = appendLong(batch.rowId(index), cursor);
        cursor = append(ROW_VERSION, cursor);
        cursor = appendLong(batch.rowVersion(index), cursor);
        cursor = append(DELETED, cursor);
        cursor = append(batch.deleted(index) ? TRUE : FALSE, cursor);
        cursor = append(EVENT_TS, cursor);
        cursor = appendEventField(
                source, batch.eventTsStart(index), batch.eventTsEnd(index),
                batch.ownedEventTs(index), cursor);
        cursor = append(PAYLOAD, cursor);
        cursor = appendEventField(
                source, batch.payloadStart(index), batch.payloadEnd(index),
                batch.ownedPayload(index), cursor);
        cursor = append(SOURCE_FILE, cursor);
        cursor = appendEscaped(backupName, cursor);
        cursor = append(BACKUP_ID, cursor);
        cursor = appendLong(backupId, cursor);
        cursor = append(BACKUP_FIRST, cursor);
        cursor = appendLong(backupFirst, cursor);
        cursor = append(BACKUP_LAST, cursor);
        cursor = appendLong(backupLast, cursor);
        cursor = append(LOG_POSITION, cursor);
        cursor = appendLong(batch.logPosition(index), cursor);
        cursor = append(TID, cursor);
        cursor = appendLong(batch.tid(index), cursor);
        cursor = append(RECORD_SIZE, cursor);
        cursor = appendLong(batch.recordSize(index), cursor);
        return append(CAPTURED, cursor);
    }

    private int append(byte[] value, int cursor) {
        System.arraycopy(value, 0, line, cursor, value.length);
        return cursor + value.length;
    }

    private int appendEscaped(byte[] value, int cursor) {
        for (byte item : value) {
            cursor = appendEscapedByte(item, cursor);
        }
        return cursor;
    }

    private int appendEscaped(
            BoundedFileData value,
            long start,
            long end,
            int cursor) throws IOException {
        int length = Math.toIntExact(end - start);
        ensureSourceFieldCapacity(length);
        value.copy(start, end, sourceField, 0);
        for (int index = 0; index < length; index++) {
            cursor = appendEscapedByte(sourceField[index], cursor);
        }
        return cursor;
    }

    private int appendEventField(
            BoundedFileData source,
            long start,
            long end,
            byte[] owned,
            int cursor) throws IOException {
        return owned == null
                ? appendEscaped(source, start, end, cursor)
                : appendEscaped(owned, cursor);
    }

    private int appendEscapedByte(byte item, int cursor) {
        int unsigned = item & 0xff;
        if (item == '"' || item == '\\') {
            line[cursor++] = '\\';
            line[cursor++] = item;
        }
        else if (unsigned < 0x20) {
            line[cursor++] = '\\';
            line[cursor++] = 'u';
            line[cursor++] = '0';
            line[cursor++] = '0';
            line[cursor++] = HEX[unsigned >>> 4];
            line[cursor++] = HEX[unsigned & 0xf];
        }
        else {
            line[cursor++] = item;
        }
        return cursor;
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
        while (count > 0) {
            line[cursor++] = digits[--count];
        }
        return cursor;
    }

    private void ensureLineCapacity(int wanted) {
        if (wanted <= line.length) {
            return;
        }
        int capacity = line.length;
        while (capacity < wanted) {
            capacity = Math.multiplyExact(capacity, 2);
        }
        line = java.util.Arrays.copyOf(line, capacity);
    }

    private void ensureSourceFieldCapacity(int wanted) {
        if (wanted <= sourceField.length) {
            return;
        }
        int capacity = sourceField.length;
        while (capacity < wanted) {
            capacity = Math.multiplyExact(capacity, 2);
        }
        sourceField = java.util.Arrays.copyOf(sourceField, capacity);
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("local NDJSON sink is closed");
        }
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static double elapsed(long started) {
        return (System.nanoTime() - started) / 1_000_000_000.0;
    }
}

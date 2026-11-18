package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;

import java.io.IOException;
import java.util.Objects;

/** Bounded production entry point for one immutable native log backup. */
public final class NativeBackupDecoder {
    public record CursorMetrics(
            long records,
            long payloadBytes,
            long recordScanWaitNanos,
            long payloadReadNanos,
            long recordBuildNanos) {}

    public static final class NativeRecord {
        private final NativeRecordHeader header;
        private final byte[] payload;

        /** Public boundary keeps defensive-copy semantics for callers. */
        public NativeRecord(NativeRecordHeader header, byte[] payload) {
            this(header, payload, false);
        }

        private NativeRecord(
                NativeRecordHeader header, byte[] payload, boolean takeOwnership) {
            this.header = Objects.requireNonNull(header, "header");
            Objects.requireNonNull(payload, "payload");
            this.payload = takeOwnership ? payload : payload.clone();
        }

        static NativeRecord takeOwnership(NativeRecordHeader header, byte[] payload) {
            return new NativeRecord(header, payload, true);
        }

        public NativeRecordHeader header() {
            return header;
        }

        public byte[] payload() {
            return payload.clone();
        }

        /** Trusted decoder-only view; the returned array must never be mutated. */
        byte[] payloadView() {
            return payload;
        }
    }

    public static final class Cursor implements AutoCloseable {
        private final NativeBackupHeaderValidator.Metadata metadata;
        private final NativeRecordCursor records;
        private long recordCount;
        private long payloadBytes;
        private long recordScanWaitNanos;
        private long payloadReadNanos;
        private long recordBuildNanos;

        private Cursor(
                NativeBackupHeaderValidator.Metadata metadata,
                NativeRecordCursor records) {
            this.metadata = metadata;
            this.records = records;
        }

        public NativeBackupHeaderValidator.Metadata metadata() {
            return metadata;
        }

        public boolean hasNext() throws IOException {
            long started = System.nanoTime();
            try {
                return records.hasNext();
            }
            finally {
                recordScanWaitNanos += System.nanoTime() - started;
            }
        }

        public NativeRecord next() throws IOException {
            long scanStarted = System.nanoTime();
            NativeRecordHeader header = records.next();
            recordScanWaitNanos += System.nanoTime() - scanStarted;

            long payloadStarted = System.nanoTime();
            byte[] payload = records.readPayload(header);
            payloadReadNanos += System.nanoTime() - payloadStarted;

            long buildStarted = System.nanoTime();
            NativeRecord result = NativeRecord.takeOwnership(header, payload);
            recordBuildNanos += System.nanoTime() - buildStarted;
            recordCount++;
            payloadBytes += payload.length;
            return result;
        }

        public CursorMetrics metrics() {
            return new CursorMetrics(recordCount, payloadBytes, recordScanWaitNanos,
                    payloadReadNanos, recordBuildNanos);
        }

        @Override
        public void close() throws IOException {
            records.close();
        }
    }

    private final NativeBackupHeaderValidator headerValidator;
    public NativeBackupDecoder() {
        this(new NativeBackupHeaderValidator());
    }

    NativeBackupDecoder(NativeBackupHeaderValidator headerValidator) {
        this.headerValidator = Objects.requireNonNull(headerValidator, "headerValidator");
    }

    public Cursor open(BackupFile backup) throws IOException {
        NativeBackupHeaderValidator.Metadata metadata = headerValidator.validate(backup);
        NativeRecordCursor records = NativeRecordReader.open(backup);
        return new Cursor(metadata, records);
    }
}

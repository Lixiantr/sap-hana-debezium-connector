package io.hanacdc.decoder;

import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Strict reader for the outer framing of native HANA redo records.
 *
 * <p>The 32-byte outer header is followed by {@code payloadSize} bytes. HANA
 * stores the complete record in 64-byte log-position units. This differs from
 * the old HCDC1 benchmark scanner, whose synthetic fixture treated the size
 * field as the complete record size.</p>
 */
public final class NativeRecordReader {
    public static final int FILE_HEADER_BYTES = 4096;
    public static final int OUTER_HEADER_BYTES = 32;
    public static final int LOG_POSITION_BYTES = 64;
    public static final int MAX_PAYLOAD_BYTES = 8 * 1024 * 1024;

    public static final int OUTER_UNIFIED_TABLE = 0x05000002;
    /** Characterized wrapper used by HANA for a multi-row UNIFIED_TABLE payload. */
    public static final int OUTER_UNIFIED_TABLE_MASS = 0x05100002;
    public static final int OUTER_CONTAINER_DIRECTORY = 0x03000002;
    public static final int OUTER_VIRTUAL_FILE = 0x02800002;
    public static final int OUTER_UNIFIED_TABLE_INFO = 0xfb800002;
    public static final int OUTER_LOB_WRITE = 0x05c00002;
    public static final int OUTER_PREPARE_COMMIT = 0xfdc00002;
    public static final int OUTER_COMMIT = 0xfc000002;
    public static final int OUTER_PARTIAL_ROLLBACK = 0xfc800002;
    public static final int OUTER_ROLLBACK = 0xfe000002;

    public static final int UT_INSERT_DATA = 0x011e;
    public static final int UT_INSERT_MVCC = 0x011f;
    public static final int UT_DELETE = 0x0120;
    public static final int UT_UPSERT = 0x0121;
    public static final int UT_UPDATE = 0x0122;
    public static final int UT_COLUMN_CONVERSION = 0x0009;
    public static final int UT_ALTER_COLUMN_DEFAULT = 0x000c;
    public static final int UT_RUNTIME_DATA = 0x0028;
    public static final int UT_ADD_ALTER_COLUMN = 0x010a;
    public static final int UT_DDL_START = 0x0225;
    public static final int UT_CREATE_ROOT_OBJECT = 0x040f;
    public static final int UT_PREPARE_MOVE_OR_DELETE_CONTAINER = 0x0417;

    public static final int CD_CREATE_LOB_ENTRY = 0x1200;
    public static final int CD_SET_CHARACTER_SIZE = 0x1f06;
    public static final int CD_SET_BINARY_SIZE = 0x1f0c;
    public static final int CD_SET_LOB_OWNER_ID = 0x1f0d;

    public static final int VF_APPEND = 0x0020;
    public static final int VF_OVERWRITE = 0x0021;
    public static final int VF_TRUNCATE = 0x0035;

    public static final int INFO_DDL_END = 0x0026;

    private NativeRecordReader() {}

    /** Streaming cursor retaining only the current bounded input window. */
    public static final class RecordCursor implements NativeRecordCursor {
        private final BackupFile backup;
        private final BoundedFileData data;
        private final long endPosition;
        private long position;
        private NativeRecordHeader pending;
        private boolean closed;

        private RecordCursor(BackupFile backup, long startPosition, long endPosition)
                throws IOException {
            this.backup = Objects.requireNonNull(backup, "backup");
            if (startPosition < backup.first()
                    || endPosition > backup.last()
                    || startPosition > endPosition) {
                throw new IllegalArgumentException("native record range is outside backup: ["
                        + startPosition + "," + endPosition + ") backup=["
                        + backup.first() + "," + backup.last() + ")");
            }
            data = new BoundedFileData(backup.path());
            position = startPosition;
            this.endPosition = endPosition;
            long required = fileOffset(backup, backup.last());
            if (data.limit() < required) {
                close();
                throw new IOException("FATAL_BACKUP_SIZE: native data ends before redo range: required="
                        + required + " actual=" + data.limit());
            }
        }

        public boolean hasNext() throws IOException {
            ensureOpen();
            if (pending == null) {
                pending = findNext();
            }
            return pending != null;
        }

        public NativeRecordHeader next() throws IOException {
            if (!hasNext()) {
                throw new NoSuchElementException("native backup cursor is exhausted");
            }
            NativeRecordHeader result = pending;
            pending = null;
            return result;
        }

        public byte[] readPayload(NativeRecordHeader record) throws IOException {
            ensureOpen();
            if (!backup.equals(record.backup())) {
                throw new IllegalArgumentException("record belongs to another backup");
            }
            byte[] payload = new byte[record.payloadSize()];
            data.copy(record.fileOffset() + OUTER_HEADER_BYTES,
                    record.fileOffset() + OUTER_HEADER_BYTES + record.payloadSize(),
                    payload, 0);
            return payload;
        }

        private NativeRecordHeader findNext() throws IOException {
            while (position < endPosition) {
                long fileOffset = fileOffset(backup, position);
                if (fileOffset + OUTER_HEADER_BYTES > data.limit()) {
                    throw new IOException("backup ends inside native outer header at position "
                            + position);
                }
                int payloadSize = data.getInt(fileOffset + 4);
                long unsignedPayloadSize = Integer.toUnsignedLong(payloadSize);
                long encodedPosition = data.getLong(fileOffset + 16);
                long tid = data.getLong(fileOffset + 8);
                if (encodedPosition != position || tid <= 0) {
                    position++;
                    continue;
                }
                if (unsignedPayloadSize > MAX_PAYLOAD_BYTES) {
                    throw corruption("impossible payload length=" + unsignedPayloadSize,
                            position);
                }
                long totalBytes = Math.addExact(OUTER_HEADER_BYTES, unsignedPayloadSize);
                long allocationBytes = align64(totalBytes);
                long logicalEnd = fileOffset(backup, backup.last());
                if (fileOffset > logicalEnd - totalBytes) {
                    throw corruption("native record crosses redo data boundary", position);
                }
                int outerCode = data.getInt(fileOffset);
                long poid = -1;
                long containerId = -1;
                int operationCode = -1;
                long firstRowId = -1;
                if ((outerCode == OUTER_UNIFIED_TABLE
                        || outerCode == OUTER_CONTAINER_DIRECTORY
                        || outerCode == OUTER_VIRTUAL_FILE
                        || outerCode == OUTER_UNIFIED_TABLE_INFO)
                        && unsignedPayloadSize >= 24) {
                    poid = Integer.toUnsignedLong(data.getInt(fileOffset + OUTER_HEADER_BYTES + 4));
                    containerId = data.getLong(fileOffset + OUTER_HEADER_BYTES + 8);
                    operationCode = data.getInt(fileOffset + OUTER_HEADER_BYTES + 16);
                    if (unsignedPayloadSize >= 32
                            && (operationCode == UT_INSERT_DATA
                            || operationCode == UT_INSERT_MVCC)) {
                        firstRowId = data.getLong(fileOffset + OUTER_HEADER_BYTES + 24);
                    }
                    else if (unsignedPayloadSize >= 40
                            && (operationCode == UT_DELETE
                            || operationCode == UT_UPSERT
                            || operationCode == UT_UPDATE)) {
                        firstRowId = data.getLong(fileOffset + OUTER_HEADER_BYTES + 32);
                    }
                }
                NativeRecordHeader result = new NativeRecordHeader(
                        backup, fileOffset, position, tid, outerCode, payloadSize,
                        allocationBytes, poid, containerId, operationCode, firstRowId);
                position = Math.addExact(position, allocationBytes / LOG_POSITION_BYTES);
                return result;
            }
            return null;
        }

        long scanPosition() {
            return position;
        }

        private void ensureOpen() throws IOException {
            if (closed) {
                throw new IOException("native backup cursor is closed");
            }
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                data.close();
            }
        }
    }

    public record NativeRecordHeader(
            BackupFile backup,
            long fileOffset,
            long position,
            long tid,
            int outerCode,
            int payloadSize,
            long allocationBytes,
            long poid,
            long containerId,
            int operationCode,
            long firstRowId) {

        public String outerType() {
            return switch (outerCode) {
                case OUTER_UNIFIED_TABLE -> "UNIFIED_TABLE";
                case OUTER_UNIFIED_TABLE_MASS -> "UNIFIED_TABLE_MASS";
                case OUTER_CONTAINER_DIRECTORY -> "CONTAINER_DIRECTORY";
                case OUTER_VIRTUAL_FILE -> "VIRTUAL_FILE";
                case OUTER_UNIFIED_TABLE_INFO -> "UNIFIED_TABLE_INFO";
                case OUTER_LOB_WRITE -> "LOB_WRITE";
                case OUTER_PREPARE_COMMIT -> "PREPARE_COMMIT";
                case OUTER_COMMIT -> "COMMIT";
                case OUTER_PARTIAL_ROLLBACK -> "PARTIAL_ROLLBACK";
                case OUTER_ROLLBACK -> "ROLLBACK";
                default -> String.format(Locale.ROOT, "UNKNOWN_0x%08x", outerCode);
            };
        }

        public String operationType() {
            if (operationCode == -1) {
                return "";
            }
            if (outerCode == OUTER_UNIFIED_TABLE) {
                return switch (operationCode) {
                    case UT_INSERT_DATA -> "INSERT_DATA";
                    case UT_INSERT_MVCC -> "INSERT_MVCC";
                    case UT_DELETE -> "DELETE";
                    case UT_UPSERT -> "UPSERT";
                    case UT_UPDATE -> "UPDATE";
                    case UT_COLUMN_CONVERSION -> "COLUMN_CONVERSION";
                    case UT_ALTER_COLUMN_DEFAULT -> "ALTER_COLUMN_DEFAULT";
                    case UT_RUNTIME_DATA -> "RUNTIME_DATA";
                    case UT_ADD_ALTER_COLUMN -> "ADD_ALTER_COLUMN";
                    case UT_DDL_START -> "DDL_START";
                    case UT_CREATE_ROOT_OBJECT -> "CREATE_ROOT_OBJECT";
                    case UT_PREPARE_MOVE_OR_DELETE_CONTAINER ->
                            "PREPARE_MOVE_OR_DELETE_CONTAINER";
                    default -> unknownOperation();
                };
            }
            if (outerCode == OUTER_CONTAINER_DIRECTORY) {
                return switch (operationCode) {
                    case CD_CREATE_LOB_ENTRY -> "CREATE_LOB_ENTRY";
                    case CD_SET_CHARACTER_SIZE -> "SET_CHARACTER_SIZE";
                    case CD_SET_BINARY_SIZE -> "SET_BINARY_SIZE";
                    case CD_SET_LOB_OWNER_ID -> "SET_LOB_OWNER_ID";
                    default -> unknownOperation();
                };
            }
            if (outerCode == OUTER_VIRTUAL_FILE) {
                return switch (operationCode) {
                    case VF_APPEND -> "VIRTUAL_FILE_APPEND";
                    case VF_OVERWRITE -> "VIRTUAL_FILE_OVERWRITE";
                    case VF_TRUNCATE -> "VIRTUAL_FILE_TRUNCATE";
                    default -> unknownOperation();
                };
            }
            if (outerCode == OUTER_UNIFIED_TABLE_INFO && operationCode == INFO_DDL_END) {
                return "DDL_END";
            }
            return unknownOperation();
        }

        private String unknownOperation() {
            return String.format(Locale.ROOT, "UNKNOWN_0x%08x", operationCode);
        }

        public String containerHex() {
            return containerId < 0 ? "" : String.format(Locale.ROOT, "0x%016x", containerId);
        }
    }

    public static List<NativeRecordHeader> readHeaders(BackupFile backup) throws IOException {
        List<NativeRecordHeader> records = new ArrayList<>();
        try (RecordCursor cursor = open(backup)) {
            while (cursor.hasNext()) {
                records.add(cursor.next());
            }
        }
        return List.copyOf(records);
    }

    public static RecordCursor open(BackupFile backup) throws IOException {
        return new RecordCursor(backup, backup.first(), backup.last());
    }

    /** Opens a scanner for start positions in the half-open redo range. */
    public static RecordCursor openRange(
            BackupFile backup, long startPosition, long endPosition) throws IOException {
        return new RecordCursor(backup, startPosition, endPosition);
    }

    public static byte[] readPayload(NativeRecordHeader record) throws IOException {
        byte[] payload = new byte[record.payloadSize()];
        try (BoundedFileData data = new BoundedFileData(record.backup().path())) {
            data.copy(
                    record.fileOffset() + OUTER_HEADER_BYTES,
                    record.fileOffset() + OUTER_HEADER_BYTES + record.payloadSize(),
                    payload,
                    0);
        }
        return payload;
    }

    static long fileOffset(BackupFile backup, long position) {
        return Math.addExact(FILE_HEADER_BYTES,
                Math.multiplyExact(Math.subtractExact(position, backup.first()), LOG_POSITION_BYTES));
    }

    static long align64(long value) {
        return Math.multiplyExact((value + LOG_POSITION_BYTES - 1) / LOG_POSITION_BYTES,
                LOG_POSITION_BYTES);
    }

    private static IOException corruption(String detail, long position) {
        return new IOException("FATAL_CORRUPTION: " + detail + " at redo position " + position);
    }
}

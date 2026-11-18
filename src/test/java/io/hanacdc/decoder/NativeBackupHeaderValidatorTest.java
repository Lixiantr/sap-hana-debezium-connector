package io.hanacdc.decoder;

import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NativeBackupHeaderValidatorTest {
    @TempDir
    Path temporary;

    @Test
    void validatesCharacterizedHeaderAndTrailer() throws Exception {
        BackupFile backup = writeBackup("[CLEARTEXT]", "[none]");

        NativeBackupHeaderValidator.Metadata metadata =
                new NativeBackupHeaderValidator().validate(backup);

        assertEquals("f957a4ce-6279-f244-b23b-9b865704aec1", metadata.databaseId());
        assertEquals(1785840699384L, metadata.backupTimestamp());
        assertTrue(metadata.destination().contains(backup.path().getFileName().toString()));
        assertEquals(4L * 64, metadata.logicalDataBytes());
        assertEquals(Files.size(backup.path()), metadata.physicalBytes());
    }

    @Test
    void productionCursorStreamsValidatedNativeRecords() throws Exception {
        BackupFile backup = writeBackup("[CLEARTEXT]", "[none]");

        try (NativeBackupDecoder.Cursor cursor = new NativeBackupDecoder().open(backup)) {
            assertTrue(cursor.hasNext());
            NativeBackupDecoder.NativeRecord record = cursor.next();
            assertEquals("COMMIT", record.header().outerType());
            assertEquals(32, record.payload().length);
            byte[] original = record.payload();
            byte[] exposed = record.payload();
            exposed[0] ^= 0x7f;
            assertArrayEquals(original, record.payload());
            assertEquals(false, cursor.hasNext());
            var metrics = cursor.metrics();
            assertEquals(1, metrics.records());
            assertEquals(32, metrics.payloadBytes());
            assertTrue(metrics.recordScanWaitNanos() >= 0);
            assertTrue(metrics.payloadReadNanos() >= 0);
        }
    }

    @Test
    void rejectsEncryptionAndPhysicalSizeMismatch() throws Exception {
        BackupFile encrypted = writeBackup("[AES256]", "[none]");
        IOException encryption = assertThrows(IOException.class,
                () -> new NativeBackupHeaderValidator().validate(encrypted));
        assertTrue(encryption.getMessage().contains("FATAL_BACKUP_HEADER"));

        byte[] truncated = Files.readAllBytes(encrypted.path());
        Files.write(encrypted.path(), java.util.Arrays.copyOf(truncated, truncated.length - 1));
        IOException size = assertThrows(IOException.class,
                () -> new NativeBackupHeaderValidator().validate(encrypted));
        assertTrue(size.getMessage().contains("physical size mismatch"));
    }

    @Test
    void validatesAndReadsSparseBackupLargerThanTwoGiBWithLongOffsets() throws Exception {
        long first = 500;
        long positions = 33_554_433L;
        long last = first + positions;
        Path path = temporary.resolve("log_backup_2_0_500_33554933.1785840699382");
        long physicalBytes = NativeBackupHeaderValidator.FILE_HEADER_BYTES
                + positions * NativeRecordReader.LOG_POSITION_BYTES
                + NativeBackupHeaderValidator.FILE_TRAILER_BYTES;

        ByteBuffer header = ByteBuffer.allocate(NativeBackupHeaderValidator.FILE_HEADER_BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);
        putTag(header, 16, "[MAGIC]", "HANABackup - designed by SAP in Berlin");
        putTag(header, 128, "[DBID]", "f957a4ce-6279-f244-b23b-9b865704aec1");
        putTag(header, 256, "[DATE]", "1785840699384");
        putRaw(header, 384, "[FILE][/hana/backup/" + path.getFileName() + "]");
        ByteBuffer trailer = ByteBuffer.allocate(NativeBackupHeaderValidator.FILE_TRAILER_BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);
        putTag(trailer, 16, "[ENCRYPTION_ALGO]", "[CLEARTEXT]");
        putTag(trailer, 128, "[COMPRESSION_ALGO]", "[none]");
        long recordPosition = last - 2;
        ByteBuffer record = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);
        putOuter(record, 0, NativeRecordReader.OUTER_COMMIT, 32, 99, recordPosition);

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            writeFully(channel, header, 0);
            writeFully(channel, record, NativeRecordReader.fileOffset(
                    new BackupFile(path, 2, 0, first, last, 1785840699382L), recordPosition));
            writeFully(channel, trailer,
                    physicalBytes - NativeBackupHeaderValidator.FILE_TRAILER_BYTES);
        }

        BackupFile backup = BackupFile.parse(path).orElseThrow();
        var metadata = new NativeBackupHeaderValidator().validate(backup);
        assertEquals(physicalBytes, metadata.physicalBytes());
        assertTrue(metadata.physicalBytes() > Integer.MAX_VALUE);
        try (var cursor = NativeRecordReader.openRange(backup, recordPosition, last)) {
            assertTrue(cursor.hasNext());
            assertEquals(recordPosition, cursor.next().position());
            assertEquals(false, cursor.hasNext());
        }
    }

    @Test
    void rejectsRecordCrossingFromRedoDataIntoTrailer() throws Exception {
        BackupFile backup = writeBackup("[CLEARTEXT]", "[none]");
        long position = backup.last() - 1;
        ByteBuffer record = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);
        putOuter(record, 0, NativeRecordReader.OUTER_COMMIT, 64, 77, position);
        try (FileChannel channel = FileChannel.open(backup.path(), StandardOpenOption.WRITE)) {
            writeFully(channel, record, NativeRecordReader.fileOffset(backup, position));
        }
        try (var cursor = new NativeBackupDecoder().open(backup)) {
            assertTrue(cursor.hasNext());
            cursor.next();
            IOException error = assertThrows(IOException.class, cursor::hasNext);
            assertTrue(error.getMessage().contains("FATAL_CORRUPTION"));
        }
    }

    private BackupFile writeBackup(String encryption, String compression) throws IOException {
        long first = 100;
        long last = 104;
        Path path = temporary.resolve("log_backup_2_0_100_104.1785840699381");
        int bytes = NativeBackupHeaderValidator.FILE_HEADER_BYTES
                + Math.toIntExact((last - first) * NativeRecordReader.LOG_POSITION_BYTES)
                + NativeBackupHeaderValidator.FILE_TRAILER_BYTES;
        ByteBuffer file = ByteBuffer.allocate(bytes).order(ByteOrder.LITTLE_ENDIAN);
        putTag(file, 16, "[MAGIC]", "HANABackup - designed by SAP in Berlin");
        putTag(file, 128, "[DBID]", "f957a4ce-6279-f244-b23b-9b865704aec1");
        putTag(file, 256, "[DATE]", "1785840699384");
        putRaw(file, 384, "[FILE][/hana/backup/" + path.getFileName() + "]");

        int trailer = bytes - NativeBackupHeaderValidator.FILE_TRAILER_BYTES;
        putTag(file, trailer + 16, "[ENCRYPTION_ALGO]", encryption);
        putTag(file, trailer + 128, "[COMPRESSION_ALGO]", compression);

        int record = NativeBackupHeaderValidator.FILE_HEADER_BYTES;
        file.putInt(record, NativeRecordReader.OUTER_COMMIT);
        file.putInt(record + 4, 32);
        file.putLong(record + 8, 71);
        file.putLong(record + 16, first);
        Files.write(path, file.array());
        return BackupFile.parse(path).orElseThrow();
    }

    private static void putTag(ByteBuffer target, int offset, String tag, String value) {
        byte[] name = tag.getBytes(StandardCharsets.US_ASCII);
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        int cursor = offset;
        for (byte item : name) {
            target.put(cursor++, item);
        }
        target.putInt(cursor, encoded.length);
        cursor += Integer.BYTES;
        for (byte item : encoded) {
            target.put(cursor++, item);
        }
    }

    private static void putRaw(ByteBuffer target, int offset, String value) {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        for (int index = 0; index < encoded.length; index++) {
            target.put(offset + index, encoded[index]);
        }
    }

    private static void putOuter(
            ByteBuffer target, int offset, int outerCode, int payloadSize, long tid, long position) {
        target.putInt(offset, outerCode);
        target.putInt(offset + 4, payloadSize);
        target.putLong(offset + 8, tid);
        target.putLong(offset + 16, position);
    }

    private static void writeFully(FileChannel channel, ByteBuffer value, long position)
            throws IOException {
        long cursor = position;
        while (value.hasRemaining()) {
            int written = channel.write(value, cursor);
            if (written == 0) {
                Thread.onSpinWait();
                continue;
            }
            cursor += written;
        }
    }
}

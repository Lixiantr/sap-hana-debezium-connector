package io.hanacdc.testutil;

import io.hanacdc.decoder.ParallelLogDecoder;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public final class TestLogFixture {
    public static final long FIRST = 100;
    public static final long LAST = 108;
    public static final long BACKUP_ID = 1;

    private TestLogFixture() {}

    public static BackupFile write(Path directory, String runId) throws IOException {
        Path path = directory.resolve("log_backup_2_0_" + FIRST + "_" + LAST + "." + BACKUP_ID);
        ByteBuffer data = ByteBuffer.allocate(8192).order(ByteOrder.LITTLE_ENDIAN);
        putRecord(data, 4096, 256, 7, 100,
                envelope(runId, "0001", "00000000000000000010",
                        "00000000000000000042", "00000000000000000003", false,
                        "2026-08-03 12:34:56.123456", "payload-one"));
        putRecord(data, 4352, 256, 8, 104,
                envelope(runId, "0002", "00000000000000000011",
                        "00000000000000000043", "00000000000000000004", true,
                        "2026-08-03 12:34:57.654321", "payload-two"));
        Files.write(path, data.array());
        return BackupFile.parse(path).orElseThrow();
    }

    public static BackupFile writeFragmented(Path directory, String runId) throws IOException {
        long last = 112;
        Path path = directory.resolve("log_backup_2_0_" + FIRST + "_" + last + ".2");
        ByteBuffer data = ByteBuffer.allocate(8192).order(ByteOrder.LITTLE_ENDIAN);
        int firstOffset = 4096 + 2 * 64;
        int interleavedOffset = 4096 + 5 * 64;
        int continuationOffset = 4096 + 7 * 64;
        putHeader(data, firstOffset, 192, 7, 102);
        putHeader(data, interleavedOffset, 128, 8, 105);
        putHeader(data, continuationOffset, 256, 7, 107);

        byte[] encoded = envelope(
                runId, "0001", "00000000000000000010",
                "00000000000000000042", "00000000000000000003", false,
                "2026-08-03 12:34:56.123456",
                "payload-fragment-crosses-record-boundary")
                .getBytes(StandardCharsets.US_ASCII);
        int eventOffset = firstOffset + 150;
        int initialLength = firstOffset + 192 - eventOffset;
        for (int index = 0; index < initialLength; index++) {
            data.put(eventOffset + index, encoded[index]);
        }
        int continuationStart = continuationOffset
                + ParallelLogDecoder.CONTINUATION_HEADER_BYTES;
        for (int index = initialLength; index < encoded.length; index++) {
            data.put(continuationStart + index - initialLength, encoded[index]);
        }
        Files.write(path, data.array());
        return BackupFile.parse(path).orElseThrow();
    }

    public static List<BackupFile> writeAcrossBackups(Path directory, String runId)
            throws IOException {
        long boundary = 104;
        Path firstPath = directory.resolve("log_backup_2_0_100_104.10");
        Path secondPath = directory.resolve("log_backup_2_0_104_108.11");
        ByteBuffer first = ByteBuffer.allocate(8192).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer second = ByteBuffer.allocate(8192).order(ByteOrder.LITTLE_ENDIAN);
        int firstRecord = 4096 + 3 * 64;
        int secondRecord = 4096;
        putHeader(first, firstRecord, 192, 77, 103);
        putHeader(second, secondRecord, 256, 77, boundary);

        byte[] encoded = envelope(
                runId, "0009", "00000000000000000099",
                "00000000000000000123", "00000000000000000005", false,
                "2026-08-03 12:34:59.123456",
                "payload-fragment-crosses-backup-boundary")
                .getBytes(StandardCharsets.US_ASCII);
        int eventOffset = firstRecord + 150;
        int firstLength = firstRecord + 192 - eventOffset;
        for (int index = 0; index < firstLength; index++) {
            first.put(eventOffset + index, encoded[index]);
        }
        int continuationStart = secondRecord + ParallelLogDecoder.CONTINUATION_HEADER_BYTES;
        for (int index = firstLength; index < encoded.length; index++) {
            second.put(continuationStart + index - firstLength, encoded[index]);
        }
        Files.write(firstPath, first.array());
        Files.write(secondPath, second.array());
        return List.of(
                BackupFile.parse(firstPath).orElseThrow(),
                BackupFile.parse(secondPath).orElseThrow());
    }

    public static List<BackupFile> writeAcrossBackupsWithFollowingEvents(
            Path directory,
            String runId) throws IOException {
        long boundary = 104;
        long last = 116;
        Path firstPath = directory.resolve("log_backup_2_0_100_104.20");
        Path secondPath = directory.resolve("log_backup_2_0_104_116.21");
        ByteBuffer first = ByteBuffer.allocate(8192).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer second = ByteBuffer.allocate(8192).order(ByteOrder.LITTLE_ENDIAN);
        int firstRecord = 4096 + 3 * 64;
        int continuationRecord = 4096;
        putHeader(first, firstRecord, 192, 77, 103);
        putHeader(second, continuationRecord, 256, 77, boundary);

        byte[] encoded = envelope(
                runId, "0009", "00000000000000000099",
                "00000000000000000123", "00000000000000000005", false,
                "2026-08-03 12:34:59.123456",
                "payload-fragment-crosses-backup-boundary")
                .getBytes(StandardCharsets.US_ASCII);
        int eventOffset = firstRecord + 150;
        int firstLength = firstRecord + 192 - eventOffset;
        for (int index = 0; index < firstLength; index++) {
            first.put(eventOffset + index, encoded[index]);
        }
        int continuationStart = continuationRecord
                + ParallelLogDecoder.CONTINUATION_HEADER_BYTES;
        for (int index = firstLength; index < encoded.length; index++) {
            second.put(continuationStart + index - firstLength, encoded[index]);
        }
        putRecord(second, 4096 + 4 * 64, 256, 78, 108,
                envelope(runId, "0010", "00000000000000000100",
                        "00000000000000000124", "00000000000000000006", false,
                        "2026-08-03 12:35:00.123456", "payload-after-boundary-one"));
        putRecord(second, 4096 + 8 * 64, 256, 79, 112,
                envelope(runId, "0011", "00000000000000000101",
                        "00000000000000000125", "00000000000000000007", false,
                        "2026-08-03 12:35:01.123456", "payload-after-boundary-two"));
        Files.write(firstPath, first.array());
        Files.write(secondPath, second.array());
        return List.of(
                BackupFile.parse(firstPath).orElseThrow(),
                BackupFile.parse(secondPath).orElseThrow());
    }

    public static String expectedFingerprint(String runId) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
        digest.update((runId + ":0001:00000000000000000010" + '\0'
                + "7" + '\0' + "100" + '\0' + "256\n")
                .getBytes(StandardCharsets.US_ASCII));
        digest.update((runId + ":0002:00000000000000000011" + '\0'
                + "8" + '\0' + "104" + '\0' + "256\n")
                .getBytes(StandardCharsets.US_ASCII));
        return ParallelLogDecoder.hex(digest.digest());
    }

    private static void putRecord(
            ByteBuffer data,
            int offset,
            int size,
            long tid,
            long position,
            String envelope) {
        putHeader(data, offset, size, tid, position);
        byte[] encoded = envelope.getBytes(StandardCharsets.US_ASCII);
        if (encoded.length > size - 32) {
            throw new IllegalArgumentException("test envelope does not fit in record");
        }
        int envelopeOffset = offset + 32;
        for (int index = 0; index < encoded.length; index++) {
            data.put(envelopeOffset + index, encoded[index]);
        }
    }

    private static void putHeader(
            ByteBuffer data,
            int offset,
            int size,
            long tid,
            long position) {
        data.putInt(offset + 4, size);
        data.putLong(offset + 8, tid);
        data.putLong(offset + 16, position);
    }

    private static String envelope(
            String runId,
            String worker,
            String sequence,
            String rowId,
            String version,
            boolean deleted,
            String timestamp,
            String payload) {
        return "HCDC1|" + runId + '|' + worker + '|' + sequence + '|' + rowId + '|'
                + version + '|' + (deleted ? '1' : '0') + '|' + timestamp + '|'
                + payload + "|END";
    }
}

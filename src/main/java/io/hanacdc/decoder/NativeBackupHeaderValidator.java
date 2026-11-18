package io.hanacdc.decoder;

import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/** Strict validation of characterized native HANA log-backup headers and trailers. */
public final class NativeBackupHeaderValidator {
    public static final int FILE_HEADER_BYTES = 4096;
    public static final int FILE_TRAILER_BYTES = 8192;
    private static final String EXPECTED_MAGIC = "HANABackup - designed by SAP in Berlin";
    private static final String CLEAR_TEXT = "[CLEARTEXT]";
    private static final String NO_COMPRESSION = "[none]";

    public record Metadata(
            String databaseId,
            long backupTimestamp,
            String destination,
            String encryption,
            String compression,
            long logicalDataBytes,
            long physicalBytes) {}

    public Metadata validate(BackupFile backup) throws IOException {
        Objects.requireNonNull(backup, "backup");
        if (backup.first() < 0 || backup.last() <= backup.first()) {
            throw fatal("invalid redo range " + backup.first() + ".." + backup.last());
        }
        long logicalBytes;
        long expectedBytes;
        try {
            logicalBytes = Math.multiplyExact(
                    Math.subtractExact(backup.last(), backup.first()),
                    NativeRecordReader.LOG_POSITION_BYTES);
            expectedBytes = Math.addExact(
                    Math.addExact(FILE_HEADER_BYTES, logicalBytes), FILE_TRAILER_BYTES);
        }
        catch (ArithmeticException error) {
            throw fatal("backup size overflows 64-bit range", error);
        }

        try (BoundedFileData data = new BoundedFileData(backup.path())) {
            if (data.limit() != expectedBytes) {
                throw fatal("physical size mismatch: expected=" + expectedBytes
                        + " actual=" + data.limit());
            }
            byte[] header = bytes(data, 0, FILE_HEADER_BYTES);
            byte[] trailer = bytes(data, data.limit() - FILE_TRAILER_BYTES, data.limit());

            String magic = taggedValue(header, "[MAGIC]");
            if (!EXPECTED_MAGIC.equals(magic)) {
                throw fatal("unknown backup magic: " + printable(magic));
            }
            String databaseId = required(taggedValue(header, "[DBID]"), "DBID");
            String timestampText = required(taggedValue(header, "[DATE]"), "DATE");
            long timestamp;
            try {
                timestamp = Long.parseLong(timestampText);
            }
            catch (NumberFormatException error) {
                throw fatal("invalid backup DATE: " + printable(timestampText), error);
            }
            String destination = required(bracketedValueAfterTag(header, "[FILE]"), "FILE");
            if (!destination.contains(backup.fileName())) {
                throw fatal("header destination does not name catalogued object");
            }

            String encryption = required(taggedValue(trailer, "[ENCRYPTION_ALGO]"),
                    "ENCRYPTION_ALGO");
            String compression = required(taggedValue(trailer, "[COMPRESSION_ALGO]"),
                    "COMPRESSION_ALGO");
            if (!CLEAR_TEXT.equals(encryption)) {
                throw fatal("encrypted native backup is unsupported: " + printable(encryption));
            }
            if (!NO_COMPRESSION.equals(compression)) {
                throw fatal("compressed native backup is unsupported: " + printable(compression));
            }
            return new Metadata(databaseId, timestamp, destination, encryption, compression,
                    logicalBytes, data.limit());
        }
    }

    private static byte[] bytes(BoundedFileData data, long start, long end) throws IOException {
        byte[] result = new byte[Math.toIntExact(end - start)];
        data.copy(start, end, result, 0);
        return result;
    }

    private static String taggedValue(byte[] region, String tag) throws IOException {
        byte[] needle = tag.getBytes(StandardCharsets.US_ASCII);
        int index = indexOf(region, needle);
        if (index < 0) {
            throw fatal("required header tag is missing: " + tag);
        }
        int lengthOffset = index + needle.length;
        if (lengthOffset > region.length - Integer.BYTES) {
            throw fatal("value length is truncated for tag " + tag);
        }
        int length = littleEndianInt(region, lengthOffset);
        int valueOffset = lengthOffset + Integer.BYTES;
        if (length < 0 || valueOffset > region.length - length) {
            throw fatal("value is outside header region for tag " + tag);
        }
        byte[] encoded = Arrays.copyOfRange(region, valueOffset, valueOffset + length);
        String value = new String(encoded, StandardCharsets.UTF_8);
        if (!Arrays.equals(encoded, value.getBytes(StandardCharsets.UTF_8))) {
            throw fatal("value is not canonical UTF-8 for tag " + tag);
        }
        return value;
    }

    private static String bracketedValueAfterTag(byte[] region, String tag) throws IOException {
        byte[] needle = tag.getBytes(StandardCharsets.US_ASCII);
        int index = indexOf(region, needle);
        if (index < 0) {
            throw fatal("required header tag is missing: " + tag);
        }
        int start = index + needle.length;
        if (start >= region.length || region[start] != '[') {
            throw fatal("expected bracketed value after tag " + tag);
        }
        int end = start + 1;
        while (end < region.length && region[end] != ']') {
            end++;
        }
        if (end >= region.length) {
            throw fatal("unterminated bracketed value after tag " + tag);
        }
        byte[] encoded = Arrays.copyOfRange(region, start, end + 1);
        String value = new String(encoded, StandardCharsets.UTF_8);
        if (!Arrays.equals(encoded, value.getBytes(StandardCharsets.UTF_8))) {
            throw fatal("value is not canonical UTF-8 for tag " + tag);
        }
        return value;
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        outer:
        for (int index = 0; index <= haystack.length - needle.length; index++) {
            for (int offset = 0; offset < needle.length; offset++) {
                if (haystack[index + offset] != needle[offset]) {
                    continue outer;
                }
            }
            return index;
        }
        return -1;
    }

    private static int littleEndianInt(byte[] value, int offset) {
        return (value[offset] & 0xff)
                | ((value[offset + 1] & 0xff) << 8)
                | ((value[offset + 2] & 0xff) << 16)
                | ((value[offset + 3] & 0xff) << 24);
    }

    private static String required(String value, String name) throws IOException {
        if (value.isBlank()) {
            throw fatal("empty required backup field " + name);
        }
        return value;
    }

    private static String printable(String value) {
        return value.replaceAll("[^\\x20-\\x7e]", "?");
    }

    private static IOException fatal(String detail) {
        return new IOException("FATAL_BACKUP_HEADER: " + detail);
    }

    private static IOException fatal(String detail, Throwable cause) {
        return new IOException("FATAL_BACKUP_HEADER: " + detail, cause);
    }
}

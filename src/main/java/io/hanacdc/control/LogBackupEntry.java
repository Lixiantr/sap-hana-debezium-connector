package io.hanacdc.control;

import java.time.Instant;
import java.util.Locale;
import java.util.Objects;

/** One successful volume log-backup row from the authoritative HANA catalog. */
public record LogBackupEntry(
        long entryId,
        long backupId,
        long sourceId,
        String host,
        String serviceType,
        long firstRedoPosition,
        long lastRedoPosition,
        long backupSizeBytes,
        String destinationType,
        String destinationPath,
        String externalBackupId,
        int logSegmentCount,
        boolean backintFallbackUsed,
        long compressedSizeBytes,
        String compressionAlgorithm,
        Instant completedAt) {

    public LogBackupEntry {
        host = Objects.requireNonNullElse(host, "");
        serviceType = Objects.requireNonNullElse(serviceType, "");
        destinationType = normalize(destinationType);
        destinationPath = Objects.requireNonNullElse(destinationPath, "");
        externalBackupId = Objects.requireNonNullElse(externalBackupId, "");
        compressionAlgorithm = normalize(compressionAlgorithm);
        completedAt = Objects.requireNonNull(completedAt, "completedAt");
        if (entryId < 0 || backupId < 0 || sourceId < 0
                || firstRedoPosition < 0 || lastRedoPosition <= firstRedoPosition
                || backupSizeBytes < 0 || logSegmentCount < 0 || compressedSizeBytes < 0) {
            throw new IllegalArgumentException("invalid log-backup catalog entry");
        }
    }

    public long physicalSizeBytes() {
        return compressedSizeBytes > 0 ? compressedSizeBytes : backupSizeBytes;
    }

    private static String normalize(String value) {
        return Objects.requireNonNullElse(value, "").trim().toLowerCase(Locale.ROOT);
    }
}

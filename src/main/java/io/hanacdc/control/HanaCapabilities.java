package io.hanacdc.control;

import java.time.Instant;
import java.util.Locale;
import java.util.Objects;

/** Effective, read-only SAP HANA capabilities observed by the connector. */
public record HanaCapabilities(
        String systemId,
        String databaseName,
        String version,
        String logMode,
        boolean automaticLogBackupEnabled,
        long logBackupTimeoutSeconds,
        String logBackupIntervalMode,
        long logSegmentSizeMiB,
        long maxLogBackupSizeGiB,
        boolean logBackupUsingBackint,
        boolean logBackupBackintFallback,
        String fileLogBackupPath,
        boolean logBackupCompressionEnabled,
        String logBackupCompressionAlgorithm,
        boolean backupEncryptionEnabled,
        long brokenHistoryEntries,
        boolean systemReplicationConfigured,
        long earliestSuccessfulBackupPosition,
        long retainedSuccessfulBackupBytes,
        long successfulBackupHighWatermark,
        long redoHead,
        Instant observedAt) {

    public HanaCapabilities {
        systemId = required(systemId, "systemId");
        databaseName = required(databaseName, "databaseName");
        version = required(version, "version");
        logMode = normalized(logMode);
        logBackupIntervalMode = normalized(logBackupIntervalMode);
        fileLogBackupPath = Objects.requireNonNullElse(fileLogBackupPath, "");
        logBackupCompressionAlgorithm = normalized(logBackupCompressionAlgorithm);
        observedAt = Objects.requireNonNull(observedAt, "observedAt");
        if (logBackupTimeoutSeconds < 0
                || logSegmentSizeMiB < 0
                || maxLogBackupSizeGiB < 0
                || brokenHistoryEntries < 0
                || earliestSuccessfulBackupPosition < 0
                || retainedSuccessfulBackupBytes < 0
                || successfulBackupHighWatermark < 0
                || redoHead < 0) {
            throw new IllegalArgumentException("HANA capability counters cannot be negative");
        }
    }

    private static String required(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value.trim();
    }

    private static String normalized(String value) {
        return Objects.requireNonNullElse(value, "").trim().toLowerCase(Locale.ROOT);
    }
}

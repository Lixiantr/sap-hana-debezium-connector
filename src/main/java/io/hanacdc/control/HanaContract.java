package io.hanacdc.control;

import io.hanacdc.control.CapabilityAssessment.Severity;
import io.hanacdc.control.CapabilityAssessment.Violation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/** Connector-side support matrix and latency contract; it never mutates HANA. */
public record HanaContract(
        Set<String> supportedIntervalModes,
        Set<String> supportedDestinationTypes,
        Set<String> supportedCompressionAlgorithms,
        boolean encryptedBackupsSupported,
        boolean systemReplicationSupported,
        long maximumBackupBytes,
        long maximumPublicationDelayMillis,
        List<String> supportedVersionPrefixes) {

    public HanaContract {
        supportedIntervalModes = normalized(supportedIntervalModes);
        supportedDestinationTypes = normalized(supportedDestinationTypes);
        supportedCompressionAlgorithms = normalized(supportedCompressionAlgorithms);
        supportedVersionPrefixes = supportedVersionPrefixes.stream()
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .toList();
        if (supportedIntervalModes.isEmpty() || supportedDestinationTypes.isEmpty()
                || maximumBackupBytes <= 0 || maximumPublicationDelayMillis <= 0) {
            throw new IllegalArgumentException("invalid HANA connector contract");
        }
    }

    @Override
    public Set<String> supportedIntervalModes() {
        return Set.copyOf(supportedIntervalModes);
    }

    @Override
    public Set<String> supportedDestinationTypes() {
        return Set.copyOf(supportedDestinationTypes);
    }

    @Override
    public Set<String> supportedCompressionAlgorithms() {
        return Set.copyOf(supportedCompressionAlgorithms);
    }

    @Override
    public List<String> supportedVersionPrefixes() {
        return List.copyOf(supportedVersionPrefixes);
    }

    public CapabilityAssessment assess(HanaCapabilities capabilities) {
        List<Violation> violations = new ArrayList<>();
        if (!"normal".equals(capabilities.logMode())) {
            blocked(violations, "LOG_MODE", "log_mode must be normal, got " + capabilities.logMode());
        }
        if (!capabilities.automaticLogBackupEnabled()) {
            blocked(violations, "AUTO_LOG_BACKUP_DISABLED", "enable_auto_log_backup must be true");
        }
        if (capabilities.logBackupTimeoutSeconds() == 0) {
            policy(violations, "UNBOUNDED_BACKUP_DELAY",
                    "log_backup_timeout_s=0 cannot satisfy a finite CDC latency SLO");
        }
        else if (capabilities.logBackupTimeoutSeconds() * 1_000L
                > maximumPublicationDelayMillis) {
            policy(violations, "BACKUP_TIMEOUT_EXCEEDS_SLO",
                    "log_backup_timeout_s=" + capabilities.logBackupTimeoutSeconds()
                            + " exceeds connector budget " + maximumPublicationDelayMillis + " ms");
        }
        if (!supportedIntervalModes.contains(capabilities.logBackupIntervalMode())) {
            blocked(violations, "UNSUPPORTED_INTERVAL_MODE",
                    "unsupported log_backup_interval_mode=" + capabilities.logBackupIntervalMode());
        }
        if (capabilities.logBackupUsingBackint()
                && !supportedDestinationTypes.contains("backint")) {
            blocked(violations, "UNSUPPORTED_BACKINT",
                    "HANA is configured for Backint but no Backint adapter is enabled");
        }
        if (capabilities.backupEncryptionEnabled() && !encryptedBackupsSupported) {
            blocked(violations, "UNSUPPORTED_BACKUP_ENCRYPTION",
                    "encrypted HANA log backups are not supported by this build");
        }
        if (capabilities.logBackupCompressionEnabled()
                && !supportedCompressionAlgorithms.contains(
                        capabilities.logBackupCompressionAlgorithm())) {
            blocked(violations, "UNSUPPORTED_BACKUP_COMPRESSION",
                    "unsupported log-backup compression="
                            + capabilities.logBackupCompressionAlgorithm());
        }
        long configuredMaximumBytes;
        try {
            configuredMaximumBytes = Math.multiplyExact(
                    capabilities.maxLogBackupSizeGiB(), 1024L * 1024L * 1024L);
        }
        catch (ArithmeticException error) {
            configuredMaximumBytes = Long.MAX_VALUE;
        }
        if (configuredMaximumBytes > maximumBackupBytes) {
            policy(violations, "CONFIGURED_BACKUP_LIMIT_EXCEEDS_READER",
                    "max_log_backup_size permits " + configuredMaximumBytes
                            + " bytes but reader limit is " + maximumBackupBytes);
        }
        if (!supportedVersionPrefixes.isEmpty()
                && supportedVersionPrefixes.stream()
                        .noneMatch(capabilities.version()::startsWith)) {
            blocked(violations, "UNSUPPORTED_HANA_REVISION",
                    "HANA revision is outside the tested support matrix: " + capabilities.version());
        }
        if (capabilities.systemReplicationConfigured() && !systemReplicationSupported) {
            blocked(violations, "UNSUPPORTED_SYSTEM_REPLICATION",
                    "System Replication requires source-epoch/takeover support");
        }
        if (capabilities.brokenHistoryEntries() != 0) {
            violations.add(new Violation(Severity.FATAL, "FATAL_GAP",
                    "M_BACKUP_HISTORY_BROKEN contains "
                            + capabilities.brokenHistoryEntries() + " row(s)"));
        }
        return new CapabilityAssessment(violations);
    }

    public void validateEntry(LogBackupEntry entry) {
        if (!supportedDestinationTypes.contains(entry.destinationType())) {
            throw new IllegalStateException("UNSUPPORTED_DESTINATION: backup_id="
                    + entry.backupId() + " destination=" + entry.destinationType());
        }
        String compression = entry.compressionAlgorithm().isBlank()
                ? "none" : entry.compressionAlgorithm();
        if (!"none".equals(compression) && !supportedCompressionAlgorithms.contains(compression)) {
            throw new IllegalStateException("UNSUPPORTED_BACKUP_COMPRESSION: backup_id="
                    + entry.backupId() + " compression=" + compression);
        }
        if (entry.physicalSizeBytes() > maximumBackupBytes) {
            throw new IllegalStateException("BACKUP_TOO_LARGE: backup_id=" + entry.backupId()
                    + " physical_bytes=" + entry.physicalSizeBytes()
                    + " limit=" + maximumBackupBytes);
        }
    }

    private static Set<String> normalized(Collection<String> values) {
        return values.stream()
                .map(value -> value.trim().toLowerCase(Locale.ROOT))
                .filter(value -> !value.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }

    private static void policy(List<Violation> values, String code, String message) {
        values.add(new Violation(Severity.POLICY, code, message));
    }

    private static void blocked(List<Violation> values, String code, String message) {
        values.add(new Violation(Severity.BLOCKED, code, message));
    }
}

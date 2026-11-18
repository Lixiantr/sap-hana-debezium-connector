package io.hanacdc.control;

import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/** Verifies that the exact durable replay boundary remains catalogued and readable. */
public final class RetentionGuard {
    public record Status(
            long requiredBackupId,
            long requiredFileFirst,
            Instant completedAt,
            long requiredBackupAgeSeconds,
            long contractHeadroomSeconds,
            Path path) {
        public Status {
            completedAt = Objects.requireNonNull(completedAt, "completedAt");
            path = Objects.requireNonNull(path, "path").normalize();
            if (requiredBackupId < 0 || requiredFileFirst < 0
                    || requiredBackupAgeSeconds < 0 || contractHeadroomSeconds < 0) {
                throw new IllegalArgumentException("invalid retention status");
            }
        }

        public boolean insideSafetyMargin(long safetyMarginSeconds) {
            return contractHeadroomSeconds <= safetyMarginSeconds;
        }

        public String toJson() {
            return String.format(Locale.ROOT,
                    "{\"kind\":\"retention_guard\",\"required_backup_id\":%d,"
                            + "\"required_file_first\":%d,\"completed_at\":\"%s\","
                            + "\"required_backup_age_seconds\":%d,"
                            + "\"contract_headroom_seconds\":%d}",
                    requiredBackupId, requiredFileFirst, completedAt,
                    requiredBackupAgeSeconds, contractHeadroomSeconds);
        }
    }

    private RetentionGuard() {
    }

    public static Status inspect(
            BackupFile required,
            List<LogBackupEntry> catalogEntries,
            PathMapper mapper,
            HanaContract contract,
            Instant observedAt,
            long minimumRetentionSeconds) {
        Objects.requireNonNull(required, "required");
        Objects.requireNonNull(catalogEntries, "catalogEntries");
        Objects.requireNonNull(mapper, "mapper");
        Objects.requireNonNull(contract, "contract");
        Objects.requireNonNull(observedAt, "observedAt");
        if (minimumRetentionSeconds <= 0) {
            throw new IllegalArgumentException("minimum retention must be positive");
        }

        List<LogBackupEntry> matching = catalogEntries.stream()
                .filter(value -> value.backupId() == required.backupId())
                .filter(value -> value.firstRedoPosition() == required.first())
                .filter(value -> value.lastRedoPosition() == required.last())
                .toList();
        if (matching.size() != 1) {
            throw new IllegalStateException("FATAL_RETENTION_CATALOG_GAP: backup_id="
                    + required.backupId() + " expected exactly one entry, found "
                    + matching.size());
        }
        LogBackupEntry entry = matching.get(0);
        contract.validateEntry(entry);
        Path mapped = mapper.resolve(entry.destinationPath()).normalize();
        if (!mapped.equals(required.path().normalize())) {
            throw new IllegalStateException("FATAL_RETENTION_PATH_CHANGED: backup_id="
                    + required.backupId() + " expected=" + required.path()
                    + " catalog=" + mapped);
        }
        if (!Files.isRegularFile(mapped)) {
            throw new IllegalStateException("FATAL_RETENTION_OBJECT_MISSING: backup_id="
                    + required.backupId() + " path=" + mapped);
        }

        long age = Math.max(0L, Duration.between(entry.completedAt(), observedAt).getSeconds());
        long headroom = Math.max(0L, minimumRetentionSeconds - age);
        return new Status(required.backupId(), required.first(), entry.completedAt(),
                age, headroom, mapped);
    }
}

package io.hanacdc.control;

import java.time.Instant;
import java.util.Objects;

/** Catalog and redo watermarks for one HANA persistence volume. */
public record PersistenceStreamStatus(
        long sourceId,
        long earliestSuccessfulBackupPosition,
        long retainedSuccessfulBackupBytes,
        long successfulBackupHighWatermark,
        long redoHead,
        Instant observedAt) {
    public PersistenceStreamStatus {
        observedAt = Objects.requireNonNull(observedAt, "observedAt");
        if (sourceId <= 0 || earliestSuccessfulBackupPosition < 0
                || retainedSuccessfulBackupBytes < 0
                || successfulBackupHighWatermark < 0 || redoHead < 0) {
            throw new IllegalArgumentException("invalid persistence stream status");
        }
    }
}

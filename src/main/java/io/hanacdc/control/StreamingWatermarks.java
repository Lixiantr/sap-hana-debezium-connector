package io.hanacdc.control;

import java.time.Instant;
import java.util.Locale;
import java.util.Objects;

/** Immutable operational view of one streaming-only HANA redo stream. */
public record StreamingWatermarks(
        long captureStart,
        long redoHead,
        long successfulBackupHwm,
        long objectVisibleHwm,
        long decodedHwm,
        long publishedHwm,
        long kafkaCommittedHwm,
        long earliestAvailablePosition,
        long oldestRequiredPosition,
        long retainedBackupBytes,
        double observedRedoBytesPerSecond,
        Instant observedAt) {

    private static final long REDO_POSITION_BYTES = 64L;

    public StreamingWatermarks {
        observedAt = Objects.requireNonNull(observedAt, "observedAt");
        if (captureStart < 0 || redoHead < 0 || successfulBackupHwm < 0
                || objectVisibleHwm < 0 || decodedHwm < 0 || publishedHwm < 0
                || kafkaCommittedHwm < 0 || earliestAvailablePosition < 0
                || oldestRequiredPosition < 0 || retainedBackupBytes < 0
                || !Double.isFinite(observedRedoBytesPerSecond)
                || observedRedoBytesPerSecond < 0) {
            throw new IllegalArgumentException("streaming watermarks cannot be negative");
        }
    }

    public boolean retentionSafe() {
        return earliestAvailablePosition == 0
                || earliestAvailablePosition <= oldestRequiredPosition;
    }

    public long backupLagBytes() {
        return positionDistanceBytes(successfulBackupHwm, redoHead);
    }

    public long decodeLagBytes() {
        return positionDistanceBytes(decodedHwm, successfulBackupHwm);
    }

    public long publishLagBytes() {
        return positionDistanceBytes(publishedHwm, decodedHwm);
    }

    public long kafkaCommitLagBytes() {
        return positionDistanceBytes(kafkaCommittedHwm, publishedHwm);
    }

    public long retentionHeadroomBytes() {
        if (!retentionSafe() || earliestAvailablePosition == 0) {
            return 0;
        }
        return positionDistanceBytes(earliestAvailablePosition, oldestRequiredPosition);
    }

    public double estimatedCatchupSeconds() {
        if (observedRedoBytesPerSecond == 0) {
            return Double.NaN;
        }
        return (backupLagBytes() + decodeLagBytes() + publishLagBytes()
                + kafkaCommitLagBytes()) / observedRedoBytesPerSecond;
    }

    public String phase() {
        if (!retentionSafe()) {
            return "FATAL_GAP";
        }
        return kafkaCommittedHwm <= captureStart
                ? "STREAMING_WARMUP"
                : "STREAMING_UNVERIFIED";
    }

    public String toJson() {
        return String.format(Locale.ROOT,
                "{\"kind\":\"streaming_watermarks\",\"phase\":\"%s\","
                        + "\"capture_start\":%d,\"redo_head\":%d,"
                        + "\"successful_backup_hwm\":%d,\"object_visible_hwm\":%d,"
                        + "\"decoded_hwm\":%d,\"published_hwm\":%d,"
                        + "\"kafka_committed_hwm\":%d,"
                        + "\"earliest_available_position\":%d,"
                        + "\"oldest_required_position\":%d,"
                        + "\"retained_backup_bytes\":%d,"
                        + "\"retention_safe\":%s,\"retention_headroom_bytes\":%d,"
                        + "\"backup_lag_bytes\":%d,\"decode_lag_bytes\":%d,"
                        + "\"publish_lag_bytes\":%d,\"kafka_commit_lag_bytes\":%d,"
                        + "\"estimated_catchup_seconds\":%s,\"observed_at\":\"%s\"}",
                phase(), captureStart, redoHead, successfulBackupHwm, objectVisibleHwm,
                decodedHwm, publishedHwm, kafkaCommittedHwm, earliestAvailablePosition,
                oldestRequiredPosition, retainedBackupBytes, retentionSafe(),
                retentionHeadroomBytes(), backupLagBytes(), decodeLagBytes(),
                publishLagBytes(), kafkaCommitLagBytes(), jsonNumber(estimatedCatchupSeconds()),
                observedAt);
    }

    private static long positionDistanceBytes(long low, long high) {
        if (high <= low) {
            return 0;
        }
        try {
            return Math.multiplyExact(high - low, REDO_POSITION_BYTES);
        }
        catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static String jsonNumber(double value) {
        return Double.isFinite(value) ? String.format(Locale.ROOT, "%.6f", value) : "null";
    }
}

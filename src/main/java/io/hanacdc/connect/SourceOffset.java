package io.hanacdc.connect;

import org.apache.kafka.connect.errors.ConnectException;

import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/** Strict versioned Kafka Connect offset for one emitted logical event. */
public record SourceOffset(
        int version,
        String sourceEpoch,
        long backupId,
        long fileFirst,
        long fileLast,
        long logPosition,
        long eventOffset,
        long sequence,
        long transactionId,
        int changeIndex,
        int changeCount,
        long stateReplayFileFirst,
        long stateGeneration,
        String stateSha256) {

    public static final int CURRENT_VERSION = 3;
    public static final String NO_STATE_SHA256 = "none";

    public SourceOffset {
        if (version != CURRENT_VERSION || backupId < 0 || fileFirst < 0
                || fileLast <= fileFirst || logPosition < fileFirst || eventOffset < 0
                || sequence < 0 || transactionId <= 0 || changeIndex < 0
                || changeCount <= 0 || changeIndex >= changeCount
                || stateReplayFileFirst < 0 || stateGeneration < 0) {
            throw new IllegalArgumentException("invalid source offset coordinates");
        }
        sourceEpoch = sha256(sourceEpoch, "sourceEpoch");
        stateSha256 = Objects.requireNonNull(stateSha256, "stateSha256");
        if (!NO_STATE_SHA256.equals(stateSha256)) {
            stateSha256 = sha256(stateSha256, "stateSha256");
        }
        if (stateGeneration == 0 && !NO_STATE_SHA256.equals(stateSha256)) {
            throw new IllegalArgumentException("generation zero cannot reference durable state");
        }
        if (stateGeneration > 0 && NO_STATE_SHA256.equals(stateSha256)) {
            throw new IllegalArgumentException("durable state generation requires a checksum");
        }
    }

    public static SourceOffset withoutDurableState(
            SourceEpoch epoch,
            long backupId,
            long fileFirst,
            long fileLast,
            long logPosition,
            long eventOffset,
            long sequence,
            long transactionId) {
        return new SourceOffset(CURRENT_VERSION, epoch.id(), backupId, fileFirst,
                fileLast, logPosition, eventOffset, sequence, transactionId,
                0, 1, fileFirst, 0, NO_STATE_SHA256);
    }

    public static SourceOffset withDurableState(
            SourceEpoch epoch,
            long backupId,
            long fileFirst,
            long fileLast,
            long logPosition,
            long eventOffset,
            long sequence,
            long transactionId,
            int changeIndex,
            int changeCount,
            ReplayBoundary stateBoundary,
            DurableReducerStateStore.Checkpoint checkpoint) {
        Objects.requireNonNull(stateBoundary, "stateBoundary");
        Objects.requireNonNull(checkpoint, "checkpoint");
        return new SourceOffset(CURRENT_VERSION, epoch.id(), backupId, fileFirst,
                fileLast, logPosition, eventOffset, sequence, transactionId,
                changeIndex, changeCount, stateBoundary.fileFirst(),
                checkpoint.generation(), checkpoint.sha256());
    }

    public Map<String, Object> toMap() {
        Map<String, Object> result = new LinkedHashMap<>(19);
        result.put("offset_version", version);
        result.put("source_epoch", sourceEpoch);
        result.put("backup_id", backupId);
        result.put("file_first", fileFirst);
        result.put("file_last", fileLast);
        result.put("log_position", logPosition);
        result.put("event_offset", eventOffset);
        result.put("sequence", sequence);
        result.put("transaction_id", transactionId);
        result.put("change_index", changeIndex);
        result.put("change_count", changeCount);
        result.put("state_replay_file_first", stateReplayFileFirst);
        result.put("state_generation", stateGeneration);
        result.put("state_sha256", stateSha256);
        return Collections.unmodifiableMap(result);
    }

    public static SourceOffset fromMap(Map<String, ?> values) {
        Objects.requireNonNull(values, "values");
        int version = Math.toIntExact(number(values, "offset_version"));
        if (version != CURRENT_VERSION) {
            throw new ConnectException("UNSUPPORTED_OFFSET_VERSION: " + version
                    + "; expected " + CURRENT_VERSION);
        }
        try {
            return new SourceOffset(version, string(values, "source_epoch"),
                    number(values, "backup_id"), number(values, "file_first"),
                    number(values, "file_last"), number(values, "log_position"),
                    number(values, "event_offset"), number(values, "sequence"),
                    number(values, "transaction_id"),
                    Math.toIntExact(number(values, "change_index")),
                    Math.toIntExact(number(values, "change_count")),
                    number(values, "state_replay_file_first"),
                    number(values, "state_generation"), string(values, "state_sha256"));
        }
        catch (IllegalArgumentException error) {
            throw new ConnectException("INVALID_SOURCE_OFFSET: " + error.getMessage(), error);
        }
    }

    public void requireEpoch(SourceEpoch expected) {
        Objects.requireNonNull(expected, "expected");
        if (!sourceEpoch.equals(expected.id())) {
            throw new ConnectException("SOURCE_EPOCH_MISMATCH: stored=" + sourceEpoch
                    + " current=" + expected.id());
        }
    }

    private static long number(Map<String, ?> values, String key) {
        Object value = values.get(key);
        if (!(value instanceof Number number)) {
            throw new ConnectException("stored offset " + key + " is missing or not numeric");
        }
        return number.longValue();
    }

    private static String string(Map<String, ?> values, String key) {
        Object value = values.get(key);
        if (!(value instanceof String text) || text.isBlank()) {
            throw new ConnectException("stored offset " + key + " is missing or blank");
        }
        return text;
    }

    private static String sha256(String value, String name) {
        Objects.requireNonNull(value, name);
        if (!isLowercaseSha256(value)) {
            throw new IllegalArgumentException(name + " must be a lowercase SHA-256");
        }
        return value;
    }

    private static boolean isLowercaseSha256(String value) {
        if (value.length() != 64) {
            return false;
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= '0' && character <= '9')
                    || (character >= 'a' && character <= 'f'))) {
                return false;
            }
        }
        return true;
    }
}

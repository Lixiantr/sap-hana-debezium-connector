package io.hanacdc.connect;

import io.hanacdc.control.PersistenceStreamStatus;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Resolves a complete per-volume start vector for streaming-only scale-out capture. */
public final class ScaleOutStartPositions {
    private ScaleOutStartPositions() {}

    public static Map<SourceStreamId, Long> resolve(
            String specification,
            List<SourceStreamId> topologyStreams,
            Map<SourceStreamId, PersistenceStreamStatus> currentStatuses) {
        List<SourceStreamId> streams = Objects.requireNonNull(topologyStreams,
                "topologyStreams").stream().distinct().sorted().toList();
        if (streams.size() < 2 || !currentStatuses.keySet().equals(Set.copyOf(streams))) {
            throw new IllegalArgumentException("start vector does not match scale-out topology");
        }
        if (specification == null || specification.isBlank()) {
            Map<SourceStreamId, Long> latest = new LinkedHashMap<>();
            for (SourceStreamId stream : streams) {
                PersistenceStreamStatus status = currentStatuses.get(stream);
                if (status.sourceId() != stream.volumeId()) {
                    throw new IllegalArgumentException(
                            "stream status identity mismatch for " + stream.canonical());
                }
                latest.put(stream, status.successfulBackupHighWatermark());
            }
            return java.util.Collections.unmodifiableMap(latest);
        }

        Map<Long, Long> configured = parse(specification);
        Map<SourceStreamId, Long> result = new LinkedHashMap<>();
        for (SourceStreamId stream : streams) {
            Long position = configured.remove(stream.volumeId());
            if (position == null) {
                throw new IllegalArgumentException(
                        "missing start position for " + stream.canonical());
            }
            PersistenceStreamStatus status = currentStatuses.get(stream);
            if (position > status.successfulBackupHighWatermark()) {
                throw new IllegalArgumentException("start position is ahead of backup HWM for "
                        + stream.canonical());
            }
            if (status.earliestSuccessfulBackupPosition() > 0
                    && position < status.earliestSuccessfulBackupPosition()) {
                throw new IllegalArgumentException("FATAL_GAP: configured start position "
                        + position + " is older than retained position "
                        + status.earliestSuccessfulBackupPosition() + " for "
                        + stream.canonical());
            }
            result.put(stream, position);
        }
        if (!configured.isEmpty()) {
            throw new IllegalArgumentException(
                    "start vector contains unknown volumes " + configured.keySet());
        }
        return java.util.Collections.unmodifiableMap(result);
    }

    private static Map<Long, Long> parse(String specification) {
        Map<Long, Long> result = new LinkedHashMap<>();
        for (String item : specification.split(",")) {
            String[] pair = item.strip().split("=", -1);
            if (pair.length != 2) {
                throw new IllegalArgumentException("invalid scale-out start position: " + item);
            }
            try {
                long volume = Long.parseLong(pair[0].strip());
                long position = Long.parseLong(pair[1].strip());
                if (volume <= 0 || position < 0 || result.putIfAbsent(volume, position) != null) {
                    throw new IllegalArgumentException(
                            "invalid or duplicate scale-out start position: " + item);
                }
            }
            catch (NumberFormatException error) {
                throw new IllegalArgumentException(
                        "invalid scale-out start position: " + item, error);
            }
        }
        return result;
    }
}

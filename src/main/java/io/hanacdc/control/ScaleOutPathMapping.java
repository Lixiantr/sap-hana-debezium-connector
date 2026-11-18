package io.hanacdc.control;

import io.hanacdc.connect.SourceStreamId;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Per-volume path mappings for hosts that use identical HANA backup paths. */
public final class ScaleOutPathMapping {
    private final Map<Long, PathMapper> mappings;

    private ScaleOutPathMapping(Map<Long, PathMapper> mappings) {
        this.mappings = Map.copyOf(mappings);
    }

    /**
     * Format: {@code volume@hana-root=local-root[;volume@hana-root=local-root...]};
     * multiple entries for one volume are joined into one ordinary PathMapper.
     */
    public static ScaleOutPathMapping parse(String specification) {
        if (specification == null || specification.isBlank()) {
            throw new IllegalArgumentException(
                    "log.backup.scale.out.path.mapping is required in scale_out mode");
        }
        Map<Long, StringBuilder> grouped = new LinkedHashMap<>();
        for (String item : specification.split(";")) {
            int marker = item.indexOf('@');
            if (marker <= 0 || marker == item.length() - 1) {
                throw new IllegalArgumentException("invalid scale-out path mapping: " + item);
            }
            long volume;
            try {
                volume = Long.parseLong(item.substring(0, marker).strip());
            }
            catch (NumberFormatException error) {
                throw new IllegalArgumentException(
                        "invalid scale-out path mapping volume: " + item, error);
            }
            if (volume <= 0) {
                throw new IllegalArgumentException("path mapping volume must be positive");
            }
            String mapping = item.substring(marker + 1).strip();
            StringBuilder value = grouped.computeIfAbsent(volume,
                    ignored -> new StringBuilder());
            if (!value.isEmpty()) value.append(';');
            value.append(mapping);
        }
        Map<Long, PathMapper> result = new LinkedHashMap<>();
        grouped.forEach((volume, value) ->
                result.put(volume, PathMapper.parse(value.toString())));
        return new ScaleOutPathMapping(result);
    }

    public Map<SourceStreamId, PathMapper> requireStreams(
            Iterable<SourceStreamId> streams) {
        Objects.requireNonNull(streams, "streams");
        Map<SourceStreamId, PathMapper> result = new LinkedHashMap<>();
        java.util.Set<Long> required = new java.util.LinkedHashSet<>();
        for (SourceStreamId stream : streams) {
            required.add(stream.volumeId());
            PathMapper mapper = mappings.get(stream.volumeId());
            if (mapper == null) {
                throw new IllegalArgumentException(
                        "missing path mapping for " + stream.canonical());
            }
            result.put(stream, mapper);
        }
        java.util.Set<Long> unknown = new java.util.LinkedHashSet<>(mappings.keySet());
        unknown.removeAll(required);
        if (!unknown.isEmpty()) {
            throw new IllegalArgumentException(
                    "path mappings contain unknown volumes " + unknown);
        }
        return java.util.Collections.unmodifiableMap(result);
    }
}

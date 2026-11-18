package io.hanacdc.connect;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/** Stable one-field Kafka Connect source partition identity. */
final class SourcePartitionIdentity {
    static final String KEY = "partition_id";
    private static final String VERSION = "v1";

    private SourcePartitionIdentity() {
    }

    static Map<String, Object> filesystem(
            String server,
            long sourceId,
            long volume,
            long partition) {
        return singleton("filesystem", server, sourceId, volume, partition, null, null);
    }

    static Map<String, Object> catalog(
            String server,
            long sourceId,
            long volume,
            long partition,
            String systemId,
            String database) {
        return singleton("catalog", server, sourceId, volume, partition, systemId, database);
    }

    static Map<String, Object> catalogScaleOut(
            String server,
            String systemId,
            String database,
            String topologyFingerprint) {
        Objects.requireNonNull(topologyFingerprint, "topologyFingerprint");
        if (topologyFingerprint.length() != 64
                || !topologyFingerprint.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException(
                    "topologyFingerprint must be a lowercase SHA-256");
        }
        String value = VERSION + "|mode=catalog_scale_out"
                + "|server=" + encoded(server, "server")
                + "|system_id=" + encoded(systemId, "systemId")
                + "|database=" + encoded(database, "database")
                + "|topology=" + topologyFingerprint;
        return Collections.singletonMap(KEY, value);
    }

    private static Map<String, Object> singleton(
            String mode,
            String server,
            long sourceId,
            long volume,
            long partition,
            String systemId,
            String database) {
        if (sourceId < 0 || volume < 0 || partition < 0) {
            throw new IllegalArgumentException("source partition coordinates must be non-negative");
        }
        StringBuilder value = new StringBuilder(VERSION)
                .append("|mode=").append(mode)
                .append("|server=").append(encoded(server, "server"))
                .append("|source_id=").append(sourceId)
                .append("|volume=").append(volume)
                .append("|partition=").append(partition);
        if (systemId != null) {
            value.append("|system_id=").append(encoded(systemId, "systemId"));
            value.append("|database=").append(encoded(database, "database"));
        }
        return Collections.singletonMap(KEY, value.toString());
    }

    private static String encoded(String value, String name) {
        Objects.requireNonNull(value, name);
        String normalized = value.strip();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return URLEncoder.encode(normalized, StandardCharsets.UTF_8).replace("+", "%20");
    }
}

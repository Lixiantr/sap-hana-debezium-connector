package io.hanacdc.connect;

import io.hanacdc.control.HanaCapabilities;
import io.hanacdc.schema.TableDecodePlan;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/** Immutable identity of one HANA redo history and its pinned table schemas. */
public record SourceEpoch(
        int version,
        String systemId,
        String databaseName,
        long sourceId,
        long volume,
        long partition,
        String databaseId,
        String schemaSetFingerprint,
        String id) {

    public static final int CURRENT_VERSION = 1;

    public SourceEpoch {
        if (version != CURRENT_VERSION || sourceId < 0 || volume < 0 || partition < 0) {
            throw new IllegalArgumentException("invalid source epoch coordinates");
        }
        systemId = required(systemId, "systemId");
        databaseName = required(databaseName, "databaseName");
        databaseId = required(databaseId, "databaseId");
        schemaSetFingerprint = hex(required(schemaSetFingerprint, "schemaSetFingerprint"),
                "schemaSetFingerprint");
        id = hex(required(id, "id"), "id");
        String expected = calculateId(version, systemId, databaseName, sourceId,
                volume, partition, databaseId, schemaSetFingerprint);
        if (!expected.equals(id)) {
            throw new IllegalArgumentException("source epoch id does not match its fields");
        }
    }

    public static SourceEpoch catalog(
            HanaCapabilities capabilities,
            long sourceId,
            long volume,
            long partition,
            String databaseId,
            List<TableDecodePlan> plans) {
        Objects.requireNonNull(capabilities, "capabilities");
        String schemas = schemaSetFingerprint(plans);
        String id = calculateId(CURRENT_VERSION, capabilities.systemId(),
                capabilities.databaseName(), sourceId, volume, partition, databaseId, schemas);
        return new SourceEpoch(CURRENT_VERSION, capabilities.systemId(),
                capabilities.databaseName(), sourceId, volume, partition,
                databaseId, schemas, id);
    }

    /** Deterministic test-mode epoch; it is never a production HANA identity. */
    public static SourceEpoch prototype(
            String serverName,
            String runId,
            long sourceId,
            long volume,
            long partition) {
        String schemas = sha256("prototype-schema:HCDC1");
        String databaseId = "prototype-run:" + required(runId, "runId");
        String id = calculateId(CURRENT_VERSION, serverName, "prototype",
                sourceId, volume, partition, databaseId, schemas);
        return new SourceEpoch(CURRENT_VERSION, serverName, "prototype",
                sourceId, volume, partition, databaseId, schemas, id);
    }

    public static String schemaSetFingerprint(List<TableDecodePlan> plans) {
        Objects.requireNonNull(plans, "plans");
        if (plans.isEmpty()) {
            throw new IllegalArgumentException("source epoch requires at least one table plan");
        }
        MessageDigest digest = digest();
        plans.stream()
                .sorted(Comparator.comparing(value -> value.metadata().qualifiedName()))
                .forEach(plan -> {
                    update(digest, plan.metadata().qualifiedName());
                    update(digest, plan.schemaFingerprint());
                    update(digest, plan.topologyFingerprint());
                });
        return HexFormat.of().formatHex(digest.digest());
    }

    private static String calculateId(
            int version,
            String systemId,
            String databaseName,
            long sourceId,
            long volume,
            long partition,
            String databaseId,
            String schemaSetFingerprint) {
        MessageDigest digest = digest();
        update(digest, Integer.toString(version));
        update(digest, required(systemId, "systemId"));
        update(digest, required(databaseName, "databaseName"));
        update(digest, Long.toString(sourceId));
        update(digest, Long.toString(volume));
        update(digest, Long.toString(partition));
        update(digest, required(databaseId, "databaseId"));
        update(digest, required(schemaSetFingerprint, "schemaSetFingerprint"));
        return HexFormat.of().formatHex(digest.digest());
    }

    private static String sha256(String value) {
        MessageDigest digest = digest();
        update(digest, value);
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value.trim();
    }

    private static String hex(String value, String name) {
        if (value.length() != 64 || !value.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException(name + " must be a lowercase SHA-256");
        }
        return value;
    }
}

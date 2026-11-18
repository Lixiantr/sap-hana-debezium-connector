package io.hanacdc.connect;

import io.hanacdc.control.HanaCapabilities;
import io.hanacdc.schema.TableDecodePlan;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/** Immutable identity of a distributed HANA redo history and pinned topology. */
public record ScaleOutSourceEpoch(
        int version,
        String systemId,
        String databaseName,
        String databaseId,
        String schemaSetFingerprint,
        String topologyFingerprint,
        String id) {
    public static final int CURRENT_VERSION = 1;

    public ScaleOutSourceEpoch {
        if (version != CURRENT_VERSION) {
            throw new IllegalArgumentException("invalid scale-out epoch version");
        }
        systemId = required(systemId, "systemId");
        databaseName = required(databaseName, "databaseName");
        databaseId = required(databaseId, "databaseId");
        schemaSetFingerprint = sha256(schemaSetFingerprint, "schemaSetFingerprint");
        topologyFingerprint = sha256(topologyFingerprint, "topologyFingerprint");
        id = sha256(id, "id");
        String expected = calculateId(version, systemId, databaseName, databaseId,
                schemaSetFingerprint, topologyFingerprint);
        if (!expected.equals(id)) {
            throw new IllegalArgumentException("scale-out epoch id does not match its fields");
        }
    }

    public static ScaleOutSourceEpoch catalog(
            HanaCapabilities capabilities,
            String databaseId,
            String topologyFingerprint,
            List<TableDecodePlan> plans) {
        Objects.requireNonNull(capabilities, "capabilities");
        String schemas = SourceEpoch.schemaSetFingerprint(plans);
        String id = calculateId(CURRENT_VERSION, capabilities.systemId(),
                capabilities.databaseName(), databaseId, schemas, topologyFingerprint);
        return new ScaleOutSourceEpoch(CURRENT_VERSION, capabilities.systemId(),
                capabilities.databaseName(), databaseId, schemas, topologyFingerprint, id);
    }

    /** Deterministic test-only distributed epoch. */
    public static ScaleOutSourceEpoch prototype(String serverName, String runId,
            String topologyFingerprint) {
        String schemas = digest("prototype-schema:HCDC1");
        String databaseId = "prototype-run:" + required(runId, "runId");
        String id = calculateId(CURRENT_VERSION, serverName, "prototype", databaseId,
                schemas, topologyFingerprint);
        return new ScaleOutSourceEpoch(CURRENT_VERSION, serverName, "prototype",
                databaseId, schemas, topologyFingerprint, id);
    }

    private static String calculateId(int version, String systemId, String databaseName,
            String databaseId, String schemaFingerprint, String topologyFingerprint) {
        MessageDigest digest = messageDigest();
        update(digest, Integer.toString(version));
        update(digest, required(systemId, "systemId"));
        update(digest, required(databaseName, "databaseName"));
        update(digest, required(databaseId, "databaseId"));
        update(digest, sha256(schemaFingerprint, "schemaSetFingerprint"));
        update(digest, sha256(topologyFingerprint, "topologyFingerprint"));
        return HexFormat.of().formatHex(digest.digest());
    }

    private static String digest(String value) {
        MessageDigest digest = messageDigest();
        update(digest, value);
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static MessageDigest messageDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " is required");
        return value.strip();
    }

    private static String sha256(String value, String name) {
        value = required(value, name);
        if (value.length() != 64 || !value.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException(name + " must be a lowercase SHA-256");
        }
        return value;
    }
}

package io.hanacdc.connect;

import io.hanacdc.control.PersistenceService;
import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Auto-discovered and pinned HANA persistence topology for one source epoch. */
record SourceTopology(
        SourceTopologyMode configuredMode,
        SourceTopologyMode actualMode,
        List<PersistenceService> services,
        String fingerprint) {

    SourceTopology {
        Objects.requireNonNull(configuredMode, "configuredMode");
        Objects.requireNonNull(actualMode, "actualMode");
        services = List.copyOf(Objects.requireNonNull(services, "services"));
        fingerprint = Objects.requireNonNull(fingerprint, "fingerprint");
    }

    static SourceTopology discover(
            SourceTopologyMode configured,
            List<PersistenceService> discoveredServices,
            List<TableDecodePlan> plans,
            long configuredSourceId) {
        Objects.requireNonNull(configured, "configured");
        Objects.requireNonNull(discoveredServices, "discoveredServices");
        Objects.requireNonNull(plans, "plans");
        if (plans.isEmpty()) {
            throw fatal("no selected table plans");
        }
        List<PersistenceService> services = discoveredServices.stream()
                .distinct().sorted().toList();
        if (services.isEmpty()) {
            throw fatal("no indexserver persistence service discovered");
        }
        Set<ServiceIdentity> serviceIdentities = new HashSet<>();
        Set<Long> volumeIds = new HashSet<>();
        for (PersistenceService service : services) {
            ServiceIdentity identity = new ServiceIdentity(
                    service.host(), service.port(), service.volumeId());
            if (!serviceIdentities.add(identity)) {
                throw fatal("duplicate persistence service " + identity);
            }
            if (!volumeIds.add(service.volumeId())) {
                throw fatal("duplicate active persistence volume_id="
                        + service.volumeId());
            }
        }

        for (TableDecodePlan plan : plans) {
            for (TableMetadata.Partition partition : plan.metadata().partitions()) {
                if (!partition.catalogBound()) {
                    throw fatal("unbound table container for "
                            + plan.metadata().qualifiedName());
                }
                ServiceIdentity identity = new ServiceIdentity(
                        partition.host(), partition.port(), partition.volumeId());
                if (!serviceIdentities.contains(identity)) {
                    throw fatal("table partition is not owned by a discovered service: table="
                            + plan.metadata().qualifiedName() + " part_id="
                            + partition.partId() + " service=" + identity);
                }
            }
        }

        boolean partitioned = plans.stream().anyMatch(value -> value.metadata().partitioned());
        SourceTopologyMode actual = services.size() > 1
                ? SourceTopologyMode.SCALE_OUT
                : partitioned ? SourceTopologyMode.SINGLE_SERVICE
                : SourceTopologyMode.SINGLE_CONTAINER;
        if (!configured.permits(actual)) {
            throw fatal("configured mode=" + configured.configValue()
                    + " does not permit discovered mode=" + actual.configValue());
        }
        if (actual != SourceTopologyMode.SCALE_OUT
                && (services.size() != 1 || services.get(0).volumeId() != configuredSourceId)) {
            throw fatal("single-service source.id=" + configuredSourceId
                    + " does not match discovered persistence service " + services);
        }
        return new SourceTopology(configured, actual, services,
                fingerprint(actual, services, plans));
    }

    void requireUnchanged(SourceTopology current) {
        if (!actualMode.equals(current.actualMode) || !fingerprint.equals(current.fingerprint)) {
            throw new IllegalStateException("FATAL_TOPOLOGY_CHANGE: startup=" + summary()
                    + " current=" + current.summary());
        }
    }

    String summary() {
        return "configured=" + configuredMode.configValue()
                + ",actual=" + actualMode.configValue()
                + ",services=" + services.size()
                + ",fingerprint=" + fingerprint;
    }

    List<SourceStreamId> streams() {
        return services.stream().map(SourceStreamId::from).sorted().toList();
    }

    private static String fingerprint(
            SourceTopologyMode actual,
            List<PersistenceService> services,
            List<TableDecodePlan> plans) {
        MessageDigest digest = digest();
        update(digest, actual.configValue());
        for (PersistenceService service : services) {
            update(digest, service.host());
            update(digest, Integer.toString(service.port()));
            update(digest, service.serviceName());
            update(digest, Long.toString(service.volumeId()));
        }
        plans.stream()
                .sorted(java.util.Comparator.comparing(value ->
                        value.metadata().qualifiedName()))
                .forEach(plan -> {
                    update(digest, plan.metadata().qualifiedName());
                    update(digest, plan.topologyFingerprint());
                });
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
            throw new IllegalStateException(impossible);
        }
    }

    private static IllegalArgumentException fatal(String detail) {
        return new IllegalArgumentException("FATAL_SOURCE_TOPOLOGY: " + detail);
    }

    private record ServiceIdentity(String host, int port, long volumeId) {}
}

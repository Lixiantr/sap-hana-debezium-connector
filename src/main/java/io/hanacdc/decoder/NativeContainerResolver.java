package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;
import io.hanacdc.schema.TableDecodePlan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Binds a supported catalog data container to its plan and generation. */
public final class NativeContainerResolver {
    public record Binding(
            TableDecodePlan plan,
            int partId,
            long containerId,
            long generation) {}

    private final Map<Long, TableDecodePlan> plansByObject = new HashMap<>();
    private final Map<Long, Binding> dynamicBindingsByObject = new HashMap<>();
    private final Map<Long, Binding> bindingsByContainer = new HashMap<>();

    public NativeContainerResolver(List<TableDecodePlan> plans) {
        for (TableDecodePlan plan : plans) {
            TableDecodePlan previous = plansByObject.putIfAbsent(
                    plan.metadata().objectId(), plan);
            if (previous != null) {
                throw fatal("duplicate TABLE_OID " + plan.metadata().objectId());
            }
            for (var partition : plan.metadata().partitions()) {
                pin(plan, partition.partId(), partition.containerId(), 0, false);
            }
        }
    }

    public Optional<Binding> observe(NativeRecordHeader record) {
        if (record.outerCode() != NativeRecordReader.OUTER_UNIFIED_TABLE
                || record.containerId() <= 0) {
            return Optional.empty();
        }
        Binding pinned = bindingsByContainer.get(record.containerId());
        if (pinned != null) {
            return Optional.of(pinned);
        }

        // Characterization plans may omit catalog container identity and use a
        // corpus-certified POID. Production selection rejects such plans.
        TableDecodePlan plan = plansByObject.get(record.poid());
        if (plan == null) {
            return Optional.empty();
        }
        if (!plan.metadata().partitions().isEmpty()) {
            throw fatal("unknown container for selected table "
                    + plan.metadata().qualifiedName() + ": 0x"
                    + Long.toUnsignedString(record.containerId(), 16));
        }
        Binding current = dynamicBindingsByObject.get(record.poid());
        if (current == null) {
            return Optional.of(pin(plan, 0, record.containerId(), 0, true));
        }
        if (current.containerId() != record.containerId()) {
            throw fatal("container changed without certified barrier for "
                    + plan.metadata().qualifiedName() + ": old=0x"
                    + Long.toUnsignedString(current.containerId(), 16) + " new=0x"
                    + Long.toUnsignedString(record.containerId(), 16));
        }
        return Optional.of(current);
    }

    public Optional<Binding> byContainer(long containerId) {
        return Optional.ofNullable(bindingsByContainer.get(containerId));
    }

    /** Called only after a certified committed TRUNCATE barrier. */
    public Binding advanceGeneration(long objectId, long newContainerId) {
        TableDecodePlan plan = plansByObject.get(objectId);
        if (plan == null) {
            throw fatal("unknown TABLE_OID " + objectId);
        }
        if (plan.metadata().partitions().size() > 1) {
            throw fatal("multi-partition generation changes require a new topology epoch");
        }
        Binding previous = dynamicBindingsByObject.remove(objectId);
        if (previous == null && plan.metadata().partitions().size() == 1) {
            previous = bindingsByContainer.remove(
                    plan.metadata().partitions().get(0).containerId());
        }
        if (previous != null) {
            bindingsByContainer.remove(previous.containerId());
        }
        long generation = previous == null ? 1 : Math.addExact(previous.generation(), 1);
        return pin(plan, 0, newContainerId, generation, true);
    }

    private Binding pin(
            TableDecodePlan plan,
            int partId,
            long containerId,
            long generation,
            boolean dynamic) {
        if (containerId <= 0) {
            throw fatal("invalid native container id");
        }
        Binding binding = new Binding(plan, partId, containerId, generation);
        Binding collision = bindingsByContainer.putIfAbsent(containerId, binding);
        if (collision != null && collision.plan().metadata().objectId()
                != plan.metadata().objectId()) {
            throw fatal("container belongs to multiple TABLE_OID values");
        }
        if (dynamic) {
            dynamicBindingsByObject.put(plan.metadata().objectId(), binding);
        }
        return binding;
    }

    private static IllegalStateException fatal(String detail) {
        return new IllegalStateException("FATAL_TABLE_BINDING: " + detail);
    }
}

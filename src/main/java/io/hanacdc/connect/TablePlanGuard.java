package io.hanacdc.connect;

import io.hanacdc.schema.TableDecodePlan;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Fail-closed comparison of the startup table plans with a refreshed JDBC catalog view. */
final class TablePlanGuard {
    private record Identity(
            long objectId,
            String schemaFingerprint,
            String topologyFingerprint) {}

    private final Map<String, Identity> startup;

    TablePlanGuard(List<TableDecodePlan> plans) {
        startup = identities(plans);
        if (startup.isEmpty()) {
            throw new IllegalArgumentException("at least one startup table plan is required");
        }
    }

    void validate(List<TableDecodePlan> currentPlans) {
        Map<String, Identity> current = identities(currentPlans);
        if (!startup.keySet().equals(current.keySet())) {
            throw fatal("selected table set changed: startup=" + startup.keySet()
                    + " current=" + current.keySet());
        }
        for (Map.Entry<String, Identity> entry : startup.entrySet()) {
            Identity actual = current.get(entry.getKey());
            if (!entry.getValue().equals(actual)) {
                throw fatal("catalog identity/fingerprint changed for " + entry.getKey()
                        + ": startup=" + entry.getValue() + " current=" + actual);
            }
        }
    }

    private static Map<String, Identity> identities(List<TableDecodePlan> plans) {
        Map<String, Identity> result = new LinkedHashMap<>();
        for (TableDecodePlan plan : plans) {
            String name = plan.metadata().qualifiedName();
            Identity identity = new Identity(plan.metadata().objectId(),
                    plan.schemaFingerprint(), plan.topologyFingerprint());
            if (result.putIfAbsent(name, identity) != null) {
                throw fatal("duplicate selected table " + name);
            }
        }
        return Map.copyOf(result);
    }

    private static IllegalStateException fatal(String detail) {
        return new IllegalStateException("FATAL_SCHEMA_CHANGE: " + detail);
    }
}

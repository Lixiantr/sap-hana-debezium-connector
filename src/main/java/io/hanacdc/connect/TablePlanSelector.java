package io.hanacdc.connect;

import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Builds a deterministic set of plans for selected tables only. */
public final class TablePlanSelector {
    private TablePlanSelector() {}

    public static List<TableDecodePlan> select(
            TableIncludeList includeList,
            List<TableMetadata> availableTables) {
        List<TableDecodePlan> plans = availableTables.stream()
                .filter(table -> includeList.includes(table.schemaName(), table.tableName()))
                .sorted(Comparator.comparing(TableMetadata::qualifiedName))
                .map(TableDecodePlan::compile)
                .toList();
        if (plans.isEmpty()) {
            throw fatal("table.include.list did not match an accessible column table");
        }
        Set<String> names = new HashSet<>();
        Set<Long> objectIds = new HashSet<>();
        Set<Long> containerIds = new HashSet<>();
        for (TableDecodePlan plan : plans) {
            TableMetadata table = plan.metadata();
            if (!names.add(table.qualifiedName())) {
                throw fatal("duplicate catalog table " + table.qualifiedName());
            }
            if (!objectIds.add(table.objectId())) {
                throw fatal("TABLE_OID belongs to multiple selected tables: " + table.objectId());
            }
            if (table.partitions().isEmpty()) {
                throw fatal("at least one current data container is required for selected table "
                        + table.qualifiedName());
            }
            if (table.replicated()) {
                throw fatal("replicated table topology is unsupported: "
                        + table.qualifiedName());
            }
            for (TableMetadata.Partition partition : table.partitions()) {
                if (!containerIds.add(partition.containerId())) {
                    throw fatal("data container belongs to multiple selected tables: 0x"
                            + Long.toUnsignedString(partition.containerId(), 16));
                }
            }
        }
        return List.copyOf(plans);
    }

    private static IllegalArgumentException fatal(String detail) {
        return new IllegalArgumentException("FATAL_SCHEMA_CONTRACT: " + detail);
    }
}

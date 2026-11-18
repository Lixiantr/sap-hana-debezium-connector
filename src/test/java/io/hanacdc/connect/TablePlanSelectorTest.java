package io.hanacdc.connect;

import io.hanacdc.schema.TableMetadata;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class TablePlanSelectorTest {
    @Test
    void selectsOnlyIncludedTablesInStableOrder() {
        TableIncludeList include = TableIncludeList.compile(
                List.of("CDC\\.A", "Quoted Schema\\.Quoted Table"));

        var plans = TablePlanSelector.select(include, List.of(
                table("IGNORED", "T", 3),
                table("Quoted Schema", "Quoted Table", 2),
                table("CDC", "A", 1)));

        assertEquals(List.of("CDC.A", "Quoted Schema.Quoted Table"), plans.stream()
                .map(plan -> plan.metadata().qualifiedName())
                .toList());
    }

    @Test
    void failsClosedWhenNoAccessibleTableMatches() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> TablePlanSelector.select(
                        TableIncludeList.compile(List.of("CDC\\.MISSING")),
                        List.of(table("CDC", "A", 1))));
        assertTrue(error.getMessage().contains("FATAL_SCHEMA_CONTRACT"));
    }

    @Test
    void rejectsDuplicateNativeObjectIdentity() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> TablePlanSelector.select(
                        TableIncludeList.compile(List.of("CDC\\..*")),
                        List.of(table("CDC", "A", 1), table("CDC", "B", 1))));
        assertTrue(error.getMessage().contains("TABLE_OID"));
    }

    @Test
    void acceptsSeveralLeafContainersForOneTable() {
        TableMetadata base = table("CDC", "A", 1);
        TableMetadata partitioned = new TableMetadata(
                base.schemaName(), base.tableName(), base.storageType(), base.objectId(),
                List.of(
                        partition(1, 0xfe00000001L),
                        partition(2, 0xfe00000002L),
                        partition(3, 0xfe00000003L)),
                false, base.columns(), base.auxiliaryColumns(),
                base.primaryKeyColumns());

        var plans = TablePlanSelector.select(
                TableIncludeList.compile(List.of("CDC\\.A")), List.of(partitioned));

        assertEquals(3, plans.get(0).metadata().partitions().size());
    }

    @Test
    void rejectsReplicatedTableAndContainerCollision() {
        TableMetadata base = table("CDC", "A", 1);
        TableMetadata replicated = new TableMetadata(
                base.schemaName(), base.tableName(), base.storageType(), base.objectId(),
                List.of(partition(1, 0xfe00000001L)), true,
                base.columns(), base.auxiliaryColumns(), base.primaryKeyColumns());
        IllegalArgumentException replication = assertThrows(IllegalArgumentException.class,
                () -> TablePlanSelector.select(
                        TableIncludeList.compile(List.of("CDC\\.A")), List.of(replicated)));
        assertTrue(replication.getMessage().contains("replicated"));

        TableMetadata first = withContainer(table("CDC", "A", 1), 0xfe00000001L);
        TableMetadata second = withContainer(table("CDC", "B", 2), 0xfe00000001L);
        IllegalArgumentException collision = assertThrows(IllegalArgumentException.class,
                () -> TablePlanSelector.select(
                        TableIncludeList.compile(List.of("CDC\\..*")),
                        List.of(first, second)));
        assertTrue(collision.getMessage().contains("container"));
    }

    private static TableMetadata withContainer(TableMetadata base, long containerId) {
        return new TableMetadata(
                base.schemaName(), base.tableName(), base.storageType(), base.objectId(),
                List.of(partition(0, containerId)), false, base.columns(),
                base.auxiliaryColumns(), base.primaryKeyColumns());
    }

    private static TableMetadata.Partition partition(int partId, long containerId) {
        return new TableMetadata.Partition(
                partId, containerId, "hana01", 39040, 2, "indexserver");
    }

    private static TableMetadata table(String schema, String name, long objectId) {
        return new TableMetadata(schema, name, "COLUMN", objectId, 0xfe00000000L + objectId,
                List.of(
                        new TableMetadata.Column(1, 201, "ID", "BIGINT", Types.BIGINT,
                                8, 19, 0, false, false, false, ""),
                        new TableMetadata.Column(2, 202, "IS_DELETED", "BOOLEAN", Types.BOOLEAN,
                                1, 0, 0, false, false, false, "FALSE")),
                List.of(),
                List.of("ID"));
    }
}

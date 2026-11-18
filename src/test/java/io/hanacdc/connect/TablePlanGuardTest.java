package io.hanacdc.connect;

import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class TablePlanGuardTest {
    @Test
    void acceptsEquivalentFreshCatalogPlans() {
        TablePlanGuard guard = new TablePlanGuard(List.of(plan(999, 777, false)));
        assertDoesNotThrow(() -> guard.validate(List.of(plan(999, 777, false))));
    }

    @Test
    void rejectsFingerprintObjectContainerAndSelectionDrift() {
        TablePlanGuard guard = new TablePlanGuard(List.of(plan(999, 777, false)));
        assertDrift(guard, List.of(plan(999, 777, true)));
        assertDrift(guard, List.of(plan(1000, 777, false)));
        assertDrift(guard, List.of(plan(999, 778, false)));
        assertDrift(guard, List.of(plan(999, 777, false), otherPlan()));
    }

    private static void assertDrift(TablePlanGuard guard, List<TableDecodePlan> current) {
        var error = assertThrows(IllegalStateException.class,
                () -> guard.validate(current));
        assertTrue(error.getMessage().startsWith("FATAL_SCHEMA_CHANGE:"));
    }

    private static TableDecodePlan plan(long objectId, long containerId, boolean addColumn) {
        List<TableMetadata.Column> columns = new ArrayList<>(List.of(
                column(1, 201, "ID", "BIGINT", Types.BIGINT),
                column(2, 202, "IS_DELETED", "BOOLEAN", Types.BOOLEAN)));
        if (addColumn) columns.add(column(3, 203, "VALUE_TEXT", "NVARCHAR", Types.NVARCHAR));
        return TableDecodePlan.compile(new TableMetadata(
                "CDC", "TARGET", "COLUMN", objectId, containerId,
                columns, List.of(), List.of("ID")));
    }

    private static TableDecodePlan otherPlan() {
        return TableDecodePlan.compile(new TableMetadata(
                "CDC", "OTHER", "COLUMN", 2000, 1777,
                List.of(column(1, 301, "ID", "BIGINT", Types.BIGINT),
                        column(2, 302, "IS_DELETED", "BOOLEAN", Types.BOOLEAN)),
                List.of(), List.of("ID")));
    }

    private static TableMetadata.Column column(
            int ordinal, long internalId, String name, String type, int jdbcType) {
        return new TableMetadata.Column(ordinal, internalId, name, type, jdbcType,
                64, 34, 0, false, false, false, "");
    }
}

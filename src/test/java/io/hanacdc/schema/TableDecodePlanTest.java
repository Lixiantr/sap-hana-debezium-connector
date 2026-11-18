package io.hanacdc.schema;

import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class TableDecodePlanTest {
    @Test
    void compilesStableCompositeKeyAndFingerprint() {
        TableMetadata metadata = metadata(columns(false), List.of("TENANT_ID", "ID"));

        TableDecodePlan first = TableDecodePlan.compile(metadata);
        TableDecodePlan second = TableDecodePlan.compile(metadata);

        assertEquals(List.of(0, 1), first.primaryKeyIndexes());
        assertEquals(2, first.deletedFlagIndex());
        assertEquals(first.schemaFingerprint(), second.schemaFingerprint());

        List<TableMetadata.Column> changed = new ArrayList<>(columns(false));
        changed.set(3, new TableMetadata.Column(4, 204, "VALUE_TEXT", "NVARCHAR",
                Types.NVARCHAR, 201, 0, 0, true, false, false, ""));
        assertNotEquals(first.schemaFingerprint(),
                TableDecodePlan.compile(metadata(changed, List.of("TENANT_ID", "ID")))
                        .schemaFingerprint());
    }

    @Test
    void rejectsMissingKeyGeneratedColumnAndInvalidDeleteFlag() {
        assertViolation(metadata(columns(false), List.of()));
        assertViolation(metadata(columns(true), List.of("TENANT_ID", "ID")));

        List<TableMetadata.Column> nullableDelete = new ArrayList<>(columns(false));
        nullableDelete.set(2, column(3, "IS_DELETED", "BOOLEAN", true));
        assertViolation(metadata(nullableDelete, List.of("TENANT_ID", "ID")));
    }

    @Test
    void projectsVisibleAndCataloguedAuxiliaryInternalColumns() {
        TableMetadata base = metadata(columns(false), List.of("TENANT_ID", "ID"));
        TableMetadata withAuxiliary = new TableMetadata(
                base.schemaName(), base.tableName(), base.storageType(),
                base.objectId(), base.containerId(), base.columns(),
                List.of(new TableMetadata.AuxiliaryColumn(
                        205, 5, "C_ST_GEOMETRY.SPATIAL", "SPATIAL")),
                base.primaryKeyColumns());
        TableDecodePlan plan = TableDecodePlan.compile(withAuxiliary);

        var full = plan.project(List.of(201L, 202L, 203L, 204L, 205L));
        assertEquals(List.of(0, 1, 2, 3, -1), full.descriptorToColumnIndex());
        assertTrue(full.fullImage());

        var sparse = plan.project(List.of(202L, 204L));
        assertEquals(List.of(1, 3), sparse.descriptorToColumnIndex());
        assertFalse(sparse.fullImage());

        assertViolation(() -> plan.project(List.of(999L)));
    }

    private static void assertViolation(TableMetadata metadata) {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> TableDecodePlan.compile(metadata));
        assertTrue(error.getMessage().contains("FATAL_SCHEMA_CONTRACT"));
    }

    private static void assertViolation(org.junit.jupiter.api.function.Executable executable) {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, executable);
        assertTrue(error.getMessage().contains("FATAL_SCHEMA_CONTRACT"));
    }

    private static TableMetadata metadata(
            List<TableMetadata.Column> columns, List<String> primaryKey) {
        return new TableMetadata("CDC", "ALL_TYPES", "COLUMN",
                337217991, 0xfe0000017dL, columns, List.of(), primaryKey);
    }

    private static List<TableMetadata.Column> columns(boolean generatedValue) {
        return List.of(
                column(1, "TENANT_ID", "BIGINT", false),
                column(2, "ID", "BIGINT", false),
                column(3, "IS_DELETED", "BOOLEAN", false),
                new TableMetadata.Column(4, 204, "VALUE_TEXT", "NVARCHAR", Types.NVARCHAR,
                        200, 0, 0, true, generatedValue, false, ""));
    }

    private static TableMetadata.Column column(
            int ordinal, String name, String type, boolean nullable) {
        int jdbc = "BOOLEAN".equals(type) ? Types.BOOLEAN
                : "BIGINT".equals(type) ? Types.BIGINT : Types.NVARCHAR;
        return new TableMetadata.Column(ordinal, 200L + ordinal, name, type, jdbc,
                200, 0, 0, nullable, false, false, "");
    }
}

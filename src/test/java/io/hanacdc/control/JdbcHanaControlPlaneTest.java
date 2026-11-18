package io.hanacdc.control;

import io.hanacdc.schema.TableMetadata;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class JdbcHanaControlPlaneTest {
    @Test
    void bindsGlobalCatalogColumnIdsToNativeIdsByNameAndPosition() {
        TableMetadata metadata = JdbcHanaControlPlane.bindNativeColumns(
                "CDC", "ALL_TYPES", "COLUMN", 999,
                0xfe00000181L,
                List.of(column(1, 165913, "ID"), column(2, 165914, "IS_DELETED")),
                List.of(
                        nativeColumn(5, 65536, "$trex_udiv$", "TREX_UDIV"),
                        nativeColumn(7, 65536, "$rowid$", "ROWID"),
                        nativeColumn(201, 1, "ID", null),
                        nativeColumn(202, 2, "IS_DELETED", null),
                        nativeColumn(234, 65536, "$_SYS_SHADOW_X$", "TEXT")),
                List.of("ID"));

        assertEquals(List.of(201L, 202L), metadata.columns().stream()
                .map(TableMetadata.Column::internalColumnId).toList());
        assertEquals(List.of(234L), metadata.auxiliaryColumns().stream()
                .map(TableMetadata.AuxiliaryColumn::internalColumnId).toList());
    }

    @Test
    void failsClosedForMissingOrAmbiguousNativeMapping() {
        var missing = assertThrows(IllegalArgumentException.class,
                () -> JdbcHanaControlPlane.bindNativeColumns(
                        "CDC", "T", "COLUMN", 999,
                        0xfe000001L,
                        List.of(column(1, 1, "ID")), List.of(), List.of("ID")));
        assertTrue(missing.getMessage().contains("expected one native column"));

        var ambiguous = assertThrows(IllegalArgumentException.class,
                () -> JdbcHanaControlPlane.bindNativeColumns(
                        "CDC", "T", "COLUMN", 999,
                        0xfe000001L,
                        List.of(column(1, 1, "ID")),
                        List.of(nativeColumn(201, 1, "ID", null),
                                nativeColumn(301, 1, "ID", "")),
                        List.of("ID")));
        assertTrue(ambiguous.getMessage().contains("found 2"));
    }

    @Test
    void preservesAllCataloguedPartitionsAndReplicaState() {
        List<TableMetadata.Partition> partitions = List.of(
                new TableMetadata.Partition(0, 1001, "hana01", 39003, 2, "indexserver"),
                new TableMetadata.Partition(1, 1002, "hana01", 39003, 2, "indexserver"));

        TableMetadata metadata = JdbcHanaControlPlane.bindNativeColumns(
                "CDC", "T", "COLUMN", 999, partitions, true,
                List.of(column(1, 1, "ID")),
                List.of(nativeColumn(201, 1, "ID", null)),
                List.of("ID"));

        assertEquals(partitions, metadata.partitions());
        assertTrue(metadata.replicated());
    }

    private static TableMetadata.Column column(
            int position, long catalogId, String name) {
        String type = "IS_DELETED".equals(name) ? "BOOLEAN" : "BIGINT";
        int sqlType = "IS_DELETED".equals(name) ? Types.BOOLEAN : Types.BIGINT;
        return new TableMetadata.Column(position, catalogId, name, type, sqlType,
                19, 19, 0, false, false, false, "");
    }

    private static JdbcHanaControlPlane.NativeColumn nativeColumn(
            long id, int position, String name, String attribute) {
        return new JdbcHanaControlPlane.NativeColumn(id, position, name, attribute);
    }
}

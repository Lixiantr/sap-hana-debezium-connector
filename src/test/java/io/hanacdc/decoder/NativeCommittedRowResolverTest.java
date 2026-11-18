package io.hanacdc.decoder;

import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NativeCommittedRowResolverTest {
    private static final long LOCATOR = 0xfe000194L;

    @Test
    void missingReferencedLocatorBlocksWholeCommittedBatch() {
        var error = assertThrows(java.io.IOException.class,
                () -> NativeCommittedRowResolver.resolve(
                        List.of(row(1, "inline"), row(2, null)), Map.of()));
        assertTrue(error.getMessage().contains("FATAL_INCOMPLETE_LARGE_VALUE"));
    }

    @Test
    void completeLocatorResolvesBeforeBatchCanEscape() throws Exception {
        var resolved = NativeCommittedRowResolver.resolve(
                List.of(row(1, null)), Map.of(LOCATOR,
                        new NativeLobReconstructor.LobValue(LOCATOR,
                                "готово".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                                6, "VIRTUAL_FILE")));
        assertEquals(1, resolved.size());
        assertEquals("готово", resolved.get(0).values().get(3).value());
    }

    private static NativeRowDecoder.DecodedRow row(long position, String inline) {
        var lob = inline == null
                ? NativeRowDecoder.ColumnValue.lobReference("CLOB", LOCATOR)
                : NativeRowDecoder.ColumnValue.scalar(inline);
        return new NativeRowDecoder.DecodedRow(
                plan(), 77, position, 42, 0xfe000001L, NativeRecordReader.UT_UPSERT, true,
                List.of(
                        NativeRowDecoder.ColumnValue.scalar(7L),
                        NativeRowDecoder.ColumnValue.scalar(8L),
                        NativeRowDecoder.ColumnValue.scalar(false),
                        lob));
    }

    private static TableDecodePlan plan() {
        return TableDecodePlan.compile(new TableMetadata(
                "CDC", "LOB_ROW", "COLUMN", 999, 0,
                List.of(
                        column(1, "ID", "BIGINT", Types.BIGINT),
                        column(2, "ROW_VERSION", "BIGINT", Types.BIGINT),
                        column(3, "IS_DELETED", "BOOLEAN", Types.BOOLEAN),
                        column(4, "C_CLOB", "CLOB", Types.CLOB)),
                List.of(), List.of("ID")));
    }

    private static TableMetadata.Column column(
            int ordinal, String name, String type, int dataTypeId) {
        return new TableMetadata.Column(ordinal, 200 + ordinal, name, type, dataTypeId,
                64, 34, 0, false, false, false, "");
    }
}

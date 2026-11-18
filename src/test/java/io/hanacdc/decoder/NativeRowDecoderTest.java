package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;
import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.sql.Types;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NativeRowDecoderTest {
    @Test
    void decodesDescriptorDrivenFullSimpleRow() throws Exception {
        byte[] payload = payload();
        TableDecodePlan plan = plan();
        NativeRecordHeader record = new NativeRecordHeader(
                new BackupFile(Path.of("log_backup_2_0_1_4.5"), 2, 0, 1, 4, 5),
                4096, 1, 77, NativeRecordReader.OUTER_UNIFIED_TABLE,
                payload.length, 192, 999, 0xfe000001L,
                NativeRecordReader.UT_UPSERT, 42);

        var row = new NativeRowDecoder().decode(record, plan, payload);

        assertTrue(row.fullImage());
        assertEquals(42, row.rowId());
        assertEquals(7L, row.values().get(0).value());
        assertEquals(8L, row.values().get(1).value());
        assertEquals(false, row.values().get(2).value());
        assertEquals("hello", row.values().get(3).value());
        assertSame(row, new NativeRowDecoder().resolveLobs(row, java.util.Map.of()));
    }

    @Test
    void resolvesLobReferenceOnlyFromCompletedTransactionValue() throws Exception {
        TableDecodePlan plan = plan();
        List<NativeRowDecoder.ColumnValue> values = List.of(
                NativeRowDecoder.ColumnValue.scalar(7L),
                NativeRowDecoder.ColumnValue.scalar(8L),
                NativeRowDecoder.ColumnValue.scalar(false),
                NativeRowDecoder.ColumnValue.lobReference("CLOB", 0xfe000194L));
        var row = new NativeRowDecoder.DecodedRow(
                plan, 77, 1, 42, 0xfe000001L, NativeRecordReader.UT_UPSERT, true, values);
        NativeRowDecoder decoder = new NativeRowDecoder();

        assertEquals(List.of(0xfe000194L), row.lobLocatorIds());
        assertThrows(java.io.IOException.class, () -> decoder.resolveLobs(row, java.util.Map.of()));
        var resolved = decoder.resolveLobs(row, java.util.Map.of(
                0xfe000194L, new NativeLobReconstructor.LobValue(
                        0xfe000194L, "готово".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        6, "VIRTUAL_FILE")));
        assertEquals("готово", resolved.values().get(3).value());
        assertTrue(resolved.lobLocatorIds().isEmpty());
    }

    private static byte[] payload() {
        byte[] payload = new byte[139];
        payload[48] = (byte) 0x81;
        payload[50] = 4;
        payload[51] = 1;
        payload[52] = 1;
        ByteBuffer bytes = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        for (int index = 0; index < 4; index++) {
            bytes.putInt(53 + index * 4, 201 + index);
        }
        int body = 69;
        byte[] descriptor = {
                0x00, (byte) 0xc1, 0x60, 0x04, 0x01, 0x00, 0x27,
                (byte) 0xf0, 0x20, 0x59, 0x04,
                (byte) 0xf0, 0x20, 0x59, 0x0c,
                0x49, 0x20, 0x54, 0x14,
                0x53, 0x22, 0x30, 0x18
        };
        System.arraycopy(descriptor, 0, payload, body, descriptor.length);
        bytes.putLong(96, 7);
        bytes.putLong(104, 8);
        payload[112] = 0;
        payload[116] = 5;
        System.arraycopy("hello".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                0, payload, 117, 5);
        return payload;
    }

    private static TableDecodePlan plan() {
        return TableDecodePlan.compile(new TableMetadata(
                "CDC", "SIMPLE_ROW", "COLUMN", 999, 0,
                List.of(
                        column(1, 201, "ID", "BIGINT", Types.BIGINT, false),
                        column(2, 202, "ROW_VERSION", "BIGINT", Types.BIGINT, false),
                        column(3, 203, "IS_DELETED", "BOOLEAN", Types.BOOLEAN, false),
                        column(4, 204, "VALUE_TEXT", "NVARCHAR", Types.NVARCHAR, false)),
                List.of(), List.of("ID")));
    }

    private static TableMetadata.Column column(
            int ordinal, long internalId, String name, String type,
            int dataTypeId, boolean nullable) {
        return new TableMetadata.Column(ordinal, internalId, name, type, dataTypeId,
                64, 34, 0, nullable, false, false, "");
    }
}

package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;
import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NativeMultiRowPayloadTest {
    @Test
    void directCellMapKeepsViewsIntoDecoderOwnedMassPayload() throws Exception {
        byte[] wrapped = Files.readAllBytes(Path.of(
                "src/test/resources/native/mass-boundary.bin"));
        ByteBuffer wrapper = ByteBuffer.wrap(wrapped).order(ByteOrder.LITTLE_ENDIAN);
        NativeRecordHeader outer = new NativeRecordHeader(
                new BackupFile(Path.of("log_backup_2_0_400897984_401028480.1786109422595"),
                        2, 0, 400_897_984, 401_028_480, 1_786_109_422_595L),
                3_709_440, wrapper.getLong(0), 720_636,
                NativeRecordReader.OUTER_UNIFIED_TABLE_MASS, wrapped.length,
                NativeRecordReader.align64(wrapped.length + 32L),
                -1, -1, -1, -1);
        var inner = NativeMassRecord.unwrap(
                new NativeBackupDecoder.NativeRecord(outer, wrapped));
        byte[] payload = inner.payloadView();

        NativeMultiRowPayload.Rows rows = NativeMultiRowPayload.parse(
                inner.header(), allTypesPlan(
                        inner.header().poid(), inner.header().containerId()), payload);

        assertEquals(293, rows.rowCount());
        for (ByteSlice[] column : rows.cells()) {
            for (ByteSlice cell : column) {
                assertSame(payload, cell.source());
            }
        }
    }

    @Test
    void unwrapsAndDecodesRealTwoHundredNinetyThreeRowHashUpsert()
            throws Exception {
        byte[] wrapped = Files.readAllBytes(Path.of(
                "src/test/resources/native/mass-boundary.bin"));
        ByteBuffer wrapper = ByteBuffer.wrap(wrapped).order(ByteOrder.LITTLE_ENDIAN);
        long position = wrapper.getLong(0);
        NativeRecordHeader outer = new NativeRecordHeader(
                new BackupFile(Path.of("log_backup_2_0_400897984_401028480.1786109422595"),
                        2, 0, 400_897_984, 401_028_480, 1_786_109_422_595L),
                3_709_440, position, 720_636,
                NativeRecordReader.OUTER_UNIFIED_TABLE_MASS, wrapped.length,
                NativeRecordReader.align64(wrapped.length + 32L),
                -1, -1, -1, -1);
        var inner = NativeMassRecord.unwrap(
                new NativeBackupDecoder.NativeRecord(outer, wrapped));

        List<NativeRowDecoder.DecodedRow> rows = new NativeRowDecoder().decodeAll(
                inner.header(), allTypesPlan(
                        inner.header().poid(), inner.header().containerId()), inner.payload());

        assertEquals(293, rows.size());
        assertTrue(rows.stream().allMatch(NativeRowDecoder.DecodedRow::fullImage));
        assertEquals(inner.header().firstRowId(), rows.get(0).rowId());
        assertEquals(inner.header().firstRowId() + 292, rows.get(292).rowId());
        assertTrue(rows.stream().flatMap(row -> row.values().stream())
                .noneMatch(value -> value.kind() == NativeRowDecoder.ValueKind.MISSING));
    }

    @Test
    void parallelDecodePreservesAllRowsAndOrder() throws Exception {
        byte[] wrapped = Files.readAllBytes(Path.of(
                "src/test/resources/native/mass-boundary.bin"));
        ByteBuffer wrapper = ByteBuffer.wrap(wrapped).order(ByteOrder.LITTLE_ENDIAN);
        long position = wrapper.getLong(0);
        NativeRecordHeader outer = new NativeRecordHeader(
                new BackupFile(Path.of("log_backup_2_0_400897984_401028480.1786109422595"),
                        2, 0, 400_897_984, 401_028_480, 1_786_109_422_595L),
                3_709_440, position, 720_636,
                NativeRecordReader.OUTER_UNIFIED_TABLE_MASS, wrapped.length,
                NativeRecordReader.align64(wrapped.length + 32L),
                -1, -1, -1, -1);
        var inner = NativeMassRecord.unwrap(
                new NativeBackupDecoder.NativeRecord(outer, wrapped));
        var executor = Executors.newFixedThreadPool(3);
        try {
            List<NativeRowDecoder.DecodedRow> rows = new NativeRowDecoder().decodeAll(
                    inner.header(), allTypesPlan(
                            inner.header().poid(), inner.header().containerId()),
                    inner.payload(), executor, 3);
            assertEquals(293, rows.size());
            for (int index = 0; index < rows.size(); index++) {
                assertEquals(inner.header().firstRowId() + index, rows.get(index).rowId());
            }
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    void unwrapsAndDecodesRealOneHundredEightySevenRowRangeInsert()
            throws Exception {
        byte[] wrapped = Files.readAllBytes(Path.of(
                "src/test/resources/native/mass-frame.bin"));
        ByteBuffer wrapper = ByteBuffer.wrap(wrapped).order(ByteOrder.LITTLE_ENDIAN);
        long position = wrapper.getLong(0);
        NativeRecordHeader outer = new NativeRecordHeader(
                new BackupFile(Path.of("log_backup_2_0_399865344_400022016.1786108942192"),
                        2, 0, 399_865_344, 400_022_016, 1_786_108_942_192L),
                2_834_560, position, 719_987,
                NativeRecordReader.OUTER_UNIFIED_TABLE_MASS, wrapped.length,
                NativeRecordReader.align64(wrapped.length + 32L),
                -1, -1, -1, -1);
        var inner = NativeMassRecord.unwrap(
                new NativeBackupDecoder.NativeRecord(outer, wrapped));

        List<NativeRowDecoder.DecodedRow> rows = new NativeRowDecoder().decodeAll(
                inner.header(), allTypesPlan(
                        inner.header().poid(), inner.header().containerId()), inner.payload());

        assertEquals(187, rows.size());
        assertTrue(rows.stream().allMatch(NativeRowDecoder.DecodedRow::fullImage));
        assertEquals(inner.header().firstRowId(), rows.get(0).rowId());
        assertEquals(inner.header().firstRowId() + 186, rows.get(186).rowId());
        assertTrue(rows.stream().flatMap(row -> row.values().stream())
                .noneMatch(value -> value.kind() == NativeRowDecoder.ValueKind.MISSING));
    }

    @Test
    void unwrapsAndDecodesRealTwoHundredNinetyThreeRowPartitionedInsert()
            throws Exception {
        byte[] wrapped = Files.readAllBytes(Path.of(
                "src/test/resources/native/mass-insert-293.bin"));
        ByteBuffer wrapper = ByteBuffer.wrap(wrapped).order(ByteOrder.LITTLE_ENDIAN);
        long position = wrapper.getLong(0);
        NativeRecordHeader outer = new NativeRecordHeader(
                new BackupFile(Path.of("log_backup_2_0_398403456_398685696.1786107951378"),
                        2, 0, 398_403_456, 398_685_696, 1_786_107_951_378L),
                98_432, position, 718_663,
                NativeRecordReader.OUTER_UNIFIED_TABLE_MASS, wrapped.length,
                NativeRecordReader.align64(wrapped.length + 32L),
                -1, -1, -1, -1);
        var inner = NativeMassRecord.unwrap(
                new NativeBackupDecoder.NativeRecord(outer, wrapped));

        List<NativeRowDecoder.DecodedRow> rows = new NativeRowDecoder().decodeAll(
                inner.header(), allTypesPlan(
                        inner.header().poid(), inner.header().containerId()), inner.payload());

        assertEquals(293, rows.size());
        assertTrue(rows.stream().allMatch(NativeRowDecoder.DecodedRow::fullImage));
        assertEquals(inner.header().firstRowId(), rows.get(0).rowId());
        assertEquals(inner.header().firstRowId() + 292, rows.get(292).rowId());
        assertTrue(rows.stream().flatMap(row -> row.values().stream())
                .noneMatch(value -> value.kind() == NativeRowDecoder.ValueKind.MISSING));
    }

    @Test
    void decodesTwoRealAllTypesRowsFromCharacterizedCorpus() throws Exception {
        byte[] payload = Files.readAllBytes(Path.of(
                "src/test/resources/native/replace-upsert-batch2.bin"));
        ByteBuffer bytes = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        long poid = Integer.toUnsignedLong(bytes.getInt(4));
        long container = bytes.getLong(8);
        long firstRowId = bytes.getLong(32);
        NativeRecordHeader header = new NativeRecordHeader(
                new BackupFile(Path.of("log_backup_2_0_0_100.1"), 2, 0, 0, 100, 1),
                4096, 1, 77, NativeRecordReader.OUTER_UNIFIED_TABLE,
                payload.length, NativeRecordReader.align64(payload.length + 32L),
                poid, container, NativeRecordReader.UT_UPSERT, firstRowId);

        List<NativeRowDecoder.DecodedRow> rows = new NativeRowDecoder().decodeAll(
                header, allTypesPlan(poid, container), payload);

        assertEquals(2, rows.size());
        assertTrue(rows.stream().allMatch(NativeRowDecoder.DecodedRow::fullImage));
        assertEquals(firstRowId, rows.get(0).rowId());
        assertEquals(firstRowId + 1, rows.get(1).rowId());
        assertTrue((Boolean) rows.get(0).values().get(2).value());
        assertFalse((Boolean) rows.get(1).values().get(2).value());
        assertNotEquals(rows.get(0).values().get(0).value(),
                rows.get(1).values().get(0).value());
        assertTrue(rows.stream().flatMap(row -> row.values().stream())
                .noneMatch(value -> value.kind() == NativeRowDecoder.ValueKind.MISSING));
    }

    @Test
    void unwrapsAndDecodesRealOneHundredRowMassRecord() throws Exception {
        byte[] wrapped = Files.readAllBytes(Path.of(
                "src/test/resources/native/mass-upsert-100.bin"));
        ByteBuffer wrapper = ByteBuffer.wrap(wrapped).order(ByteOrder.LITTLE_ENDIAN);
        long position = wrapper.getLong(0);
        NativeRecordHeader outer = new NativeRecordHeader(
                new BackupFile(Path.of("log_backup_2_0_0_300000000.1"),
                        2, 0, 0, 300_000_000, 1),
                4096, position, 348556,
                NativeRecordReader.OUTER_UNIFIED_TABLE_MASS, wrapped.length,
                NativeRecordReader.align64(wrapped.length + 32L),
                -1, -1, -1, -1);
        var inner = NativeMassRecord.unwrap(
                new NativeBackupDecoder.NativeRecord(outer, wrapped));
        List<NativeRowDecoder.DecodedRow> rows = new NativeRowDecoder().decodeAll(
                inner.header(), allTypesPlan(
                        inner.header().poid(), inner.header().containerId()), inner.payload());

        assertEquals(100, rows.size());
        assertTrue(rows.stream().allMatch(NativeRowDecoder.DecodedRow::fullImage));
        assertEquals(inner.header().firstRowId(), rows.get(0).rowId());
        assertEquals(inner.header().firstRowId() + 99, rows.get(99).rowId());
        assertTrue(rows.stream().flatMap(row -> row.values().stream())
                .noneMatch(value -> value.kind() == NativeRowDecoder.ValueKind.MISSING));
    }

    @Test
    void pipelinePairsPhysicalDeleteWithOneHundredRowMassUpsert() throws Exception {
        byte[] wrapped = Files.readAllBytes(Path.of(
                "src/test/resources/native/mass-upsert-100.bin"));
        ByteBuffer wrapper = ByteBuffer.wrap(wrapped).order(ByteOrder.LITTLE_ENDIAN);
        long position = wrapper.getLong(0);
        byte[] inner = java.util.Arrays.copyOfRange(wrapped, 24, wrapped.length);
        ByteBuffer row = ByteBuffer.wrap(inner).order(ByteOrder.LITTLE_ENDIAN);
        long poid = Integer.toUnsignedLong(row.getInt(4));
        long container = row.getLong(8);
        BackupFile backup = new BackupFile(Path.of("log_backup_2_0_0_300000000.1"),
                2, 0, 0, 300_000_000, 1);
        try (NativeRowLobPipeline pipeline = new NativeRowLobPipeline(
                List.of(allTypesPlan(poid, container)), 1, 8, 200, 512 * 1024)) {
            byte[] deletePayload = new byte[40];
            pipeline.accept(new NativeBackupDecoder.NativeRecord(
                    header(backup, position - 1, 348556,
                            NativeRecordReader.OUTER_UNIFIED_TABLE,
                            deletePayload.length, poid, container,
                            NativeRecordReader.UT_DELETE, 1801), deletePayload));
            pipeline.accept(new NativeBackupDecoder.NativeRecord(
                    header(backup, position, 348556,
                            NativeRecordReader.OUTER_UNIFIED_TABLE_MASS,
                            wrapped.length, -1, -1, -1, -1), wrapped));
            pipeline.accept(new NativeBackupDecoder.NativeRecord(
                    header(backup, position + 1, 348556,
                            NativeRecordReader.OUTER_PREPARE_COMMIT,
                            16, -1, -1, -1, -1), new byte[16]));
            var completed = pipeline.accept(new NativeBackupDecoder.NativeRecord(
                    header(backup, position + 2, 348556,
                            NativeRecordReader.OUTER_COMMIT,
                            32, -1, -1, -1, -1), new byte[32])).orElseThrow();

            assertEquals(100, completed.rows().size());
            assertTrue(completed.physicalDeletes().isEmpty());
        }
    }

    @Test
    void pipelineReconstructsFragmentedMassUpsert() throws Exception {
        byte[] wrapped = Files.readAllBytes(Path.of(
                "src/test/resources/native/mass-upsert-100.bin"));
        byte[] inner = java.util.Arrays.copyOfRange(wrapped, 24, wrapped.length);
        ByteBuffer row = ByteBuffer.wrap(inner).order(ByteOrder.LITTLE_ENDIAN);
        long position = ByteBuffer.wrap(wrapped).order(ByteOrder.LITTLE_ENDIAN).getLong(0);
        long poid = Integer.toUnsignedLong(row.getInt(4));
        long container = row.getLong(8);
        BackupFile backup = new BackupFile(Path.of("log_backup_2_0_0_300000000.1"),
                2, 0, 0, 300_000_000, 1);
        int split = 100_000;
        byte[] first = fragment(position, inner, 0, split);
        byte[] second = fragment(position, inner, split, inner.length);
        try (NativeRowLobPipeline pipeline = new NativeRowLobPipeline(
                List.of(allTypesPlan(poid, container)), 1, 8, 200, 512 * 1024)) {
            assertTrue(pipeline.accept(new NativeBackupDecoder.NativeRecord(
                    header(backup, position, 348556,
                            NativeRecordReader.OUTER_UNIFIED_TABLE_MASS,
                            first.length, -1, -1, -1, -1), first)).isEmpty());
            assertTrue(pipeline.accept(new NativeBackupDecoder.NativeRecord(
                    header(backup, position + 2_000, 348556,
                            NativeRecordReader.OUTER_UNIFIED_TABLE_MASS,
                            second.length, -1, -1, -1, -1), second)).isEmpty());
            pipeline.accept(new NativeBackupDecoder.NativeRecord(
                    header(backup, position + 4_000, 348556,
                            NativeRecordReader.OUTER_PREPARE_COMMIT,
                            16, -1, -1, -1, -1), new byte[16]));
            var completed = pipeline.accept(new NativeBackupDecoder.NativeRecord(
                    header(backup, position + 4_001, 348556,
                            NativeRecordReader.OUTER_COMMIT,
                            32, -1, -1, -1, -1), new byte[32])).orElseThrow();
            assertEquals(100, completed.rows().size());
        }
    }

    private static byte[] fragment(long position, byte[] inner, int start, int end) {
        byte[] result = new byte[24 + end - start];
        ByteBuffer header = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
        header.putLong(position).putLong(start).putInt(inner.length).putInt(0);
        System.arraycopy(inner, start, result, 24, end - start);
        return result;
    }

    private static NativeRecordHeader header(
            BackupFile backup, long position, long tid, int outer, int payload,
            long poid, long container, int operation, long rowId) {
        return new NativeRecordHeader(backup, 4096, position, tid, outer, payload,
                NativeRecordReader.align64(payload + 32L), poid, container, operation, rowId);
    }

    private static TableDecodePlan allTypesPlan(long poid, long container) {
        String[][] definitions = {
                {"ID", "BIGINT"}, {"ROW_VERSION", "BIGINT"},
                {"IS_DELETED", "BOOLEAN"}, {"EVENT_TS", "TIMESTAMP"},
                {"EVENT_ENVELOPE", "NVARCHAR"}, {"C_TINYINT", "TINYINT"},
                {"C_SMALLINT", "SMALLINT"}, {"C_INTEGER", "INTEGER"},
                {"C_BIGINT", "BIGINT"}, {"C_DECIMAL", "DECIMAL"},
                {"C_SMALLDECIMAL", "SMALLDECIMAL"}, {"C_REAL", "REAL"},
                {"C_DOUBLE", "DOUBLE"}, {"C_BOOLEAN", "BOOLEAN"},
                {"C_CHAR", "CHAR"}, {"C_VARCHAR", "VARCHAR"},
                {"C_NCHAR", "NCHAR"}, {"C_NVARCHAR", "NVARCHAR"},
                {"C_ALPHANUM", "ALPHANUM"}, {"C_SHORTTEXT", "SHORTTEXT"},
                {"C_BINARY", "BINARY"}, {"C_VARBINARY", "VARBINARY"},
                {"C_DATE", "DATE"}, {"C_TIME", "TIME"},
                {"C_SECONDDATE", "SECONDDATE"}, {"C_TIMESTAMP", "TIMESTAMP"},
                {"C_CLOB", "CLOB"}, {"C_NCLOB", "NCLOB"},
                {"C_BLOB", "BLOB"}, {"C_TEXT", "TEXT"},
                {"C_BINTEXT", "BINTEXT"}, {"C_ST_POINT", "ST_POINT"},
                {"C_ST_GEOMETRY", "ST_GEOMETRY"}
        };
        List<TableMetadata.Column> columns = new ArrayList<>();
        for (int index = 0; index < definitions.length; index++) {
            String type = definitions[index][1];
            columns.add(new TableMetadata.Column(index + 1, 201L + index,
                    definitions[index][0], type, jdbcType(type),
                    2048, "DECIMAL".equals(type) ? 34 : 0,
                    "DECIMAL".equals(type) ? 10 : 0,
                    false, false, false, ""));
        }
        return TableDecodePlan.compile(new TableMetadata(
                "CDC_CONTRACT_C0803B", "ALL_TYPES", "COLUMN", poid, container,
                columns, List.of(new TableMetadata.AuxiliaryColumn(
                        234, 34, "$_SYS_SHADOW_C_SHORTTEXT$", "TREX")),
                List.of("ID")));
    }

    private static int jdbcType(String type) {
        return switch (type) {
            case "TINYINT" -> Types.TINYINT;
            case "SMALLINT" -> Types.SMALLINT;
            case "INTEGER" -> Types.INTEGER;
            case "BIGINT" -> Types.BIGINT;
            case "REAL" -> Types.REAL;
            case "DOUBLE" -> Types.DOUBLE;
            case "BOOLEAN" -> Types.BOOLEAN;
            case "DECIMAL", "SMALLDECIMAL" -> Types.DECIMAL;
            case "DATE" -> Types.DATE;
            case "TIME" -> Types.TIME;
            case "SECONDDATE", "TIMESTAMP" -> Types.TIMESTAMP;
            case "BINARY", "VARBINARY", "BLOB" -> Types.VARBINARY;
            default -> Types.NVARCHAR;
        };
    }
}

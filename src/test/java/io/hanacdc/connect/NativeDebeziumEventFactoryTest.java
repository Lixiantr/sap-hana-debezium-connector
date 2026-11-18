package io.hanacdc.connect;

import io.hanacdc.decoder.NativeRecordReader;
import io.hanacdc.decoder.NativeRowDecoder;
import io.hanacdc.decoder.NativeRowLobPipeline;
import io.hanacdc.decoder.NativeTransactionReducer;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;
import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Types;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

final class NativeDebeziumEventFactoryTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void createsTypedCreateAndDeleteEnvelopes() {
        TableDecodePlan plan = plan();
        NativeDebeziumEventFactory factory = new NativeDebeziumEventFactory(
                "hana.events", "hana01", "HXE", List.of(plan));
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        var checkpoint = new DurableReducerStateStore.Checkpoint(
                2, "a".repeat(64), temporaryDirectory.resolve("state.bin"));

        SourceRecord create = factory.create(publication(plan,
                NativeTransactionReducer.Operation.CREATE, checkpoint), epoch,
                Map.of("server", "hana01"));
        Struct createEnvelope = (Struct) create.value();
        Struct after = createEnvelope.getStruct("after");
        assertEquals("c", createEnvelope.getString("op"));
        assertNull(createEnvelope.get("before"));
        assertEquals(7L, after.getInt64("ID"));
        assertEquals(new BigDecimal("12.3400"), after.get("AMOUNT"));
        Struct smallDecimal = after.getStruct("FLEX_AMOUNT");
        assertEquals("io.debezium.data.VariableScaleDecimal",
                smallDecimal.schema().name());
        assertEquals(3, smallDecimal.getInt32("scale"));
        assertArrayEquals(new BigDecimal("123.456").unscaledValue().toByteArray(),
                smallDecimal.getBytes("value"));
        JsonConverter converter = new JsonConverter();
        converter.configure(Map.of("schemas.enable", true), false);
        converter.fromConnectData("hana.events", create.valueSchema(), create.value());
        assertEquals("2026-08-05", after.getString("EVENT_DATE"));
        assertArrayEquals(new byte[] {1, 2, 3}, after.getBytes("PAYLOAD"));
        assertEquals(3, create.sourceOffset().get("offset_version"));
        assertEquals(0, create.sourceOffset().get("change_index"));

        SourceRecord delete = factory.create(publication(plan,
                NativeTransactionReducer.Operation.DELETE, checkpoint), epoch,
                Map.of("server", "hana01"));
        Struct deleteEnvelope = (Struct) delete.value();
        assertEquals("d", deleteEnvelope.getString("op"));
        assertNull(deleteEnvelope.get("after"));
        assertEquals(7L, deleteEnvelope.getStruct("before").getInt64("ID"));
    }

    @Test
    void parallelBuilderPreservesPublicationOrder() throws Exception {
        TableDecodePlan plan = plan();
        NativeDebeziumEventFactory factory = new NativeDebeziumEventFactory(
                "hana.events", "hana01", "HXE", List.of(plan));
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        var checkpoint = new DurableReducerStateStore.Checkpoint(
                2, "a".repeat(64), temporaryDirectory.resolve("state-parallel.bin"));
        List<NativeCheckpointCoordinator.Publication> publications = new java.util.ArrayList<>();
        for (int index = 0; index < 96; index++) {
            publications.add(publication(plan,
                    index % 2 == 0 ? NativeTransactionReducer.Operation.CREATE
                            : NativeTransactionReducer.Operation.DELETE,
                    checkpoint));
        }
        try (OrderedNativeEventBuilder builder =
                     new OrderedNativeEventBuilder(factory, 2_000_000_000L, 3)) {
            List<SourceRecord> records = builder.build(
                    publications, epoch, Map.of("server", "hana01"));
            assertEquals(96, records.size());
            for (int index = 0; index < records.size(); index++) {
                Struct envelope = (Struct) records.get(index).value();
                assertEquals(index % 2 == 0 ? "c" : "d", envelope.getString("op"));
            }
        }
    }

    private NativeCheckpointCoordinator.Publication publication(
            TableDecodePlan plan,
            NativeTransactionReducer.Operation operation,
            DurableReducerStateStore.Checkpoint checkpoint) {
        NativeRowDecoder.DecodedRow row = new NativeRowDecoder.DecodedRow(
                plan, 71, 90, 47, 0xfe000001L, NativeRecordReader.UT_UPSERT, true, List.of(
                        NativeRowDecoder.ColumnValue.scalar(7L),
                        NativeRowDecoder.ColumnValue.scalar(new BigDecimal("12.3400")),
                        NativeRowDecoder.ColumnValue.scalar(new BigDecimal("123.456")),
                        NativeRowDecoder.ColumnValue.scalar(LocalDate.of(2026, 8, 5)),
                        NativeRowDecoder.ColumnValue.scalar(new byte[] {1, 2, 3}),
                        NativeRowDecoder.ColumnValue.scalar(
                                operation == NativeTransactionReducer.Operation.DELETE)));
        var change = new NativeTransactionReducer.LogicalChange(
                new NativeTransactionReducer.RowKey(
                        plan.metadata().qualifiedName() + "#" + plan.schemaFingerprint(),
                        List.of(new NativeTransactionReducer.KeyPart(
                                "ID", "BIGINT", "java.lang.Long:7"))),
                operation, row, row.logPosition());
        BackupFile backup = new BackupFile(
                Path.of("log_backup_2_0_0_1000.99"), 2, 0, 0, 1000, 99);
        return new NativeCheckpointCoordinator.Publication(change,
                new NativeRowLobPipeline.TerminalRecord(
                        backup, 100, 4096 + 100 * 64, 101),
                71, 0, 1, checkpoint,
                new ReplayBoundary(99, 0, 1000, 1, 1));
    }

    private static TableDecodePlan plan() {
        return TableDecodePlan.compile(new TableMetadata(
                "CDC", "ALL_TYPES", "COLUMN", 999, 0xfe000001L,
                List.of(
                        column(1, 201, "ID", "BIGINT", Types.BIGINT, 19, 0),
                        column(2, 202, "AMOUNT", "DECIMAL", Types.DECIMAL, 20, 4),
                        column(3, 203, "FLEX_AMOUNT", "SMALLDECIMAL", Types.DECIMAL, 16, 0),
                        column(4, 204, "EVENT_DATE", "DATE", Types.DATE, 10, 0),
                        column(5, 205, "PAYLOAD", "VARBINARY", Types.VARBINARY, 32, 0),
                        column(6, 206, "IS_DELETED", "BOOLEAN", Types.BOOLEAN, 1, 0)),
                List.of(), List.of("ID")));
    }

    private static TableMetadata.Column column(
            int ordinal,
            long internalId,
            String name,
            String type,
            int dataTypeId,
            int precision,
            int scale) {
        return new TableMetadata.Column(ordinal, internalId, name, type, dataTypeId,
                Math.max(1, precision), precision, scale,
                false, false, false, "");
    }
}

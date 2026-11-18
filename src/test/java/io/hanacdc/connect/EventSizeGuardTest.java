package io.hanacdc.connect;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class EventSizeGuardTest {
    @Test
    void smallEventsUseConservativeFastPathWithoutPerRecordSerialization() throws Exception {
        EventSizeGuard guard = new EventSizeGuard(1_048_576);

        for (int index = 0; index < 100; index++) {
            assertTrue(guard.requireWithinLimit(record("value-" + index)) > 0);
        }

        assertEquals(0, guard.exactChecks());
    }

    @Test
    void exactFallbackFailsBeforePublicationAndReportsCertifiedBytes() throws Exception {
        SourceRecord record = record("x".repeat(512));
        long actual = exactBytes(record);

        EventSizeGuard exactPass = new EventSizeGuard(actual);
        assertEquals(actual, exactPass.requireWithinLimit(record));
        assertEquals(1, exactPass.exactChecks());

        EventSizeGuard rejected = new EventSizeGuard(actual - 1);
        IOException error = assertThrows(IOException.class,
                () -> rejected.requireWithinLimit(record));
        assertEquals(1, rejected.exactChecks());
        assertTrue(error.getMessage().contains("FATAL_EVENT_TOO_LARGE"));
        assertTrue(error.getMessage().contains("serialized_bytes=" + actual));
        assertTrue(error.getMessage().contains("maximum_bytes=" + (actual - 1)));
    }

    @Test
    void conservativeBoundCoversAllConnectorValueKindsAndJsonEscapes() throws Exception {
        Schema variableDecimal = SchemaBuilder.struct()
                .name("io.debezium.data.VariableScaleDecimal")
                .field("scale", Schema.INT32_SCHEMA)
                .field("value", Schema.BYTES_SCHEMA)
                .build();
        Schema valueSchema = SchemaBuilder.struct().name("test.AllTypes")
                .field("I32", Schema.INT32_SCHEMA)
                .field("I64", Schema.INT64_SCHEMA)
                .field("F32", Schema.FLOAT32_SCHEMA)
                .field("F64", Schema.FLOAT64_SCHEMA)
                .field("BOOL", Schema.BOOLEAN_SCHEMA)
                .field("TEXT", Schema.STRING_SCHEMA)
                .field("BIN", Schema.BYTES_SCHEMA)
                .field("DEC", Decimal.schema(9))
                .field("VAR_DEC", variableDecimal)
                .build();
        Struct value = new Struct(valueSchema)
                .put("I32", Integer.MIN_VALUE)
                .put("I64", Long.MIN_VALUE)
                .put("F32", Float.NEGATIVE_INFINITY)
                .put("F64", Double.NaN)
                .put("BOOL", false)
                .put("TEXT", "quote=\" slash=\\ control=\n unicode=Ж😀")
                .put("BIN", new byte[] {0, 1, 2, 3, -1})
                .put("DEC", new BigDecimal("-12345678901234567890.123456789"))
                .put("VAR_DEC", new Struct(variableDecimal)
                        .put("scale", 7)
                        .put("value", new byte[] {1, 2, 3, 4, 5}));
        SourceRecord record = record(valueSchema, value);

        EventSizeGuard guard = new EventSizeGuard(Long.MAX_VALUE);
        long upperBound = guard.requireWithinLimit(record);

        assertTrue(upperBound >= exactBytes(record));
        assertEquals(0, guard.exactChecks());
    }

    private static long exactBytes(SourceRecord record) {
        JsonConverter key = converter(true);
        JsonConverter value = converter(false);
        return length(key.fromConnectData(record.topic(), record.keySchema(), record.key()))
                + length(value.fromConnectData(
                        record.topic(), record.valueSchema(), record.value()));
    }

    private static JsonConverter converter(boolean key) {
        JsonConverter converter = new JsonConverter();
        converter.configure(Map.of("schemas.enable", true), key);
        return converter;
    }

    private static int length(byte[] value) {
        return value == null ? 0 : value.length;
    }

    private static SourceRecord record(String payload) {
        Schema valueSchema = SchemaBuilder.struct().name("test.Value")
                .field("PAYLOAD", Schema.STRING_SCHEMA).build();
        return record(valueSchema, new Struct(valueSchema).put("PAYLOAD", payload));
    }

    private static SourceRecord record(Schema valueSchema, Struct value) {
        Schema keySchema = SchemaBuilder.struct().name("test.Key")
                .field("ID", Schema.INT64_SCHEMA).build();
        return new SourceRecord(
                Map.of("server", "hana01"),
                Map.of("position", 7L),
                "hana.events",
                keySchema,
                new Struct(keySchema).put("ID", 7L),
                valueSchema,
                value);
    }
}

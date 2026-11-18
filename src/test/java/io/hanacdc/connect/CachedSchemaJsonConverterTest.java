package io.hanacdc.connect;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class CachedSchemaJsonConverterTest {
    @Test
    void remainsByteIdenticalAcrossAllConnectorValueKindsAndChangingRows() {
        Schema nested = SchemaBuilder.struct().name("test.VariableDecimal")
                .field("scale", Schema.INT32_SCHEMA)
                .field("value", Schema.BYTES_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct().name("test.AllTypes")
                .field("ID", Schema.INT64_SCHEMA)
                .field("TEXT", Schema.STRING_SCHEMA)
                .field("BIN", Schema.BYTES_SCHEMA)
                .field("DEC", Decimal.schema(4))
                .field("FLOAT", Schema.FLOAT64_SCHEMA)
                .field("FLAG", Schema.BOOLEAN_SCHEMA)
                .field("NESTED", nested)
                .build();
        Struct first = row(schema, nested, 1, "quote=\" unicode=Ж😀",
                new byte[] {0, 1, 2, -1}, new BigDecimal("12345.6789"));
        Struct second = row(schema, nested, 2, "different\\value\n".repeat(10),
                ByteBuffer.wrap(new byte[] {9, 8, 7, 6, 5}),
                new BigDecimal("-99999999.0001"));

        JsonConverter reference = reference(true);
        CachedSchemaJsonConverter optimized = optimized(true, 1000);

        assertArrayEquals(reference.fromConnectData("topic", schema, first),
                optimized.fromConnectData("topic", schema, first));
        assertArrayEquals(reference.fromConnectData("topic", schema, second),
                optimized.fromConnectData("topic", schema, second));
        assertEquals(1, optimized.cacheSize());
        assertEquals(reference.toConnectData("topic",
                        reference.fromConnectData("topic", schema, second)),
                optimized.toConnectData("topic",
                        optimized.fromConnectData("topic", schema, second)));
    }

    @Test
    void preservesTombstonesAndSchemalessMode() {
        Schema schema = SchemaBuilder.struct().name("test.Value").optional()
                .field("ID", Schema.INT64_SCHEMA).build();
        Struct value = new Struct(schema).put("ID", 7L);

        JsonConverter referenceWithSchemas = reference(false);
        CachedSchemaJsonConverter withSchemas = optimized(false, 1000);
        assertArrayEquals(referenceWithSchemas.fromConnectData("topic", schema, null),
                withSchemas.fromConnectData("topic", schema, null));
        assertEquals(0, withSchemas.cacheSize());

        Map<String, Object> config = Map.of("schemas.enable", false);
        JsonConverter reference = new JsonConverter();
        reference.configure(config, false);
        CachedSchemaJsonConverter optimized = new CachedSchemaJsonConverter();
        optimized.configure(config, false);
        assertArrayEquals(reference.fromConnectData("topic", schema, value),
                optimized.fromConnectData("topic", schema, value));
        assertEquals(0, optimized.cacheSize());
    }

    @Test
    void boundsIdentityCache() {
        CachedSchemaJsonConverter optimized = optimized(false, 2);
        assertEquals("0.6.0-rc.4", optimized.version());
        for (int index = 0; index < 3; index++) {
            Schema schema = SchemaBuilder.struct().name("test.Value" + index)
                    .field("ID", Schema.INT64_SCHEMA).build();
            optimized.fromConnectData("topic", schema, new Struct(schema).put("ID", 1L));
        }
        assertEquals(2, optimized.cacheSize());
    }

    private static Struct row(
            Schema schema,
            Schema nested,
            long id,
            String text,
            Object bytes,
            BigDecimal decimal) {
        return new Struct(schema)
                .put("ID", id)
                .put("TEXT", text)
                .put("BIN", bytes)
                .put("DEC", decimal)
                .put("FLOAT", Double.NaN)
                .put("FLAG", id % 2 == 0)
                .put("NESTED", new Struct(nested)
                        .put("scale", 3)
                        .put("value", new byte[] {1, 2, 3}));
    }

    private static JsonConverter reference(boolean key) {
        JsonConverter converter = new JsonConverter();
        converter.configure(Map.of("schemas.enable", true), key);
        return converter;
    }

    private static CachedSchemaJsonConverter optimized(boolean key, int cacheSize) {
        CachedSchemaJsonConverter converter = new CachedSchemaJsonConverter();
        converter.configure(Map.of(
                "schemas.enable", true,
                "schemas.cache.size", cacheSize), key);
        return converter;
    }
}

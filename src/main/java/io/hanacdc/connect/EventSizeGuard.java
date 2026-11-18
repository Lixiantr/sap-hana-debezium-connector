package io.hanacdc.connect;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/** Fail-closed pre-publication size gate for the certified JsonConverter deployment. */
final class EventSizeGuard {
    private static final long JSON_NUMBER_BYTES = 32;

    private final long maximumBytes;
    private final JsonConverter keyConverter = converter(true, true);
    private final JsonConverter valueConverter = converter(false, true);
    private final JsonConverter schemalessKeyConverter = converter(true, false);
    private final JsonConverter schemalessValueConverter = converter(false, false);
    private final Map<Schema, Long> keyEnvelopeBytes = new IdentityHashMap<>();
    private final Map<Schema, Long> valueEnvelopeBytes = new IdentityHashMap<>();
    private long exactChecks;

    EventSizeGuard(long maximumBytes) {
        if (maximumBytes <= 0) {
            throw new IllegalArgumentException("maximumBytes must be positive");
        }
        this.maximumBytes = maximumBytes;
    }

    long requireWithinLimit(SourceRecord record) throws IOException {
        long upperBound;
        try {
            upperBound = genericRecordUpperBound(record);
        }
        catch (UnsupportedBound | ArithmeticException ignored) {
            return exactAndRequire(record);
        }
        if (upperBound <= maximumBytes) {
            return upperBound;
        }
        return exactAndRequire(record);
    }

    private long genericRecordUpperBound(SourceRecord record) {
        return add(
                serializedUpperBound(record.topic(), record.keySchema(), record.key(),
                        keyConverter, schemalessKeyConverter, keyEnvelopeBytes),
                serializedUpperBound(record.topic(), record.valueSchema(), record.value(),
                        valueConverter, schemalessValueConverter, valueEnvelopeBytes));
    }

    long exactChecks() {
        return exactChecks;
    }

    private long exactAndRequire(SourceRecord record) throws IOException {
        exactChecks++;
        byte[] key = keyConverter.fromConnectData(
                record.topic(), record.keySchema(), record.key());
        byte[] value = valueConverter.fromConnectData(
                record.topic(), record.valueSchema(), record.value());
        long actual = add(length(key), length(value));
        if (actual > maximumBytes) {
            throw new IOException("FATAL_EVENT_TOO_LARGE: serialized_bytes="
                    + actual + " maximum_bytes=" + maximumBytes
                    + " format=json_with_schema");
        }
        return actual;
    }

    private static long serializedUpperBound(
            String topic,
            Schema schema,
            Object value,
            JsonConverter converter,
            JsonConverter schemalessConverter,
            Map<Schema, Long> envelopeCache) {
        if (value == null) {
            return 0;
        }
        if (schema == null) {
            throw UnsupportedBound.INSTANCE;
        }
        Long envelope = envelopeCache.get(schema);
        if (envelope == null) {
            long full = length(converter.fromConnectData(topic, schema, value));
            long payload = length(schemalessConverter.fromConnectData(topic, schema, value));
            envelope = Math.subtractExact(full, payload);
            if (envelope < 0) {
                throw UnsupportedBound.INSTANCE;
            }
            envelopeCache.put(schema, envelope);
        }
        return add(envelope, payloadUpperBound(schema, value));
    }

    private static long payloadUpperBound(Schema schema, Object value) {
        if (value == null) {
            return 4;
        }
        String logicalName = schema.name();
        if (Decimal.LOGICAL_NAME.equals(logicalName)) {
            if (!(value instanceof BigDecimal decimal)) {
                throw UnsupportedBound.INSTANCE;
            }
            return binaryUpperBound(Decimal.fromLogical(schema, decimal).length);
        }
        if (Date.LOGICAL_NAME.equals(logicalName)
                || Time.LOGICAL_NAME.equals(logicalName)
                || Timestamp.LOGICAL_NAME.equals(logicalName)) {
            return JSON_NUMBER_BYTES;
        }
        return switch (schema.type()) {
            case INT8, INT16, INT32, INT64, FLOAT32, FLOAT64 -> JSON_NUMBER_BYTES;
            case BOOLEAN -> 5;
            case STRING -> value instanceof CharSequence text
                    ? jsonStringUpperBound(text) : unsupported();
            case BYTES -> bytesUpperBound(value);
            case STRUCT -> structUpperBound(schema, value);
            case ARRAY -> arrayUpperBound(schema, value);
            case MAP -> throw UnsupportedBound.INSTANCE;
        };
    }

    private static long structUpperBound(Schema schema, Object value) {
        if (!(value instanceof Struct struct)) {
            throw UnsupportedBound.INSTANCE;
        }
        long result = 2;
        List<Field> fields = schema.fields();
        for (int index = 0; index < fields.size(); index++) {
            Field field = fields.get(index);
            if (index > 0) {
                result = add(result, 1);
            }
            result = add(result, jsonStringUpperBound(field.name()));
            result = add(result, 1);
            result = add(result, payloadUpperBound(field.schema(), struct.get(field)));
        }
        return result;
    }

    private static long arrayUpperBound(Schema schema, Object value) {
        if (!(value instanceof List<?> values)) {
            throw UnsupportedBound.INSTANCE;
        }
        long result = 2;
        for (int index = 0; index < values.size(); index++) {
            if (index > 0) {
                result = add(result, 1);
            }
            result = add(result, payloadUpperBound(schema.valueSchema(), values.get(index)));
        }
        return result;
    }

    private static long bytesUpperBound(Object value) {
        if (value instanceof byte[] bytes) {
            return binaryUpperBound(bytes.length);
        }
        if (value instanceof ByteBuffer bytes) {
            return binaryUpperBound(bytes.remaining());
        }
        throw UnsupportedBound.INSTANCE;
    }

    private static long binaryUpperBound(long bytes) {
        long groups = Math.addExact(bytes, 2) / 3;
        return add(Math.multiplyExact(groups, 4), 2);
    }

    private static long jsonStringUpperBound(CharSequence value) {
        return add(Math.multiplyExact((long) value.length(), 6), 2);
    }

    private static long add(long left, long right) {
        return Math.addExact(left, right);
    }

    private static long unsupported() {
        throw UnsupportedBound.INSTANCE;
    }

    private static JsonConverter converter(boolean key, boolean schemasEnabled) {
        JsonConverter converter = new JsonConverter();
        converter.configure(Map.of("schemas.enable", schemasEnabled), key);
        return converter;
    }

    private static int length(byte[] value) {
        return value == null ? 0 : value.length;
    }

    private static final class UnsupportedBound extends RuntimeException {
        private static final UnsupportedBound INSTANCE = new UnsupportedBound();

        private UnsupportedBound() {
            super(null, null, false, false);
        }
    }
}

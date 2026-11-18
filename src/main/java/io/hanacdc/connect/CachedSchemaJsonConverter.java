package io.hanacdc.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Byte-compatible JsonConverter wrapper that caches the immutable schema frame.
 * It delegates all Connect-to-JSON value conversion to the public Kafka converter.
 */
public final class CachedSchemaJsonConverter implements Converter {
    private static final String SCHEMAS_ENABLE = "schemas.enable";
    private static final String SCHEMAS_CACHE_SIZE = "schemas.cache.size";
    private static final int DEFAULT_CACHE_SIZE = 1_000;

    private final JsonConverter fullConverter = new JsonConverter();
    private final JsonConverter payloadConverter = new JsonConverter();
    private final Map<SchemaIdentity, Framing> framings = new LinkedHashMap<>(16, 0.75f, true);

    private volatile boolean schemasEnabled;
    private volatile int maximumCacheEntries = DEFAULT_CACHE_SIZE;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> fullConfig = new HashMap<>(configs);
        schemasEnabled = booleanValue(fullConfig.get(SCHEMAS_ENABLE), true);
        maximumCacheEntries = positiveInt(
                fullConfig.get(SCHEMAS_CACHE_SIZE), DEFAULT_CACHE_SIZE);
        fullConverter.configure(fullConfig, isKey);

        Map<String, Object> payloadConfig = new HashMap<>(fullConfig);
        payloadConfig.put(SCHEMAS_ENABLE, false);
        payloadConverter.configure(payloadConfig, isKey);
        synchronized (framings) {
            framings.clear();
        }
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (!schemasEnabled || value == null) {
            return fullConverter.fromConnectData(topic, schema, value);
        }

        byte[] payload = payloadConverter.fromConnectData(topic, schema, value);
        SchemaIdentity key = new SchemaIdentity(schema);
        Framing cached;
        synchronized (framings) {
            cached = framings.get(key);
        }
        if (cached != null) {
            return cached.wrap(payload);
        }

        byte[] exact = fullConverter.fromConnectData(topic, schema, value);
        Framing inferred = Framing.infer(exact, payload);
        if (inferred != null) {
            synchronized (framings) {
                framings.put(key, inferred);
                while (framings.size() > maximumCacheEntries) {
                    SchemaIdentity eldest = framings.keySet().iterator().next();
                    framings.remove(eldest);
                }
            }
        }
        return exact;
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return fullConverter.toConnectData(topic, value);
    }

    @Override
    public ConfigDef config() {
        return fullConverter.config();
    }

    @Override
    public String version() {
        return HanaSourceConnector.VERSION;
    }

    @Override
    public void close() throws IOException {
        fullConverter.close();
        payloadConverter.close();
        synchronized (framings) {
            framings.clear();
        }
    }

    int cacheSize() {
        synchronized (framings) {
            return framings.size();
        }
    }

    private static boolean booleanValue(Object value, boolean defaultValue) {
        return value == null ? defaultValue : Boolean.parseBoolean(value.toString());
    }

    private static int positiveInt(Object value, int defaultValue) {
        int parsed = value == null ? defaultValue : Integer.parseInt(value.toString());
        if (parsed <= 0) {
            throw new IllegalArgumentException(SCHEMAS_CACHE_SIZE + " must be positive");
        }
        return parsed;
    }

    private static final class SchemaIdentity {
        private final Schema schema;
        private final int hash;

        private SchemaIdentity(Schema schema) {
            this.schema = schema;
            this.hash = System.identityHashCode(schema);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof SchemaIdentity identity && schema == identity.schema;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    private record Framing(byte[] prefix, byte[] suffix) {
        private Framing {
            prefix = prefix.clone();
            suffix = suffix.clone();
        }

        private static Framing infer(byte[] full, byte[] payload) {
            if (full == null || payload == null || full.length <= payload.length) {
                return null;
            }
            int payloadStart = full.length - payload.length - 1;
            if (payloadStart < 0 || full[full.length - 1] != '}') {
                return null;
            }
            for (int index = 0; index < payload.length; index++) {
                if (full[payloadStart + index] != payload[index]) {
                    return null;
                }
            }
            return new Framing(
                    Arrays.copyOfRange(full, 0, payloadStart),
                    Arrays.copyOfRange(full, payloadStart + payload.length, full.length));
        }

        private byte[] wrap(byte[] payload) {
            byte[] output = new byte[Math.addExact(
                    Math.addExact(prefix.length, payload.length), suffix.length)];
            System.arraycopy(prefix, 0, output, 0, prefix.length);
            System.arraycopy(payload, 0, output, prefix.length, payload.length);
            System.arraycopy(suffix, 0, output, prefix.length + payload.length, suffix.length);
            return output;
        }
    }
}

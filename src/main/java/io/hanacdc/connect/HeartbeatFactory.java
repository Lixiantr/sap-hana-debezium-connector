package io.hanacdc.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;
import java.util.Objects;

/** Debezium-compatible heartbeat record that advances the framework offset. */
final class HeartbeatFactory {
    private static final Schema KEY_SCHEMA = SchemaBuilder.struct()
            .name("io.debezium.connector.common.ServerNameKey")
            .field("serverName", Schema.STRING_SCHEMA)
            .build();
    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("io.debezium.connector.common.Heartbeat")
            .field("ts_ms", Schema.INT64_SCHEMA)
            .build();

    private final String serverName;
    private final String topic;

    HeartbeatFactory(String serverName, String topic) {
        this.serverName = required(serverName, "serverName");
        this.topic = required(topic, "topic");
    }

    SourceRecord create(
            Map<String, ?> sourcePartition,
            Map<String, ?> sourceOffset,
            long timestampMillis) {
        Objects.requireNonNull(sourcePartition, "sourcePartition");
        Objects.requireNonNull(sourceOffset, "sourceOffset");
        Struct key = new Struct(KEY_SCHEMA).put("serverName", serverName);
        Struct value = new Struct(VALUE_SCHEMA).put("ts_ms", timestampMillis);
        return new SourceRecord(sourcePartition, sourceOffset, topic, null,
                KEY_SCHEMA, key, VALUE_SCHEMA, value, timestampMillis);
    }

    private static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        String normalized = value.strip();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return normalized;
    }
}

package io.hanacdc.connect;

import io.hanacdc.decoder.ParallelLogDecoder.EventPointer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/** Builds a stable Debezium-compatible envelope without depending on Debezium internals. */
final class DebeziumEventFactory {
    private static final DateTimeFormatter EVENT_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private final String topic;
    private final String connectorName;
    private final String database;
    private final String schemaName;
    private final String tableName;
    private final Schema keySchema;
    private final Schema rowSchema;
    private final Schema sourceSchema;
    private final Schema transactionSchema;
    private final Schema envelopeSchema;

    DebeziumEventFactory(
            String topic,
            String connectorName,
            String database,
            String schemaName,
            String tableName) {
        this.topic = topic;
        this.connectorName = connectorName;
        this.database = database;
        this.schemaName = schemaName;
        this.tableName = tableName;

        String base = connectorName + "." + schemaName + "." + tableName;
        keySchema = SchemaBuilder.struct()
                .name(base + ".Key")
                .field("id", Schema.INT64_SCHEMA)
                .build();
        rowSchema = SchemaBuilder.struct()
                .name(base + ".Value")
                .optional()
                .field("id", Schema.INT64_SCHEMA)
                .field("row_version", Schema.INT64_SCHEMA)
                .field("is_deleted", Schema.BOOLEAN_SCHEMA)
                .field("event_ts", Schema.STRING_SCHEMA)
                .field("payload", Schema.STRING_SCHEMA)
                .build();
        sourceSchema = SchemaBuilder.struct()
                .name(base + ".Source")
                .field("version", Schema.STRING_SCHEMA)
                .field("connector", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("ts_ms", optionalInt64())
                .field("ts_us", optionalInt64())
                .field("ts_ns", optionalInt64())
                .field("snapshot", Schema.STRING_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field("schema", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .field("file", Schema.STRING_SCHEMA)
                .field("backup_id", Schema.INT64_SCHEMA)
                .field("log_position", Schema.INT64_SCHEMA)
                .field("tid", Schema.INT64_SCHEMA)
                .build();
        transactionSchema = SchemaBuilder.struct()
                .name(base + ".Transaction")
                .optional()
                .field("id", Schema.STRING_SCHEMA)
                .field("total_order", Schema.INT64_SCHEMA)
                .field("data_collection_order", Schema.INT64_SCHEMA)
                .build();
        envelopeSchema = SchemaBuilder.struct()
                .name(base + ".Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("source", sourceSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", optionalInt64())
                .field("ts_us", optionalInt64())
                .field("ts_ns", optionalInt64())
                .field("transaction", transactionSchema)
                .build();
    }

    SourceRecord create(
            EventPointer event,
            Map<String, Object> sourcePartition,
            Map<String, Object> sourceOffset) {
        var sourceBackup = event.sourceBackup();
        String timestamp = event.timestamp();
        String payload = event.payload();
        Instant eventInstant = parseInstant(timestamp);
        long emittedMs = System.currentTimeMillis();
        long emittedUs = Math.multiplyExact(emittedMs, 1_000L);
        long emittedNs = Math.multiplyExact(emittedMs, 1_000_000L);

        Struct key = new Struct(keySchema).put("id", event.rowId());
        Struct row = new Struct(rowSchema)
                .put("id", event.rowId())
                .put("row_version", event.rowVersion())
                .put("is_deleted", event.deleted())
                .put("event_ts", timestamp)
                .put("payload", payload);

        Long sourceMs = eventInstant == null ? null : eventInstant.toEpochMilli();
        Long sourceUs = eventInstant == null ? null
                : Math.addExact(Math.multiplyExact(eventInstant.getEpochSecond(), 1_000_000L),
                        eventInstant.getNano() / 1_000L);
        Long sourceNs = eventInstant == null ? null
                : Math.addExact(Math.multiplyExact(eventInstant.getEpochSecond(), 1_000_000_000L),
                        eventInstant.getNano());
        Struct source = new Struct(sourceSchema)
                .put("version", HanaSourceConnector.VERSION)
                .put("connector", "sap-hana-log-backup")
                .put("name", connectorName)
                .put("ts_ms", sourceMs)
                .put("ts_us", sourceUs)
                .put("ts_ns", sourceNs)
                .put("snapshot", "false")
                .put("db", database)
                .put("schema", schemaName)
                .put("table", tableName)
                .put("file", sourceBackup.fileName())
                .put("backup_id", sourceBackup.backupId())
                .put("log_position", event.logPosition())
                .put("tid", event.transactionId());
        Struct transaction = new Struct(transactionSchema)
                .put("id", Long.toString(event.transactionId()))
                .put("total_order", event.sequence())
                .put("data_collection_order", event.sequence());

        Struct envelope = new Struct(envelopeSchema)
                .put("before", event.deleted() ? row : null)
                .put("after", event.deleted() ? null : row)
                .put("source", source)
                .put("op", event.deleted() ? "d" : "u")
                .put("ts_ms", emittedMs)
                .put("ts_us", emittedUs)
                .put("ts_ns", emittedNs)
                .put("transaction", transaction);

        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                topic,
                null,
                keySchema,
                key,
                envelopeSchema,
                envelope,
                emittedMs);
    }

    Schema keySchema() {
        return keySchema;
    }

    Schema envelopeSchema() {
        return envelopeSchema;
    }

    private static Schema optionalInt64() {
        return SchemaBuilder.int64().optional().build();
    }

    private static Instant parseInstant(String value) {
        try {
            return LocalDateTime.parse(value, EVENT_TIME).toInstant(ZoneOffset.UTC);
        }
        catch (DateTimeException ignored) {
            return null;
        }
    }
}

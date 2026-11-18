package io.hanacdc.connect;

import io.hanacdc.decoder.NativeRowDecoder;
import io.hanacdc.decoder.NativeTransactionReducer;
import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.math.BigDecimal;
import java.time.temporal.TemporalAccessor;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Builds typed Debezium-compatible row events from native logical changes. */
final class NativeDebeziumEventFactory {
    private static final String VARIABLE_SCALE_DECIMAL =
            "io.debezium.data.VariableScaleDecimal";
    private record Schemas(
            Schema key,
            Schema row,
            Schema source,
            Schema transaction,
            Schema envelope,
            List<Field> keyFields,
            List<Field> rowFields,
            List<Field> sourceFields,
            List<Field> transactionFields,
            List<Field> envelopeFields) {}

    private final String topic;
    private final String connectorName;
    private final String database;
    private final Map<String, Schemas> schemasByFingerprint = new LinkedHashMap<>();

    NativeDebeziumEventFactory(
            String topic,
            String connectorName,
            String database,
            List<TableDecodePlan> plans) {
        this.topic = required(topic, "topic");
        this.connectorName = required(connectorName, "connectorName");
        this.database = required(database, "database");
        for (TableDecodePlan plan : plans) {
            Schemas previous = schemasByFingerprint.putIfAbsent(
                    plan.schemaFingerprint(), compile(plan));
            if (previous != null) {
                throw new IllegalArgumentException("duplicate schema fingerprint");
            }
        }
    }

    SourceRecord create(
            NativeCheckpointCoordinator.Publication publication,
            SourceEpoch epoch,
            Map<String, Object> sourcePartition) {
        NativeTransactionReducer.LogicalChange change = publication.change();
        NativeRowDecoder.DecodedRow decoded = change.row();
        TableDecodePlan plan = decoded.plan();
        Schemas schemas = schemasByFingerprint.get(plan.schemaFingerprint());
        if (schemas == null) {
            throw new IllegalArgumentException("unregistered native table plan");
        }
        Struct row = row(schemas, decoded);
        Struct key = key(schemas, decoded);
        long emittedMs = System.currentTimeMillis();
        var terminal = publication.terminal();
        TableMetadata metadata = plan.metadata();
        Struct source = new Struct(schemas.source())
                .put(schemas.sourceFields().get(0), HanaSourceConnector.VERSION)
                .put(schemas.sourceFields().get(1), "sap-hana-log-backup")
                .put(schemas.sourceFields().get(2), connectorName)
                .put(schemas.sourceFields().get(3), null)
                .put(schemas.sourceFields().get(4), "false")
                .put(schemas.sourceFields().get(5), database)
                .put(schemas.sourceFields().get(6), metadata.schemaName())
                .put(schemas.sourceFields().get(7), metadata.tableName())
                .put(schemas.sourceFields().get(8), epoch.id())
                .put(schemas.sourceFields().get(9), terminal.backup().fileName())
                .put(schemas.sourceFields().get(10), terminal.backup().backupId())
                .put(schemas.sourceFields().get(11), terminal.logPosition())
                .put(schemas.sourceFields().get(12), publication.transactionId());
        Struct transaction = new Struct(schemas.transaction())
                .put(schemas.transactionFields().get(0),
                        Long.toString(publication.transactionId()))
                .put(schemas.transactionFields().get(1), terminal.logPosition())
                .put(schemas.transactionFields().get(2),
                        (long) publication.changeIndex() + 1);
        boolean deleted = change.operation() == NativeTransactionReducer.Operation.DELETE;
        Struct envelope = new Struct(schemas.envelope())
                .put(schemas.envelopeFields().get(0), deleted ? row : null)
                .put(schemas.envelopeFields().get(1), deleted ? null : row)
                .put(schemas.envelopeFields().get(2), source)
                .put(schemas.envelopeFields().get(3), change.operation().debeziumCode())
                .put(schemas.envelopeFields().get(4), emittedMs)
                .put(schemas.envelopeFields().get(5), transaction);
        return new SourceRecord(sourcePartition, publication.sourceOffset(epoch), topic, null,
                schemas.key(), key, schemas.envelope(), envelope, emittedMs);
    }

    private Schemas compile(TableDecodePlan plan) {
        String base = connectorName + ".hana." + plan.schemaFingerprint();
        SchemaBuilder key = SchemaBuilder.struct().name(base + ".Key");
        for (int index : plan.primaryKeyIndexes()) {
            TableMetadata.Column column = plan.metadata().columns().get(index);
            key.field(column.name(), columnSchema(column, false));
        }
        Schema keySchema = key.build();

        SchemaBuilder row = SchemaBuilder.struct().name(base + ".Value").optional();
        for (TableMetadata.Column column : plan.metadata().columns()) {
            row.field(column.name(), columnSchema(column, column.nullable()));
        }
        Schema rowSchema = row.build();
        Schema sourceSchema = SchemaBuilder.struct().name(base + ".Source")
                .field("version", Schema.STRING_SCHEMA)
                .field("connector", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("ts_ms", optional(SchemaBuilder.int64()))
                .field("snapshot", Schema.STRING_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field("schema", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .field("source_epoch", Schema.STRING_SCHEMA)
                .field("file", Schema.STRING_SCHEMA)
                .field("backup_id", Schema.INT64_SCHEMA)
                .field("log_position", Schema.INT64_SCHEMA)
                .field("tid", Schema.INT64_SCHEMA)
                .build();
        Schema transactionSchema = SchemaBuilder.struct().name(base + ".Transaction")
                .optional()
                .field("id", Schema.STRING_SCHEMA)
                .field("total_order", Schema.INT64_SCHEMA)
                .field("data_collection_order", Schema.INT64_SCHEMA)
                .build();
        Schema envelopeSchema = SchemaBuilder.struct().name(base + ".Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("source", sourceSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .field("transaction", transactionSchema)
                .build();
        return new Schemas(keySchema, rowSchema, sourceSchema,
                transactionSchema, envelopeSchema,
                keySchema.fields(), rowSchema.fields(), sourceSchema.fields(),
                transactionSchema.fields(), envelopeSchema.fields());
    }

    private static Struct key(Schemas schemas, NativeRowDecoder.DecodedRow row) {
        Struct result = new Struct(schemas.key());
        for (int keyIndex = 0;
                keyIndex < row.plan().primaryKeyIndexes().size(); keyIndex++) {
            int columnIndex = row.plan().primaryKeyIndexes().get(keyIndex);
            TableMetadata.Column column = row.plan().metadata().columns().get(columnIndex);
            Field field = schemas.keyFields().get(keyIndex);
            result.put(field, connectValue(field.schema(),
                    column, row.values().get(columnIndex).value()));
        }
        return result;
    }

    private static Struct row(Schemas schemas, NativeRowDecoder.DecodedRow row) {
        Struct result = new Struct(schemas.row());
        for (int index = 0; index < row.values().size(); index++) {
            NativeRowDecoder.ColumnValue value = row.values().get(index);
            if (value.kind() == NativeRowDecoder.ValueKind.MISSING
                    || value.kind() == NativeRowDecoder.ValueKind.LOB_REFERENCE) {
                throw new IllegalArgumentException("native publication contains incomplete row");
            }
            TableMetadata.Column column = row.plan().metadata().columns().get(index);
            Field field = schemas.rowFields().get(index);
            result.put(field, value.kind() == NativeRowDecoder.ValueKind.NULL
                    ? null : connectValue(field.schema(),
                            column, value.value()));
        }
        return result;
    }

    private static Schema columnSchema(TableMetadata.Column column, boolean nullable) {
        String type = column.typeName().toUpperCase(Locale.ROOT);
        SchemaBuilder builder = switch (type) {
            case "TINYINT", "SMALLINT", "INTEGER", "INT" -> SchemaBuilder.int32();
            case "BIGINT" -> SchemaBuilder.int64();
            case "REAL" -> SchemaBuilder.float32();
            case "DOUBLE" -> SchemaBuilder.float64();
            case "BOOLEAN" -> SchemaBuilder.bool();
            case "DECIMAL" -> Decimal.builder(column.scale());
            case "SMALLDECIMAL" -> variableScaleDecimalBuilder();
            case "BINARY", "VARBINARY", "BLOB" -> SchemaBuilder.bytes();
            case "DATE", "TIME", "SECONDDATE", "TIMESTAMP",
                    "CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "SHORTTEXT",
                    "ALPHANUM", "CLOB", "NCLOB", "TEXT", "BINTEXT",
                    "ST_POINT", "ST_GEOMETRY" -> SchemaBuilder.string();
            default -> throw new IllegalArgumentException(
                    "unsupported Connect schema type " + type);
        };
        return nullable ? optional(builder) : builder.build();
    }

    private static Schema optional(SchemaBuilder builder) {
        return builder.optional().build();
    }

    private static SchemaBuilder variableScaleDecimalBuilder() {
        return SchemaBuilder.struct()
                .name(VARIABLE_SCALE_DECIMAL)
                .doc("Variable scaled decimal")
                .field("scale", Schema.INT32_SCHEMA)
                .field("value", Schema.BYTES_SCHEMA);
    }

    private static Object connectValue(
            Schema schema, TableMetadata.Column column, Object value) {
        if (value instanceof BigDecimal decimal
                && "SMALLDECIMAL".equals(column.typeName())) {
            return new Struct(schema)
                    .put("scale", decimal.scale())
                    .put("value", decimal.unscaledValue().toByteArray());
        }
        return connectValue(value);
    }

    private static Object connectValue(Object value) {
        if (value instanceof TemporalAccessor) {
            return value.toString();
        }
        if (value instanceof BigDecimal decimal) {
            return decimal;
        }
        return value;
    }

    private static String required(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }
}

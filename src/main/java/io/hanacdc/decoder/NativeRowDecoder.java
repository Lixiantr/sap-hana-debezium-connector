package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;
import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/** Generic fail-closed row-image decoder governed by a pinned catalog plan. */
public final class NativeRowDecoder {
    public enum ValueKind { MISSING, NULL, SCALAR, LOB_REFERENCE }

    public record LobReference(String typeName, long locatorId) {
        public LobReference {
            if (typeName == null || typeName.isBlank() || locatorId <= 0) {
                throw new IllegalArgumentException("invalid LOB reference");
            }
        }
    }

    public record ColumnValue(ValueKind kind, Object value) {
        public ColumnValue {
            if ((kind == ValueKind.MISSING || kind == ValueKind.NULL) && value != null) {
                throw new IllegalArgumentException("missing/null value must not carry data");
            }
            if ((kind == ValueKind.SCALAR || kind == ValueKind.LOB_REFERENCE) && value == null) {
                throw new IllegalArgumentException("present value must carry data");
            }
            if (value instanceof byte[] bytes) {
                value = bytes.clone();
            }
        }

        @Override
        public Object value() {
            return value instanceof byte[] bytes ? bytes.clone() : value;
        }

        public static ColumnValue missing() {
            return new ColumnValue(ValueKind.MISSING, null);
        }

        public static ColumnValue nullValue() {
            return new ColumnValue(ValueKind.NULL, null);
        }

        public static ColumnValue scalar(Object value) {
            return new ColumnValue(ValueKind.SCALAR, value);
        }

        public static ColumnValue lobReference(String typeName, long locatorId) {
            return new ColumnValue(ValueKind.LOB_REFERENCE,
                    new LobReference(typeName, locatorId));
        }
    }

    public record DecodedRow(
            TableDecodePlan plan,
            long tid,
            long logPosition,
            long rowId,
            long containerId,
            int operationCode,
            boolean fullImage,
            List<ColumnValue> values,
            List<Long> lobLocatorIds) {
        public DecodedRow {
            values = List.copyOf(values);
            lobLocatorIds = lobLocatorIds == null || lobLocatorIds.isEmpty()
                    ? null : List.copyOf(lobLocatorIds);
            if (values.size() != plan.metadata().columns().size()) {
                throw new IllegalArgumentException("decoded row width does not match plan");
            }
            if (containerId <= 0) {
                throw new IllegalArgumentException("decoded row has no container identity");
            }
        }

        @Override
        public List<Long> lobLocatorIds() {
            return lobLocatorIds == null ? List.of() : lobLocatorIds;
        }

        public DecodedRow(
                TableDecodePlan plan,
                long tid,
                long logPosition,
                long rowId,
                long containerId,
                int operationCode,
                boolean fullImage,
                List<ColumnValue> values) {
            this(plan, tid, logPosition, rowId, containerId, operationCode,
                    fullImage, values, collectLobLocatorIds(values));
        }

        private static List<Long> collectLobLocatorIds(List<ColumnValue> values) {
            List<Long> result = null;
            for (ColumnValue value : values) {
                if (value.kind() == ValueKind.LOB_REFERENCE) {
                    if (result == null) {
                        result = new ArrayList<>();
                    }
                    result.add(((LobReference) value.value()).locatorId());
                }
            }
            return result;
        }
    }

    public DecodedRow decode(
            NativeRecordHeader record,
            TableDecodePlan plan,
            byte[] payload) throws IOException {
        return decode(record, plan, payload, null);
    }

    private DecodedRow decode(
            NativeRecordHeader record,
            TableDecodePlan plan,
            byte[] payload,
            TableDecodePlan.RowProjection cachedProjection) throws IOException {
        try {
            requireRowRecord(record, plan);
            UnifiedTableRowLayout vector = UnifiedTableRowLayout
                    .parse(record.operationCode(), payload)
                    .orElseThrow(() -> fatal("unsupported row vector"));
            NativeRowDescriptorLayout layout = NativeRowDescriptorLayout
                    .parse(vector, payload)
                    .orElseThrow(() -> fatal("unsupported row descriptor"));
            if (layout.descriptors().size() != vector.internalColumnIds().size()) {
                throw fatal("descriptor/vector width mismatch");
            }
            TableDecodePlan.RowProjection projection = cachedProjection == null
                    ? plan.project(vector.internalColumnIds()) : cachedProjection;
            if (projection.descriptorToColumnIndex().size()
                    != vector.internalColumnIds().size()) {
                throw fatal("cached projection/vector width mismatch");
            }
            NativeVariableSlotLayout variables = NativeVariableSlotLayout
                    .parseImmutablePayload(layout, payload)
                    .orElseThrow(() -> fatal("unsupported variable-slot layout"));
            int[] variableIndex = variableIndexes(layout);

            List<ColumnValue> values = new ArrayList<>(
                    java.util.Collections.nCopies(
                            plan.metadata().columns().size(), ColumnValue.missing()));
            List<Long> lobLocatorIds = null;
            for (int descriptorIndex = 0;
                    descriptorIndex < layout.descriptors().size(); descriptorIndex++) {
                int columnIndex = projection.descriptorToColumnIndex().get(descriptorIndex);
                if (columnIndex < 0) {
                    continue;
                }
                TableMetadata.Column column = plan.metadata().columns().get(columnIndex);
                var descriptor = layout.descriptors().get(descriptorIndex);
                if (descriptor.isNull()) {
                    if (!column.nullable()) {
                        throw fatal("NULL in NOT NULL column " + column.name());
                    }
                    values.set(columnIndex, ColumnValue.nullValue());
                    continue;
                }
                NativeVariableSlotLayout.Slot variable = variableIndex[descriptorIndex] < 0
                        ? null : variables.slots().get(variableIndex[descriptorIndex]);
                if (variable != null && variable.isNull()) {
                    throw fatal("descriptor/variable NULL state mismatch for " + column.name());
                }
                ColumnValue decoded = decodeColumn(
                        column, descriptorIndex, layout, variable, payload);
                values.set(columnIndex, decoded);
                if (decoded.kind() == ValueKind.LOB_REFERENCE) {
                    if (lobLocatorIds == null) {
                        lobLocatorIds = new ArrayList<>();
                    }
                    lobLocatorIds.add(((LobReference) decoded.value()).locatorId());
                }
            }
            boolean complete = projection.fullImage();
            if (complete) {
                for (ColumnValue value : values) {
                    if (value.kind() == ValueKind.MISSING) {
                        complete = false;
                        break;
                    }
                }
            }
            if ((record.operationCode() == NativeRecordReader.UT_INSERT_DATA
                    || record.operationCode() == NativeRecordReader.UT_UPSERT) && !complete) {
                throw fatal("full-image operation produced sparse row");
            }
            return new DecodedRow(plan, record.tid(), record.position(), record.firstRowId(),
                    record.containerId(), record.operationCode(), complete,
                    values, lobLocatorIds);
        }
        catch (IllegalArgumentException error) {
            throw new IOException("FATAL_NATIVE_ROW: position=" + record.position()
                    + " table=" + plan.metadata().qualifiedName() + ": "
                    + error.getMessage(), error);
        }
    }

    /** Decodes either one row image or a characterized columnar multi-row image. */
    public List<DecodedRow> decodeAll(
            NativeRecordHeader record,
            TableDecodePlan plan,
            byte[] payload) throws IOException {
        return decodeAll(record, plan, payload, null, 1);
    }

    List<DecodedRow> decodeAll(
            NativeRecordHeader record,
            TableDecodePlan plan,
            byte[] payload,
            ExecutorService executor,
            int workers) throws IOException {
        return decodeAll(record, plan, payload, executor, workers, 1);
    }

    List<DecodedRow> decodeAll(
            NativeRecordHeader record,
            TableDecodePlan plan,
            byte[] payload,
            ExecutorService executor,
            int workers,
            int chunksPerThread) throws IOException {
        UnifiedTableRowLayout vector = UnifiedTableRowLayout
                .parse(record.operationCode(), payload)
                .orElseThrow(() -> fatal("unsupported row vector"));
        if (vector.vectorVersion() <= 1) {
            return List.of(decode(record, plan, payload));
        }
        NativeMultiRowPayload.Rows rows = NativeMultiRowPayload.parse(record, plan, payload);
        if (executor == null || workers <= 1 || rows.rowCount() < 32) {
            return decodeMultiRange(record, plan, rows, 0, rows.rowCount());
        }
        int lanes = Math.min(
                Math.multiplyExact(workers, chunksPerThread), rows.rowCount());
        int chunk = (rows.rowCount() + lanes - 1) / lanes;
        List<Future<List<DecodedRow>>> futures = new ArrayList<>(lanes);
        for (int start = 0; start < rows.rowCount(); start += chunk) {
            int from = start;
            int to = Math.min(rows.rowCount(), start + chunk);
            futures.add(executor.submit(
                    () -> decodeMultiRange(record, plan, rows, from, to)));
        }
        List<DecodedRow> result = new ArrayList<>(rows.rowCount());
        try {
            for (Future<List<DecodedRow>> future : futures) {
                result.addAll(future.get());
            }
        }
        catch (InterruptedException error) {
            futures.forEach(value -> value.cancel(true));
            Thread.currentThread().interrupt();
            throw new IOException("parallel native row decode was interrupted", error);
        }
        catch (ExecutionException error) {
            futures.forEach(value -> value.cancel(true));
            Throwable cause = error.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error fatal) {
                throw fatal;
            }
            throw new IOException("parallel native row decode failed", cause);
        }
        return List.copyOf(result);
    }

    private List<DecodedRow> decodeMultiRange(
            NativeRecordHeader record,
            TableDecodePlan plan,
            NativeMultiRowPayload.Rows rows,
            int from,
            int to) throws IOException {
        List<DecodedRow> result = new ArrayList<>(to - from);
        for (int index = from; index < to; index++) {
            result.add(decodeMultiRow(record, plan, rows, index));
        }
        return List.copyOf(result);
    }

    private DecodedRow decodeMultiRow(
            NativeRecordHeader record,
            TableDecodePlan plan,
            NativeMultiRowPayload.Rows rows,
            int rowIndex) throws IOException {
        try {
            requireRowRecord(record, plan);
            if (rows.descriptors().size() != rows.vector().internalColumnIds().size()
                    || rows.projection().descriptorToColumnIndex().size()
                    != rows.descriptors().size()) {
                throw fatal("multi-row descriptor/vector width mismatch");
            }
            List<ColumnValue> values = new ArrayList<>(
                    java.util.Collections.nCopies(
                            plan.metadata().columns().size(), ColumnValue.missing()));
            List<Long> lobLocatorIds = null;
            for (int descriptorIndex = 0;
                    descriptorIndex < rows.descriptors().size(); descriptorIndex++) {
                int columnIndex = rows.projection()
                        .descriptorToColumnIndex().get(descriptorIndex);
                if (columnIndex < 0) continue;
                TableMetadata.Column column = plan.metadata().columns().get(columnIndex);
                NativeMultiRowPayload.Descriptor descriptor =
                        rows.descriptors().get(descriptorIndex);
                if (descriptor.isNull()) {
                    if (!column.nullable()) {
                        throw fatal("NULL in NOT NULL column " + column.name());
                    }
                    values.set(columnIndex, ColumnValue.nullValue());
                    continue;
                }
                ColumnValue decoded = decodeMultiColumn(
                        column, descriptorIndex, rows.cell(descriptorIndex, rowIndex));
                values.set(columnIndex, decoded);
                if (decoded.kind() == ValueKind.LOB_REFERENCE) {
                    if (lobLocatorIds == null) lobLocatorIds = new ArrayList<>();
                    lobLocatorIds.add(((LobReference) decoded.value()).locatorId());
                }
            }
            boolean complete = rows.projection().fullImage();
            if (complete) {
                for (ColumnValue value : values) {
                    if (value.kind() == ValueKind.MISSING) {
                        complete = false;
                        break;
                    }
                }
            }
            if ((record.operationCode() == NativeRecordReader.UT_INSERT_DATA
                    || record.operationCode() == NativeRecordReader.UT_UPSERT) && !complete) {
                throw fatal("full-image operation produced sparse row");
            }
            return new DecodedRow(plan, record.tid(), record.position(),
                    Math.addExact(record.firstRowId(), rowIndex), record.containerId(),
                    record.operationCode(), complete, values, lobLocatorIds);
        }
        catch (IllegalArgumentException error) {
            throw new IOException("FATAL_NATIVE_ROW: position=" + record.position()
                    + " table=" + plan.metadata().qualifiedName() + ": "
                    + error.getMessage(), error);
        }
    }

    /** Replaces a transaction-scoped locator only with a fully assembled LOB. */
    public DecodedRow resolveLobs(
            DecodedRow row,
            Map<Long, NativeLobReconstructor.LobValue> reconstructed) throws IOException {
        if (row.lobLocatorIds().isEmpty()) {
            return row;
        }
        List<ColumnValue> values = new ArrayList<>(row.values());
        for (int index = 0; index < values.size(); index++) {
            ColumnValue current = values.get(index);
            if (current.kind() != ValueKind.LOB_REFERENCE) {
                continue;
            }
            LobReference reference = (LobReference) current.value();
            NativeLobReconstructor.LobValue lob = reconstructed.get(reference.locatorId());
            if (lob == null) {
                throw new IOException("FATAL_INCOMPLETE_LARGE_VALUE: locator=0x"
                        + Long.toUnsignedString(reference.locatorId(), 16));
            }
            byte[] bytes = lob.data();
            values.set(index, ColumnValue.scalar("BLOB".equals(reference.typeName())
                    ? bytes : strictUtf8(bytes)));
        }
        return new DecodedRow(row.plan(), row.tid(), row.logPosition(), row.rowId(),
                row.containerId(), row.operationCode(), row.fullImage(), values, null);
    }

    private static void requireRowRecord(NativeRecordHeader record, TableDecodePlan plan) {
        boolean identityMatches = plan.metadata().partitions().isEmpty()
                ? record.poid() == plan.metadata().objectId()
                : containsContainer(plan, record.containerId());
        if (record.outerCode() != NativeRecordReader.OUTER_UNIFIED_TABLE
                || !identityMatches) {
            throw fatal("record does not belong to table plan");
        }
        if (record.operationCode() != NativeRecordReader.UT_INSERT_DATA
                && record.operationCode() != NativeRecordReader.UT_UPSERT
                && record.operationCode() != NativeRecordReader.UT_UPDATE) {
            throw fatal("operation has no supported row image: " + record.operationType());
        }
    }

    private static boolean containsContainer(TableDecodePlan plan, long containerId) {
        for (TableMetadata.Partition partition : plan.metadata().partitions()) {
            if (partition.containerId() == containerId) {
                return true;
            }
        }
        return false;
    }

    private static int[] variableIndexes(NativeRowDescriptorLayout layout) {
        int[] result = new int[layout.descriptors().size()];
        Arrays.fill(result, -1);
        int variable = 0;
        for (int index = 0; index < result.length; index++) {
            if ((layout.descriptors().get(index).typeCode() & 0x0100) != 0) {
                result[index] = variable++;
            }
        }
        if (variable != layout.variableSlotCount()) {
            throw fatal("variable descriptor count does not match slot count");
        }
        return result;
    }

    private static ColumnValue decodeColumn(
            TableMetadata.Column column,
            int descriptorIndex,
            NativeRowDescriptorLayout layout,
            NativeVariableSlotLayout.Slot variable,
            byte[] payload) {
        // TableMetadata normalizes catalog type names once at plan creation.
        String type = column.typeName();
        Object value = switch (type) {
            case "TINYINT" -> fixed(layout, descriptorIndex,
                    NativeFixedScalarDecoder.Type.TINYINT, payload);
            case "SMALLINT" -> fixed(layout, descriptorIndex,
                    NativeFixedScalarDecoder.Type.SMALLINT, payload);
            case "INTEGER", "INT" -> fixed(layout, descriptorIndex,
                    NativeFixedScalarDecoder.Type.INTEGER, payload);
            case "BIGINT" -> fixed(layout, descriptorIndex,
                    NativeFixedScalarDecoder.Type.BIGINT, payload);
            case "REAL" -> fixed(layout, descriptorIndex,
                    NativeFixedScalarDecoder.Type.REAL, payload);
            case "DOUBLE" -> fixed(layout, descriptorIndex,
                    NativeFixedScalarDecoder.Type.DOUBLE, payload);
            case "BOOLEAN" -> fixed(layout, descriptorIndex,
                    NativeFixedScalarDecoder.Type.BOOLEAN, payload);
            case "DECIMAL" -> NativeDecimalDecoder.decodeDecimal(
                    layout, descriptorIndex, column.scale(),
                    decimalWidth(column.precision()), payload);
            case "SMALLDECIMAL" -> NativeDecimalDecoder.decodeSmallDecimal(
                    layout, descriptorIndex, payload);
            case "DATE" -> NativeTemporalDecoder.decodeDate(layout, descriptorIndex, payload);
            case "TIME" -> NativeTemporalDecoder.decodeTime(layout, descriptorIndex, payload);
            case "SECONDDATE" -> NativeTemporalDecoder.decodeSecondDate(
                    layout, descriptorIndex, payload);
            case "TIMESTAMP" -> NativeTemporalDecoder.decodeTimestamp(
                    layout, descriptorIndex, payload);
            case "CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "SHORTTEXT" ->
                    variable == null
                            ? NativeLengthPrefixedDecoder.decodeUtf8(
                                    layout, descriptorIndex, payload)
                            : NativeVariableValueDecoder.decodeUtf8(variable);
            case "ALPHANUM" -> NativeVariableValueDecoder.decodeTaggedUtf8(
                    requireVariable(column, variable), 0x20);
            case "BINARY", "VARBINARY" -> variable == null
                    ? NativeLengthPrefixedDecoder.decodeBytes(layout, descriptorIndex, payload)
                    : variable.data();
            case "CLOB" -> decodeLob(column, requireVariable(column, variable), 0xb0, 0x023f);
            case "NCLOB" -> decodeLob(column, requireVariable(column, variable), 0xd0, 0x025f);
            case "BLOB" -> decodeLob(column, requireVariable(column, variable), 0x90, 0x021f);
            case "TEXT" -> NativeTextDecoder.decodeText(
                    requireVariable(column, variable)).text();
            case "BINTEXT" -> NativeTextDecoder.decodeBinText(
                    requireVariable(column, variable)).text();
            case "ST_POINT" -> NativeSpatialDecoder.decodePointWkt(
                    requireVariable(column, variable));
            case "ST_GEOMETRY" -> NativeSpatialDecoder.decodeLineStringWkt(
                    requireVariable(column, variable));
            default -> throw fatal("unsupported catalog type " + type + " for " + column.name());
        };
        return value instanceof ColumnValue encoded ? encoded : ColumnValue.scalar(value);
    }

    private static ColumnValue decodeMultiColumn(
            TableMetadata.Column column,
            int descriptorIndex,
            ByteSlice cell) {
        NativeVariableSlotLayout.Slot slot = NativeVariableSlotLayout.Slot.view(
                descriptorIndex, 0, cell.length(),
                cell.source(), cell.offset(), cell.length());
        String type = column.typeName();
        Object value = switch (type) {
            case "TINYINT" -> NativeFixedScalarDecoder.decode(
                    NativeFixedScalarDecoder.Type.TINYINT, cell);
            case "SMALLINT" -> NativeFixedScalarDecoder.decode(
                    NativeFixedScalarDecoder.Type.SMALLINT, cell);
            case "INTEGER", "INT" -> NativeFixedScalarDecoder.decode(
                    NativeFixedScalarDecoder.Type.INTEGER, cell);
            case "BIGINT" -> NativeFixedScalarDecoder.decode(
                    NativeFixedScalarDecoder.Type.BIGINT, cell);
            case "REAL" -> NativeFixedScalarDecoder.decode(
                    NativeFixedScalarDecoder.Type.REAL, cell);
            case "DOUBLE" -> NativeFixedScalarDecoder.decode(
                    NativeFixedScalarDecoder.Type.DOUBLE, cell);
            case "BOOLEAN" -> NativeFixedScalarDecoder.decode(
                    NativeFixedScalarDecoder.Type.BOOLEAN, cell);
            case "DECIMAL" -> NativeDecimalDecoder.decodeDecimal(
                    column.scale(), decimalWidth(column.precision()), cell);
            case "SMALLDECIMAL" -> NativeDecimalDecoder.decodeSmallDecimal(cell);
            case "DATE" -> NativeTemporalDecoder.decodeDate(cell);
            case "TIME" -> NativeTemporalDecoder.decodeTime(cell);
            case "SECONDDATE" -> NativeTemporalDecoder.decodeSecondDate(cell);
            case "TIMESTAMP" -> NativeTemporalDecoder.decodeTimestamp(cell);
            case "CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "SHORTTEXT" ->
                    NativeVariableValueDecoder.decodeUtf8(slot);
            case "ALPHANUM" -> NativeVariableValueDecoder.decodeTaggedUtf8(slot, 0x20);
            case "BINARY", "VARBINARY" -> cell.copy();
            case "CLOB" -> decodeLob(column, slot, 0xb0, 0x023f);
            case "NCLOB" -> decodeLob(column, slot, 0xd0, 0x025f);
            case "BLOB" -> decodeLob(column, slot, 0x90, 0x021f);
            case "TEXT" -> NativeTextDecoder.decodeText(slot).text();
            case "BINTEXT" -> NativeTextDecoder.decodeBinText(slot).text();
            case "ST_POINT" -> NativeSpatialDecoder.decodePointWkt(slot);
            case "ST_GEOMETRY" -> NativeSpatialDecoder.decodeLineStringWkt(slot);
            default -> throw fatal("unsupported catalog type " + type
                    + " for " + column.name());
        };
        return value instanceof ColumnValue encoded ? encoded : ColumnValue.scalar(value);
    }

    private static Object fixed(
            NativeRowDescriptorLayout layout,
            int descriptorIndex,
            NativeFixedScalarDecoder.Type type,
            byte[] payload) {
        return NativeFixedScalarDecoder.decode(layout, descriptorIndex, type, payload);
    }

    private static int decimalWidth(int precision) {
        if (precision >= 1 && precision <= 18) {
            return 8;
        }
        if (precision >= 19 && precision <= 38) {
            return 16;
        }
        throw fatal("unsupported DECIMAL precision " + precision);
    }

    private static NativeVariableSlotLayout.Slot requireVariable(
            TableMetadata.Column column,
            NativeVariableSlotLayout.Slot variable) {
        if (variable == null) {
            throw fatal("expected variable slot for " + column.name());
        }
        return variable;
    }

    private static ColumnValue decodeLob(
            TableMetadata.Column column,
            NativeVariableSlotLayout.Slot slot,
            int inlineTag,
            int locatorPrefix) {
        ByteSlice bytes = slot.slice();
        if (bytes.length() == 0 && "BLOB".equals(column.typeName())) {
            return ColumnValue.scalar(new byte[0]);
        }
        if (bytes.length() > 0 && bytes.unsigned(0) == inlineTag) {
            return ColumnValue.scalar("BLOB".equals(column.typeName())
                    ? NativeVariableValueDecoder.decodeTaggedBytes(slot, inlineTag)
                    : NativeVariableValueDecoder.decodeTaggedUtf8(slot, inlineTag));
        }
        if (bytes.length() != 12) {
            throw fatal("unsupported LOB locator width for " + column.name());
        }
        ByteBuffer value = bytes.buffer(ByteOrder.LITTLE_ENDIAN);
        if (value.getInt() != locatorPrefix) {
            throw fatal("unexpected LOB locator prefix for " + column.name());
        }
        return ColumnValue.lobReference(column.typeName(), value.getLong());
    }

    private static String strictUtf8(byte[] value) {
        try {
            return StrictUtf8.decode(value);
        }
        catch (java.nio.charset.CharacterCodingException error) {
            throw fatal("invalid UTF-8 LOB", error);
        }
    }

    private static IllegalArgumentException fatal(String detail) {
        return new IllegalArgumentException("FATAL_NATIVE_ROW: " + detail);
    }

    private static IllegalArgumentException fatal(String detail, Throwable cause) {
        return new IllegalArgumentException("FATAL_NATIVE_ROW: " + detail, cause);
    }
}

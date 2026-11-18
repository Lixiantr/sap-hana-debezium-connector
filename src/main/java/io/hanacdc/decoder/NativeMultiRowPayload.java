package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;
import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Maps a characterized HANA columnar multi-row image to immutable cell views. */
final class NativeMultiRowPayload {
    record Descriptor(int typeCode, long slot, boolean variable) {
        boolean isNull() {
            return (typeCode & 0x2000) == 0;
        }
    }

    record Rows(
            UnifiedTableRowLayout vector,
            List<Descriptor> descriptors,
            List<ByteSlice[]> cells,
            TableDecodePlan.RowProjection projection,
            int rowCount) {
        Rows {
            descriptors = List.copyOf(descriptors);
            cells = List.copyOf(cells);
            if (rowCount <= 1 || descriptors.size() != cells.size()) {
                throw new IllegalArgumentException("invalid multi-row cell map");
            }
        }

        ByteSlice cell(int descriptorIndex, int rowIndex) {
            if (rowIndex < 0 || rowIndex >= rowCount) {
                throw new IndexOutOfBoundsException("multi-row index outside image");
            }
            return cells.get(descriptorIndex)[rowIndex];
        }
    }

    private NativeMultiRowPayload() {}

    static Rows parse(
            NativeRecordHeader record,
            TableDecodePlan plan,
            byte[] payload) throws IOException {
        UnifiedTableRowLayout vector = UnifiedTableRowLayout
                .parse(record.operationCode(), payload)
                .orElseThrow(() -> fatal(record, "unsupported row vector"));
        int rows = vector.vectorVersion();
        if (rows <= 1) throw fatal(record, "row vector is not multi-row");
        if (vector.imageVersion() != 1 || rows > 0xffff) {
            throw fatal(record, "unsupported multi-row vector version=" + rows);
        }
        int body = vector.encodedBodyOffset();
        if (body + 4 > payload.length || payload[body] != 0) {
            throw fatal(record, "truncated multi-row body");
        }
        int mode = payload[body + 2] & 0xff;
        int columnCount = payload[body + 3] & 0xff;
        if (columnCount != vector.columnCount()) {
            throw fatal(record, "body/vector column count mismatch");
        }

        int descriptorWidth;
        int descriptorStart;
        int variableCount;
        long encodedFixedEnd;
        long variableBytes;
        long tailBytes;
        if (mode == 0x01 && (payload[body + 1] & 0xff) == 0x41) {
            if (body + 21 > payload.length) {
                throw fatal(record, "truncated mode 0x01 header");
            }
            descriptorWidth = 7;
            descriptorStart = body + 21;
            variableCount = u16(payload, body + 7);
            encodedFixedEnd = u32(payload, body + 9);
            variableBytes = u32(payload, body + 13);
            tailBytes = Math.multiplyExact((long) rows, Long.BYTES);
            if (u32(payload, body + 17) != 0) {
                throw fatal(record, "mode 0x01 reserved field is non-zero");
            }
        }
        else if (mode == 0x31 && (payload[body + 1] & 0xff) == 0x01) {
            if (body + 23 > payload.length) {
                throw fatal(record, "truncated mode 0x31 header");
            }
            descriptorWidth = 9;
            descriptorStart = body + 23;
            variableCount = u16(payload, body + 7);
            variableBytes = u64(payload, body + 15);
            tailBytes = Math.multiplyExact((long) rows, Long.BYTES);
            encodedFixedEnd = -1; // mode 0x31 header field is not yet used as an offset.
        }
        else if (mode == 0x2a && (payload[body + 1] & 0xff) == 0x01) {
            if (body + 29 > payload.length
                    || payload[body + 4] != 0
                    || payload[body + 5] != 0
                    || payload[body + 8] != 0
                    || payload[body + 11] != 0) {
                throw fatal(record, "truncated or invalid mode 0x2a header");
            }
            int encodedRows = u16(payload, body + 6);
            if (encodedRows != rows) {
                throw fatal(record, "mode 0x2a body/vector row count mismatch");
            }
            descriptorWidth = 12;
            descriptorStart = body + 29;
            variableCount = u16(payload, body + 9);
            long fixedEnd = u24(payload, body + 12);
            if (fixedEnd < 3) {
                throw fatal(record, "invalid mode 0x2a fixed boundary");
            }
            encodedFixedEnd = fixedEnd - 3;
            tailBytes = u48(payload, body + 15);
            variableBytes = u64(payload, body + 21);
        }
        else {
            throw fatal(record, String.format(Locale.ROOT,
                    "unsupported multi-row mode=0x%02x", mode));
        }

        int descriptorEnd = checkedAdd(descriptorStart,
                checkedMultiply(columnCount, descriptorWidth, record), record);
        int offsetsEnd = checkedAdd(descriptorEnd,
                checkedMultiply(variableCount, Long.BYTES, record), record);
        int dataStart = align8(offsetsEnd);
        if (dataStart > payload.length || variableBytes < 0
                || variableBytes > payload.length) {
            throw fatal(record, "multi-row areas exceed payload");
        }
        boolean indexBeforeVariables =
                record.operationCode() == NativeRecordReader.UT_INSERT_DATA;
        long beforeVariableBytes = mode == 0x2a || indexBeforeVariables
                ? tailBytes : 0;
        long afterVariableBytes = mode == 0x2a
                ? (indexBeforeVariables ? 0 : Math.multiplyExact((long) rows, Long.BYTES))
                : (indexBeforeVariables ? 0 : tailBytes);
        long variableBaseLong = payload.length - afterVariableBytes - variableBytes;
        if (variableBaseLong < dataStart || variableBaseLong > Integer.MAX_VALUE) {
            throw fatal(record, "invalid multi-row variable base");
        }
        int variableBase = (int) variableBaseLong;
        long expectedVariableBase = dataStart + encodedFixedEnd + beforeVariableBytes;
        if ((mode == 0x01 || mode == 0x2a)
                && expectedVariableBase != variableBase) {
            throw fatal(record, String.format(Locale.ROOT,
                    "mode 0x%02x fixed/variable boundary mismatch", mode));
        }

        List<Descriptor> descriptors = new ArrayList<>(columnCount);
        int observedVariables = 0;
        for (int index = 0; index < columnCount; index++) {
            int offset = descriptorStart + index * descriptorWidth;
            int type = u24(payload, offset);
            long slot = switch (descriptorWidth) {
                case 7 -> u32(payload, offset + 3);
                case 9 -> u48(payload, offset + 3);
                case 12 -> u64(payload, offset + 3);
                default -> throw fatal(record, "unsupported descriptor width");
            };
            if (descriptorWidth == 12 && payload[offset + 11] != 0) {
                throw fatal(record, "mode 0x2a descriptor reserved byte is non-zero");
            }
            boolean variable = (type & 0x0100) != 0;
            if (variable) observedVariables++;
            descriptors.add(new Descriptor(type, slot, variable));
        }
        if (observedVariables != variableCount) {
            throw fatal(record, "variable descriptor count mismatch");
        }

        long[] variableOffsets = new long[variableCount];
        for (int index = 0; index < variableCount; index++) {
            variableOffsets[index] = u64(payload, descriptorEnd + index * Long.BYTES);
            if (variableOffsets[index] < 0
                    || (index == 0 && variableOffsets[index] != 0)
                    || (index > 0 && variableOffsets[index] < variableOffsets[index - 1])
                    || variableOffsets[index] > variableBytes) {
                throw fatal(record, "invalid variable column offsets");
            }
        }

        TableDecodePlan.RowProjection projection;
        try {
            projection = plan.project(vector.internalColumnIds());
        }
        catch (IllegalArgumentException error) {
            throw fatal(record, error.getMessage());
        }
        Map<Integer, TableMetadata.Column> columns = new HashMap<>();
        for (int index = 0; index < columnCount; index++) {
            int visible = projection.descriptorToColumnIndex().get(index);
            if (visible >= 0) columns.put(index, plan.metadata().columns().get(visible));
        }

        List<ByteSlice[]> cells = new ArrayList<>(columnCount);
        int variableIndex = 0;
        List<Integer> fixedIndexes = new ArrayList<>();
        for (int index = 0; index < columnCount; index++) {
            if (!descriptors.get(index).variable()) fixedIndexes.add(index);
        }
        for (int index = 0; index < columnCount; index++) {
            Descriptor descriptor = descriptors.get(index);
            if (descriptor.variable()) {
                long start = variableOffsets[variableIndex];
                long end = variableIndex + 1 < variableCount
                        ? variableOffsets[variableIndex + 1] : variableBytes;
                cells.add(splitFrames(
                        payload, variableBase, start, end, rows, record, false));
                variableIndex++;
                continue;
            }
            int fixedPosition = fixedIndexes.indexOf(index);
            long start = directOffset(mode, descriptor.slot(), record);
            long end = fixedPosition + 1 < fixedIndexes.size()
                    ? directOffset(mode,
                            descriptors.get(fixedIndexes.get(fixedPosition + 1)).slot(), record)
                    : encodedFixedEnd >= 0 ? encodedFixedEnd : variableBase - dataStart;
            if (start < 0 || end < start || dataStart + end > variableBase) {
                throw fatal(record, "invalid fixed column offsets");
            }
            TableMetadata.Column column = columns.get(index);
            if (column == null) {
                throw fatal(record, "uncertified fixed auxiliary column id="
                        + vector.internalColumnIds().get(index));
            }
            cells.add(splitFixed(payload, dataStart + (int) start,
                    dataStart + (int) end, rows, column, record));
        }

        return new Rows(vector, descriptors, cells, projection, rows);
    }

    private static ByteSlice[] splitFixed(
            byte[] payload, int start, int limit, int rows,
            TableMetadata.Column column, NativeRecordHeader record) throws IOException {
        if (isInline(column.typeName())) {
            try {
                return splitFrames(
                        payload, start, 0, limit - start, rows, record, true);
            }
            catch (IOException error) {
                throw fatal(record, "inline column " + column.name() + ": "
                        + error.getMessage());
            }
        }
        int width = massWidth(column);
        if ((long) start + (long) width * rows > limit) {
            throw fatal(record, "fixed column " + column.name() + " exceeds its block");
        }
        ByteSlice[] result = new ByteSlice[rows];
        for (int row = 0; row < rows; row++) {
            result[row] = ByteSlice.view(payload, start + row * width, width);
        }
        return result;
    }

    private static ByteSlice[] splitFrames(
            byte[] payload, int base, long relativeStart, long relativeEnd,
            int rows, NativeRecordHeader record, boolean allowAlignmentPadding)
            throws IOException {
        long absoluteStart = base + relativeStart;
        long absoluteEnd = base + relativeEnd;
        if (absoluteStart < 0 || absoluteEnd < absoluteStart || absoluteEnd > payload.length) {
            throw fatal(record, "variable frame block exceeds payload");
        }
        int cursor = (int) absoluteStart;
        int limit = (int) absoluteEnd;
        ByteSlice[] result = new ByteSlice[rows];
        for (int row = 0; row < rows; row++) {
            int header = lengthHeader(payload, cursor, limit, record);
            int length = decodedLength(payload, cursor, header);
            int end = checkedAdd(cursor, checkedAdd(header, length, record), record);
            if (end > limit) throw fatal(record, "length-framed value exceeds column block");
            result[row] = ByteSlice.view(payload, cursor + header, length);
            cursor = end;
        }
        if (cursor != limit) {
            int trailing = limit - cursor;
            if (allowAlignmentPadding && trailing <= 7) {
                boolean zero = true;
                for (int index = cursor; index < limit; index++) {
                    zero &= payload[index] == 0;
                }
                if (zero) {
                    return result;
                }
            }
            throw fatal(record, "length-framed column has trailing bytes=" + (limit - cursor));
        }
        return result;
    }

    private static boolean isInline(String typeName) {
        return switch (typeName.toUpperCase(Locale.ROOT)) {
            case "CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "SHORTTEXT",
                    "BINARY", "VARBINARY" -> true;
            default -> false;
        };
    }

    private static int massWidth(TableMetadata.Column column) {
        return switch (column.typeName().toUpperCase(Locale.ROOT)) {
            case "TINYINT", "SMALLINT", "INTEGER", "INT", "REAL", "BOOLEAN",
                    "DATE", "TIME" -> 4;
            case "BIGINT", "DOUBLE", "SMALLDECIMAL", "SECONDDATE", "TIMESTAMP" -> 8;
            case "DECIMAL" -> column.precision() <= 18 ? 8 : 16;
            default -> throw new IllegalArgumentException(
                    "unsupported fixed multi-row type " + column.typeName());
        };
    }

    private static long directOffset(int mode, long slot, NativeRecordHeader record)
            throws IOException {
        if (mode == 0x01) return slot;
        if (mode == 0x2a) {
            long encoded = slot & 0xfffffL;
            if (encoded < 3) {
                throw fatal(record, "uncertified mode 0x2a fixed slot");
            }
            return encoded - 3;
        }
        if ((slot >>> 32) != 0 && (slot >>> 32) != 0x60) {
            throw fatal(record, "uncertified mode 0x31 fixed slot high bits");
        }
        long low = slot & 0xffffL;
        if (low < 2) {
            throw fatal(record, "uncertified mode 0x31 fixed slot");
        }
        // The encoded offset carries a constant bias of two. Fixed-width
        // columns naturally produce tags 2,6,a,e; inline variable-width
        // columns may end at any byte remainder.
        long result = low - 2;
        if (result >= 0x10000L) {
            throw fatal(record, "mode 0x31 fixed offset exceeds certified window");
        }
        return result;
    }

    private static int lengthHeader(byte[] value, int offset, int limit,
            NativeRecordHeader record) throws IOException {
        if (offset >= limit) throw fatal(record, "missing length frame");
        int first = value[offset] & 0xff;
        int bytes = first < 0x80 ? 1
                : first >= 0xc2 && first <= 0xdf ? 2
                : first >= 0xe0 && first <= 0xef ? 3
                : first >= 0xf0 && first <= 0xf4 ? 4 : -1;
        if (bytes < 0 || offset + bytes > limit) {
            throw fatal(record, "unsupported length frame prefix");
        }
        for (int index = 1; index < bytes; index++) {
            if ((value[offset + index] & 0xc0) != 0x80) {
                throw fatal(record, "invalid continuation in length frame");
            }
        }
        return bytes;
    }

    private static int decodedLength(byte[] value, int offset, int bytes) {
        int first = value[offset] & 0xff;
        if (bytes == 1) return first;
        int result = first & (0xff >>> (bytes + 1));
        for (int index = 1; index < bytes; index++) {
            result = (result << 6) | (value[offset + index] & 0x3f);
        }
        return result;
    }

    private static int u16(byte[] value, int offset) {
        return (value[offset] & 0xff) | ((value[offset + 1] & 0xff) << 8);
    }

    private static int u24(byte[] value, int offset) {
        return u16(value, offset) | ((value[offset + 2] & 0xff) << 16);
    }

    private static long u32(byte[] value, int offset) {
        return Integer.toUnsignedLong(ByteBuffer.wrap(value, offset, 4)
                .order(ByteOrder.LITTLE_ENDIAN).getInt());
    }

    private static long u48(byte[] value, int offset) {
        long result = 0;
        for (int index = 0; index < 6; index++) {
            result |= (long) (value[offset + index] & 0xff) << (index * 8);
        }
        return result;
    }

    private static long u64(byte[] value, int offset) {
        return ByteBuffer.wrap(value, offset, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    private static int checkedMultiply(int left, int right, NativeRecordHeader record)
            throws IOException {
        try { return Math.multiplyExact(left, right); }
        catch (ArithmeticException error) { throw fatal(record, "integer overflow"); }
    }

    private static int checkedAdd(int left, int right, NativeRecordHeader record)
            throws IOException {
        try { return Math.addExact(left, right); }
        catch (ArithmeticException error) { throw fatal(record, "integer overflow"); }
    }

    private static int align8(int value) {
        return (value + 7) & ~7;
    }

    private static IOException fatal(NativeRecordHeader record, String detail) {
        return new IOException("FATAL_NATIVE_MULTI_ROW: position=" + record.position()
                + ": " + detail);
    }
}

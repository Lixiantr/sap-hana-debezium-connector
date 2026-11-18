package io.hanacdc.decoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Characterized descriptor/data-area grammar shared by SIMPLE_ROW, FULL_ROW
 * and ALL_TYPES column-table row images on HANA 2.00.088.
 */
public record NativeRowDescriptorLayout(
        int rowBodyOffset,
        int encodingMode,
        int descriptorStart,
        int descriptorWidth,
        int descriptorBias,
        int variableSlotCount,
        int dataAreaOffset,
        List<ColumnDescriptor> descriptors) {

    public record ColumnDescriptor(int index, int typeCode, int slot, byte[] raw) {
        public ColumnDescriptor {
            raw = raw.clone();
        }

        @Override
        public byte[] raw() {
            return raw.clone();
        }

        /** Characterized nullable-row presence flag; false means SQL NULL. */
        public boolean isNull() {
            return (typeCode & 0x2000) == 0;
        }
    }

    public NativeRowDescriptorLayout {
        descriptors = List.copyOf(descriptors);
    }

    public static Optional<NativeRowDescriptorLayout> parse(
            UnifiedTableRowLayout vector, byte[] payload) {
        int body = vector.encodedBodyOffset();
        if (body < 0 || body + 8 > payload.length || (payload[body + 1] & 0xff) != 0xc1) {
            return Optional.empty();
        }
        int mode = payload[body + 2] & 0xff;
        int count = payload[body + 3] & 0xff;
        if (count != vector.columnCount()) {
            return Optional.empty();
        }

        int relativeStart;
        int width;
        int bias;
        int variableSlots;
        boolean alignedDataArea;
        if (mode == 0x60) {
            variableSlots = payload[body + 5] & 0xff;
            relativeStart = 7 + (variableSlots == 0 ? 0 : 8);
            width = 4;
            bias = 4;
            alignedDataArea = true;
        }
        else if (mode == 0x51) {
            if (body + 19 > payload.length) return Optional.empty();
            variableSlots = payload[body + 7] & 0xff;
            relativeStart = 19;
            width = 5;
            bias = 6;
            alignedDataArea = true;
        }
        else if (mode == 0x20) {
            if (body + 15 > payload.length) return Optional.empty();
            variableSlots = payload[body + 5] & 0xff;
            relativeStart = 15;
            width = 4;
            bias = 0;
            alignedDataArea = false;
        }
        else {
            return Optional.empty();
        }

        int descriptorStart;
        int descriptorEnd;
        int variableEnd;
        int dataArea;
        try {
            descriptorStart = Math.addExact(body, relativeStart);
            descriptorEnd = Math.addExact(descriptorStart, Math.multiplyExact(count, width));
            variableEnd = Math.addExact(descriptorEnd,
                    Math.multiplyExact(variableSlots, Long.BYTES));
            dataArea = alignedDataArea ? align8(variableEnd) : variableEnd;
        }
        catch (ArithmeticException error) {
            return Optional.empty();
        }
        if (descriptorEnd > payload.length || dataArea > payload.length) {
            return Optional.empty();
        }

        List<ColumnDescriptor> descriptors = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            int offset = descriptorStart + index * width;
            int typeCode = (payload[offset] & 0xff)
                    | ((payload[offset + 1] & 0xff) << 8)
                    | ((payload[offset + 2] & 0xff) << 16);
            int slot = payload[offset + 3] & 0xff;
            if (width == 5) slot |= (payload[offset + 4] & 0xff) << 8;
            byte[] raw = new byte[width];
            System.arraycopy(payload, offset, raw, 0, width);
            descriptors.add(new ColumnDescriptor(index, typeCode, slot, raw));
        }
        return Optional.of(new NativeRowDescriptorLayout(
                body, mode, descriptorStart, width, bias, variableSlots,
                dataArea, descriptors));
    }

    /** Offset for scalar/direct descriptors; callers still validate the catalog type. */
    public Optional<Integer> directValueOffset(int descriptorIndex) {
        if (descriptorIndex < 0 || descriptorIndex >= descriptors.size()) {
            return Optional.empty();
        }
        int relative = descriptors.get(descriptorIndex).slot() - descriptorBias;
        if (relative < 0) {
            return Optional.empty();
        }
        try {
            return Optional.of(Math.addExact(dataAreaOffset, relative));
        }
        catch (ArithmeticException error) {
            return Optional.empty();
        }
    }

    private static int align8(int value) {
        return (value + 7) & ~7;
    }
}

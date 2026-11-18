package io.hanacdc.decoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Parses the ordered variable-slot offset table and length-framed records. */
public record NativeVariableSlotLayout(int dataOffset, List<Slot> slots) {
    public static final class Slot {
        private final int index;
        private final long relativeOffset;
        private final int encodedLength;
        private final ByteSlice data;

        public Slot(int index, long relativeOffset, int encodedLength, byte[] data) {
            this(index, relativeOffset, encodedLength, ByteSlice.copyOf(data));
        }

        private Slot(int index, long relativeOffset, int encodedLength, ByteSlice data) {
            this.index = index;
            this.relativeOffset = relativeOffset;
            this.encodedLength = encodedLength;
            this.data = data;
        }

        static Slot view(
                int index, long relativeOffset, int encodedLength,
                byte[] source, int offset, int length) {
            return new Slot(index, relativeOffset, encodedLength,
                    ByteSlice.view(source, offset, length));
        }

        static Slot copyOfRange(
                int index, long relativeOffset, int encodedLength,
                byte[] source, int offset, int length) {
            return new Slot(index, relativeOffset, encodedLength,
                    ByteSlice.copyOfRange(source, offset, length));
        }

        public int index() {
            return index;
        }

        public long relativeOffset() {
            return relativeOffset;
        }

        public int encodedLength() {
            return encodedLength;
        }

        public byte[] data() {
            return data.copy();
        }

        ByteSlice slice() {
            return data;
        }

        public boolean isNull() {
            return encodedLength < 0;
        }
    }

    public NativeVariableSlotLayout {
        slots = List.copyOf(slots);
    }

    public static Optional<NativeVariableSlotLayout> parse(
            NativeRowDescriptorLayout row, byte[] payload) {
        return parse(row, payload, false);
    }

    static Optional<NativeVariableSlotLayout> parseImmutablePayload(
            NativeRowDescriptorLayout row, byte[] payload) {
        return parse(row, payload, true);
    }

    private static Optional<NativeVariableSlotLayout> parse(
            NativeRowDescriptorLayout row, byte[] payload, boolean immutablePayload) {
        int count = row.variableSlotCount();
        if (count == 0) {
            return Optional.of(new NativeVariableSlotLayout(payload.length, List.of()));
        }
        int descriptorEnd;
        try {
            descriptorEnd = Math.addExact(row.descriptorStart(),
                    Math.multiplyExact(row.descriptors().size(), row.descriptorWidth()));
        }
        catch (ArithmeticException error) {
            return Optional.empty();
        }
        if (descriptorEnd + count * Long.BYTES > payload.length) return Optional.empty();

        int encodedFixedEnd;
        if (row.encodingMode() == 0x60) {
            if (row.rowBodyOffset() + 7 > payload.length) return Optional.empty();
            encodedFixedEnd = payload[row.rowBodyOffset() + 6] & 0xff;
        }
        else if (row.encodingMode() == 0x51) {
            if (row.rowBodyOffset() + 11 > payload.length) return Optional.empty();
            encodedFixedEnd = (payload[row.rowBodyOffset() + 9] & 0xff)
                    | ((payload[row.rowBodyOffset() + 10] & 0xff) << 8);
        }
        else if (row.encodingMode() == 0x20) {
            if (row.rowBodyOffset() + 7 > payload.length) return Optional.empty();
            encodedFixedEnd = payload[row.rowBodyOffset() + 6] & 0xff;
        }
        else {
            return Optional.empty();
        }
        int variableDataOffset;
        try {
            variableDataOffset = Math.addExact(row.dataAreaOffset(),
                    Math.subtractExact(encodedFixedEnd, row.descriptorBias()));
        }
        catch (ArithmeticException error) {
            return Optional.empty();
        }
        if (variableDataOffset < row.dataAreaOffset() || variableDataOffset > payload.length) {
            return Optional.empty();
        }

        List<Long> offsets = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            int offset = descriptorEnd + index * Long.BYTES;
            long value = 0;
            for (int byteIndex = 0; byteIndex < Long.BYTES; byteIndex++) {
                value |= (long) (payload[offset + byteIndex] & 0xff) << (byteIndex * 8);
            }
            if (value < 0 || (index > 0 && value < offsets.get(index - 1))) {
                return Optional.empty();
            }
            offsets.add(value);
        }
        if (offsets.get(0) != 0) return Optional.empty();

        List<NativeRowDescriptorLayout.ColumnDescriptor> variableDescriptors =
                new ArrayList<>(count);
        for (NativeRowDescriptorLayout.ColumnDescriptor descriptor : row.descriptors()) {
            if ((descriptor.typeCode() & 0x0100) != 0) {
                variableDescriptors.add(descriptor);
            }
        }
        boolean exactVariableDescriptors = variableDescriptors.size() == count;
        List<Slot> slots = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            long relative = offsets.get(index);
            if (exactVariableDescriptors && variableDescriptors.get(index).isNull()) {
                slots.add(new Slot(index, relative, -1, new byte[0]));
                continue;
            }
            long absoluteLong;
            try {
                absoluteLong = Math.addExact(variableDataOffset, relative);
            }
            catch (ArithmeticException error) {
                return Optional.empty();
            }
            if (absoluteLong < 0 || absoluteLong > payload.length) return Optional.empty();
            int absolute = (int) absoluteLong;
            long limitLong = index + 1 < count
                    ? variableDataOffset + offsets.get(index + 1) : payload.length;
            if (limitLong < absoluteLong || limitLong > payload.length) return Optional.empty();
            int limit = (int) limitLong;
            if (limit == absolute) {
                slots.add(new Slot(index, relative, -1, new byte[0]));
                continue;
            }
            Frame frame = decodeLength(payload, absolute, limit);
            if (frame == null) return Optional.empty();
            int end;
            try {
                end = Math.addExact(absolute + frame.headerBytes(), frame.encodedLength());
            }
            catch (ArithmeticException error) {
                return Optional.empty();
            }
            if (end > limit || (index + 1 < count && end != limit)) {
                return Optional.empty();
            }
            int valueOffset = absolute + frame.headerBytes();
            slots.add(immutablePayload
                    ? Slot.view(index, relative, frame.encodedLength(),
                            payload, valueOffset, frame.encodedLength())
                    : Slot.copyOfRange(index, relative, frame.encodedLength(),
                            payload, valueOffset, frame.encodedLength()));
        }
        return Optional.of(new NativeVariableSlotLayout(variableDataOffset, slots));
    }

    private static Frame decodeLength(byte[] payload, int offset, int limit) {
        int first = payload[offset] & 0xff;
        if (first < 0x80) return new Frame(1, first);
        int bytes;
        int value;
        int minimum;
        if (first >= 0xc2 && first <= 0xdf) {
            bytes = 2;
            value = first & 0x1f;
            minimum = 0x80;
        }
        else if (first >= 0xe0 && first <= 0xef) {
            bytes = 3;
            value = first & 0x0f;
            minimum = 0x800;
        }
        else if (first >= 0xf0 && first <= 0xf4) {
            bytes = 4;
            value = first & 0x07;
            minimum = 0x10000;
        }
        else {
            return null;
        }
        if (offset + bytes > limit) return null;
        for (int index = 1; index < bytes; index++) {
            int next = payload[offset + index] & 0xff;
            if ((next & 0xc0) != 0x80) return null;
            value = (value << 6) | (next & 0x3f);
        }
        if (value < minimum || value > 0x10ffff
                || (value >= 0xd800 && value <= 0xdfff)) {
            return null;
        }
        return new Frame(bytes, value);
    }

    private record Frame(int headerBytes, int encodedLength) {}
}

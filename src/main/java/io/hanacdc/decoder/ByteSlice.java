package io.hanacdc.decoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/** Package-private immutable view over decoder-owned bytes. */
final class ByteSlice {
    private static final ByteSlice EMPTY = new ByteSlice(new byte[0], 0, 0);

    private final byte[] source;
    private final int offset;
    private final int length;

    private ByteSlice(byte[] source, int offset, int length) {
        if (offset < 0 || length < 0 || offset > source.length - length) {
            throw new IndexOutOfBoundsException("byte slice is outside input");
        }
        this.source = source;
        this.offset = offset;
        this.length = length;
    }

    static ByteSlice empty() {
        return EMPTY;
    }

    static ByteSlice copyOf(byte[] value) {
        return value.length == 0 ? EMPTY
                : new ByteSlice(value.clone(), 0, value.length);
    }

    static ByteSlice copyOfRange(byte[] value, int offset, int length) {
        return length == 0 ? EMPTY
                : new ByteSlice(Arrays.copyOfRange(value, offset, offset + length), 0, length);
    }

    static ByteSlice view(byte[] value, int offset, int length) {
        return length == 0 ? EMPTY : new ByteSlice(value, offset, length);
    }

    int length() {
        return length;
    }

    byte get(int index) {
        if (index < 0 || index >= length) {
            throw new IndexOutOfBoundsException("byte slice index is outside input");
        }
        return source[offset + index];
    }

    int unsigned(int index) {
        return Byte.toUnsignedInt(get(index));
    }

    byte[] copy() {
        return length == 0 ? new byte[0]
                : Arrays.copyOfRange(source, offset, offset + length);
    }

    byte[] copyFrom(int relativeOffset) {
        if (relativeOffset < 0 || relativeOffset > length) {
            throw new IndexOutOfBoundsException("byte slice offset is outside input");
        }
        return Arrays.copyOfRange(source, offset + relativeOffset, offset + length);
    }

    ByteBuffer buffer(ByteOrder order) {
        return ByteBuffer.wrap(source, offset, length).slice().order(order);
    }

    byte[] source() {
        return source;
    }

    int offset() {
        return offset;
    }
}

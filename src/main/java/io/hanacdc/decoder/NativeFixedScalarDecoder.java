package io.hanacdc.decoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Strict little-endian decoder for characterized fixed-width scalar slots. */
public final class NativeFixedScalarDecoder {
    public enum Type { TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, BOOLEAN }

    private NativeFixedScalarDecoder() {}

    public static Object decode(
            NativeRowDescriptorLayout layout, int descriptorIndex, Type type, byte[] payload) {
        int offset = layout.directValueOffset(descriptorIndex)
                .orElseThrow(() -> new IllegalArgumentException("descriptor has no direct slot"));
        return decode(type, ByteSlice.view(payload, offset, payload.length - offset));
    }

    static Object decode(Type type, ByteSlice value) {
        int width = switch (type) {
            case TINYINT, BOOLEAN -> 1;
            case SMALLINT -> 2;
            case INTEGER, REAL -> 4;
            case BIGINT, DOUBLE -> 8;
        };
        if (value.length() < width) {
            throw new IllegalArgumentException("scalar exceeds native payload");
        }
        ByteBuffer bytes = value.buffer(ByteOrder.LITTLE_ENDIAN);
        return switch (type) {
            case TINYINT -> Byte.toUnsignedInt(bytes.get());
            case SMALLINT -> (int) bytes.getShort();
            case INTEGER -> bytes.getInt();
            case BIGINT -> bytes.getLong();
            case REAL -> bytes.getFloat();
            case DOUBLE -> bytes.getDouble();
            case BOOLEAN -> {
                int encoded = Byte.toUnsignedInt(bytes.get());
                if (encoded != 0 && encoded != 1) {
                    throw new IllegalArgumentException(
                            "invalid native BOOLEAN value " + encoded);
                }
                yield encoded == 1;
            }
        };
    }
}

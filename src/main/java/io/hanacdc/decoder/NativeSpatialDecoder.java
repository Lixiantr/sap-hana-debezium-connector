package io.hanacdc.decoder;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/** Strict decoder for the characterized 2D POINT and LINESTRING slot forms. */
public final class NativeSpatialDecoder {
    private static final int HEADER_BYTES = 16;

    private NativeSpatialDecoder() {}

    public static String decodePointWkt(NativeVariableSlotLayout.Slot slot) {
        ByteSlice value = slot.slice();
        requireHeader(value, 1);
        if (value.length() != HEADER_BYTES + 2 * Double.BYTES) {
            throw new IllegalArgumentException("unexpected native POINT size");
        }
        ByteBuffer bytes = value.buffer(ByteOrder.LITTLE_ENDIAN);
        return "POINT (" + coordinate(bytes.getDouble(16)) + " "
                + coordinate(bytes.getDouble(24)) + ")";
    }

    public static String decodeLineStringWkt(NativeVariableSlotLayout.Slot slot) {
        ByteSlice value = slot.slice();
        requireHeader(value, 2);
        if (value.length() < 56) {
            throw new IllegalArgumentException("native LINESTRING is truncated");
        }
        ByteBuffer bytes = value.buffer(ByteOrder.LITTLE_ENDIAN);
        long count = bytes.getLong(48);
        if (count < 2 || count > (Integer.MAX_VALUE - 56) / 16
                || 56L + count * 16L != value.length()) {
            throw new IllegalArgumentException("invalid native LINESTRING point count");
        }
        List<String> points = new ArrayList<>((int) count);
        for (int index = 0; index < count; index++) {
            points.add(coordinate(bytes.getDouble(56 + index * 16)) + " "
                    + coordinate(bytes.getDouble(64 + index * 16)));
        }
        validateBounds(bytes, (int) count);
        return "LINESTRING (" + String.join(", ", points) + ")";
    }

    private static void requireHeader(ByteSlice value, int geometryType) {
        if (value.length() < HEADER_BYTES
                || value.unsigned(0) != 0x8f
                || value.unsigned(1) != 0x07
                || value.unsigned(2) != 0x01
                || value.unsigned(13) != geometryType
                || value.unsigned(14) != 0x01) {
            throw new IllegalArgumentException("unexpected native spatial header");
        }
    }

    private static void validateBounds(ByteBuffer bytes, int count) {
        double minX = Double.POSITIVE_INFINITY;
        double minY = Double.POSITIVE_INFINITY;
        double maxX = Double.NEGATIVE_INFINITY;
        double maxY = Double.NEGATIVE_INFINITY;
        for (int index = 0; index < count; index++) {
            double x = bytes.getDouble(56 + index * 16);
            double y = bytes.getDouble(64 + index * 16);
            minX = Math.min(minX, x);
            minY = Math.min(minY, y);
            maxX = Math.max(maxX, x);
            maxY = Math.max(maxY, y);
        }
        if (Double.doubleToLongBits(minX) != Double.doubleToLongBits(bytes.getDouble(16))
                || Double.doubleToLongBits(minY) != Double.doubleToLongBits(bytes.getDouble(24))
                || Double.doubleToLongBits(maxX) != Double.doubleToLongBits(bytes.getDouble(32))
                || Double.doubleToLongBits(maxY) != Double.doubleToLongBits(bytes.getDouble(40))) {
            throw new IllegalArgumentException("native LINESTRING bounds do not match coordinates");
        }
    }

    private static String coordinate(double value) {
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException("non-finite native spatial coordinate");
        }
        return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
    }
}

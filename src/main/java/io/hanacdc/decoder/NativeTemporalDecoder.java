package io.hanacdc.decoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/** Characterized HANA 2.00.088 DATE/TIME/SECONDDATE/TIMESTAMP codecs. */
public final class NativeTemporalDecoder {
    private static final LocalDateTime BASE = LocalDateTime.of(1, 1, 1, 0, 0);
    private static final long SECONDDATE_EPOCH_ADJUSTMENT = 2L * 86_400L + 1L;
    private static final long TIMESTAMP_EPOCH_ADJUSTMENT_TICKS = 2L * 86_400L * 10_000_000L;

    private NativeTemporalDecoder() {}

    public static LocalDate decodeDate(
            NativeRowDescriptorLayout layout, int descriptorIndex, byte[] payload) {
        return decodeDate(directSlice(layout, descriptorIndex, payload, 4));
    }

    static LocalDate decodeDate(ByteSlice value) {
        long raw = Integer.toUnsignedLong(read(value, 4).getInt());
        if (raw < 3) throw new IllegalArgumentException("invalid native DATE ordinal " + raw);
        try {
            return LocalDate.of(1, 1, 1).plusDays(raw - 3);
        }
        catch (DateTimeException | ArithmeticException error) {
            throw new IllegalArgumentException("native DATE outside Java range: " + raw, error);
        }
    }

    public static LocalTime decodeTime(
            NativeRowDescriptorLayout layout, int descriptorIndex, byte[] payload) {
        return decodeTime(directSlice(layout, descriptorIndex, payload, 4));
    }

    static LocalTime decodeTime(ByteSlice value) {
        long raw = Integer.toUnsignedLong(read(value, 4).getInt());
        long secondOfDay = raw - 1;
        if (secondOfDay < 0 || secondOfDay >= 86_400) {
            throw new IllegalArgumentException("invalid native TIME second " + raw);
        }
        return LocalTime.ofSecondOfDay(secondOfDay);
    }

    public static LocalDateTime decodeSecondDate(
            NativeRowDescriptorLayout layout, int descriptorIndex, byte[] payload) {
        return decodeSecondDate(directSlice(layout, descriptorIndex, payload, 8));
    }

    static LocalDateTime decodeSecondDate(ByteSlice value) {
        long raw = read(value, 8).getLong();
        long seconds = raw - SECONDDATE_EPOCH_ADJUSTMENT;
        if (seconds < 0) throw new IllegalArgumentException("invalid native SECONDDATE " + raw);
        try {
            return BASE.plusSeconds(seconds);
        }
        catch (DateTimeException | ArithmeticException error) {
            throw new IllegalArgumentException("native SECONDDATE outside Java range: " + raw, error);
        }
    }

    public static LocalDateTime decodeTimestamp(
            NativeRowDescriptorLayout layout, int descriptorIndex, byte[] payload) {
        return decodeTimestamp(directSlice(layout, descriptorIndex, payload, 8));
    }

    static LocalDateTime decodeTimestamp(ByteSlice value) {
        long raw = read(value, 8).getLong();
        long ticks = raw - TIMESTAMP_EPOCH_ADJUSTMENT_TICKS;
        if (ticks < 0) throw new IllegalArgumentException("invalid native TIMESTAMP " + raw);
        long seconds = ticks / 10_000_000L;
        int nanos = Math.toIntExact((ticks % 10_000_000L) * 100L);
        try {
            return BASE.plusSeconds(seconds).plusNanos(nanos);
        }
        catch (DateTimeException | ArithmeticException error) {
            throw new IllegalArgumentException("native TIMESTAMP outside Java range: " + raw, error);
        }
    }

    private static ByteSlice directSlice(
            NativeRowDescriptorLayout layout, int descriptorIndex, byte[] payload, int width) {
        int offset = layout.directValueOffset(descriptorIndex)
                .orElseThrow(() -> new IllegalArgumentException("descriptor has no direct slot"));
        if (offset < 0 || offset + width > payload.length) {
            throw new IllegalArgumentException("temporal value exceeds native payload");
        }
        return ByteSlice.view(payload, offset, width);
    }

    private static ByteBuffer read(ByteSlice value, int width) {
        if (value.length() < width) {
            throw new IllegalArgumentException("temporal value exceeds native payload");
        }
        return value.buffer(ByteOrder.LITTLE_ENDIAN);
    }
}

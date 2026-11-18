package io.hanacdc.decoder;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class NativeTemporalDecoderTest {
    @Test
    void decodesBothA0804BDifferentialTemporalVectors() {
        var layout = new NativeRowDescriptorLayout(
                0, 0x51, 0, 5, 6, 0, 32,
                List.of(descriptor(0, 6), descriptor(1, 14),
                        descriptor(2, 22), descriptor(3, 30)));

        byte[] variant = new byte[80];
        ByteBuffer variantBytes = ByteBuffer.wrap(variant).order(ByteOrder.LITTLE_ENDIAN);
        variantBytes.putInt(32, 741_446);
        variantBytes.putInt(40, 85_502);
        variantBytes.putLong(48, 64_060_933_502L);
        variantBytes.putLong(56, 640_609_335_011_112_224L);
        assertEquals(LocalDate.of(2031, 1, 2),
                NativeTemporalDecoder.decodeDate(layout, 0, variant));
        assertEquals(LocalTime.of(23, 45, 1),
                NativeTemporalDecoder.decodeTime(layout, 1, variant));
        assertEquals(LocalDateTime.of(2031, 1, 2, 23, 45, 1),
                NativeTemporalDecoder.decodeSecondDate(layout, 2, variant));
        assertEquals(LocalDateTime.of(2031, 1, 2, 23, 45, 1, 111_222_400),
                NativeTemporalDecoder.decodeTimestamp(layout, 3, variant));

        byte[] baseline = new byte[80];
        ByteBuffer baselineBytes = ByteBuffer.wrap(baseline).order(ByteOrder.LITTLE_ENDIAN);
        baselineBytes.putInt(32, 739_104);
        baselineBytes.putInt(40, 45_297);
        baselineBytes.putLong(48, 63_858_544_497L);
        baselineBytes.putLong(56, 638_585_444_969_876_544L);
        assertEquals(LocalDate.of(2024, 8, 4),
                NativeTemporalDecoder.decodeDate(layout, 0, baseline));
        assertEquals(LocalTime.of(12, 34, 56),
                NativeTemporalDecoder.decodeTime(layout, 1, baseline));
        assertEquals(LocalDateTime.of(2024, 8, 4, 12, 34, 56),
                NativeTemporalDecoder.decodeSecondDate(layout, 2, baseline));
        assertEquals(LocalDateTime.of(2024, 8, 4, 12, 34, 56, 987_654_400),
                NativeTemporalDecoder.decodeTimestamp(layout, 3, baseline));
    }

    private static NativeRowDescriptorLayout.ColumnDescriptor descriptor(int index, int slot) {
        return new NativeRowDescriptorLayout.ColumnDescriptor(index, 0, slot, new byte[5]);
    }
}

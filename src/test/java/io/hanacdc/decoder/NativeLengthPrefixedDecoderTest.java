package io.hanacdc.decoder;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class NativeLengthPrefixedDecoderTest {
    @Test
    void decodesInlineUtf8AndBinaryValuesFromDescriptorSlot() {
        var layout = new NativeRowDescriptorLayout(
                0, 0x51, 0, 5, 6, 0, 32,
                List.of(descriptor(0, 6), descriptor(1, 62)));
        byte[] payload = new byte[128];
        byte[] text = "nvarchar-вариант".getBytes(StandardCharsets.UTF_8);
        payload[32] = (byte) text.length;
        System.arraycopy(text, 0, payload, 33, text.length);
        byte[] binary = {15, 14, 13, 12};
        payload[88] = (byte) binary.length;
        System.arraycopy(binary, 0, payload, 89, binary.length);

        assertEquals("nvarchar-вариант",
                NativeLengthPrefixedDecoder.decodeUtf8(layout, 0, payload));
        assertArrayEquals(binary, NativeLengthPrefixedDecoder.decodeBytes(layout, 1, payload));
    }

    private static NativeRowDescriptorLayout.ColumnDescriptor descriptor(int index, int slot) {
        return new NativeRowDescriptorLayout.ColumnDescriptor(index, 0, slot, new byte[5]);
    }
}

package io.hanacdc.decoder;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class NativeDecimalDecoderTest {
    @Test
    void decodesCharacterizedA0804BExactNumerics() {
        byte[] payload = new byte[128];
        byte[] decimal = hex("3297ff6fde3c0000d28aaf75cf5c82de");
        byte[] smallDecimal = hex("15cd5b0700004031");
        System.arraycopy(decimal, 0, payload, 80, decimal.length);
        System.arraycopy(smallDecimal, 0, payload, 96, smallDecimal.length);
        var layout = new NativeRowDescriptorLayout(
                0, 0x51, 0, 5, 6, 0, 64,
                List.of(descriptor(0, 22), descriptor(1, 38)));

        assertEquals("123456789012345678901234.1234567890",
                NativeDecimalDecoder.decodeDecimal(layout, 0, 10, 16, payload).toPlainString());
        assertEquals("12345.6789",
                NativeDecimalDecoder.decodeSmallDecimal(layout, 1, payload).toPlainString());

        byte[] negative = hex("b168de3a000040b1");
        System.arraycopy(negative, 0, payload, 96, negative.length);
        assertEquals("-98765.4321",
                NativeDecimalDecoder.decodeSmallDecimal(layout, 1, payload).toPlainString());
    }

    private static NativeRowDescriptorLayout.ColumnDescriptor descriptor(int index, int slot) {
        return new NativeRowDescriptorLayout.ColumnDescriptor(index, 0, slot, new byte[5]);
    }

    private static byte[] hex(String value) {
        byte[] result = new byte[value.length() / 2];
        for (int index = 0; index < result.length; index++) {
            result[index] = (byte) Integer.parseInt(value.substring(index * 2, index * 2 + 2), 16);
        }
        return result;
    }
}

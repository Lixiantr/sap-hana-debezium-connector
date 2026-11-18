package io.hanacdc.decoder;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class NativeSpatialDecoderTest {
    @Test
    void decodesCharacterizedPointAndLineString() {
        var point = new NativeVariableSlotLayout.Slot(7, 0, 32,
                hex("8f07010000000000000000000001010000000000000008400000000000001040"));
        var line = new NativeVariableSlotLayout.Slot(8, 0, 88,
                hex("8f070100000000000000000000020100"
                        + "00000000000000400000000000000840"
                        + "00000000000010400000000000001440"
                        + "0200000000000000"
                        + "00000000000000400000000000000840"
                        + "00000000000010400000000000001440"));

        assertEquals("POINT (3 4)", NativeSpatialDecoder.decodePointWkt(point));
        assertEquals("LINESTRING (2 3, 4 5)", NativeSpatialDecoder.decodeLineStringWkt(line));
    }

    private static byte[] hex(String value) {
        byte[] result = new byte[value.length() / 2];
        for (int index = 0; index < result.length; index++) {
            result[index] = (byte) Integer.parseInt(value.substring(index * 2, index * 2 + 2), 16);
        }
        return result;
    }
}

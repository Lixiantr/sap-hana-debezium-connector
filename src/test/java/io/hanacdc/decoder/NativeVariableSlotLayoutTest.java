package io.hanacdc.decoder;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NativeVariableSlotLayoutTest {
    @Test
    void parsesA0804BStyleOffsetsAndTaggedValues() {
        byte[] payload = new byte[160];
        int body = 8;
        payload[body + 1] = (byte) 0xc1;
        payload[body + 2] = 0x51;
        payload[body + 3] = 2;
        payload[body + 7] = 2;
        // fixed-area end plus descriptor bias: 16 + 6
        payload[body + 9] = 22;
        int descriptorStart = body + 19;
        int descriptorEnd = descriptorStart + 10;
        ByteBuffer bytes = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        bytes.putLong(descriptorEnd, 0);
        bytes.putLong(descriptorEnd + 8, 4);
        int dataArea = 56;
        int variable = dataArea + 16;
        payload[variable] = 3;
        payload[variable + 1] = 'a';
        payload[variable + 2] = 'b';
        payload[variable + 3] = 'c';
        payload[variable + 4] = 4;
        payload[variable + 5] = (byte) 0x90;
        payload[variable + 6] = 1;
        payload[variable + 7] = 2;
        payload[variable + 8] = 3;
        var row = new NativeRowDescriptorLayout(
                body, 0x51, descriptorStart, 5, 6, 2, dataArea,
                List.of(descriptor(0), descriptor(1)));

        var layout = NativeVariableSlotLayout.parse(row, payload).orElseThrow();
        assertEquals(variable, layout.dataOffset());
        assertEquals("abc", NativeVariableValueDecoder.decodeUtf8(layout.slots().get(0)));
        assertArrayEquals(new byte[] {1, 2, 3},
                NativeVariableValueDecoder.decodeTaggedBytes(layout.slots().get(1), 0x90));
        payload[variable + 1] = 'z';
        byte[] detached = layout.slots().get(0).data();
        detached[0] = 'x';
        assertEquals("abc", NativeVariableValueDecoder.decodeUtf8(layout.slots().get(0)));
    }

    @Test
    void parsesMode20NullAndUtf8EncodedLength() {
        byte[] payload = new byte[440];
        int body = 93;
        payload[body + 1] = (byte) 0xc1;
        payload[body + 2] = 0x20;
        payload[body + 3] = 2;
        payload[body + 5] = 2;
        payload[body + 6] = 0;
        int descriptorStart = body + 15;
        int descriptorEnd = descriptorStart + 8;
        ByteBuffer bytes = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        bytes.putLong(descriptorEnd, 0);
        bytes.putLong(descriptorEnd + 8, 0);
        int dataArea = descriptorEnd + 16;
        // First slot is NULL. Second slot has a 300-byte value whose length
        // is the UTF-8 representation of U+012C: c4 ac.
        payload[dataArea] = (byte) 0xc4;
        payload[dataArea + 1] = (byte) 0xac;
        for (int index = 0; index < 300; index++) payload[dataArea + 2 + index] = 'N';
        var row = new NativeRowDescriptorLayout(
                body, 0x20, descriptorStart, 4, 0, 2, dataArea,
                List.of(descriptor20(0), descriptor20(1)));

        var layout = NativeVariableSlotLayout.parse(row, payload).orElseThrow();
        assertTrue(layout.slots().get(0).isNull());
        assertFalse(layout.slots().get(1).isNull());
        assertEquals(300, layout.slots().get(1).encodedLength());
    }

    private static NativeRowDescriptorLayout.ColumnDescriptor descriptor(int index) {
        return new NativeRowDescriptorLayout.ColumnDescriptor(index, 0, 0, new byte[5]);
    }

    private static NativeRowDescriptorLayout.ColumnDescriptor descriptor20(int index) {
        return new NativeRowDescriptorLayout.ColumnDescriptor(index, 0, 0, new byte[4]);
    }
}

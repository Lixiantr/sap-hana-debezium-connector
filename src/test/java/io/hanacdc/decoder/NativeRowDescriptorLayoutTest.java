package io.hanacdc.decoder;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class NativeRowDescriptorLayoutTest {
    @Test
    void parsesShortFourColumnGrammarAndComputesDataArea() {
        byte[] payload = new byte[139];
        int body = 69;
        byte[] descriptor = {
                0x00, (byte) 0xc1, 0x60, 0x04, 0x01, 0x00, 0x27,
                (byte) 0xf0, 0x20, 0x59, 0x04,
                (byte) 0xf0, 0x20, 0x59, 0x0c,
                0x49, 0x20, 0x54, 0x14,
                0x53, 0x22, 0x30, 0x18
        };
        System.arraycopy(descriptor, 0, payload, body, descriptor.length);
        var vector = new UnifiedTableRowLayout(
                48, 0x81, 4, 1, 1, List.of(201L, 202L, 203L, 204L), body);

        var layout = NativeRowDescriptorLayout.parse(vector, payload).orElseThrow();
        assertEquals(4, layout.descriptorWidth());
        assertEquals(0, layout.variableSlotCount());
        assertEquals(96, layout.dataAreaOffset());
        assertEquals(96, layout.directValueOffset(0).orElseThrow());
        assertEquals(104, layout.directValueOffset(1).orElseThrow());
        assertEquals(112, layout.directValueOffset(2).orElseThrow());
    }

    @Test
    void parsesExtendedAllTypesGrammarAndComputesDataArea() {
        byte[] payload = new byte[1079];
        int body = 189;
        payload[body] = 0;
        payload[body + 1] = (byte) 0xc1;
        payload[body + 2] = 0x51;
        payload[body + 3] = 34;
        payload[body + 7] = 10;
        int descriptors = body + 19;
        for (int index = 0; index < 34; index++) {
            payload[descriptors + index * 5] = (byte) 0xf0;
            payload[descriptors + index * 5 + 1] = 0x20;
            payload[descriptors + index * 5 + 2] = 0x59;
            int slot = 6 + index * 8;
            payload[descriptors + index * 5 + 3] = (byte) slot;
            payload[descriptors + index * 5 + 4] = (byte) (slot >>> 8);
        }
        var vector = new UnifiedTableRowLayout(
                48, 0x81, 34, 1, 1,
                java.util.stream.LongStream.rangeClosed(201, 234).boxed().toList(), body);

        var layout = NativeRowDescriptorLayout.parse(vector, payload).orElseThrow();
        assertEquals(5, layout.descriptorWidth());
        assertEquals(10, layout.variableSlotCount());
        assertEquals(464, layout.dataAreaOffset());
        assertEquals(464, layout.directValueOffset(0).orElseThrow());
        assertEquals(472, layout.directValueOffset(1).orElseThrow());
    }
}

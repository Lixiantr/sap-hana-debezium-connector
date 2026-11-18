package io.hanacdc.decoder;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class NativeFixedScalarDecoderTest {
    @Test
    void decodesCharacterizedFixedScalarsFromDescriptorSlots() {
        byte[] payload = new byte[128];
        var descriptors = List.of(
                descriptor(0, 6), descriptor(1, 14), descriptor(2, 22),
                descriptor(3, 26), descriptor(4, 30));
        var layout = new NativeRowDescriptorLayout(
                0, 0x51, 0, 5, 6, 0, 64, descriptors);
        ByteBuffer bytes = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        bytes.putLong(64, 940_804_001L);
        bytes.putLong(72, 19_002L);
        bytes.put(80, (byte) 1);
        bytes.putShort(84, (short) 20_202);
        bytes.putInt(88, 202_020_202);

        assertEquals(940_804_001L, NativeFixedScalarDecoder.decode(
                layout, 0, NativeFixedScalarDecoder.Type.BIGINT, payload));
        assertEquals(19_002L, NativeFixedScalarDecoder.decode(
                layout, 1, NativeFixedScalarDecoder.Type.BIGINT, payload));
        assertEquals(true, NativeFixedScalarDecoder.decode(
                layout, 2, NativeFixedScalarDecoder.Type.BOOLEAN, payload));
        assertEquals(20_202, NativeFixedScalarDecoder.decode(
                layout, 3, NativeFixedScalarDecoder.Type.SMALLINT, payload));
        assertEquals(202_020_202, NativeFixedScalarDecoder.decode(
                layout, 4, NativeFixedScalarDecoder.Type.INTEGER, payload));
    }

    private static NativeRowDescriptorLayout.ColumnDescriptor descriptor(int index, int slot) {
        return new NativeRowDescriptorLayout.ColumnDescriptor(index, 0x5920f0, slot, new byte[5]);
    }
}

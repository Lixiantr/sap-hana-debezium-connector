package io.hanacdc.decoder;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class UnifiedTableRowLayoutTest {
    @Test
    void parsesInsertColumnVectorHeader() {
        byte[] payload = new byte[96];
        ByteBuffer value = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        value.putInt(16, NativeRecordReader.UT_INSERT_DATA);
        value.put(32, (byte) 0x81);
        value.put(34, (byte) 3);
        value.put(35, (byte) 1);
        value.put(36, (byte) 1);
        value.putInt(37, 201);
        value.putInt(41, 202);
        value.putInt(45, 203);

        var layout = UnifiedTableRowLayout.parse(
                NativeRecordReader.UT_INSERT_DATA, payload).orElseThrow();
        assertEquals(32, layout.vectorOffset());
        assertEquals(3, layout.columnCount());
        assertEquals(List.of(201L, 202L, 203L), layout.internalColumnIds());
        assertEquals(49, layout.encodedBodyOffset());
    }

    @Test
    void rejectsNonRowOperation() {
        assertTrue(UnifiedTableRowLayout.parse(
                NativeRecordReader.UT_DELETE, new byte[96]).isEmpty());
    }
}

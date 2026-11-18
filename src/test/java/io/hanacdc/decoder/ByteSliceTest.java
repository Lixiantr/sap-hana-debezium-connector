package io.hanacdc.decoder;

import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class ByteSliceTest {
    @Test
    void copiedSliceDoesNotExposeEitherArray() {
        byte[] source = new byte[] {1, 2, 3};
        ByteSlice slice = ByteSlice.copyOf(source);
        source[1] = 9;
        byte[] copy = slice.copy();
        copy[0] = 8;

        assertArrayEquals(new byte[] {1, 2, 3}, slice.copy());
    }

    @Test
    void internalViewUsesExactRange() {
        byte[] source = new byte[] {9, 1, 2, 3, 4, 8};
        ByteSlice slice = ByteSlice.view(source, 1, 4);

        assertEquals(4, slice.length());
        assertEquals(2, slice.unsigned(1));
        assertEquals(0x04030201, slice.buffer(ByteOrder.LITTLE_ENDIAN).getInt(0));
        assertArrayEquals(new byte[] {2, 3, 4}, slice.copyFrom(1));
    }
}

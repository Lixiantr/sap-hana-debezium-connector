package io.hanacdc.decoder;

import org.junit.jupiter.api.Test;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class StrictUtf8Test {
    @Test
    void decodesAsciiIncludingNulOnFastPath() throws Exception {
        assertEquals("ascii\0value", StrictUtf8.decode(
                "ascii\0value".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void decodesExactRangeWithoutCopying() throws Exception {
        byte[] value = "xприветy".getBytes(StandardCharsets.UTF_8);
        assertEquals("привет", StrictUtf8.decode(value, 1, value.length - 2));
    }

    @Test
    void rejectsMalformedInputAndRecoversOnSameThread() throws Exception {
        assertThrows(CharacterCodingException.class,
                () -> StrictUtf8.decode(new byte[] {(byte) 0xc3, 0x28}));
        assertEquals("valid", StrictUtf8.decode("valid".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void isolatesDecodersAcrossProcessorThreads() throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> StrictUtf8.decode(
                    "первый".getBytes(StandardCharsets.UTF_8)));
            var second = executor.submit(() -> StrictUtf8.decode(
                    "второй".getBytes(StandardCharsets.UTF_8)));
            assertEquals("первый", first.get());
            assertEquals("второй", second.get());
        }
        finally {
            executor.shutdownNow();
        }
    }
}

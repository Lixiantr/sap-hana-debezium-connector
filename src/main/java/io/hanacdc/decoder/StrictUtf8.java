package io.hanacdc.decoder;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/** Reuses one strict UTF-8 decoder per processor thread. */
final class StrictUtf8 {
    private static final ThreadLocal<CharsetDecoder> DECODERS = ThreadLocal.withInitial(
            () -> StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT));

    private StrictUtf8() {}

    static String decode(byte[] value) throws CharacterCodingException {
        return decode(value, 0, value.length);
    }

    static String decode(byte[] value, int offset, int length)
            throws CharacterCodingException {
        if (offset < 0 || length < 0 || offset > value.length - length) {
            throw new IndexOutOfBoundsException("UTF-8 range is outside input");
        }
        int limit = offset + length;
        int index = offset;
        while (index < limit && value[index] >= 0) {
            index++;
        }
        if (index == limit) {
            return new String(value, offset, length, StandardCharsets.ISO_8859_1);
        }
        return DECODERS.get().decode(ByteBuffer.wrap(value, offset, length)).toString();
    }
}

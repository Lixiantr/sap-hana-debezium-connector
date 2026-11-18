package io.hanacdc.decoder;

import java.nio.charset.CharacterCodingException;
import java.util.Arrays;

/** Characterized short inline byte/string values with a one-byte length. */
public final class NativeLengthPrefixedDecoder {
    private NativeLengthPrefixedDecoder() {}

    public static byte[] decodeBytes(
            NativeRowDescriptorLayout layout, int descriptorIndex, byte[] payload) {
        int offset = layout.directValueOffset(descriptorIndex)
                .orElseThrow(() -> new IllegalArgumentException("descriptor has no direct slot"));
        if (offset < 0 || offset >= payload.length) {
            throw new IllegalArgumentException("length prefix outside native payload");
        }
        int length = Byte.toUnsignedInt(payload[offset]);
        // 0xf6+ are reserved/extended forms in the characterized descriptor
        // family. Do not guess their grammar.
        if (length >= 0xf6) {
            throw new IllegalArgumentException("unsupported extended native length prefix " + length);
        }
        if (offset + 1 + length > payload.length) {
            throw new IllegalArgumentException("inline value exceeds native payload");
        }
        return Arrays.copyOfRange(payload, offset + 1, offset + 1 + length);
    }

    public static String decodeUtf8(
            NativeRowDescriptorLayout layout, int descriptorIndex, byte[] payload) {
        int offset = layout.directValueOffset(descriptorIndex)
                .orElseThrow(() -> new IllegalArgumentException("descriptor has no direct slot"));
        if (offset < 0 || offset >= payload.length) {
            throw new IllegalArgumentException("length prefix outside native payload");
        }
        int length = Byte.toUnsignedInt(payload[offset]);
        if (length >= 0xf6) {
            throw new IllegalArgumentException("unsupported extended native length prefix " + length);
        }
        if (offset + 1 + length > payload.length) {
            throw new IllegalArgumentException("inline value exceeds native payload");
        }
        try {
            return StrictUtf8.decode(payload, offset + 1, length);
        }
        catch (CharacterCodingException error) {
            throw new IllegalArgumentException("invalid UTF-8 in native inline value", error);
        }
    }
}

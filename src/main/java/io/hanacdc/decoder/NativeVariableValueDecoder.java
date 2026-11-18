package io.hanacdc.decoder;

import java.nio.charset.CharacterCodingException;

/** Decodes characterized inline values carried by a variable-slot record. */
public final class NativeVariableValueDecoder {
    private NativeVariableValueDecoder() {}

    public static String decodeUtf8(NativeVariableSlotLayout.Slot slot) {
        requirePresent(slot);
        return strictUtf8(slot.slice());
    }

    public static String decodeTaggedUtf8(NativeVariableSlotLayout.Slot slot, int expectedTag) {
        requirePresent(slot);
        ByteSlice value = slot.slice();
        if (value.length() == 0 || value.unsigned(0) != expectedTag) {
            throw new IllegalArgumentException("unexpected native variable tag");
        }
        return strictUtf8(value, 1, value.length() - 1);
    }

    public static byte[] decodeTaggedBytes(NativeVariableSlotLayout.Slot slot, int expectedTag) {
        requirePresent(slot);
        ByteSlice value = slot.slice();
        if (value.length() == 0 || value.unsigned(0) != expectedTag) {
            throw new IllegalArgumentException("unexpected native variable tag");
        }
        return value.copyFrom(1);
    }

    private static void requirePresent(NativeVariableSlotLayout.Slot slot) {
        if (slot.isNull()) throw new IllegalArgumentException("native variable value is NULL");
    }

    private static String strictUtf8(ByteSlice value) {
        return strictUtf8(value, 0, value.length());
    }

    private static String strictUtf8(ByteSlice value, int offset, int length) {
        try {
            return StrictUtf8.decode(
                    value.source(), value.offset() + offset, length);
        }
        catch (CharacterCodingException error) {
            throw new IllegalArgumentException("invalid UTF-8 in native variable slot", error);
        }
    }

}

package io.hanacdc.decoder;

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.List;

/** Reconstructs characterized inline HANA TEXT/BINTEXT token records. */
public final class NativeTextDecoder {
    public record Document(String text, String language) {}

    private NativeTextDecoder() {}

    public static Document decodeText(NativeVariableSlotLayout.Slot slot) {
        return decode(slot, 0x70);
    }

    public static Document decodeBinText(NativeVariableSlotLayout.Slot slot) {
        return decode(slot, 0x30);
    }

    private static Document decode(NativeVariableSlotLayout.Slot slot, int expectedType) {
        if (slot.isNull()) throw new IllegalArgumentException("native text value is NULL");
        ByteSlice value = slot.slice();
        if (value.length() < 13 || value.unsigned(0) != 0x05
                || value.unsigned(1) != 0x09
                || value.unsigned(2) != expectedType) {
            throw new IllegalArgumentException("unexpected native text header");
        }
        int tokenCount = value.unsigned(4);
        int languageLength = value.unsigned(5);
        if (languageLength > 2 || 6 + languageLength > value.length()) {
            throw new IllegalArgumentException("invalid native text language");
        }
        String language = strictUtf8(value, 6, languageLength);
        if (value.unsigned(8) != 0x02 || value.get(9) != 0) {
            throw new IllegalArgumentException("unexpected native text dictionary header");
        }
        int dictionaryCount = value.unsigned(10);
        int offset = 13 + dictionaryCount;
        if (offset > value.length()) throw new IllegalArgumentException("text dictionary is truncated");
        List<String> dictionary = new ArrayList<>(dictionaryCount);
        for (int index = 0; index < dictionaryCount; index++) {
            DecodedString decoded = decodeString(value, offset);
            dictionary.add(decoded.value());
            offset = decoded.nextOffset();
        }
        if (offset + 2 + tokenCount > value.length()
                || value.unsigned(offset++) != tokenCount
                || value.unsigned(offset++) != 1) {
            throw new IllegalArgumentException("invalid native text token index vector");
        }
        int[] tokenIndexes = new int[tokenCount];
        for (int index = 0; index < tokenCount; index++) {
            tokenIndexes[index] = checkedIndex(value.get(offset++), dictionaryCount, "token");
        }
        if (expectedType == 0x30) {
            if (offset + 1 + tokenCount > value.length()
                    || value.unsigned(offset++) != tokenCount) {
                throw new IllegalArgumentException("invalid native BINTEXT token metadata");
            }
            offset += tokenCount;
        }
        if (offset >= value.length()) throw new IllegalArgumentException("missing separator dictionary");
        int separatorCount = value.unsigned(offset++);
        List<String> separators = new ArrayList<>(separatorCount);
        for (int index = 0; index < separatorCount; index++) {
            DecodedString decoded = decodeString(value, offset);
            separators.add(decoded.value());
            offset = decoded.nextOffset();
        }
        int gaps = Math.max(0, tokenCount - 1);
        if (offset + 2 + gaps != value.length()
                || value.unsigned(offset++) != gaps
                || value.unsigned(offset++) != 1) {
            throw new IllegalArgumentException("invalid native text separator index vector");
        }
        int[] separatorIndexes = new int[gaps];
        for (int index = 0; index < gaps; index++) {
            separatorIndexes[index] = checkedIndex(value.get(offset++), separatorCount, "separator");
        }
        StringBuilder result = new StringBuilder();
        for (int index = 0; index < tokenCount; index++) {
            result.append(dictionary.get(tokenIndexes[index]));
            if (index < gaps) result.append(separators.get(separatorIndexes[index]));
        }
        return new Document(result.toString(), language);
    }

    private static int checkedIndex(byte encoded, int size, String kind) {
        int index = Byte.toUnsignedInt(encoded) - 1;
        if (index < 0 || index >= size) {
            throw new IllegalArgumentException("invalid native text " + kind + " index");
        }
        return index;
    }

    private static DecodedString decodeString(ByteSlice value, int offset) {
        if (offset >= value.length()) throw new IllegalArgumentException("native text string is truncated");
        int length = value.unsigned(offset);
        int start = offset + 1;
        int end = start + length;
        if (end > value.length()) throw new IllegalArgumentException("native text string exceeds record");
        return new DecodedString(strictUtf8(value, start, length), end);
    }

    private static String strictUtf8(ByteSlice value, int offset, int length) {
        try {
            return StrictUtf8.decode(value.source(), value.offset() + offset, length);
        }
        catch (CharacterCodingException error) {
            throw new IllegalArgumentException("invalid UTF-8 in native text record", error);
        }
    }

    private record DecodedString(String value, int nextOffset) {}
}

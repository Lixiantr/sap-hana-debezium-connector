package io.hanacdc.decoder;

import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class NativeTextDecoderTest {
    @Test
    void reconstructsComplexTextAndBinTextWithExactSeparators() {
        var text = slot("BQlwHwYAAAACAAoBAv8E/wb/CP///woDT25lA29uZQNUV08DdHdv"
                + "BWNhZsOpBGNhZmUGRTA4MDRDBmUwODA0YwRsaW5lATIGAQEDBQcJCgUDLCAg"
                + "AyEgIAEgAQoBLQUBAQIDBAU=");
        var bintext = slot("BQkwHgYCZW4CAAoBAv8E/wb/CP///woDQmluA2JpbgRURVhUBHRleHQF"
                + "Y2Fmw6kEY2FmZQZFMDgwNEMGZTA4MDRjBGxpbmUBMgYBAQMFBwkKBgUFBQmF"
                + "hQUCLCACISABIAEKAS0FAQECAwQF");

        assertEquals("One,  TWO!  café E0804C\nline-2",
                NativeTextDecoder.decodeText(text).text());
        assertEquals("", NativeTextDecoder.decodeText(text).language());
        assertEquals("Bin, TEXT! café E0804C\nline-2",
                NativeTextDecoder.decodeBinText(bintext).text());
        assertEquals("en", NativeTextDecoder.decodeBinText(bintext).language());
    }

    private static NativeVariableSlotLayout.Slot slot(String base64) {
        byte[] value = Base64.getDecoder().decode(base64);
        return new NativeVariableSlotLayout.Slot(0, 0, value.length, value);
    }
}

package io.hanacdc.decoder;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;

/** Decodes characterized fixed-width exact numerics from native row slots. */
public final class NativeDecimalDecoder {
    private NativeDecimalDecoder() {}

    /**
     * HANA's characterized DECIMAL slot stores most-significant 64-bit limbs
     * first, while every limb itself is little-endian.
     */
    public static BigDecimal decodeDecimal(
            NativeRowDescriptorLayout layout, int descriptorIndex,
            int scale, int width, byte[] payload) {
        return decodeDecimal(scale, width,
                directSlice(layout, descriptorIndex, width, payload));
    }

    static BigDecimal decodeDecimal(int scale, int width, ByteSlice encoded) {
        if (width <= 0 || width % Long.BYTES != 0) {
            throw new IllegalArgumentException("DECIMAL width must be a multiple of 8");
        }
        if (encoded.length() < width) {
            throw new IllegalArgumentException("numeric exceeds native payload");
        }
        byte[] bigEndian = new byte[width];
        for (int limb = 0; limb < width / Long.BYTES; limb++) {
            int encodedStart = limb * Long.BYTES;
            for (int index = 0; index < Long.BYTES; index++) {
                bigEndian[encodedStart + index] =
                        encoded.get(encodedStart + Long.BYTES - 1 - index);
            }
        }
        return new BigDecimal(new BigInteger(bigEndian), scale);
    }

    /** HANA SMALLDECIMAL in the characterized native slot uses IEEE 754 decimal64 BID. */
    public static BigDecimal decodeSmallDecimal(
            NativeRowDescriptorLayout layout, int descriptorIndex, byte[] payload) {
        return decodeSmallDecimal(
                directSlice(layout, descriptorIndex, Long.BYTES, payload));
    }

    static BigDecimal decodeSmallDecimal(ByteSlice encoded) {
        if (encoded.length() < Long.BYTES) {
            throw new IllegalArgumentException("numeric exceeds native payload");
        }
        long bits = encoded.buffer(ByteOrder.LITTLE_ENDIAN).getLong();
        long combination = bits & 0x7c00_0000_0000_0000L;
        if (combination == 0x7800_0000_0000_0000L
                || combination == 0x7c00_0000_0000_0000L) {
            throw new IllegalArgumentException("non-finite SMALLDECIMAL is unsupported");
        }

        int biasedExponent;
        long coefficient;
        if ((bits & 0x6000_0000_0000_0000L) != 0x6000_0000_0000_0000L) {
            biasedExponent = (int) ((bits >>> 53) & 0x3ffL);
            coefficient = bits & 0x001f_ffff_ffff_ffffL;
        }
        else {
            biasedExponent = (int) ((bits >>> 51) & 0x3ffL);
            coefficient = (bits & 0x0007_ffff_ffff_ffffL) | 0x0020_0000_0000_0000L;
        }
        if (coefficient > 9_999_999_999_999_999L) {
            throw new IllegalArgumentException("non-canonical SMALLDECIMAL coefficient");
        }
        BigInteger signed = BigInteger.valueOf(coefficient);
        if (bits < 0) {
            signed = signed.negate();
        }
        return new BigDecimal(signed, 398 - biasedExponent);
    }

    private static ByteSlice directSlice(
            NativeRowDescriptorLayout layout, int descriptorIndex,
            int width, byte[] payload) {
        if (width <= 0) throw new IllegalArgumentException("numeric width must be positive");
        int offset = layout.directValueOffset(descriptorIndex)
                .orElseThrow(() -> new IllegalArgumentException("descriptor has no direct slot"));
        if (offset < 0 || offset > payload.length - width) {
            throw new IllegalArgumentException("numeric exceeds native payload");
        }
        return ByteSlice.view(payload, offset, width);
    }
}

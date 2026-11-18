package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Schema-specific decoder used to prove the first native row vertical slice.
 * It is intentionally limited to F0804A.SIMPLE_ROW and is not a generic HANA
 * row decoder.
 */
public final class SimpleRowProbeDecoder {
    public static final List<Long> FULL_COLUMN_IDS = List.of(201L, 202L, 203L, 204L);
    public static final List<Long> SPARSE_UPDATE_COLUMN_IDS = List.of(202L, 204L);

    private SimpleRowProbeDecoder() {}

    public record FullRow(long id, long rowVersion, boolean deleted, String valueText) {}

    public record SparseUpdate(long rowVersion, String valueText) {}

    public record DecodedImage(
            UnifiedTableRowLayout layout,
            Optional<FullRow> fullRow,
            Optional<SparseUpdate> sparseUpdate) {}

    public static Optional<DecodedImage> decode(NativeRecordHeader record) throws IOException {
        byte[] payload = NativeRecordReader.readPayload(record);
        var layoutValue = UnifiedTableRowLayout.parse(record.operationCode(), payload);
        if (layoutValue.isEmpty()) {
            return Optional.empty();
        }
        UnifiedTableRowLayout layout = layoutValue.get();
        if (layout.internalColumnIds().equals(FULL_COLUMN_IDS)) {
            int data = layout.vectorOffset() + 48;
            require(payload, data, 21, record);
            long id = littleLong(payload, data);
            long version = littleLong(payload, data + 8);
            boolean deleted = littleInt(payload, data + 16) != 0;
            String text = shortUtf8(payload, data + 20, record);
            return Optional.of(new DecodedImage(
                    layout, Optional.of(new FullRow(id, version, deleted, text)), Optional.empty()));
        }
        if (layout.internalColumnIds().equals(SPARSE_UPDATE_COLUMN_IDS)) {
            int data = layout.vectorOffset() + 32;
            require(payload, data, 9, record);
            long version = littleLong(payload, data);
            String text = shortUtf8(payload, data + 8, record);
            return Optional.of(new DecodedImage(
                    layout, Optional.empty(), Optional.of(new SparseUpdate(version, text))));
        }
        return Optional.of(new DecodedImage(layout, Optional.empty(), Optional.empty()));
    }

    private static String shortUtf8(byte[] payload, int lengthOffset, NativeRecordHeader record)
            throws IOException {
        require(payload, lengthOffset, 1, record);
        int length = payload[lengthOffset] & 0xff;
        if (length >= 0xf6) {
            throw new IOException("multi-byte variable length is not characterized at position "
                    + record.position());
        }
        require(payload, lengthOffset + 1, length, record);
        return new String(payload, lengthOffset + 1, length, StandardCharsets.UTF_8);
    }

    private static void require(
            byte[] payload, int offset, int length, NativeRecordHeader record) throws IOException {
        if (offset < 0 || length < 0 || offset > payload.length - length) {
            throw new IOException("row value exceeds payload at log position " + record.position());
        }
    }

    private static int littleInt(byte[] value, int offset) {
        return (value[offset] & 0xff)
                | ((value[offset + 1] & 0xff) << 8)
                | ((value[offset + 2] & 0xff) << 16)
                | ((value[offset + 3] & 0xff) << 24);
    }

    private static long littleLong(byte[] value, int offset) {
        long result = 0;
        for (int index = 0; index < Long.BYTES; index++) {
            result |= (long) (value[offset + index] & 0xff) << (index * 8);
        }
        return result;
    }
}

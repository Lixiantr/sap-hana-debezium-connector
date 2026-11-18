package io.hanacdc.decoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** First verified structural layer of INSERT/UPSERT/UPDATE row payloads. */
public record UnifiedTableRowLayout(
        int vectorOffset,
        int vectorFlags,
        int columnCount,
        int vectorVersion,
        int imageVersion,
        List<Long> internalColumnIds,
        int encodedBodyOffset) {

    public UnifiedTableRowLayout {
        internalColumnIds = List.copyOf(internalColumnIds);
    }

    public static Optional<UnifiedTableRowLayout> parse(int operationCode, byte[] payload) {
        int vectorOffset = switch (operationCode) {
            case NativeRecordReader.UT_INSERT_DATA -> 32;
            case NativeRecordReader.UT_UPSERT, NativeRecordReader.UT_UPDATE -> 48;
            default -> -1;
        };
        if (vectorOffset < 0 || payload.length < vectorOffset + 5) {
            return Optional.empty();
        }
        int flags = payload[vectorOffset] & 0xff;
        int columnCount = payload[vectorOffset + 2] & 0xff;
        int vectorVersion;
        int imageVersion;
        int idsOffset;
        int vectorFormat = payload[vectorOffset + 1] & 0xff;
        if (vectorFormat == 0) {
            vectorVersion = payload[vectorOffset + 3] & 0xff;
            imageVersion = payload[vectorOffset + 4] & 0xff;
            idsOffset = vectorOffset + 5;
        }
        else if (vectorFormat == 1) {
            if (payload.length < vectorOffset + 8
                    || payload[vectorOffset + 3] != 0
                    || payload[vectorOffset + 7] != 0) {
                return Optional.empty();
            }
            vectorVersion = (payload[vectorOffset + 4] & 0xff)
                    | ((payload[vectorOffset + 5] & 0xff) << 8);
            imageVersion = payload[vectorOffset + 6] & 0xff;
            idsOffset = vectorOffset + 8;
        }
        else {
            return Optional.empty();
        }
        int bodyOffset;
        try {
            bodyOffset = Math.addExact(idsOffset, Math.multiplyExact(columnCount, Integer.BYTES));
        }
        catch (ArithmeticException error) {
            return Optional.empty();
        }
        if ((flags & 0x80) == 0 || columnCount == 0 || bodyOffset > payload.length) {
            return Optional.empty();
        }
        List<Long> columnIds = new ArrayList<>(columnCount);
        for (int index = 0; index < columnCount; index++) {
            int offset = idsOffset + index * Integer.BYTES;
            long value = Integer.toUnsignedLong(
                    (payload[offset] & 0xff)
                            | ((payload[offset + 1] & 0xff) << 8)
                            | ((payload[offset + 2] & 0xff) << 16)
                            | ((payload[offset + 3] & 0xff) << 24));
            columnIds.add(value);
        }
        return Optional.of(new UnifiedTableRowLayout(
                vectorOffset, flags, columnCount, vectorVersion, imageVersion,
                columnIds, bodyOffset));
    }
}

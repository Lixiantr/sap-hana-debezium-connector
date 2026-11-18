package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeBackupDecoder.NativeRecord;
import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/** Strictly unwraps the characterized 0x05100002 multi-row record. */
final class NativeMassRecord {
    private static final int WRAPPER_BYTES = 24;

    record Fragment(long startPosition, long offset, int totalLength, byte[] data) {
        Fragment {
            data = data.clone();
        }
        @Override public byte[] data() { return data.clone(); }
    }

    private NativeMassRecord() {}

    static NativeRecord unwrap(NativeRecord record) throws IOException {
        NativeRecordHeader outer = record.header();
        if (outer.outerCode() != NativeRecordReader.OUTER_UNIFIED_TABLE_MASS) {
            return record;
        }
        Fragment fragment = fragment(record);
        if (fragment.offset() != 0 || fragment.data().length != fragment.totalLength()) {
            throw fatal(outer, "wrapper contains a fragmented row image");
        }
        return fromInner(outer, fragment.data());
    }

    static Fragment fragment(NativeRecord record) throws IOException {
        NativeRecordHeader outer = record.header();
        if (outer.outerCode() != NativeRecordReader.OUTER_UNIFIED_TABLE_MASS) {
            throw fatal(outer, "record is not a mass wrapper");
        }
        byte[] wrapped = record.payloadView();
        if (wrapped.length < WRAPPER_BYTES) {
            throw fatal(outer, "wrapper header is truncated");
        }
        ByteBuffer prefix = ByteBuffer.wrap(wrapped).order(ByteOrder.LITTLE_ENDIAN);
        long encodedPosition = prefix.getLong(0);
        long fragmentOffset = prefix.getLong(8);
        long innerLength = Integer.toUnsignedLong(prefix.getInt(16));
        long reservedB = Integer.toUnsignedLong(prefix.getInt(20));
        long fragmentLength = wrapped.length - WRAPPER_BYTES;
        if (encodedPosition <= 0 || encodedPosition > outer.position()
                || fragmentOffset < 0 || innerLength < 40
                || innerLength > Integer.MAX_VALUE || reservedB != 0
                || fragmentOffset > innerLength
                || fragmentLength > innerLength - fragmentOffset) {
            throw fatal(outer, "unknown wrapper grammar: encodedPosition="
                    + encodedPosition + " fragmentOffset=" + fragmentOffset
                    + " innerLength=" + innerLength + " fragmentLength=" + fragmentLength);
        }
        return new Fragment(encodedPosition, fragmentOffset, (int) innerLength,
                Arrays.copyOfRange(wrapped, WRAPPER_BYTES, wrapped.length));
    }

    static NativeRecord fromInner(NativeRecordHeader outer, byte[] payload) throws IOException {
        if (payload.length < 40) {
            throw fatal(outer, "assembled row header is truncated");
        }
        ByteBuffer inner = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        long poid = Integer.toUnsignedLong(inner.getInt(4));
        long containerId = inner.getLong(8);
        int operationCode = inner.getInt(16);
        if (poid <= 0 || containerId <= 0
                || (operationCode != NativeRecordReader.UT_UPSERT
                && operationCode != NativeRecordReader.UT_INSERT_DATA)) {
            throw fatal(outer, "unsupported inner table operation");
        }
        int firstRowOffset = operationCode == NativeRecordReader.UT_INSERT_DATA ? 24 : 32;
        if (payload.length < firstRowOffset + Long.BYTES) {
            throw fatal(outer, "inner row locator is truncated");
        }
        long firstRowId = inner.getLong(firstRowOffset);
        if (firstRowId < 0) {
            throw fatal(outer, "invalid inner first row locator");
        }
        NativeRecordHeader header = new NativeRecordHeader(
                outer.backup(), outer.fileOffset(), outer.position(), outer.tid(),
                NativeRecordReader.OUTER_UNIFIED_TABLE, payload.length,
                outer.allocationBytes(), poid, containerId, operationCode, firstRowId);
        return NativeRecord.takeOwnership(header, payload);
    }

    private static IOException fatal(NativeRecordHeader header, String detail) {
        return new IOException("FATAL_NATIVE_MASS_WRAPPER: position=" + header.position()
                + ": " + detail);
    }
}

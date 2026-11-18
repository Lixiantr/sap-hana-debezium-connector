package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeBackupDecoder.NativeRecord;
import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Bounded transaction-aware reconstruction of fragmented 0x05100002 row images. */
final class NativeMassRecordAssembler {
    private static final class State {
        private final NativeRecordHeader firstHeader;
        private final byte[] payload;
        private int written;

        private State(NativeRecordHeader firstHeader, int length) {
            this.firstHeader = firstHeader;
            payload = new byte[length];
        }
    }

    private final Map<Long, State> states = new HashMap<>();
    private final long maxBufferedBytes;
    private long bufferedBytes;

    NativeMassRecordAssembler(long maxBufferedBytes) {
        if (maxBufferedBytes <= 0) throw new IllegalArgumentException("invalid mass buffer limit");
        this.maxBufferedBytes = maxBufferedBytes;
    }

    Optional<NativeRecord> accept(NativeRecord record) throws IOException {
        NativeRecordHeader header = record.header();
        if (header.outerCode() != NativeRecordReader.OUTER_UNIFIED_TABLE_MASS) {
            if (states.containsKey(header.tid())) {
                throw fatal(header, "non-fragment record before mass image completion");
            }
            return Optional.of(record);
        }
        NativeMassRecord.Fragment fragment = NativeMassRecord.fragment(record);
        State state = states.get(header.tid());
        if (state == null) {
            if (fragment.offset() != 0 || fragment.startPosition() != header.position()) {
                throw fatal(header, "mass image does not start at offset zero");
            }
            if (bufferedBytes > maxBufferedBytes - fragment.totalLength()) {
                throw resource(header, "mass image buffer limit exceeded");
            }
            state = new State(header, fragment.totalLength());
            states.put(header.tid(), state);
            bufferedBytes += fragment.totalLength();
        }
        else if (fragment.startPosition() != state.firstHeader.position()
                || fragment.totalLength() != state.payload.length) {
            throw fatal(header, "fragment identity changed inside mass image");
        }
        if (fragment.offset() != state.written) {
            throw fatal(header, "fragment gap/overlap: expected=" + state.written
                    + " actual=" + fragment.offset());
        }
        byte[] bytes = fragment.data();
        System.arraycopy(bytes, 0, state.payload, state.written, bytes.length);
        state.written += bytes.length;
        if (state.written < state.payload.length) return Optional.empty();

        states.remove(header.tid());
        bufferedBytes -= state.payload.length;
        return Optional.of(NativeMassRecord.fromInner(state.firstHeader, state.payload));
    }

    void finish() throws IOException {
        if (!states.isEmpty()) {
            throw new IOException("FATAL_NATIVE_MASS_INCOMPLETE: open images=" + states.size());
        }
    }

    void clear() {
        states.clear();
        bufferedBytes = 0;
    }

    private static IOException fatal(NativeRecordHeader header, String detail) {
        return new IOException("FATAL_NATIVE_MASS_RECONSTRUCTION: position="
                + header.position() + " tid=" + header.tid() + ": " + detail);
    }

    private static IOException resource(NativeRecordHeader header, String detail) {
        return new IOException("FATAL_RESOURCE_LIMIT: MASS position="
                + header.position() + " tid=" + header.tid() + ": " + detail);
    }
}

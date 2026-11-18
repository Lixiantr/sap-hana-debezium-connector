package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Reconstructs characterized inline-write and chunked virtual-file LOB values. */
public final class NativeLobReconstructor {
    public static final long DEFAULT_MAX_BUFFERED_BYTES = 64L * 1024 * 1024;
    public static final long DEFAULT_MAX_LOB_BYTES = 16L * 1024 * 1024;
    public static final int DEFAULT_MAX_OPEN_LOCATORS = 1024;

    public record Limits(long maxBufferedBytes, long maxLobBytes, int maxOpenLocators) {
        public Limits {
            if (maxBufferedBytes <= 0 || maxLobBytes <= 0 || maxOpenLocators <= 0
                    || maxLobBytes > maxBufferedBytes) {
                throw new IllegalArgumentException("invalid LOB reconstruction limits");
            }
        }
    }

    public static final Limits DEFAULT_LIMITS = new Limits(
            DEFAULT_MAX_BUFFERED_BYTES, DEFAULT_MAX_LOB_BYTES,
            DEFAULT_MAX_OPEN_LOCATORS);

    public record LobValue(long locatorId, byte[] data, long characterSize, String storage) {
        public LobValue {
            data = data.clone();
        }

        @Override
        public byte[] data() {
            return data.clone();
        }
    }

    private NativeLobReconstructor() {}

    /** Transaction-scoped state retained across any number of backup objects. */
    public static final class Session implements AutoCloseable {
        private final long maxBufferedBytes;
        private final long maxLobBytes;
        private final int maxOpenLocators;
        private final Map<Long, LobValue> completed = new LinkedHashMap<>();
        private final Map<Long, ChunkState> chunks = new LinkedHashMap<>();
        private long bufferedBytes;
        private boolean closed;

        public Session() {
            this(DEFAULT_LIMITS);
        }

        public Session(Limits limits) {
            this(limits.maxBufferedBytes(), limits.maxLobBytes(),
                    limits.maxOpenLocators());
        }

        public Session(long maxBufferedBytes, long maxLobBytes, int maxOpenLocators) {
            if (maxBufferedBytes <= 0 || maxLobBytes <= 0 || maxOpenLocators <= 0
                    || maxLobBytes > maxBufferedBytes) {
                throw new IllegalArgumentException("invalid LOB reconstruction limits");
            }
            this.maxBufferedBytes = maxBufferedBytes;
            this.maxLobBytes = maxLobBytes;
            this.maxOpenLocators = maxOpenLocators;
        }

        public void accept(NativeRecordHeader record) throws IOException {
            ensureOpen();
            byte[] payload = (record.outerCode() == NativeRecordReader.OUTER_LOB_WRITE
                    || record.outerCode() == NativeRecordReader.OUTER_VIRTUAL_FILE)
                    ? NativeRecordReader.readPayload(record) : new byte[0];
            accept(record, payload);
        }

        /** Accepts a payload already read by the owning bounded backup cursor. */
        public void accept(NativeRecordHeader record, byte[] payload) throws IOException {
            ensureOpen();
            if (record.outerCode() != NativeRecordReader.OUTER_LOB_WRITE
                    && record.outerCode() != NativeRecordReader.OUTER_VIRTUAL_FILE) {
                return;
            }
            if (payload == null || payload.length != record.payloadSize()) {
                throw fatal("record payload length mismatch");
            }
            if (record.outerCode() == NativeRecordReader.OUTER_LOB_WRITE) {
                if (payload.length < 48) throw fatal("LOB_WRITE header is truncated");
                ByteBuffer bytes = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
                long locator = bytes.getLong(24);
                long binarySize = bytes.getLong(32);
                long characterSize = bytes.getLong(40);
                if (binarySize < 0 || binarySize != payload.length - 48L) {
                    throw fatal("LOB_WRITE binary size mismatch");
                }
                reserve(binarySize);
                putUnique(completed, new LobValue(locator,
                        Arrays.copyOfRange(payload, 48, payload.length),
                        characterSize, "LOB_WRITE"));
                return;
            }
            if (record.outerCode() != NativeRecordReader.OUTER_VIRTUAL_FILE) return;
            long locator = record.containerId();
            if (record.operationCode() == NativeRecordReader.VF_TRUNCATE) {
                if (!chunks.containsKey(locator) && chunks.size() >= maxOpenLocators) {
                    throw resource("LOB: too many open locators");
                }
                ChunkState previous = chunks.put(locator, new ChunkState(maxLobBytes));
                if (previous != null) {
                    bufferedBytes -= previous.size();
                }
                return;
            }
            if (record.operationCode() != NativeRecordReader.VF_OVERWRITE) return;
            if (payload.length < 32) throw fatal("LOB overwrite header is truncated");
            ByteBuffer bytes = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
            long expectedRemaining = Integer.toUnsignedLong(bytes.getInt(24));
            int isFirst = bytes.getInt(28);
            if (isFirst != 1) throw fatal("unexpected LOB overwrite first flag");
            ChunkState state = chunks.get(locator);
            if (state == null) {
                if (chunks.size() >= maxOpenLocators) {
                    throw resource("LOB: too many open locators");
                }
                state = new ChunkState(maxLobBytes);
                chunks.put(locator, state);
            }
            byte[] chunk = Arrays.copyOfRange(payload, 32, payload.length);
            reserve(chunk.length);
            try {
                state.append(expectedRemaining, chunk);
            }
            catch (IOException error) {
                bufferedBytes -= chunk.length;
                throw error;
            }
            if (state.complete()) {
                byte[] file = state.bytes();
                if (file.length < 4 || file[0] != 0 || file[1] != 0
                        || file[2] != 0 || file[3] != 0) {
                    throw fatal("unexpected characterized LOB file prefix");
                }
                byte[] logical = Arrays.copyOfRange(file, 4, file.length);
                bufferedBytes -= 4;
                putUnique(completed, new LobValue(locator, logical, -1, "VIRTUAL_FILE"));
                chunks.remove(locator);
            }
        }

        /**
         * Decodes only a LOB locator referenced by a selected-table row image.
         * HANA uses the same outer LOB family for catalog/DDL metadata whose
         * payload grammar is intentionally outside the row CDC contract.
         */
        public void acceptRequired(
                NativeRecordHeader record,
                byte[] payload,
                Set<Long> requiredLocators) throws IOException {
            ensureOpen();
            if (record.outerCode() == NativeRecordReader.OUTER_LOB_WRITE) {
                if (payload == null || payload.length != record.payloadSize()) {
                    throw fatal("record payload length mismatch");
                }
                if (payload.length < 32) {
                    throw fatal("LOB_WRITE locator header is truncated");
                }
                long locator = ByteBuffer.wrap(payload)
                        .order(ByteOrder.LITTLE_ENDIAN).getLong(24);
                if (!requiredLocators.contains(locator)) {
                    return;
                }
            }
            else if (record.outerCode() == NativeRecordReader.OUTER_VIRTUAL_FILE) {
                if (!requiredLocators.contains(record.containerId())) {
                    return;
                }
            }
            else {
                return;
            }
            accept(record, payload);
        }

        public Map<Long, LobValue> finish() throws IOException {
            ensureOpen();
            if (!chunks.isEmpty()) {
                throw incomplete("transaction ended with incomplete LOB virtual files");
            }
            return Map.copyOf(completed);
        }

        public long bufferedBytes() {
            return bufferedBytes;
        }

        private void reserve(long bytes) throws IOException {
            if (bytes < 0) {
                throw fatal("negative LOB size");
            }
            if (bytes > maxLobBytes) {
                throw tooLarge("LOB value exceeds configured limit");
            }
            if (bufferedBytes > maxBufferedBytes - bytes) {
                throw resource("LOB: reconstruction memory budget exceeded");
            }
            bufferedBytes += bytes;
        }

        private void ensureOpen() throws IOException {
            if (closed) throw fatal("LOB reconstruction session is closed");
        }

        @Override
        public void close() {
            closed = true;
            completed.clear();
            chunks.clear();
            bufferedBytes = 0;
        }
    }

    public static Map<Long, LobValue> reconstruct(List<NativeRecordHeader> records)
            throws IOException {
        try (Session session = new Session()) {
            for (NativeRecordHeader record : records) {
                session.accept(record);
            }
            return session.finish();
        }
    }

    private static void putUnique(Map<Long, LobValue> values, LobValue value) throws IOException {
        if (values.putIfAbsent(value.locatorId(), value) != null) {
            throw fatal("duplicate reconstructed LOB locator");
        }
    }

    private static final class ChunkState {
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();
        private final long maxLobBytes;
        private long total = -1;

        private ChunkState(long maxLobBytes) {
            this.maxLobBytes = maxLobBytes;
        }

        void append(long expectedRemaining, byte[] chunk) throws IOException {
            if (total < 0) {
                if (expectedRemaining > maxLobBytes) {
                    throw tooLarge("LOB value exceeds configured limit");
                }
                total = expectedRemaining;
            }
            long actualRemaining = total - output.size();
            if (expectedRemaining != actualRemaining || chunk.length > expectedRemaining) {
                throw incomplete("LOB overwrite sequence has a gap or overlap");
            }
            output.write(chunk);
        }

        boolean complete() {
            return total >= 0 && output.size() == total;
        }

        byte[] bytes() {
            return output.toByteArray();
        }

        int size() {
            return output.size();
        }
    }

    private static IOException fatal(String detail) {
        return new IOException("FATAL_LOB_RECONSTRUCTION: " + detail);
    }

    private static IOException incomplete(String detail) {
        return new IOException("FATAL_INCOMPLETE_LARGE_VALUE: " + detail);
    }

    private static IOException tooLarge(String detail) {
        return new IOException("FATAL_EVENT_TOO_LARGE: " + detail);
    }

    private static IOException resource(String detail) {
        return new IOException("FATAL_RESOURCE_LIMIT: " + detail);
    }
}

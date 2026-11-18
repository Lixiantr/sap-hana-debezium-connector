package io.hanacdc.decoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * Thread-safe positional reader with one bounded heap window per calling
 * thread. File positions are longs, so an individual backup is not limited by
 * {@link ByteBuffer}'s two-GiB address space.
 */
final class BoundedFileData implements AutoCloseable {
    static final int DEFAULT_WINDOW_BYTES = 16 * 1024 * 1024;
    private static final long BYTE_ONES = 0x0101010101010101L;
    private static final long BYTE_HIGH_BITS = 0x8080808080808080L;

    private final FileChannel channel;
    private final long size;
    private final int windowBytes;
    private final ThreadLocal<Window> localWindow;
    private volatile boolean closed;

    private static final class Window {
        private final ByteBuffer bytes;
        private long start = -1;
        private int length;

        private Window(int capacity) {
            // Keep the window on the managed heap. Decoder executors are
            // intentionally short-lived per immutable backup, so direct
            // buffers would otherwise depend on delayed Cleaner execution and
            // could accumulate native memory across a long-running stream.
            bytes = ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
        }

        private boolean contains(long position, int width) {
            return start >= 0
                    && position >= start
                    && position - start <= length
                    && width <= length - (position - start);
        }
    }

    BoundedFileData(Path path) throws IOException {
        this(path, DEFAULT_WINDOW_BYTES);
    }

    BoundedFileData(Path path, int windowBytes) throws IOException {
        Objects.requireNonNull(path, "path");
        if (windowBytes < 4096) {
            throw new IllegalArgumentException("reader window must be at least 4096 bytes");
        }
        channel = FileChannel.open(path, StandardOpenOption.READ);
        size = channel.size();
        this.windowBytes = windowBytes;
        localWindow = ThreadLocal.withInitial(() -> new Window(windowBytes));
    }

    long limit() {
        return size;
    }

    byte get(long position) throws IOException {
        Window value = window(position, 1);
        return value.bytes.get(relative(value, position));
    }

    int getInt(long position) throws IOException {
        Window value = window(position, Integer.BYTES);
        if (value.contains(position, Integer.BYTES)) {
            return value.bytes.getInt(relative(value, position));
        }
        return (get(position) & 0xff)
                | ((get(position + 1) & 0xff) << 8)
                | ((get(position + 2) & 0xff) << 16)
                | ((get(position + 3) & 0xff) << 24);
    }

    long getLong(long position) throws IOException {
        Window value = window(position, Long.BYTES);
        if (value.contains(position, Long.BYTES)) {
            return value.bytes.getLong(relative(value, position));
        }
        long result = 0;
        for (int offset = 0; offset < Long.BYTES; offset++) {
            result |= (long) (get(position + offset) & 0xff) << (offset * 8);
        }
        return result;
    }

    long findByte(byte target, long from, long toExclusive) throws IOException {
        if (from >= toExclusive) {
            return -1;
        }
        checkRange(from, Math.toIntExact(Math.min(toExclusive - from, Integer.MAX_VALUE)));
        if (toExclusive > size) {
            throw new IOException("byte search ends outside file: " + toExclusive);
        }
        long repeated = (target & 0xffL) * BYTE_ONES;
        long cursor = from;
        while (cursor < toExclusive) {
            Window value = window(cursor, 1);
            int relative = relative(value, cursor);
            int localEnd = (int) Math.min(value.length, relative + toExclusive - cursor);
            int index = relative;
            int wordLimit = localEnd - Long.BYTES;
            while (index <= wordLimit) {
                long word = value.bytes.getLong(index) ^ repeated;
                if (((word - BYTE_ONES) & ~word & BYTE_HIGH_BITS) != 0) {
                    for (int offset = 0; offset < Long.BYTES; offset++) {
                        if (value.bytes.get(index + offset) == target) {
                            return value.start + index + offset;
                        }
                    }
                }
                index += Long.BYTES;
            }
            while (index < localEnd) {
                if (value.bytes.get(index) == target) {
                    return value.start + index;
                }
                index++;
            }
            cursor += localEnd - relative;
        }
        return -1;
    }

    long parseLong(long start, long end) throws IOException {
        if (start >= end) {
            throw new NumberFormatException("empty number");
        }
        Window value = window(start, 1);
        if (end - start <= Integer.MAX_VALUE
                && value.contains(start, (int) (end - start))) {
            int index = relative(value, start);
            int limit = relative(value, end);
            boolean negative = false;
            byte first = value.bytes.get(index);
            if (first == '-' || first == '+') {
                negative = first == '-';
                index++;
            }
            if (index >= limit) {
                throw new NumberFormatException("sign only");
            }
            long result = 0;
            while (index < limit) {
                int digit = value.bytes.get(index++) - '0';
                if (digit < 0 || digit > 9 || result > (Long.MAX_VALUE - digit) / 10) {
                    throw new NumberFormatException("invalid or overflowing number");
                }
                result = result * 10 + digit;
            }
            return negative ? -result : result;
        }
        int length = Math.toIntExact(end - start);
        byte[] encoded = new byte[length];
        copy(start, end, encoded, 0);
        return parseLong(encoded);
    }

    boolean isValidUtf8(long start, long end) throws IOException {
        int length = Math.toIntExact(end - start);
        Window value = window(start, 1);
        if (value.contains(start, length)) {
            return isValidUtf8(value.bytes, relative(value, start), relative(value, end));
        }
        byte[] encoded = new byte[length];
        copy(start, end, encoded, 0);
        return isValidUtf8(ByteBuffer.wrap(encoded), 0, encoded.length);
    }

    void copy(long start, long end, byte[] target, int targetStart) throws IOException {
        checkRange(start, Math.toIntExact(end - start));
        long cursor = start;
        int output = targetStart;
        while (cursor < end) {
            Window value = window(cursor, 1);
            int relative = relative(value, cursor);
            int count = (int) Math.min(end - cursor, value.length - relative);
            ByteBuffer source = value.bytes.duplicate();
            source.position(relative).limit(relative + count);
            source.get(target, output, count);
            cursor += count;
            output += count;
        }
    }

    String ascii(long start, long end) throws IOException {
        int length = Math.toIntExact(end - start);
        byte[] value = new byte[length];
        copy(start, end, value, 0);
        return new String(value, java.nio.charset.StandardCharsets.UTF_8);
    }

    private static long parseLong(byte[] data) {
        int index = 0;
        boolean negative = false;
        if (data[0] == '-' || data[0] == '+') {
            negative = data[0] == '-';
            index++;
        }
        if (index >= data.length) {
            throw new NumberFormatException("sign only");
        }
        long result = 0;
        while (index < data.length) {
            int digit = data[index++] - '0';
            if (digit < 0 || digit > 9 || result > (Long.MAX_VALUE - digit) / 10) {
                throw new NumberFormatException("invalid or overflowing number");
            }
            result = result * 10 + digit;
        }
        return negative ? -result : result;
    }

    private static boolean isValidUtf8(ByteBuffer data, int start, int end) {
        int index = start;
        while (index < end) {
            int first = data.get(index++) & 0xff;
            if (first <= 0x7f) {
                continue;
            }
            if (first >= 0xc2 && first <= 0xdf) {
                if (!continuation(data, index++, end)) {
                    return false;
                }
                continue;
            }
            if (first >= 0xe0 && first <= 0xef) {
                if (index + 1 >= end) {
                    return false;
                }
                int second = data.get(index++) & 0xff;
                int third = data.get(index++) & 0xff;
                if (third < 0x80 || third > 0xbf) {
                    return false;
                }
                if (first == 0xe0) {
                    if (second < 0xa0 || second > 0xbf) {
                        return false;
                    }
                }
                else if (first == 0xed) {
                    if (second < 0x80 || second > 0x9f) {
                        return false;
                    }
                }
                else if (second < 0x80 || second > 0xbf) {
                    return false;
                }
                continue;
            }
            if (first >= 0xf0 && first <= 0xf4) {
                if (index + 2 >= end) {
                    return false;
                }
                int second = data.get(index++) & 0xff;
                int third = data.get(index++) & 0xff;
                int fourth = data.get(index++) & 0xff;
                if (third < 0x80 || third > 0xbf || fourth < 0x80 || fourth > 0xbf) {
                    return false;
                }
                if (first == 0xf0) {
                    if (second < 0x90 || second > 0xbf) {
                        return false;
                    }
                }
                else if (first == 0xf4) {
                    if (second < 0x80 || second > 0x8f) {
                        return false;
                    }
                }
                else if (second < 0x80 || second > 0xbf) {
                    return false;
                }
                continue;
            }
            return false;
        }
        return true;
    }

    private static boolean continuation(ByteBuffer data, int index, int end) {
        if (index >= end) {
            return false;
        }
        int value = data.get(index) & 0xff;
        return value >= 0x80 && value <= 0xbf;
    }

    private Window window(long position, int width) throws IOException {
        checkRange(position, width);
        Window value = localWindow.get();
        if (value.contains(position, width)) {
            return value;
        }
        load(value, position);
        return value;
    }

    private void load(Window value, long position) throws IOException {
        ensureOpen();
        long aligned = position - position % windowBytes;
        int wanted = (int) Math.min(windowBytes, size - aligned);
        value.bytes.clear().limit(wanted);
        int read = 0;
        while (read < wanted) {
            int count = channel.read(value.bytes, aligned + read);
            if (count < 0) {
                throw new IOException("unexpected EOF at " + (aligned + read));
            }
            if (count == 0) {
                Thread.onSpinWait();
                continue;
            }
            read += count;
        }
        value.bytes.flip().order(ByteOrder.LITTLE_ENDIAN);
        value.start = aligned;
        value.length = wanted;
    }

    private int relative(Window value, long position) {
        return Math.toIntExact(position - value.start);
    }

    private void checkRange(long position, int width) throws IOException {
        ensureOpen();
        if (position < 0 || width < 0 || position > size - width) {
            throw new IOException("file read outside [0," + size + "): position="
                    + position + " width=" + width);
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("bounded file reader is closed");
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            localWindow.remove();
            channel.close();
        }
    }
}

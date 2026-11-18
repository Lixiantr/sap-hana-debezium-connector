package io.hanacdc.runtime;

import io.hanacdc.connect.CachedSchemaJsonConverter;

import org.apache.kafka.connect.source.SourceRecord;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Writes the exact Kafka Connect key/value JSON representation as one NDJSON record. */
public final class NdjsonRecordWriter implements AutoCloseable {
    public enum OpenMode {
        APPEND,
        FAIL,
        TRUNCATE;

        static OpenMode parse(String value) {
            return valueOf(value.trim().toUpperCase());
        }
    }

    public enum FsyncMode {
        BATCH,
        NONE;

        static FsyncMode parse(String value) {
            return valueOf(value.trim().toUpperCase());
        }
    }

    public record BatchMetrics(long records, long bytes, long writeNanos, long fsyncNanos) {}

    public record Totals(long records, long bytes, long writeNanos, long fsyncNanos) {}

    private static final int BUFFER_BYTES = 1024 * 1024;
    private static final byte[] TOPIC = ascii("{\"topic\":\"");
    private static final byte[] PARTITION = ascii("\",\"partition\":");
    private static final byte[] TIMESTAMP = ascii(",\"timestamp\":");
    private static final byte[] KEY = ascii(",\"key\":");
    private static final byte[] VALUE = ascii(",\"value\":");
    private static final byte[] END = ascii("}\n");
    private static final byte[] NULL = ascii("null");

    private final Path path;
    private final FsyncMode fsyncMode;
    private final FileOutputStream file;
    private final BufferedOutputStream output;
    private final CachedSchemaJsonConverter keyConverter = new CachedSchemaJsonConverter();
    private final CachedSchemaJsonConverter valueConverter = new CachedSchemaJsonConverter();
    private boolean closed;
    private long totalRecords;
    private long totalBytes;
    private long totalWriteNanos;
    private long totalFsyncNanos;

    public NdjsonRecordWriter(Path path, OpenMode openMode, FsyncMode fsyncMode)
            throws IOException {
        this.path = Objects.requireNonNull(path, "path").toAbsolutePath();
        this.fsyncMode = Objects.requireNonNull(fsyncMode, "fsyncMode");
        Objects.requireNonNull(openMode, "openMode");
        Path parent = this.path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        prepare(this.path, openMode);
        file = new FileOutputStream(this.path.toFile(), true);
        output = new BufferedOutputStream(file, BUFFER_BYTES);
        Map<String, Object> converterConfig = Map.of(
                "schemas.enable", true,
                "schemas.cache.size", 1000);
        keyConverter.configure(converterConfig, true);
        valueConverter.configure(converterConfig, false);
    }

    public synchronized BatchMetrics writeBatch(List<SourceRecord> records) throws IOException {
        ensureOpen();
        Objects.requireNonNull(records, "records");
        long bytesBefore = totalBytes;
        long writeStarted = System.nanoTime();
        for (SourceRecord record : records) {
            write(record);
            totalRecords++;
        }
        output.flush();
        long writeNanos = System.nanoTime() - writeStarted;
        long fsyncNanos = 0;
        if (fsyncMode == FsyncMode.BATCH) {
            long fsyncStarted = System.nanoTime();
            file.getChannel().force(false);
            fsyncNanos = System.nanoTime() - fsyncStarted;
        }
        totalWriteNanos += writeNanos;
        totalFsyncNanos += fsyncNanos;
        return new BatchMetrics(records.size(), totalBytes - bytesBefore, writeNanos, fsyncNanos);
    }

    public synchronized Totals totals() {
        return new Totals(totalRecords, totalBytes, totalWriteNanos, totalFsyncNanos);
    }

    public Path path() {
        return path;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        IOException failure = null;
        try {
            output.flush();
            if (fsyncMode == FsyncMode.BATCH) {
                file.getChannel().force(false);
            }
        }
        catch (IOException error) {
            failure = error;
        }
        try {
            output.close();
        }
        catch (IOException error) {
            if (failure == null) {
                failure = error;
            }
            else {
                failure.addSuppressed(error);
            }
        }
        closed = true;
        try {
            keyConverter.close();
            valueConverter.close();
        }
        catch (IOException error) {
            if (failure == null) {
                failure = error;
            }
            else {
                failure.addSuppressed(error);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void write(SourceRecord record) throws IOException {
        Objects.requireNonNull(record, "record");
        byte[] key = keyConverter.fromConnectData(
                record.topic(), record.keySchema(), record.key());
        byte[] value = valueConverter.fromConnectData(
                record.topic(), record.valueSchema(), record.value());
        write(TOPIC);
        writeEscaped(record.topic());
        write(PARTITION);
        writeNumber(record.kafkaPartition());
        write(TIMESTAMP);
        writeNumber(record.timestamp());
        write(KEY);
        writeJson(key);
        write(VALUE);
        writeJson(value);
        write(END);
    }

    private void writeJson(byte[] json) throws IOException {
        write(json == null ? NULL : json);
    }

    private void writeNumber(Number value) throws IOException {
        write(value == null ? NULL : ascii(value.toString()));
    }

    private void writeEscaped(String value) throws IOException {
        for (int index = 0; index < value.length();) {
            int character = value.codePointAt(index);
            index += Character.charCount(character);
            switch (character) {
                case '\\' -> write(ascii("\\\\"));
                case '"' -> write(ascii("\\\""));
                case '\b' -> write(ascii("\\b"));
                case '\f' -> write(ascii("\\f"));
                case '\n' -> write(ascii("\\n"));
                case '\r' -> write(ascii("\\r"));
                case '\t' -> write(ascii("\\t"));
                default -> {
                    if (character < 0x20) {
                        write(ascii(String.format("\\u%04x", (int) character)));
                    }
                    else {
                        write(new String(Character.toChars(character))
                                .getBytes(StandardCharsets.UTF_8));
                    }
                }
            }
        }
    }

    private void write(byte[] bytes) throws IOException {
        output.write(bytes);
        totalBytes += bytes.length;
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("NDJSON writer is closed");
        }
    }

    private static void prepare(Path path, OpenMode mode) throws IOException {
        if (mode == OpenMode.FAIL && Files.exists(path)) {
            throw new IOException("NDJSON output already exists: " + path);
        }
        if (mode == OpenMode.TRUNCATE) {
            Files.newByteChannel(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)
                    .close();
            return;
        }
        if (mode == OpenMode.APPEND && Files.isRegularFile(path)) {
            truncatePartialLine(path);
        }
    }

    private static void truncatePartialLine(Path path) throws IOException {
        try (var channel = Files.newByteChannel(path, StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {
            long size = channel.size();
            if (size == 0) {
                return;
            }
            var tail = java.nio.ByteBuffer.allocate(1);
            channel.position(size - 1);
            channel.read(tail);
            if (tail.array()[0] == '\n') {
                return;
            }
            long position = size - 1;
            while (position >= 0) {
                tail.clear();
                channel.position(position);
                channel.read(tail);
                if (tail.array()[0] == '\n') {
                    channel.truncate(position + 1);
                    return;
                }
                position--;
            }
            channel.truncate(0);
        }
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}

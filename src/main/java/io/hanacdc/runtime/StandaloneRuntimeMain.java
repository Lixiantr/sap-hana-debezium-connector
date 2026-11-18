package io.hanacdc.runtime;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.embedded.Connect;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Standalone Debezium Engine runtime with local NDJSON and NoOp outputs. */
public final class StandaloneRuntimeMain {
    private StandaloneRuntimeMain() {}

    public static void main(String[] args) throws Exception {
        Arguments arguments = Arguments.parse(args);
        Properties properties = load(arguments.connectorConfig());
        if (arguments.secretsFile() != null) {
            properties.putAll(load(arguments.secretsFile()));
        }
        if (arguments.passwordFile() != null) {
            String password = Files.readString(arguments.passwordFile()).strip();
            if (password.isEmpty()) {
                throw new IllegalArgumentException("password file is empty");
            }
            properties.setProperty("database.password", password);
        }
        properties.setProperty("offset.storage",
                "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        properties.setProperty("offset.storage.file.filename",
                arguments.offsetFile().toAbsolutePath().toString());
        properties.putIfAbsent("offset.flush.interval.ms", "1000");
        properties.putIfAbsent("record.processing.threads", "1");
        properties.putIfAbsent("record.processing.order", "ORDERED");
        properties.remove("tasks.max");
        require(properties, "name");
        require(properties, "connector.class");
        Path offsetParent = arguments.offsetFile().toAbsolutePath().getParent();
        if (offsetParent != null) {
            Files.createDirectories(offsetParent);
        }

        if (arguments.mode() == Mode.NDJSON) {
            validateOutputOffsetPair(arguments.outputFile(), arguments.offsetFile(),
                    arguments.openMode());
        }
        NdjsonRecordWriter writer = arguments.mode() == Mode.NDJSON
                ? new NdjsonRecordWriter(arguments.outputFile(), arguments.openMode(),
                        arguments.fsyncMode())
                : null;
        RuntimeMetrics metrics = new RuntimeMetrics();
        CountDownLatch terminal = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean closeRequested = new AtomicBoolean();

        DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>> consumer =
                (events, committer) -> {
                    if (events.isEmpty()) {
                        return;
                    }
                    List<SourceRecord> records = new ArrayList<>(events.size());
                    for (RecordChangeEvent<SourceRecord> event : events) {
                        records.add(event.record());
                    }
                    long callbackStarted = System.nanoTime();
                    NdjsonRecordWriter.BatchMetrics written;
                    try {
                        written = writer == null ? null : writer.writeBatch(records);
                    }
                    catch (java.io.IOException error) {
                        throw new ConnectException("cannot durably write NDJSON batch", error);
                    }
                    for (RecordChangeEvent<SourceRecord> event : events) {
                        committer.markProcessed(event);
                    }
                    committer.markBatchFinished();
                    metrics.accept(records.size(), written,
                            System.nanoTime() - callbackStarted);
                    if (arguments.expectedRecords() > 0
                            && metrics.records() >= arguments.expectedRecords()) {
                        terminal.countDown();
                    }
                };

        DebeziumEngine<RecordChangeEvent<SourceRecord>> engine = DebeziumEngine
                .create(ChangeEventFormat.of(Connect.class))
                .using(properties)
                .using((success, message, error) -> {
                    if (!success) {
                        failure.compareAndSet(null, error == null
                                ? new IllegalStateException(message)
                                : error);
                    }
                    terminal.countDown();
                })
                .notifying(consumer)
                .build();
        Runnable closeEngine = () -> {
            if (closeRequested.compareAndSet(false, true)) {
                try {
                    engine.close();
                }
                catch (Exception error) {
                    failure.compareAndSet(null, error);
                }
            }
        };
        Thread shutdownHook = new Thread(closeEngine, "hana-cdc-standalone-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        long processStarted = System.nanoTime();
        ExecutorService executor = Executors.newSingleThreadExecutor(runnable ->
                new Thread(runnable, "hana-cdc-embedded-engine"));
        executor.submit(engine);

        boolean completed = awaitCompletion(arguments, metrics, terminal, processStarted);
        closeEngine.run();
        executor.shutdown();
        if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        }
        catch (IllegalStateException ignored) {
            // JVM shutdown already started.
        }
        if (writer != null) {
            writer.close();
        }
        if (!completed) {
            throw new IllegalStateException("standalone runtime timed out");
        }
        Throwable error = failure.get();
        if (error != null) {
            throw new IllegalStateException("standalone runtime failed", error);
        }
        if (arguments.expectedRecords() > 0
                && metrics.records() < arguments.expectedRecords()) {
            throw new IllegalStateException("insufficient record count: " + metrics.records()
                    + " expected at least: " + arguments.expectedRecords());
        }
        printResult(arguments, metrics, System.nanoTime() - processStarted);
    }

    private static void printResult(Arguments arguments, RuntimeMetrics metrics,
            long processNanos) {
        double eventSeconds = metrics.eventElapsedNanos() / 1_000_000_000.0;
        double eventsPerSecond = eventSeconds == 0 ? 0 : metrics.records() / eventSeconds;
        System.out.printf(Locale.ROOT,
                "{\"kind\":\"standalone_runtime_result\",\"success\":true,"
                        + "\"mode\":\"%s\",\"records\":%d,\"batches\":%d,"
                        + "\"output_bytes\":%d,\"event_seconds\":%.9f,"
                        + "\"events_per_second\":%.6f,\"callback_seconds\":%.9f,"
                        + "\"write_seconds\":%.9f,\"fsync_seconds\":%.9f,"
                        + "\"process_seconds\":%.9f,\"completed_at\":\"%s\"}%n",
                arguments.mode().name().toLowerCase(Locale.ROOT), metrics.records(),
                metrics.batches(), metrics.outputBytes(), eventSeconds, eventsPerSecond,
                metrics.callbackNanos() / 1_000_000_000.0,
                metrics.writeNanos() / 1_000_000_000.0,
                metrics.fsyncNanos() / 1_000_000_000.0,
                processNanos / 1_000_000_000.0, Instant.now());
    }

    private static boolean awaitCompletion(Arguments arguments, RuntimeMetrics metrics,
            CountDownLatch terminal, long processStarted) throws InterruptedException {
        long deadline = arguments.timeoutSeconds() == 0
                ? Long.MAX_VALUE
                : processStarted + TimeUnit.SECONDS.toNanos(arguments.timeoutSeconds());
        while (true) {
            if (terminal.await(200, TimeUnit.MILLISECONDS)) {
                return true;
            }
            long now = System.nanoTime();
            if (arguments.idleExitSeconds() > 0
                    && now - metrics.lastActivityNanos(processStarted)
                            >= TimeUnit.SECONDS.toNanos(arguments.idleExitSeconds())) {
                return true;
            }
            if (now >= deadline) {
                return false;
            }
        }
    }

    private static Properties load(Path path) throws Exception {
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(path)) {
            properties.load(input);
        }
        return properties;
    }

    private static void require(Properties properties, String name) {
        String value = properties.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing connector property: " + name);
        }
    }

    static void validateOutputOffsetPair(Path output, Path offset,
            NdjsonRecordWriter.OpenMode openMode) throws Exception {
        boolean outputHasData = Files.isRegularFile(output) && Files.size(output) > 0;
        boolean offsetHasData = Files.isRegularFile(offset) && Files.size(offset) > 0;
        if (openMode == NdjsonRecordWriter.OpenMode.APPEND
                && outputHasData != offsetHasData) {
            throw new IllegalArgumentException(
                    "append requires output and offset to be both fresh or both existing");
        }
        if (openMode != NdjsonRecordWriter.OpenMode.APPEND && offsetHasData) {
            throw new IllegalArgumentException(
                    "fail/truncate output requires a fresh offset file");
        }
    }

    enum Mode {
        NDJSON,
        NOOP;

        static Mode parse(String value) {
            return valueOf(value.trim().toUpperCase(Locale.ROOT));
        }
    }

    record Arguments(
            Mode mode,
            Path connectorConfig,
            Path secretsFile,
            Path passwordFile,
            Path offsetFile,
            Path outputFile,
            NdjsonRecordWriter.OpenMode openMode,
            NdjsonRecordWriter.FsyncMode fsyncMode,
            long expectedRecords,
            long timeoutSeconds,
            long idleExitSeconds) {
        static Arguments parse(String[] args) {
            Mode mode = null;
            Path connectorConfig = null;
            Path secretsFile = null;
            Path passwordFile = null;
            Path offsetFile = null;
            Path outputFile = null;
            NdjsonRecordWriter.OpenMode openMode = NdjsonRecordWriter.OpenMode.APPEND;
            NdjsonRecordWriter.FsyncMode fsyncMode = NdjsonRecordWriter.FsyncMode.BATCH;
            long expectedRecords = 0;
            long timeoutSeconds = 0;
            long idleExitSeconds = 0;
            for (int index = 0; index < args.length; index++) {
                switch (args[index]) {
                    case "--mode" -> mode = Mode.parse(args[++index]);
                    case "--connector-config" -> connectorConfig = Path.of(args[++index]);
                    case "--secrets-file" -> secretsFile = Path.of(args[++index]);
                    case "--password-file" -> passwordFile = Path.of(args[++index]);
                    case "--offset-file" -> offsetFile = Path.of(args[++index]);
                    case "--output" -> outputFile = Path.of(args[++index]);
                    case "--file-open" -> openMode =
                            NdjsonRecordWriter.OpenMode.parse(args[++index]);
                    case "--fsync" -> fsyncMode =
                            NdjsonRecordWriter.FsyncMode.parse(args[++index]);
                    case "--expected-records" -> expectedRecords = Long.parseLong(args[++index]);
                    case "--timeout-seconds" -> timeoutSeconds = Long.parseLong(args[++index]);
                    case "--idle-exit-seconds" -> idleExitSeconds = Long.parseLong(args[++index]);
                    default -> throw new IllegalArgumentException("unknown argument: " + args[index]);
                }
            }
            if (mode == null || connectorConfig == null || offsetFile == null) {
                throw usage();
            }
            if (mode == Mode.NDJSON && outputFile == null) {
                throw new IllegalArgumentException("--output is required for mode=ndjson");
            }
            if (expectedRecords < 0 || timeoutSeconds < 0 || idleExitSeconds < 0) {
                throw new IllegalArgumentException("record count and timeouts cannot be negative");
            }
            return new Arguments(mode, connectorConfig, secretsFile, passwordFile,
                    offsetFile, outputFile,
                    openMode, fsyncMode, expectedRecords, timeoutSeconds, idleExitSeconds);
        }

        private static IllegalArgumentException usage() {
            return new IllegalArgumentException(
                    "required: --mode noop|ndjson --connector-config FILE --offset-file FILE "
                            + "[--secrets-file FILE] [--password-file FILE] [--output FILE] "
                            + "[--file-open append|fail|truncate] [--fsync batch|none] "
                            + "[--expected-records N] [--timeout-seconds N] "
                            + "[--idle-exit-seconds N]");
        }
    }

    private static final class RuntimeMetrics {
        private final AtomicLong records = new AtomicLong();
        private final AtomicLong batches = new AtomicLong();
        private final AtomicLong outputBytes = new AtomicLong();
        private final AtomicLong callbackNanos = new AtomicLong();
        private final AtomicLong writeNanos = new AtomicLong();
        private final AtomicLong fsyncNanos = new AtomicLong();
        private final AtomicLong firstEventNanos = new AtomicLong();
        private final AtomicLong lastEventNanos = new AtomicLong();

        void accept(int count, NdjsonRecordWriter.BatchMetrics written, long callback) {
            long now = System.nanoTime();
            firstEventNanos.compareAndSet(0, now);
            lastEventNanos.set(now);
            records.addAndGet(count);
            batches.incrementAndGet();
            callbackNanos.addAndGet(callback);
            if (written != null) {
                outputBytes.addAndGet(written.bytes());
                writeNanos.addAndGet(written.writeNanos());
                fsyncNanos.addAndGet(written.fsyncNanos());
            }
        }

        long records() { return records.get(); }
        long batches() { return batches.get(); }
        long outputBytes() { return outputBytes.get(); }
        long callbackNanos() { return callbackNanos.get(); }
        long writeNanos() { return writeNanos.get(); }
        long fsyncNanos() { return fsyncNanos.get(); }
        long eventElapsedNanos() {
            return firstEventNanos.get() == 0 ? 0 : lastEventNanos.get() - firstEventNanos.get();
        }
        long lastActivityNanos(long defaultValue) {
            long value = lastEventNanos.get();
            return value == 0 ? defaultValue : value;
        }
    }
}

package io.hanacdc.cli;

import io.hanacdc.decoder.LocalNdjsonSink;
import io.hanacdc.decoder.LocalNdjsonSink.PublishMetrics;
import io.hanacdc.decoder.LocalNdjsonSink.Totals;
import io.hanacdc.decoder.ParallelLogDecoder;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;
import io.hanacdc.decoder.ParallelLogDecoder.DecodedFile;
import io.hanacdc.decoder.ParallelLogDecoder.DecoderSession;
import io.hanacdc.decoder.ParallelLogDecoder.ScanConfig;
import io.hanacdc.decoder.ParallelLogDecoder.ScanMetrics;

import java.nio.file.Path;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;

/** Command-line benchmark with output compatible with the Rust reference runner. */
public final class BenchmarkMain {
    private BenchmarkMain() {}

    public static void main(String[] args) {
        try {
            run(args);
        }
        catch (Exception error) {
            System.err.println("error: " + error.getMessage());
            if (Boolean.getBoolean("hana.cdc.debug")) {
                error.printStackTrace(System.err);
            }
            System.exit(1);
        }
    }

    private static void run(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            throw new IllegalArgumentException("missing command");
        }
        String command = args[0];
        Map<String, String> values = argumentMap(args);
        switch (command) {
            case "scan-file" -> scanFile(values);
            case "scan-corpus" -> scanCorpus(values);
            case "follow-corpus" -> followCorpus(values);
            default -> {
                usage();
                throw new IllegalArgumentException("unknown command " + command);
            }
        }
    }

    private static void scanFile(Map<String, String> values) throws Exception {
        Path path = Path.of(required(values, "--path"));
        BackupFile backup = BackupFile.parse(path)
                .orElseThrow(() -> new IllegalArgumentException(
                        "path is not a HANA log-backup filename: " + path));
        String runId = required(values, "--run-id");
        ScanConfig config = scanConfig(values, true);
        ParallelLogDecoder decoder = new ParallelLogDecoder();
        try (DecodedFile decoded = decoder.decode(
                backup, runId, config, ParallelLogDecoder.newCorpusDigest())) {
            System.out.println(decoded.metrics().toJson());
        }
    }

    private static void scanCorpus(Map<String, String> values) throws Exception {
        Path directory = Path.of(required(values, "--backup-dir"));
        String runId = required(values, "--run-id");
        long first = positiveLong(required(values, "--first-position"), "--first-position");
        long last = positiveLong(required(values, "--last-position"), "--last-position");
        if (last <= first) {
            throw new IllegalArgumentException("--last-position must be greater than --first-position");
        }
        Path outputPath = values.containsKey("--output")
                ? Path.of(required(values, "--output"))
                : null;
        ScanConfig config = scanConfig(values, outputPath == null);
        List<BackupFile> backups = ParallelLogDecoder.discoverCorpus(directory, first, last);
        ParallelLogDecoder decoder = new ParallelLogDecoder();
        DecoderSession session = new DecoderSession();
        MessageDigest corpusDigest = config.calculateFingerprint()
                ? ParallelLogDecoder.newCorpusDigest()
                : null;

        long wallStarted = System.nanoTime();
        long bytes = 0;
        long events = 0;
        double scanSeconds = 0.0;
        double mmapSeconds = 0.0;
        LocalNdjsonSink sink = outputPath == null ? null : new LocalNdjsonSink(outputPath);
        try {
            for (BackupFile backup : backups) {
                try (DecodedFile decoded = decoder.decode(
                        backup, runId, config, corpusDigest, session)) {
                    ScanMetrics metrics = decoded.metrics();
                    bytes += metrics.bytes();
                    events += metrics.events();
                    mmapSeconds += metrics.mmapSeconds();
                    scanSeconds += metrics.scanSeconds();
                    System.out.println(metrics.toJson());
                    if (sink != null) {
                        System.out.println(publishJson(sink.publish(decoded, runId)));
                    }
                }
            }
            session.finish();
        }
        finally {
            if (sink != null) {
                sink.close();
            }
        }
        double wallSeconds = (System.nanoTime() - wallStarted) / 1_000_000_000.0;
        String fingerprint = corpusDigest == null ? "" : ParallelLogDecoder.hex(corpusDigest.digest());
        System.out.println(summaryJson(
                runId,
                first,
                last,
                backups.size(),
                bytes,
                events,
                config,
                fingerprint,
                scanSeconds,
                wallSeconds));
        if (sink != null) {
            System.out.println(pipelineSummaryJson(
                    runId, first, last, backups.size(), bytes, events, config,
                    mmapSeconds, scanSeconds, sink.totals(), wallSeconds));
        }
    }

    /** Live capacity harness that retains one DecoderSession for the whole stream. */
    private static void followCorpus(Map<String, String> values) throws Exception {
        Path directory = Path.of(required(values, "--backup-dir"));
        String runId = required(values, "--run-id");
        long first = positiveLong(required(values, "--first-position"), "--first-position");
        Path targetFile = Path.of(required(values, "--target-file"));
        Path outputPath = Path.of(required(values, "--output"));
        long pollIntervalMs = positiveLong(
                values.getOrDefault("--poll-interval-ms", "500"),
                "--poll-interval-ms");
        if (pollIntervalMs == 0) {
            throw new IllegalArgumentException("--poll-interval-ms must be positive");
        }
        ScanConfig config = scanConfig(values, false);
        ParallelLogDecoder decoder = new ParallelLogDecoder();
        DecoderSession session = new DecoderSession();
        Map<Long, StableObservation> observations = new LinkedHashMap<>();
        long position = first;
        long inputBytes = 0;
        long events = 0;
        int files = 0;
        double scanSeconds = 0;
        double mmapSeconds = 0;
        long wallStarted = System.nanoTime();

        try (LocalNdjsonSink sink = new LocalNdjsonSink(outputPath)) {
            while (true) {
                OptionalLong target = readTargetPosition(targetFile);
                if (target.isPresent() && position >= target.getAsLong()) {
                    break;
                }
                BackupFile backup = stableSuccessor(
                        directory, position, observations, pollIntervalMs);
                if (backup == null) {
                    Thread.sleep(pollIntervalMs);
                    continue;
                }
                long completedAtMs;
                try (DecodedFile decoded = decoder.decode(
                        backup, runId, config, null, session)) {
                    ScanMetrics metrics = decoded.metrics();
                    if (metrics.scanWarnings() != 0) {
                        throw new IllegalStateException("FATAL_DECODE_WARNING: backup_id="
                                + backup.backupId() + " warnings=" + metrics.scanWarnings());
                    }
                    System.out.println(metrics.toJson());
                    PublishMetrics published = sink.publish(decoded, runId);
                    System.out.println(publishJson(published));
                    inputBytes += metrics.bytes();
                    events += metrics.events();
                    files++;
                    mmapSeconds += metrics.mmapSeconds();
                    scanSeconds += metrics.scanSeconds();
                    position = backup.last();
                    completedAtMs = System.currentTimeMillis();
                    Backlog backlog = contiguousBacklog(directory, position);
                    System.out.println(liveCommitJson(
                            backup,
                            metrics.events(),
                            published.bytes(),
                            completedAtMs,
                            backlog));
                }
            }
            session.finish();
            double wallSeconds = (System.nanoTime() - wallStarted) / 1_000_000_000.0;
            System.out.println(pipelineSummaryJson(
                    runId, first, position, files, inputBytes, events, config,
                    mmapSeconds, scanSeconds, sink.totals(), wallSeconds));
            System.out.println(String.format(Locale.ROOT,
                    "{\"kind\":\"live_complete\",\"run_id\":\"%s\","
                            + "\"first_position\":%d,\"last_position\":%d,"
                            + "\"files\":%d,\"events\":%d,\"wall_seconds\":%.9f}",
                    ParallelLogDecoder.escapeJson(runId), first, position,
                    files, events, wallSeconds));
        }
    }

    private static BackupFile stableSuccessor(
            Path directory,
            long position,
            Map<Long, StableObservation> observations,
            long pollIntervalMs) throws Exception {
        List<BackupFile> candidates = new ArrayList<>();
        try (var paths = Files.list(directory)) {
            paths.forEach(path -> BackupFile.parse(path).ifPresent(backup -> {
                if (backup.volume() == 2 && backup.partition() == 0
                        && backup.first() == position) {
                    candidates.add(backup);
                }
            }));
        }
        candidates.sort(Comparator.comparingLong(BackupFile::backupId));
        if (candidates.isEmpty()) {
            return null;
        }
        if (candidates.size() != 1) {
            throw new IllegalStateException("AMBIGUOUS_BACKUP: " + candidates.size()
                    + " files start at " + position);
        }
        BackupFile backup = candidates.get(0);
        long size = Files.size(backup.path());
        long modifiedMs = Files.getLastModifiedTime(backup.path()).toMillis();
        StableObservation current = new StableObservation(
                backup.path(), size, modifiedMs);
        StableObservation previous = observations.put(position, current);
        if (!current.equals(previous)) {
            return null;
        }
        long minimumAgeMs = Math.max(1_000L, Math.multiplyExact(pollIntervalMs, 2));
        if (System.currentTimeMillis() - modifiedMs < minimumAgeMs) {
            return null;
        }
        return backup;
    }

    private static OptionalLong readTargetPosition(Path targetFile) throws Exception {
        if (!Files.isRegularFile(targetFile)) {
            return OptionalLong.empty();
        }
        String value = Files.readString(targetFile).trim();
        if (value.isEmpty()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(positiveLong(value, "target position"));
    }

    private static Backlog contiguousBacklog(Path directory, long position) throws Exception {
        Map<Long, BackupFile> byFirst = new LinkedHashMap<>();
        try (var paths = Files.list(directory)) {
            paths.forEach(path -> BackupFile.parse(path).ifPresent(backup -> {
                if (backup.volume() == 2 && backup.partition() == 0) {
                    BackupFile prior = byFirst.putIfAbsent(backup.first(), backup);
                    if (prior != null && prior.backupId() != backup.backupId()) {
                        throw new IllegalStateException("AMBIGUOUS_BACKUP: multiple files start at "
                                + backup.first());
                    }
                }
            }));
        }
        long cursor = position;
        long bytes = 0;
        int files = 0;
        while (byFirst.containsKey(cursor)) {
            BackupFile backup = byFirst.get(cursor);
            bytes = Math.addExact(bytes, Files.size(backup.path()));
            files++;
            cursor = backup.last();
        }
        return new Backlog(cursor, bytes, files);
    }

    private static String liveCommitJson(
            BackupFile backup,
            long events,
            long outputBytes,
            long completedAtMs,
            Backlog backlog) throws Exception {
        long modifiedMs = Files.getLastModifiedTime(backup.path()).toMillis();
        return String.format(Locale.ROOT,
                "{\"kind\":\"live_backup_committed\",\"file\":\"%s\","
                        + "\"first_position\":%d,\"last_position\":%d,"
                        + "\"input_bytes\":%d,\"events\":%d,\"output_bytes\":%d,"
                        + "\"source_mtime_ms\":%d,\"committed_at_ms\":%d,"
                        + "\"commit_delay_seconds\":%.3f,"
                        + "\"available_position_after\":%d,"
                        + "\"backlog_bytes_after\":%d,\"backlog_files_after\":%d}",
                ParallelLogDecoder.escapeJson(backup.fileName()),
                backup.first(), backup.last(), Files.size(backup.path()), events, outputBytes,
                modifiedMs, completedAtMs, (completedAtMs - modifiedMs) / 1_000.0,
                backlog.position(), backlog.bytes(), backlog.files());
    }

    private record StableObservation(Path path, long size, long modifiedMs) {}

    private record Backlog(long position, long bytes, int files) {}

    private static ScanConfig scanConfig(Map<String, String> values, boolean fingerprint) {
        String requestedThreads = values.getOrDefault("--threads", "auto");
        int maxThreads = positiveInt(
                values.getOrDefault("--max-threads",
                        Integer.toString(Runtime.getRuntime().availableProcessors())),
                "--max-threads");
        int targetChunkMib = positiveInt(
                values.getOrDefault("--target-chunk-mib",
                        Integer.toString(ParallelLogDecoder.DEFAULT_TARGET_CHUNK_MIB)),
                "--target-chunk-mib");
        int chunksPerThread = positiveInt(
                values.getOrDefault("--chunks-per-thread",
                        Integer.toString(ParallelLogDecoder.DEFAULT_CHUNKS_PER_THREAD)),
                "--chunks-per-thread");
        return new ScanConfig(
                requestedThreads,
                maxThreads,
                Math.multiplyExact((long) targetChunkMib, 1024L * 1024L),
                targetChunkMib,
                chunksPerThread,
                fingerprint);
    }

    private static Map<String, String> argumentMap(String[] args) {
        if ((args.length - 1) % 2 != 0) {
            throw new IllegalArgumentException("options must be supplied as --name value pairs");
        }
        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 1; index < args.length; index += 2) {
            String name = args[index];
            if (!name.startsWith("--")) {
                throw new IllegalArgumentException("expected option, got " + name);
            }
            if (values.put(name, args[index + 1]) != null) {
                throw new IllegalArgumentException("duplicate option " + name);
            }
        }
        return values;
    }

    private static String required(Map<String, String> values, String name) {
        String value = values.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing " + name);
        }
        return value;
    }

    private static int positiveInt(String value, String name) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed <= 0) {
                throw new NumberFormatException();
            }
            return parsed;
        }
        catch (NumberFormatException error) {
            throw new IllegalArgumentException(name + " must be a positive integer: " + value);
        }
    }

    private static long positiveLong(String value, String name) {
        try {
            long parsed = Long.parseLong(value);
            if (parsed < 0) {
                throw new NumberFormatException();
            }
            return parsed;
        }
        catch (NumberFormatException error) {
            throw new IllegalArgumentException(name + " must be a non-negative integer: " + value);
        }
    }

    private static String summaryJson(
            String runId,
            long first,
            long last,
            int files,
            long bytes,
            long events,
            ScanConfig config,
            String fingerprint,
            double scanSeconds,
            double wallSeconds) {
        return String.format(Locale.ROOT,
                "{\"kind\":\"summary\",\"run_id\":\"%s\","
                        + "\"first_position\":%d,\"last_position\":%d,"
                        + "\"files\":%d,\"bytes\":%d,\"events\":%d,"
                        + "\"requested_threads\":\"%s\",\"max_threads\":%d,"
                        + "\"target_chunk_mib\":%d,\"chunks_per_thread\":%d,"
                        + "\"fingerprint\":\"%s\",\"scan_seconds\":%.9f,"
                        + "\"wall_seconds\":%.9f,\"megabytes_per_second\":%.9f,"
                        + "\"events_per_second\":%.9f}",
                ParallelLogDecoder.escapeJson(runId),
                first,
                last,
                files,
                bytes,
                events,
                ParallelLogDecoder.escapeJson(config.requestedThreads()),
                config.maxThreads(),
                config.targetChunkMib(),
                config.chunksPerThread(),
                fingerprint,
                scanSeconds,
                wallSeconds,
                bytes / 1_000_000.0 / Math.max(scanSeconds, 1e-9),
                events / Math.max(scanSeconds, 1e-9));
    }

    private static String publishJson(PublishMetrics metrics) {
        return String.format(Locale.ROOT,
                "{\"kind\":\"publish\",\"file\":\"%s\",\"events\":%d,"
                        + "\"bytes\":%d,\"serialize_write_seconds\":%.9f,"
                        + "\"force_seconds\":%.9f,\"publish_seconds\":%.9f}",
                ParallelLogDecoder.escapeJson(metrics.file()),
                metrics.events(), metrics.bytes(), metrics.serializeAndWriteSeconds(),
                metrics.forceSeconds(), metrics.publishSeconds());
    }

    private static String pipelineSummaryJson(
            String runId,
            long first,
            long last,
            int files,
            long inputBytes,
            long events,
            ScanConfig config,
            double mmapSeconds,
            double scanSeconds,
            Totals output,
            double wallSeconds) {
        double decodeSeconds = mmapSeconds + scanSeconds;
        double pipelineSeconds = decodeSeconds + output.publishSeconds();
        return String.format(Locale.ROOT,
                "{\"kind\":\"pipeline_summary\",\"implementation\":\"java\","
                        + "\"run_id\":\"%s\",\"first_position\":%d,"
                        + "\"last_position\":%d,\"files\":%d,"
                        + "\"input_bytes\":%d,\"events\":%d,"
                        + "\"requested_threads\":\"%s\",\"max_threads\":%d,"
                        + "\"target_chunk_mib\":%d,\"chunks_per_thread\":%d,"
                        + "\"output_path\":\"%s\",\"output_events\":%d,"
                        + "\"output_bytes\":%d,\"mmap_seconds\":%.9f,"
                        + "\"scan_seconds\":%.9f,\"decode_seconds\":%.9f,"
                        + "\"serialize_write_seconds\":%.9f,\"force_seconds\":%.9f,"
                        + "\"publish_seconds\":%.9f,\"pipeline_seconds\":%.9f,"
                        + "\"wall_seconds\":%.9f,\"pipeline_megabytes_per_second\":%.9f,"
                        + "\"pipeline_events_per_second\":%.9f}",
                ParallelLogDecoder.escapeJson(runId), first, last, files, inputBytes, events,
                ParallelLogDecoder.escapeJson(config.requestedThreads()), config.maxThreads(),
                config.targetChunkMib(), config.chunksPerThread(),
                ParallelLogDecoder.escapeJson(output.path().toString()), output.events(), output.bytes(),
                mmapSeconds, scanSeconds, decodeSeconds,
                output.serializeAndWriteSeconds(), output.forceSeconds(), output.publishSeconds(),
                pipelineSeconds, wallSeconds,
                inputBytes / 1_000_000.0 / Math.max(pipelineSeconds, 1e-9),
                events / Math.max(pipelineSeconds, 1e-9));
    }

    private static void usage() {
        System.err.println("usage:\n"
                + "  java -jar hana-cdc-java-connector.jar scan-file"
                + " --path FILE --run-id ID [--threads N|auto]"
                + " [--max-threads N] [--target-chunk-mib N] [--chunks-per-thread N]\n"
                + "  java -jar hana-cdc-java-connector.jar scan-corpus"
                + " --backup-dir DIR --run-id ID --first-position N --last-position N"
                + " [--threads N|auto] [--max-threads N]"
                + " [--target-chunk-mib N] [--chunks-per-thread N] [--output FILE]\n"
                + "  java -jar hana-cdc-java-connector.jar follow-corpus"
                + " --backup-dir DIR --run-id ID --first-position N"
                + " --target-file FILE --output FILE [--poll-interval-ms N]"
                + " [--threads N|auto] [--max-threads N]"
                + " [--target-chunk-mib N] [--chunks-per-thread N]");
    }
}

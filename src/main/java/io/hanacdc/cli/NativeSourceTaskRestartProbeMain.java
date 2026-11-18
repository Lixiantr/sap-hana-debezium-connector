package io.hanacdc.cli;

import io.hanacdc.connect.HanaSourceConnector;
import io.hanacdc.connect.HanaSourceTask;

import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** Live M3 gate: native SourceTask -> durable checkpoint -> exact restart. */
public final class NativeSourceTaskRestartProbeMain {
    private NativeSourceTaskRestartProbeMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 9) {
            throw new IllegalArgumentException("usage: NativeSourceTaskRestartProbeMain "
                    + "<properties> <password-file> <table-regex> <first> <last> "
                    + "<state-dir> <expected-events> <checkpoint-after> <workers>");
        }
        Map<String, String> properties = properties(args);
        int expected = Integer.parseInt(args[6]);
        int checkpointAfter = Integer.parseInt(args[7]);
        if (checkpointAfter <= 0 || checkpointAfter >= expected) {
            throw new IllegalArgumentException("checkpoint-after must split expected events");
        }

        HanaSourceTask first = task(properties, null);
        List<SourceRecord> prefix = read(first, checkpointAfter, false);
        Map<String, Object> checkpoint = Map.copyOf(
                prefix.get(prefix.size() - 1).sourceOffset());
        first.commitRecord(prefix.get(prefix.size() - 1), null);
        first.stop();

        HanaSourceTask resumed = task(properties, checkpoint);
        List<SourceRecord> suffix = read(resumed, expected - checkpointAfter, true);
        resumed.stop();

        List<SourceRecord> all = new ArrayList<>(prefix);
        all.addAll(suffix);
        Counts committedCounts = validate(all, expected);

        Map<String, String> uncommittedProperties = withStateDirectory(
                properties, Path.of(args[5]).resolve("uncommitted-replay"));
        HanaSourceTask abandoned = task(uncommittedProperties, null);
        read(abandoned, checkpointAfter, false);
        abandoned.stop();
        HanaSourceTask replay = task(uncommittedProperties, null);
        List<SourceRecord> replayed = read(replay, expected, true);
        replay.stop();
        Counts replayCounts = validate(replayed, expected);

        System.out.printf("native_source_task_restart=PASS events=%d prefix=%d suffix=%d "
                        + "c=%d u=%d d=%d checkpoint_generation=%s "
                        + "uncommitted_replay=%d replay_c=%d replay_u=%d replay_d=%d%n",
                all.size(), prefix.size(), suffix.size(), committedCounts.creates(),
                committedCounts.updates(), committedCounts.deletes(),
                checkpoint.get("state_generation"), replayed.size(), replayCounts.creates(),
                replayCounts.updates(), replayCounts.deletes());
    }

    private static Counts validate(List<SourceRecord> records, int expected) {
        if (records.size() != expected) {
            throw new IllegalStateException("expected events=" + expected
                    + " actual=" + records.size());
        }
        Set<String> identities = new LinkedHashSet<>();
        long creates = 0;
        long updates = 0;
        long deletes = 0;
        for (SourceRecord record : records) {
            if (!Integer.valueOf(3).equals(record.sourceOffset().get("offset_version"))) {
                throw new IllegalStateException("native task emitted non-v3 offset");
            }
            Object generation = record.sourceOffset().get("state_generation");
            if (!(generation instanceof Number number) || number.longValue() <= 0) {
                throw new IllegalStateException("native task emitted offset without state");
            }
            Struct envelope = (Struct) record.value();
            Struct source = envelope.getStruct("source");
            String identity = source.getInt64("log_position") + ":"
                    + source.getInt64("tid") + ":" + record.key();
            if (!identities.add(identity)) {
                throw new IllegalStateException("duplicate event after restart: " + identity);
            }
            switch (envelope.getString("op")) {
                case "c" -> creates++;
                case "u" -> updates++;
                case "d" -> deletes++;
                default -> throw new IllegalStateException("unexpected op");
            }
        }
        return new Counts(creates, updates, deletes);
    }

    private static Map<String, String> withStateDirectory(
            Map<String, String> properties, Path stateDirectory) {
        Map<String, String> copy = new LinkedHashMap<>(properties);
        copy.put(HanaSourceConnector.STATE_DIR, stateDirectory.toString());
        return Map.copyOf(copy);
    }

    private static Map<String, String> properties(String[] args) throws Exception {
        Properties loaded = new Properties();
        try (InputStream input = Files.newInputStream(Path.of(args[0]))) {
            loaded.load(input);
        }
        Map<String, String> result = new LinkedHashMap<>();
        loaded.forEach((key, value) -> result.put(key.toString(), value.toString()));
        result.put(HanaSourceConnector.DATABASE_PASSWORD,
                Files.readString(Path.of(args[1])).strip());
        result.put(HanaSourceConnector.SERVER_NAME, "hana-live-m3");
        result.put(HanaSourceConnector.RUN_ID, "native-m3");
        result.put(HanaSourceConnector.TOPIC, "hana.m3.native");
        result.put(HanaSourceConnector.FIRST_POSITION, args[3]);
        result.put(HanaSourceConnector.LAST_POSITION, args[4]);
        result.put(HanaSourceConnector.PROTOTYPE_MODE, "false");
        result.put(HanaSourceConnector.SNAPSHOT_MODE, "no_data");
        result.put(HanaSourceConnector.DISCOVERY_MODE, "catalog");
        result.put(HanaSourceConnector.PATH_MAPPING,
                "/hana/mounts/backup/log/DB_HXE=/var/lib/hana-cdc-parser/backups");
        result.put(HanaSourceConnector.SOURCE_VOLUME, "2");
        result.put(HanaSourceConnector.SOURCE_PARTITION, "0");
        result.put(HanaSourceConnector.TABLE_INCLUDE_LIST, args[2]);
        result.put(HanaSourceConnector.STATE_DIR, args[5]);
        result.put(HanaSourceConnector.SCAN_THREADS, args[8]);
        result.put(HanaSourceConnector.SCAN_MAX_THREADS, args[8]);
        result.put(HanaSourceConnector.POLL_MAX_BATCH_SIZE, "1");
        result.put(HanaSourceConnector.POLL_INTERVAL_MS, "1");
        result.put(HanaSourceConnector.DATABASE_NAME, "HXE");
        result.put(HanaSourceConnector.SUPPORTED_HANA_VERSIONS, "2.00.088");
        result.put(HanaSourceConnector.POLICY_VIOLATION_MODE, "fail");
        return Map.copyOf(result);
    }

    private static HanaSourceTask task(
            Map<String, String> properties, Map<String, Object> offset) {
        HanaSourceTask task = new HanaSourceTask();
        task.initialize(new Context(properties, offset));
        task.start(properties);
        return task;
    }

    private static List<SourceRecord> read(
            HanaSourceTask task, int expected, boolean requireEnd) throws Exception {
        List<SourceRecord> result = new ArrayList<>();
        for (int attempt = 0; attempt < 10_000 && result.size() < expected; attempt++) {
            List<SourceRecord> next = task.poll();
            if (next != null) {
                result.addAll(next);
            }
        }
        if (result.size() != expected) {
            throw new IllegalStateException("expected phase events=" + expected
                    + " actual=" + result.size());
        }
        if (requireEnd) {
            List<SourceRecord> extra = task.poll();
            if (extra != null && !extra.isEmpty()) {
                throw new IllegalStateException("restart emitted unexpected extra events="
                        + extra.size());
            }
        }
        return List.copyOf(result);
    }

    private record Context(
            Map<String, String> configs,
            Map<String, Object> storedOffset) implements SourceTaskContext {
        private Context {
            configs = Map.copyOf(configs);
            storedOffset = storedOffset == null ? null : Map.copyOf(storedOffset);
        }

        public Map<String, String> configs() {
            return Map.copyOf(configs);
        }

        @Override
        public OffsetStorageReader offsetStorageReader() {
            return new OffsetStorageReader() {
                @Override
                public <T> Map<String, Object> offset(Map<String, T> partition) {
                    return storedOffset;
                }

                @Override
                public <T> Map<Map<String, T>, Map<String, Object>> offsets(
                        Collection<Map<String, T>> partitions) {
                    Map<Map<String, T>, Map<String, Object>> result = new LinkedHashMap<>();
                    for (Map<String, T> partition : partitions) {
                        result.put(partition, storedOffset);
                    }
                    return result;
                }
            };
        }

        @Override
        public PluginMetrics pluginMetrics() {
            return null;
        }
    }

    private record Counts(long creates, long updates, long deletes) {}
}

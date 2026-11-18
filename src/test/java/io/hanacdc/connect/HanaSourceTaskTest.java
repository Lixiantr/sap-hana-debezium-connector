package io.hanacdc.connect;

import io.hanacdc.testutil.TestLogFixture;
import io.hanacdc.decoder.ParallelLogDecoder;

import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.NoSuchFileException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class HanaSourceTaskTest {
    @Test
    void backgroundPolicyFailureNeverDowngradesJvmError() {
        AssertionError fatal = new AssertionError("fatal worker failure");

        assertSame(fatal, assertThrows(AssertionError.class,
                () -> HanaSourceTask.policyRefreshFailure(new ExecutionException(fatal))));
    }

    @Test
    void missingCataloguedEpochBackupIsFatalRetentionGap() {
        var backup = ParallelLogDecoder.BackupFile.parse(
                Path.of("log_backup_2_0_100_200.123")).orElseThrow();

        var failure = HanaSourceTask.catalogHeaderFailure(
                backup, new NoSuchFileException(backup.path().toString()));

        assertTrue(failure instanceof FailureClassifier.ClassifiedConnectException);
        var classified = (FailureClassifier.ClassifiedConnectException) failure;
        assertEquals(FailureClassifier.Category.FATAL_GAP,
                classified.decision().category());
        assertEquals("FATAL_RETENTION_OBJECT_MISSING", classified.decision().code());
        assertFalse(classified.decision().retryable());
    }

    @Test
    void transientCatalogHeaderReadFailureRemainsRetriable() {
        var backup = ParallelLogDecoder.BackupFile.parse(
                Path.of("log_backup_2_0_100_200.123")).orElseThrow();

        var failure = HanaSourceTask.catalogHeaderFailure(
                backup, new IOException("temporary read failure"));

        assertTrue(failure instanceof org.apache.kafka.connect.errors.RetriableException);
    }

    @TempDir
    Path temporaryDirectory;

    @Test
    void resumesStrictlyAfterLastAcknowledgedEvent() throws Exception {
        TestLogFixture.write(temporaryDirectory, "RUN");
        Map<String, String> properties = properties();

        HanaSourceTask firstTask = initializedTask(null, properties);
        List<SourceRecord> initial = firstTask.poll();
        firstTask.stop();
        assertEquals(2, initial.size());
        assertEquals(42L, key(initial.get(0)));
        assertEquals(43L, key(initial.get(1)));
        assertEquals(3, initial.get(0).sourceOffset().get("offset_version"));
        assertEquals(100L, initial.get(0).sourceOffset().get("file_first"));
        assertEquals(108L, initial.get(0).sourceOffset().get("file_last"));
        assertEquals(64, ((String) initial.get(0).sourceOffset()
                .get("source_epoch")).length());

        Map<String, Object> checkpoint = new LinkedHashMap<>();
        initial.get(0).sourceOffset().forEach(checkpoint::put);
        HanaSourceTask resumedTask = initializedTask(checkpoint, properties);
        List<SourceRecord> resumed = resumedTask.poll();
        resumedTask.stop();
        assertEquals(1, resumed.size());
        assertEquals(43L, key(resumed.get(0)));
    }

    @Test
    void emitsCrossBackupEventWithOriginatingBackupOffset() throws Exception {
        TestLogFixture.writeAcrossBackups(temporaryDirectory, "RUN");
        Map<String, String> properties = properties();
        properties.put(HanaSourceConnector.POLL_MAX_BATCH_SIZE, "10");

        HanaSourceTask task = initializedTask(null, properties);
        List<SourceRecord> records = task.poll();
        task.stop();

        assertEquals(1, records.size());
        assertEquals(123L, key(records.get(0)));
        assertEquals(10L, records.get(0).sourceOffset().get("backup_id"));
        assertEquals(100L, records.get(0).sourceOffset().get("file_first"));
    }

    @Test
    void oversizedEventFailsBeforeReturnAndCanBeReplayedAfterLimitChange() throws Exception {
        TestLogFixture.write(temporaryDirectory, "RUN");
        Map<String, String> limited = properties();
        limited.put(HanaSourceConnector.EVENT_MAX_BYTES, "1");

        HanaSourceTask rejected = initializedTask(null, limited);
        ConnectException failure = assertThrows(ConnectException.class, rejected::poll);
        rejected.stop();
        assertTrue(failure.getMessage().contains("FATAL_EVENT_TOO_LARGE"));

        HanaSourceTask recovered = initializedTask(null, properties());
        List<SourceRecord> replayed = recovered.poll();
        recovered.stop();
        assertEquals(List.of(42L, 43L), replayed.stream()
                .map(HanaSourceTaskTest::key)
                .toList());
    }

    @Test
    void replaysPredecessorAndResumesAfterExactCrossBackupCheckpoint() throws Exception {
        TestLogFixture.writeAcrossBackupsWithFollowingEvents(temporaryDirectory, "RUN");
        Map<String, String> properties = properties();
        properties.put(HanaSourceConnector.LAST_POSITION, "116");
        properties.put(HanaSourceConnector.POLL_MAX_BATCH_SIZE, "10");

        HanaSourceTask firstTask = initializedTask(null, properties);
        List<SourceRecord> initial = firstTask.poll();
        firstTask.stop();
        assertEquals(List.of(123L, 124L, 125L), initial.stream()
                .map(HanaSourceTaskTest::key)
                .toList());

        Map<String, Object> checkpoint = new LinkedHashMap<>();
        initial.get(1).sourceOffset().forEach(checkpoint::put);
        HanaSourceTask resumedTask = initializedTask(checkpoint, properties);
        List<SourceRecord> resumed = resumedTask.poll();
        resumedTask.stop();
        assertEquals(List.of(125L), resumed.stream()
                .map(HanaSourceTaskTest::key)
                .toList());
    }

    @Test
    void rejectsCheckpointFromAnotherSourceEpoch() throws Exception {
        TestLogFixture.write(temporaryDirectory, "RUN");
        Map<String, String> properties = properties();
        HanaSourceTask firstTask = initializedTask(null, properties);
        List<SourceRecord> initial = firstTask.poll();
        firstTask.stop();

        Map<String, Object> checkpoint = new LinkedHashMap<>();
        initial.get(0).sourceOffset().forEach(checkpoint::put);
        checkpoint.put("source_epoch", "0".repeat(64));

        assertThrows(ConnectException.class,
                () -> initializedTask(checkpoint, properties));
    }

    @Test
    void sourcePartitionSurvivesKafkaConnectReserialization() throws Exception {
        TestLogFixture.write(temporaryDirectory, "RUN");
        HanaSourceTask task = initializedTask(null, properties());
        SourceRecord record = task.poll().get(0);
        task.stop();

        Map<String, ?> partition = record.sourcePartition();
        assertEquals(List.of("partition_id"), List.copyOf(partition.keySet()));
        @SuppressWarnings("unchecked")
        Map<String, Object> mutableView = (Map<String, Object>) partition;
        assertThrows(UnsupportedOperationException.class,
                () -> mutableView.put("database", "OTHER"));

        JsonConverter converter = new JsonConverter();
        converter.configure(Map.of("schemas.enable", false), true);
        byte[] bytes = converter.fromConnectData("offsets", null, partition);
        assertEquals("{\"partition_id\":\"v1|mode=filesystem|server=hana01|source_id=2|volume=2|partition=0\"}",
                new String(bytes, StandardCharsets.UTF_8));

        SchemaAndValue decoded = converter.toConnectData("offsets", bytes);
        byte[] reserialized = converter.fromConnectData(
                "offsets", decoded.schema(), decoded.value());
        assertArrayEquals(bytes, reserialized);
    }

    @Test
    void stopInterruptsBlockedPollWithoutClosingResourcesFromStopThread() throws Exception {
        TestLogFixture.write(temporaryDirectory, "RUN");
        Map<String, String> values = properties();
        values.put(HanaSourceConnector.POLL_INTERVAL_MS, "5000");
        HanaSourceTask task = initializedTask(null, values);
        assertEquals(2, task.poll().size());

        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<Thread> worker = new AtomicReference<>();
        try {
            var blocked = executor.submit(() -> {
                worker.set(Thread.currentThread());
                return task.poll();
            });
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (worker.get() == null || worker.get().getState() != Thread.State.TIMED_WAITING) {
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError("poll did not enter its bounded idle wait");
                }
                Thread.sleep(5);
            }

            long started = System.nanoTime();
            task.stop();
            ExecutionException interrupted = assertThrows(ExecutionException.class,
                    () -> blocked.get(1, TimeUnit.SECONDS));
            assertTrue(interrupted.getCause() instanceof InterruptedException);
            assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(1));
        }
        finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
            task.stop();
        }
    }

    private HanaSourceTask initializedTask(
            Map<String, Object> storedOffset,
            Map<String, String> properties) {
        HanaSourceTask task = new HanaSourceTask();
        task.initialize(new TestContext(properties, storedOffset));
        task.start(properties);
        return task;
    }

    private Map<String, String> properties() {
        Map<String, String> values = new LinkedHashMap<>();
        values.put(HanaSourceConnector.SERVER_NAME, "hana01");
        values.put(HanaSourceConnector.BACKUP_DIR, temporaryDirectory.toString());
        values.put(HanaSourceConnector.RUN_ID, "RUN");
        values.put(HanaSourceConnector.TOPIC, "hana.events");
        values.put(HanaSourceConnector.FIRST_POSITION, Long.toString(TestLogFixture.FIRST));
        values.put(HanaSourceConnector.LAST_POSITION, Long.toString(TestLogFixture.LAST));
        values.put(HanaSourceConnector.TABLE_INCLUDE_LIST, "BENCH\\.CDC_LOAD");
        values.put(HanaSourceConnector.SCAN_THREADS, "2");
        values.put(HanaSourceConnector.SCAN_MAX_THREADS, "2");
        values.put(HanaSourceConnector.TARGET_CHUNK_MIB, "1");
        values.put(HanaSourceConnector.CHUNKS_PER_THREAD, "1");
        values.put(HanaSourceConnector.POLL_MAX_BATCH_SIZE, "2");
        values.put(HanaSourceConnector.POLL_INTERVAL_MS, "1");
        values.put(HanaSourceConnector.DISCOVERY_MODE, "filesystem");
        return values;
    }

    private static long key(SourceRecord record) {
        return ((org.apache.kafka.connect.data.Struct) record.key()).getInt64("id");
    }

    private record TestContext(
            Map<String, String> configs,
            Map<String, Object> storedOffset) implements SourceTaskContext {
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
}

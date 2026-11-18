package io.hanacdc.connect;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class HanaSourceConnectorTest {
    @Test
    void advertisesAtLeastOnceSemanticsExplicitly() {
        HanaSourceConnector connector = new HanaSourceConnector();

        assertEquals("0.6.0-rc.4", connector.version());
        assertEquals(ExactlyOnceSupport.UNSUPPORTED, connector.exactlyOnceSupport(Map.of()));
    }

    @Test
    void acceptsOnlyStreamingWithoutSnapshot() {
        HanaSourceConnector connector = new HanaSourceConnector();
        Map<String, String> values = requiredProperties();
        values.put(HanaSourceConnector.SNAPSHOT_MODE, "initial");

        assertThrows(ConfigException.class, () -> connector.start(values));
    }

    @Test
    void defaultsToNoDataSnapshotMode() {
        HanaSourceConnector connector = new HanaSourceConnector();
        connector.start(requiredProperties());

        assertEquals("no_data", connector.config()
                .parse(requiredProperties())
                .get(HanaSourceConnector.SNAPSHOT_MODE));
        connector.stop();
    }

    @Test
    void defaultsToProductionRetentionAndStandardHeartbeatNames() {
        HanaSourceConnector connector = new HanaSourceConnector();
        Map<String, Object> parsed = connector.config().parse(requiredProperties());

        assertEquals(7_200L, parsed.get(HanaSourceConnector.RETENTION_MIN_SECONDS));
        assertEquals(900L,
                parsed.get(HanaSourceConnector.RETENTION_SAFETY_MARGIN_SECONDS));
        assertEquals(30_000L, parsed.get(HanaSourceConnector.HEARTBEAT_INTERVAL_MS));
        assertEquals("__debezium-heartbeat",
                parsed.get(HanaSourceConnector.HEARTBEAT_TOPIC_PREFIX));
        assertEquals("", parsed.get(HanaSourceConnector.HEARTBEAT_TOPIC_NAME));
        assertEquals(4_096, parsed.get(HanaSourceConnector.POLL_MAX_BATCH_SIZE));
        assertEquals("auto", parsed.get(HanaSourceConnector.LOG_PROCESSOR_THREADS));
        assertEquals(6, parsed.get(HanaSourceConnector.LOG_PROCESSOR_MAX_THREADS));
        assertEquals(4, parsed.get(HanaSourceConnector.LOG_PROCESSOR_CHUNKS_PER_THREAD));
        assertEquals(4_096, parsed.get(HanaSourceConnector.TRANSACTION_MAX_OPEN));
        assertEquals(100_000, parsed.get(HanaSourceConnector.TRANSACTION_MAX_ROWS));
        assertEquals(64L * 1024 * 1024,
                parsed.get(HanaSourceConnector.TRANSACTION_MAX_BYTES));
        assertEquals(64L * 1024 * 1024,
                parsed.get(HanaSourceConnector.LOB_MAX_BUFFERED_BYTES));
        assertEquals(16L * 1024 * 1024,
                parsed.get(HanaSourceConnector.LOB_MAX_BYTES));
        assertEquals(1_024, parsed.get(HanaSourceConnector.LOB_MAX_OPEN_LOCATORS));
        assertEquals(1_048_576L,
                parsed.get(HanaSourceConnector.EVENT_MAX_BYTES));
        assertEquals(65_536,
                parsed.get(HanaSourceConnector.STATE_MAX_UNCOMMITTED_GENERATIONS));
        assertEquals(4L * 1024 * 1024 * 1024,
                parsed.get(HanaSourceConnector.STATE_MAX_UNCOMMITTED_BYTES));
        assertEquals("single_container",
                parsed.get(HanaSourceConnector.SOURCE_TOPOLOGY_MODE));
        assertEquals("fail",
                parsed.get(HanaSourceConnector.TOPOLOGY_CHANGE_POLICY));
        assertEquals("", parsed.get(HanaSourceConnector.SCALE_OUT_START_POSITIONS));
        assertEquals(4, parsed.get(HanaSourceConnector.SCALE_OUT_CATALOG_THREADS));
        assertEquals("", parsed.get(HanaSourceConnector.SCALE_OUT_PATH_MAPPING));
        assertEquals(10_000L, parsed.get(HanaSourceConnector.POLICY_REFRESH_INTERVAL_MS));
        assertEquals(60_000L, parsed.get(HanaSourceConnector.POLICY_MAX_STALENESS_MS));
    }

    @Test
    void validatesTopologyModeAndPinsFailPolicy() {
        HanaSourceConnector connector = new HanaSourceConnector();
        Map<String, String> values = requiredProperties();
        values.put(HanaSourceConnector.SOURCE_TOPOLOGY_MODE, "single_service");
        connector.start(values);
        connector.stop();

        values.put(HanaSourceConnector.SOURCE_TOPOLOGY_MODE, "unsupported");
        assertThrows(ConfigException.class, () -> connector.start(values));
        values.put(HanaSourceConnector.SOURCE_TOPOLOGY_MODE, "single_service");
        values.put(HanaSourceConnector.TOPOLOGY_CHANGE_POLICY, "refresh");
        assertThrows(ConfigException.class, () -> connector.start(values));
    }

    @Test
    void rejectsLobValueLimitLargerThanItsBoundedBuffer() {
        HanaSourceConnector connector = new HanaSourceConnector();
        Map<String, String> values = requiredProperties();
        values.put(HanaSourceConnector.LOB_MAX_BUFFERED_BYTES, "1024");
        values.put(HanaSourceConnector.LOB_MAX_BYTES, "2048");

        assertThrows(ConfigException.class, () -> connector.start(values));
    }

    @Test
    void mapsDeprecatedScanPropertiesToNativeProcessorStage() {
        Map<String, String> values = new LinkedHashMap<>();
        values.put(HanaSourceConnector.SCAN_THREADS, "5");
        values.put(HanaSourceConnector.SCAN_MAX_THREADS, "8");
        values.put(HanaSourceConnector.CHUNKS_PER_THREAD, "6");

        Map<String, String> normalized = HanaSourceConnector
                .normalizeParallelismProperties(values);

        assertEquals("5", normalized.get(HanaSourceConnector.LOG_PROCESSOR_THREADS));
        assertEquals("8", normalized.get(HanaSourceConnector.LOG_PROCESSOR_MAX_THREADS));
        assertEquals("6", normalized.get(
                HanaSourceConnector.LOG_PROCESSOR_CHUNKS_PER_THREAD));
    }

    @Test
    void rejectsConflictingCanonicalAndDeprecatedParallelism() {
        Map<String, String> values = new LinkedHashMap<>();
        values.put(HanaSourceConnector.LOG_PROCESSOR_THREADS, "2");
        values.put(HanaSourceConnector.SCAN_THREADS, "3");

        assertThrows(ConfigException.class,
                () -> HanaSourceConnector.normalizeParallelismProperties(values));
    }

    @Test
    void rejectsRemovedNativeReaderSettings() {
        for (String removed : List.of(
                "log.reader.threads",
                "log.reader.max.threads",
                "log.reader.auto.target.bytes.per.thread",
                "log.reader.queue.max.records.per.worker",
                "log.reader.queue.capacity")) {
            Map<String, String> values = new LinkedHashMap<>();
            values.put(removed, "1");
            assertThrows(ConfigException.class,
                    () -> HanaSourceConnector.normalizeParallelismProperties(values), removed);
        }
    }

    private static Map<String, String> requiredProperties() {
        Map<String, String> values = new LinkedHashMap<>();
        values.put(HanaSourceConnector.SERVER_NAME, "hana01");
        values.put(HanaSourceConnector.RUN_ID, "RUN");
        values.put(HanaSourceConnector.TOPIC, "hana.events");
        values.put(HanaSourceConnector.FIRST_POSITION, "1");
        values.put(HanaSourceConnector.LAST_POSITION, "2");
        values.put(HanaSourceConnector.TABLE_INCLUDE_LIST, "BENCH\\.CDC_LOAD");
        return values;
    }
}

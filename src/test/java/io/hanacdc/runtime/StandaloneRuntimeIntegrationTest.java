package io.hanacdc.runtime;

import io.hanacdc.connect.HanaSourceConnector;
import io.hanacdc.testutil.TestLogFixture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class StandaloneRuntimeIntegrationTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void ndjsonAndNoOpUseTheSameDurableEngineOffset() throws Exception {
        TestLogFixture.write(temporaryDirectory, "RUN");
        Path config = temporaryDirectory.resolve("connector.properties");
        Path offsets = temporaryDirectory.resolve("offsets.dat");
        Path output = temporaryDirectory.resolve("events.ndjson");
        writeConfig(config);

        StandaloneRuntimeMain.main(new String[] {
                "--mode", "ndjson",
                "--connector-config", config.toString(),
                "--offset-file", offsets.toString(),
                "--output", output.toString(),
                "--file-open", "truncate",
                "--fsync", "batch",
                "--expected-records", "2",
                "--timeout-seconds", "15"
        });

        List<String> lines = Files.readAllLines(output);
        assertEquals(2, lines.size());
        assertTrue(lines.stream().allMatch(line -> line.contains("\"topic\":\"hana.events\"")));
        assertTrue(Files.size(offsets) > 0);

        StandaloneRuntimeMain.main(new String[] {
                "--mode", "noop",
                "--connector-config", config.toString(),
                "--offset-file", offsets.toString(),
                "--idle-exit-seconds", "1",
                "--timeout-seconds", "10"
        });
    }

    private void writeConfig(Path path) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("name", "hana-standalone-test");
        properties.setProperty("connector.class", HanaSourceConnector.class.getName());
        properties.setProperty(HanaSourceConnector.SERVER_NAME, "hana01");
        properties.setProperty(HanaSourceConnector.BACKUP_DIR, temporaryDirectory.toString());
        properties.setProperty(HanaSourceConnector.RUN_ID, "RUN");
        properties.setProperty(HanaSourceConnector.TOPIC, "hana.events");
        properties.setProperty(HanaSourceConnector.FIRST_POSITION,
                Long.toString(TestLogFixture.FIRST));
        properties.setProperty(HanaSourceConnector.LAST_POSITION,
                Long.toString(TestLogFixture.LAST));
        properties.setProperty(HanaSourceConnector.TABLE_INCLUDE_LIST, "BENCH\\.CDC_LOAD");
        properties.setProperty(HanaSourceConnector.SCAN_THREADS, "2");
        properties.setProperty(HanaSourceConnector.SCAN_MAX_THREADS, "2");
        properties.setProperty(HanaSourceConnector.TARGET_CHUNK_MIB, "1");
        properties.setProperty(HanaSourceConnector.CHUNKS_PER_THREAD, "1");
        properties.setProperty(HanaSourceConnector.POLL_MAX_BATCH_SIZE, "2");
        properties.setProperty(HanaSourceConnector.POLL_INTERVAL_MS, "1");
        properties.setProperty(HanaSourceConnector.DISCOVERY_MODE, "filesystem");
        properties.setProperty("offset.flush.interval.ms", "0");
        try (OutputStream output = Files.newOutputStream(path)) {
            properties.store(output, "standalone integration test");
        }
    }
}

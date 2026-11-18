package io.hanacdc.decoder;

import io.hanacdc.decoder.ParallelLogDecoder.DecodedFile;
import io.hanacdc.decoder.ParallelLogDecoder.ScanConfig;
import io.hanacdc.testutil.TestLogFixture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class LocalNdjsonSinkTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void writesDeterministicPrototypeSchemaAndForcesEachBackup() throws Exception {
        String runId = "RUN";
        var backup = TestLogFixture.write(temporaryDirectory, runId);
        Path output = temporaryDirectory.resolve("events.ndjson");
        var decoder = new ParallelLogDecoder();
        var config = new ScanConfig("2", 2, 1, 1, 1, false);

        LocalNdjsonSink.PublishMetrics metrics;
        LocalNdjsonSink.Totals totals;
        try (DecodedFile decoded = decoder.decode(backup, runId, config, null);
                LocalNdjsonSink sink = new LocalNdjsonSink(output)) {
            metrics = sink.publish(decoded, runId);
            totals = sink.totals();
        }

        List<String> lines = Files.readAllLines(output);
        assertEquals(2, lines.size());
        assertEquals(2, metrics.events());
        assertEquals(2, totals.events());
        assertEquals(Files.size(output), metrics.bytes());
        assertEquals(metrics.bytes(), totals.bytes());
        assertTrue(lines.get(0).startsWith(
                "{\"event_id\":\"RUN:0001:00000000000000000010\","
                        + "\"run_id\":\"RUN\",\"worker_id\":1,\"sequence\":10,"
                        + "\"id\":42,\"row_version\":3,\"is_deleted\":false,"));
        assertTrue(lines.get(0).contains("\"payload\":\"payload-one\""));
        assertTrue(lines.get(0).contains("\"log_position\":100,\"tid\":7,\"record_size\":256"));
        assertTrue(lines.get(0).endsWith("\"captured_at_ns\":0}"));
        assertTrue(lines.get(1).contains("\"is_deleted\":true"));
    }
}

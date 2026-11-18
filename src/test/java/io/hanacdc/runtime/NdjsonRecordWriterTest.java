package io.hanacdc.runtime;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NdjsonRecordWriterTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void writesConnectKeyValueAndTombstoneAsNdjson() throws Exception {
        Path output = temporaryDirectory.resolve("events.ndjson");
        NdjsonRecordWriter.BatchMetrics metrics;
        NdjsonRecordWriter.Totals totals;
        try (var writer = new NdjsonRecordWriter(output,
                NdjsonRecordWriter.OpenMode.TRUNCATE,
                NdjsonRecordWriter.FsyncMode.BATCH)) {
            metrics = writer.writeBatch(List.of(record(false), record(true)));
            totals = writer.totals();
        }

        List<String> lines = Files.readAllLines(output);
        assertEquals(2, lines.size());
        assertEquals(2, metrics.records());
        assertEquals(2, totals.records());
        assertEquals(Files.size(output), totals.bytes());
        assertTrue(metrics.fsyncNanos() >= 0);
        assertTrue(lines.get(0).startsWith(
                "{\"topic\":\"hana.topic\",\"partition\":null,\"timestamp\":1234,"));
        assertTrue(lines.get(0).contains("\"key\":{\"schema\":"));
        assertTrue(lines.get(0).contains("\"payload\":{\"ID\":7}"));
        assertTrue(lines.get(0).contains("\"value\":{\"schema\":"));
        assertTrue(lines.get(0).contains("\"NAME\":\"all-types\""));
        assertTrue(lines.get(1).endsWith("\"value\":null}"));
    }

    @Test
    void appendDropsOnlyAnIncompleteTail() throws Exception {
        Path output = temporaryDirectory.resolve("events.ndjson");
        Files.writeString(output, "{\"existing\":true}\npartial");

        try (var writer = new NdjsonRecordWriter(output,
                NdjsonRecordWriter.OpenMode.APPEND,
                NdjsonRecordWriter.FsyncMode.NONE)) {
            writer.writeBatch(List.of(record(false)));
        }

        List<String> lines = Files.readAllLines(output);
        assertEquals(2, lines.size());
        assertEquals("{\"existing\":true}", lines.get(0));
        assertTrue(lines.get(1).startsWith("{\"topic\":\"hana.topic\""));
    }

    @Test
    void failModeRejectsExistingOutput() throws Exception {
        Path output = temporaryDirectory.resolve("events.ndjson");
        Files.writeString(output, "occupied");
        assertThrows(Exception.class, () -> new NdjsonRecordWriter(output,
                NdjsonRecordWriter.OpenMode.FAIL,
                NdjsonRecordWriter.FsyncMode.BATCH));
    }

    private static SourceRecord record(boolean tombstone) {
        Schema keySchema = SchemaBuilder.struct().name("test.Key")
                .field("ID", Schema.INT32_SCHEMA).build();
        Struct key = new Struct(keySchema).put("ID", 7);
        Schema valueSchema = SchemaBuilder.struct().name("test.Value")
                .field("NAME", Schema.STRING_SCHEMA).build();
        Struct value = tombstone ? null : new Struct(valueSchema).put("NAME", "all-types");
        return new SourceRecord(Map.of("server", "hana"), Map.of("position", 42L),
                "hana.topic", null, keySchema, key,
                tombstone ? null : valueSchema, value, 1234L);
    }
}

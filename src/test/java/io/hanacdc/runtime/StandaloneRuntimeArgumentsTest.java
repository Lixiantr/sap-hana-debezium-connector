package io.hanacdc.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class StandaloneRuntimeArgumentsTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void parsesNoOpWithoutOutput() {
        var arguments = StandaloneRuntimeMain.Arguments.parse(new String[] {
                "--mode", "noop",
                "--connector-config", "connector.properties",
                "--offset-file", "offsets.dat",
                "--expected-records", "100"
        });
        assertEquals(StandaloneRuntimeMain.Mode.NOOP, arguments.mode());
        assertNull(arguments.outputFile());
        assertEquals(100, arguments.expectedRecords());
    }

    @Test
    void parsesDurableNdjsonDefaults() {
        var arguments = StandaloneRuntimeMain.Arguments.parse(new String[] {
                "--mode", "ndjson",
                "--connector-config", "connector.properties",
                "--offset-file", "offsets.dat",
                "--output", "events.ndjson"
        });
        assertEquals(StandaloneRuntimeMain.Mode.NDJSON, arguments.mode());
        assertEquals(NdjsonRecordWriter.OpenMode.APPEND, arguments.openMode());
        assertEquals(NdjsonRecordWriter.FsyncMode.BATCH, arguments.fsyncMode());
    }

    @Test
    void ndjsonRequiresOutput() {
        assertThrows(IllegalArgumentException.class,
                () -> StandaloneRuntimeMain.Arguments.parse(new String[] {
                        "--mode", "ndjson",
                        "--connector-config", "connector.properties",
                        "--offset-file", "offsets.dat"
                }));
    }

    @Test
    void appendRejectsOnlyOneExistingSideOfTheOutputOffsetPair() throws Exception {
        Path output = temporaryDirectory.resolve("events.ndjson");
        Path offset = temporaryDirectory.resolve("offsets.dat");
        Files.writeString(offset, "committed");
        assertThrows(IllegalArgumentException.class,
                () -> StandaloneRuntimeMain.validateOutputOffsetPair(output, offset,
                        NdjsonRecordWriter.OpenMode.APPEND));
        Files.writeString(output, "{}\n");
        StandaloneRuntimeMain.validateOutputOffsetPair(output, offset,
                NdjsonRecordWriter.OpenMode.APPEND);
    }

    @Test
    void truncateRequiresFreshOffset() throws Exception {
        Path output = temporaryDirectory.resolve("events.ndjson");
        Path offset = temporaryDirectory.resolve("offsets.dat");
        Files.writeString(offset, "committed");
        assertThrows(IllegalArgumentException.class,
                () -> StandaloneRuntimeMain.validateOutputOffsetPair(output, offset,
                        NdjsonRecordWriter.OpenMode.TRUNCATE));
    }
}

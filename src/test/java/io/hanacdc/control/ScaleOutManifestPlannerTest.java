package io.hanacdc.control;

import io.hanacdc.connect.SourceStreamId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ScaleOutManifestPlannerTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void plansIndependentContiguousChainsInStableOrder() {
        var files = ScaleOutManifestPlanner.contiguousFiles(services(),
                List.of(entry(3, "hana02", 200, 208, 20),
                        entry(2, "hana01", 100, 104, 10),
                        entry(2, "hana01", 104, 108, 11)),
                Map.of(stream(2), window(100, 108), stream(3), window(200, 208)),
                contract(), mapper());

        assertEquals(List.of(stream(2), stream(3)), List.copyOf(files.keySet()));
        assertEquals(2, files.get(stream(2)).size());
        assertEquals(1, files.get(stream(3)).size());
    }

    @Test
    void rejectsGapInOnlyOneStreamAndForeignHost() {
        IllegalStateException gap = assertThrows(IllegalStateException.class, () ->
                ScaleOutManifestPlanner.contiguousFiles(services(),
                        List.of(entry(2, "hana01", 100, 108, 10),
                                entry(3, "hana02", 201, 208, 20)),
                        Map.of(stream(2), window(100, 108),
                                stream(3), window(200, 208)), contract(), mapper()));
        assertTrue(gap.getMessage().contains("FATAL_GAP"));

        IllegalStateException host = assertThrows(IllegalStateException.class, () ->
                ScaleOutManifestPlanner.contiguousFiles(services(),
                        List.of(entry(2, "hana02", 100, 108, 10)),
                        Map.of(stream(2), window(100, 108),
                                stream(3), window(200, 208)), contract(), mapper()));
        assertTrue(host.getMessage().contains("identity mismatch"));
    }

    private PathMapper mapper() {
        return PathMapper.parse("/hana/log=" + temporaryDirectory.toAbsolutePath());
    }

    private static List<PersistenceService> services() {
        return List.of(new PersistenceService("hana01", 39040, "indexserver", 2),
                new PersistenceService("hana02", 39040, "indexserver", 3));
    }

    private static SourceStreamId stream(long volume) {
        return new SourceStreamId(volume, "indexserver");
    }

    private static ScaleOutManifestPlanner.StreamWindow window(long first, long last) {
        return new ScaleOutManifestPlanner.StreamWindow(first, last, 0);
    }

    private static LogBackupEntry entry(
            long source, String host, long first, long last, long backup) {
        return new LogBackupEntry(source * 100 + backup, backup, source, host,
                "indexserver", first, last, 8192, "file",
                "/hana/log/log_backup_" + source + "_0_" + first + "_" + last
                        + "." + backup,
                "", 1, false, 0, "", Instant.parse("2026-08-07T12:00:00Z"));
    }

    private static HanaContract contract() {
        return new HanaContract(Set.of("immediate"), Set.of("file"), Set.of("none"),
                false, false, Long.MAX_VALUE, 900_000, List.of("2.00.088"));
    }
}

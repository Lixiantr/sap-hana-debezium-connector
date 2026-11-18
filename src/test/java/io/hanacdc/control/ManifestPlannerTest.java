package io.hanacdc.control;

import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ManifestPlannerTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void mapsAndOrdersOneContiguousCatalogStream() {
        PathMapper mapper = PathMapper.parse(
                "/hana/log=" + temporaryDirectory.toAbsolutePath());
        List<BackupFile> result = ManifestPlanner.contiguousFiles(
                List.of(entry(2, 104, 108, 11), entry(1, 100, 104, 10)),
                100, 108, 2, 2, 0, contract(), mapper);

        assertEquals(2, result.size());
        assertEquals(10, result.get(0).backupId());
        assertEquals(11, result.get(1).backupId());
        assertTrue(result.get(0).path().startsWith(temporaryDirectory.toAbsolutePath()));
    }

    @Test
    void rejectsCatalogGapWithoutAdvancingExpectedPosition() {
        PathMapper mapper = PathMapper.parse(
                "/hana/log=" + temporaryDirectory.toAbsolutePath());
        IllegalStateException error = assertThrows(IllegalStateException.class, () ->
                ManifestPlanner.contiguousFiles(
                        List.of(entry(2, 105, 108, 11)),
                        100, 108, 2, 2, 0, contract(), mapper));
        assertTrue(error.getMessage().contains("FATAL_GAP"));
    }

    @Test
    void rejectsUnknownCatalogRoot() {
        PathMapper mapper = PathMapper.parse(
                "/hana/allowed=" + temporaryDirectory.toAbsolutePath());
        assertThrows(IllegalStateException.class, () ->
                ManifestPlanner.contiguousFiles(
                        List.of(entry(1, 100, 104, 10)),
                        100, 104, 2, 2, 0, contract(), mapper));
    }

    @Test
    void rejectsForeignCatalogSourceBeforeAcceptingNativeHeader() {
        PathMapper mapper = PathMapper.parse(
                "/hana/log=" + temporaryDirectory.toAbsolutePath());
        IllegalStateException error = assertThrows(IllegalStateException.class, () ->
                ManifestPlanner.contiguousFiles(
                        List.of(entryWithSource(3, 1, 100, 104, 10)),
                        100, 104, 2, 2, 0, contract(), mapper));
        assertTrue(error.getMessage().contains("CATALOG_SOURCE_IDENTITY_MISMATCH"));
    }

    private static LogBackupEntry entry(
            long entryId, long first, long last, long backupId) {
        return entryWithSource(2, entryId, first, last, backupId);
    }

    private static LogBackupEntry entryWithSource(
            long sourceId, long entryId, long first, long last, long backupId) {
        return new LogBackupEntry(
                entryId, backupId, sourceId, "hanaexpress", "indexserver",
                first, last, 8192, "file",
                "/hana/log/log_backup_2_0_" + first + "_" + last + "." + backupId,
                "", 1, false, 0, "", Instant.parse("2026-08-03T12:00:00Z"));
    }

    private static HanaContract contract() {
        return new HanaContract(
                Set.of("immediate"), Set.of("file"), Set.of("none"),
                false, false, Long.MAX_VALUE, 900_000, List.of("2.00.088"));
    }
}

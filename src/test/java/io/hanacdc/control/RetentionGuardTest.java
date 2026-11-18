package io.hanacdc.control;

import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RetentionGuardTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void validatesExactCatalogAndPhysicalReplayObject() throws IOException {
        Path file = temporaryDirectory.resolve("log_backup_2_0_100_200.42");
        Files.write(file, new byte[] {1});
        BackupFile required = new BackupFile(file, 2, 0, 100, 200, 42);
        Instant completed = Instant.parse("2026-08-05T10:00:00Z");

        RetentionGuard.Status status = RetentionGuard.inspect(required,
                List.of(entry(42, 100, 200, file.getFileName().toString(), completed)),
                mapper(), contract(), completed.plusSeconds(100), 7_200);

        assertEquals(100, status.requiredBackupAgeSeconds());
        assertEquals(7_100, status.contractHeadroomSeconds());
        assertTrue(status.toJson().contains("\"required_backup_id\":42"));
    }

    @Test
    void failsClosedWhenExactCatalogEntryDisappears() throws IOException {
        Path file = temporaryDirectory.resolve("log_backup_2_0_100_200.42");
        Files.write(file, new byte[] {1});
        BackupFile required = new BackupFile(file, 2, 0, 100, 200, 42);

        IllegalStateException error = assertThrows(IllegalStateException.class, () ->
                RetentionGuard.inspect(required, List.of(), mapper(), contract(),
                        Instant.parse("2026-08-05T10:00:00Z"), 7_200));
        assertTrue(error.getMessage().contains("FATAL_RETENTION_CATALOG_GAP"));
    }

    @Test
    void failsClosedWhenCataloguedObjectIsNotReadable() {
        Path file = temporaryDirectory.resolve("log_backup_2_0_100_200.42");
        BackupFile required = new BackupFile(file, 2, 0, 100, 200, 42);
        Instant completed = Instant.parse("2026-08-05T10:00:00Z");

        IllegalStateException error = assertThrows(IllegalStateException.class, () ->
                RetentionGuard.inspect(required,
                        List.of(entry(42, 100, 200, file.getFileName().toString(), completed)),
                        mapper(), contract(), completed.plusSeconds(1), 7_200));
        assertTrue(error.getMessage().contains("FATAL_RETENTION_OBJECT_MISSING"));
    }

    private PathMapper mapper() {
        return PathMapper.parse("/hana/log=" + temporaryDirectory.toAbsolutePath());
    }

    private static LogBackupEntry entry(
            long backupId, long first, long last, String name, Instant completed) {
        return new LogBackupEntry(1, backupId, 2, "hanaexpress", "indexserver",
                first, last, 8_192, "file", "/hana/log/" + name,
                "", 1, false, 0, "", completed);
    }

    private static HanaContract contract() {
        return new HanaContract(Set.of("immediate"), Set.of("file"), Set.of("none"),
                false, false, Long.MAX_VALUE, 900_000, List.of("2.00.088"));
    }
}

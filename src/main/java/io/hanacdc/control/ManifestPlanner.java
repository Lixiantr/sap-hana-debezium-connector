package io.hanacdc.control;

import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Converts successful catalog rows into one strict contiguous source stream. */
public final class ManifestPlanner {
    private ManifestPlanner() {}

    public static List<BackupFile> contiguousFiles(
            List<LogBackupEntry> entries,
            long expectedFirstPosition,
            long lastPositionExclusive,
            long expectedSourceId,
            long expectedVolume,
            long expectedPartition,
            HanaContract contract,
            PathMapper mapper) {
        List<LogBackupEntry> sorted = entries.stream()
                .filter(value -> value.lastRedoPosition() > expectedFirstPosition)
                .filter(value -> value.firstRedoPosition() < lastPositionExclusive)
                .sorted(Comparator.comparingLong(LogBackupEntry::firstRedoPosition)
                        .thenComparingLong(LogBackupEntry::lastRedoPosition)
                        .thenComparingLong(LogBackupEntry::backupId))
                .toList();
        List<BackupFile> result = new ArrayList<>();
        long expected = expectedFirstPosition;
        int cursor = 0;
        while (expected < lastPositionExclusive) {
            while (cursor < sorted.size()
                    && sorted.get(cursor).lastRedoPosition() <= expected) {
                cursor++;
            }
            if (cursor >= sorted.size()) {
                break;
            }
            LogBackupEntry entry = sorted.get(cursor);
            if (entry.firstRedoPosition() > expected) {
                throw new IllegalStateException("FATAL_GAP: expected redo position " + expected
                        + " but catalog starts at " + entry.firstRedoPosition());
            }
            if (entry.firstRedoPosition() < expected) {
                throw new IllegalStateException("AMBIGUOUS_OVERLAP: expected redo position " + expected
                        + " but catalog entry starts at " + entry.firstRedoPosition());
            }
            if (entry.lastRedoPosition() > lastPositionExclusive) {
                throw new IllegalStateException("target last.position=" + lastPositionExclusive
                        + " is not a log-backup boundary; next backup ends at "
                        + entry.lastRedoPosition());
            }
            if (cursor + 1 < sorted.size()) {
                LogBackupEntry duplicate = sorted.get(cursor + 1);
                if (duplicate.firstRedoPosition() == entry.firstRedoPosition()) {
                    throw new IllegalStateException("AMBIGUOUS_BACKUP: multiple successful entries start at "
                            + entry.firstRedoPosition());
                }
            }
            if (entry.sourceId() != expectedSourceId) {
                throw new IllegalStateException("CATALOG_SOURCE_IDENTITY_MISMATCH: expected source_id="
                        + expectedSourceId + " but catalog has source_id=" + entry.sourceId()
                        + " backup_id=" + entry.backupId());
            }
            contract.validateEntry(entry);
            Path path = mapper.resolve(entry.destinationPath());
            BackupFile backup = BackupFile.parse(path).orElseThrow(() ->
                    new IllegalStateException("catalog object is not a recognized HANA log backup: " + path));
            if (backup.volume() != expectedVolume || backup.partition() != expectedPartition
                    || backup.first() != entry.firstRedoPosition()
                    || backup.last() != entry.lastRedoPosition()
                    || backup.backupId() != entry.backupId()) {
                throw new IllegalStateException("CATALOG_HEADER_IDENTITY_MISMATCH: " + path);
            }
            result.add(backup);
            expected = backup.last();
            cursor++;
        }
        return List.copyOf(result);
    }
}

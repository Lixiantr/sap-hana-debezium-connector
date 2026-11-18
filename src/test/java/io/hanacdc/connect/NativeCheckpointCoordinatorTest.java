package io.hanacdc.connect;

import io.hanacdc.decoder.NativeRecordReader;
import io.hanacdc.decoder.NativeRowDecoder;
import io.hanacdc.decoder.NativeRowLobPipeline;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;
import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class NativeCheckpointCoordinatorTest {
    private static final BackupFile BACKUP =
            new BackupFile(Path.of("fixture"), 2, 0, 0, 1000, 99);
    private static final TableDecodePlan PLAN = plan();

    @TempDir
    Path temporaryDirectory;

    @Test
    void serializesPollMutationWithOffsetCommitPruning() throws Exception {
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        DurableReducerStateStore store = new DurableReducerStateStore(temporaryDirectory);
        NativeCheckpointCoordinator coordinator = NativeCheckpointCoordinator.initialize(
                epoch, store, new ReplayBoundary(99, 0, 1000, 1, 1), 128);
        ArrayBlockingQueue<SourceOffset> confirmed = new ArrayBlockingQueue<>(1);
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var mutation = executor.submit(() -> {
                start.await();
                for (int index = 0; index < 64; index++) {
                    long position = 200L + index;
                    var result = coordinator.process(
                            commit(1000L + index, position,
                                    row(1000L + index, position - 1,
                                            500L + index, 7 + index, true)),
                            new ReplayBoundary(99, 0, 1000,
                                    position + 1, position));
                    confirmed.put(SourceOffset.fromMap(
                            result.publications().get(0).sourceOffset(epoch)));
                }
                return null;
            });
            var pruning = executor.submit(() -> {
                start.await();
                for (int index = 0; index < 64; index++) {
                    coordinator.pruneConfirmed(confirmed.take());
                }
                return null;
            });
            start.countDown();
            mutation.get(30, TimeUnit.SECONDS);
            pruning.get(30, TimeUnit.SECONDS);
        }
        finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        }
        assertEquals(65, coordinator.currentCheckpoint().generation());
        assertEquals(263, coordinator.currentBoundary().committedPosition());
    }

    @Test
    void intermediatePublicationReferencesPreStateAndFinalReferencesPostState()
            throws Exception {
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        DurableReducerStateStore store = new DurableReducerStateStore(temporaryDirectory);
        NativeCheckpointCoordinator coordinator = NativeCheckpointCoordinator.initialize(
                epoch, store, new ReplayBoundary(99, 0, 1000, 1, 1));

        NativeCheckpointCoordinator.ProcessedTransaction result = coordinator.process(
                commit(71, 100,
                        row(71, 80, 41, 7, true),
                        row(71, 90, 42, 8, true)),
                new ReplayBoundary(99, 0, 1000, 101, 100));

        assertEquals(2, result.publications().size());
        assertEquals(result.preCheckpoint(), result.publications().get(0).checkpoint());
        assertEquals(result.postCheckpoint(), result.publications().get(1).checkpoint());
        assertEquals(0, result.publications().get(0).sourceOffset(epoch).get("change_index"));
        assertEquals(1, result.publications().get(1).sourceOffset(epoch).get("change_index"));
        assertEquals(2, result.publications().get(1).sourceOffset(epoch).get("change_count"));

        NativeCheckpointCoordinator restoredPre = NativeCheckpointCoordinator.restore(
                epoch, store, result.preCheckpoint().generation(),
                result.preCheckpoint().sha256());
        NativeCheckpointCoordinator restoredPost = NativeCheckpointCoordinator.restore(
                epoch, store, result.postCheckpoint().generation(),
                result.postCheckpoint().sha256());
        assertTrue(restoredPre.snapshot().tombstones().isEmpty());
        assertEquals(2, restoredPost.snapshot().tombstones().size());
        assertEquals(1, restoredPre.currentBoundary().committedPosition());
        assertEquals(100, restoredPost.currentBoundary().committedPosition());
    }

    @Test
    void persistsStateOnlyCleanupEvenWithoutPublication() throws Exception {
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        DurableReducerStateStore store = new DurableReducerStateStore(temporaryDirectory);
        NativeCheckpointCoordinator coordinator = NativeCheckpointCoordinator.initialize(
                epoch, store, new ReplayBoundary(99, 0, 1000, 1, 1));

        var tombstone = coordinator.process(
                commit(72, 110, row(72, 100, 47, 7, true)),
                new ReplayBoundary(99, 0, 1000, 111, 110));
        assertEquals(1, tombstone.publications().size());
        assertEquals(1, coordinator.snapshot().tombstones().size());

        var cleanup = coordinator.process(
                cleanup(73, 120, 47),
                new ReplayBoundary(99, 0, 1000, 121, 120));
        assertTrue(cleanup.publications().isEmpty());
        assertTrue(coordinator.snapshot().tombstones().isEmpty());
        assertEquals(1, coordinator.snapshot().deletedKeys().size());

        NativeCheckpointCoordinator restored = NativeCheckpointCoordinator.restore(
                epoch, store, cleanup.postCheckpoint().generation(),
                cleanup.postCheckpoint().sha256());
        assertTrue(restored.snapshot().tombstones().isEmpty());
        assertEquals(1, restored.snapshot().deletedKeys().size());
    }

    @Test
    void advancesUnchangedStateOnlyWhenBackupChanges() throws Exception {
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        DurableReducerStateStore store = new DurableReducerStateStore(temporaryDirectory);
        NativeCheckpointCoordinator coordinator = NativeCheckpointCoordinator.initialize(
                epoch, store, new ReplayBoundary(99, 0, 1000, 1, 1));
        NativeRowLobPipeline.CompletedTransaction progress =
                new NativeRowLobPipeline.CompletedTransaction(74, terminal(130),
                        NativeRowLobPipeline.Outcome.COMMIT, List.of(), List.of());

        DurableReducerStateStore.Checkpoint unchanged = coordinator.advanceProgress(
                progress, new ReplayBoundary(99, 0, 1000, 131, 130));

        assertEquals(1, unchanged.generation());
        assertEquals(1, coordinator.currentBoundary().committedPosition());

        BackupFile nextBackup = new BackupFile(
                Path.of("fixture-2"), 2, 0, 1000, 2000, 100);
        NativeRowLobPipeline.CompletedTransaction nextProgress =
                new NativeRowLobPipeline.CompletedTransaction(75,
                        terminal(nextBackup, 1130),
                        NativeRowLobPipeline.Outcome.COMMIT, List.of(), List.of());
        DurableReducerStateStore.Checkpoint checkpoint = coordinator.advanceProgress(
                nextProgress, new ReplayBoundary(100, 1000, 2000, 1131, 1130));

        assertEquals(2, checkpoint.generation());
        assertEquals(1130, coordinator.currentBoundary().committedPosition());
        assertTrue(coordinator.snapshot().tombstones().isEmpty());
        NativeCheckpointCoordinator restored = NativeCheckpointCoordinator.restore(
                epoch, store, checkpoint.generation(), checkpoint.sha256());
        assertEquals(1130, restored.currentBoundary().committedPosition());
    }

    @Test
    void prunesOnlyThroughOffsetConfirmedByConnect() throws Exception {
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        DurableReducerStateStore store = new DurableReducerStateStore(temporaryDirectory);
        NativeCheckpointCoordinator coordinator = NativeCheckpointCoordinator.initialize(
                epoch, store, new ReplayBoundary(99, 0, 1000, 1, 1));
        var second = coordinator.process(
                commit(75, 140, row(75, 135, 51, 9, true)),
                new ReplayBoundary(99, 0, 1000, 141, 140));
        var third = coordinator.process(
                commit(76, 150, row(76, 145, 52, 10, true)),
                new ReplayBoundary(99, 0, 1000, 151, 150));
        SourceOffset confirmed = SourceOffset.fromMap(
                second.publications().get(0).sourceOffset(epoch));

        assertEquals(0, coordinator.pruneConfirmed(confirmed));
        assertTrue(java.nio.file.Files.isRegularFile(second.postCheckpoint().path()));
        assertTrue(java.nio.file.Files.isRegularFile(third.postCheckpoint().path()));
        assertTrue(java.nio.file.Files.isRegularFile(second.preCheckpoint().path()));
    }

    @Test
    void stopsBeforeWritingBeyondUncommittedGenerationLimit() throws Exception {
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        DurableReducerStateStore store = new DurableReducerStateStore(temporaryDirectory);
        NativeCheckpointCoordinator coordinator = NativeCheckpointCoordinator.initialize(
                epoch, store, new ReplayBoundary(99, 0, 1000, 1, 1), 2);
        var second = coordinator.process(
                commit(77, 160, row(77, 155, 53, 11, true)),
                new ReplayBoundary(99, 0, 1000, 161, 160));

        java.io.IOException exhausted = assertThrows(java.io.IOException.class,
                () -> coordinator.process(
                        commit(78, 170, row(78, 165, 54, 12, true)),
                        new ReplayBoundary(99, 0, 1000, 171, 170)));
        assertTrue(exhausted.getMessage().contains("FATAL_RESOURCE_LIMIT"));
        try (var files = java.nio.file.Files.list(temporaryDirectory)) {
            assertEquals(2, files
                    .filter(path -> path.getFileName().toString().endsWith(".bin"))
                    .count());
        }

        SourceOffset confirmed = SourceOffset.fromMap(
                second.publications().get(0).sourceOffset(epoch));
        assertEquals(0, coordinator.pruneConfirmed(confirmed));
        coordinator.process(
                commit(78, 170, row(78, 165, 54, 12, true)),
                new ReplayBoundary(99, 0, 1000, 171, 170));
        assertEquals(3, coordinator.currentCheckpoint().generation());
    }

    @Test
    void boundsEncodedBytesAndCountsNewerOrphansOnRestore() throws Exception {
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        DurableReducerStateStore store = new DurableReducerStateStore(temporaryDirectory);
        ReplayBoundary initial = new ReplayBoundary(99, 0, 1000, 1, 1);
        var emptyReducer = new io.hanacdc.decoder.NativeTransactionReducer();
        var initialDescription = store.describe(
                epoch, 1, initial, emptyReducer.snapshot());
        long oneCheckpoint = initialDescription.sizeBytes();
        BackupFile secondBackup = new BackupFile(
                Path.of("fixture-2"), 2, 0, 1000, 2000, 100);
        var secondRow = row(79, 1075, 55, 13, false);
        var secondTransaction = commit(secondBackup, 79, 1100, secondRow);
        var secondDelta = emptyReducer.reduce(secondTransaction)
                .orElseThrow().stateDelta();
        ReplayBoundary secondBoundary =
                new ReplayBoundary(100, 1000, 2000, 1101, 1100);
        long secondDeltaBytes = store.describeDelta(
                epoch, 2, secondBoundary,
                store.reference(1, initialDescription.sha256()), secondDelta).sizeBytes();
        NativeCheckpointCoordinator coordinator = NativeCheckpointCoordinator.initialize(
                epoch, store, initial, 10,
                Math.addExact(oneCheckpoint, secondDeltaBytes));
        var second = coordinator.process(secondTransaction, secondBoundary);
        var snapshotBeforeFailure = coordinator.snapshot();
        BackupFile thirdBackup = new BackupFile(
                Path.of("fixture-3"), 2, 0, 2000, 3000, 101);

        java.io.IOException exhausted = assertThrows(java.io.IOException.class,
                () -> coordinator.process(
                        commit(thirdBackup, 80, 2100,
                                row(80, 2075, 56, 14, false)),
                        new ReplayBoundary(101, 2000, 3000, 2101, 2100)));
        assertTrue(exhausted.getMessage().contains("uncommitted bytes"));
        assertEquals(snapshotBeforeFailure, coordinator.snapshot());
        try (var files = java.nio.file.Files.list(temporaryDirectory)) {
            assertEquals(2, files
                    .filter(path -> path.getFileName().toString().endsWith(".bin"))
                    .count());
        }

        SourceOffset confirmed = SourceOffset.fromMap(
                second.publications().get(0).sourceOffset(epoch));
        coordinator.pruneConfirmed(confirmed);
        var third = coordinator.process(
                commit(thirdBackup, 80, 2100, row(80, 2075, 56, 14, false)),
                new ReplayBoundary(101, 2000, 3000, 2101, 2100));

        java.io.IOException orphanLimit = assertThrows(java.io.IOException.class,
                () -> NativeCheckpointCoordinator.restore(epoch, store,
                        second.postCheckpoint().generation(),
                        second.postCheckpoint().sha256(), 10, oneCheckpoint - 1));
        assertTrue(orphanLimit.getMessage().contains("uncommitted bytes"));
        assertEquals(3, third.postCheckpoint().generation());
    }

    @Test
    void periodicFullCheckpointBoundsChainAndReleasesAncestors() throws Exception {
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        DurableReducerStateStore store = new DurableReducerStateStore(temporaryDirectory);
        NativeCheckpointCoordinator coordinator = NativeCheckpointCoordinator.initialize(
                epoch, store, new ReplayBoundary(99, 0, 1000, 1, 1), 300);
        NativeCheckpointCoordinator.ProcessedTransaction last = null;
        for (int index = 0; index < 255; index++) {
            long terminal = 200L + index;
            last = coordinator.process(
                    commit(10_000L + index, terminal,
                            row(10_000L + index, terminal - 1,
                                    600L + index, 1000 + index, true)),
                    new ReplayBoundary(99, 0, 1000,
                            terminal + 1, terminal));
        }

        assertEquals(256, coordinator.currentCheckpoint().generation());
        try (var files = java.nio.file.Files.list(temporaryDirectory)) {
            assertEquals(256, files
                    .filter(path -> path.getFileName().toString().endsWith(".bin"))
                    .count());
        }
        NativeCheckpointCoordinator restored = NativeCheckpointCoordinator.restore(
                epoch, store, coordinator.currentCheckpoint().generation(),
                coordinator.currentCheckpoint().sha256());
        assertEquals(255, restored.snapshot().tombstones().size());
        SourceOffset confirmed = SourceOffset.fromMap(
                last.publications().get(0).sourceOffset(epoch));
        assertEquals(0, coordinator.pruneConfirmed(confirmed));
        try (var files = java.nio.file.Files.list(temporaryDirectory)) {
            assertEquals(1, files
                    .filter(path -> path.getFileName().toString().endsWith(".bin"))
                    .count());
        }
    }

    private static NativeRowLobPipeline.CompletedTransaction commit(
            long tid, long terminal, NativeRowDecoder.DecodedRow... rows) {
        return commit(BACKUP, tid, terminal, rows);
    }

    private static NativeRowLobPipeline.CompletedTransaction commit(
            BackupFile backup, long tid, long terminal,
            NativeRowDecoder.DecodedRow... rows) {
        return new NativeRowLobPipeline.CompletedTransaction(tid,
                terminal(backup, terminal), NativeRowLobPipeline.Outcome.COMMIT,
                List.of(rows), List.of());
    }

    private static NativeRowLobPipeline.CompletedTransaction cleanup(
            long tid, long terminal, long locator) {
        return new NativeRowLobPipeline.CompletedTransaction(tid,
                terminal(terminal), NativeRowLobPipeline.Outcome.COMMIT, List.of(),
                List.of(new NativeRowLobPipeline.PhysicalDelete(
                        PLAN, 0xfe000001L, locator, terminal - 5)));
    }

    private static NativeRowLobPipeline.TerminalRecord terminal(long position) {
        return terminal(BACKUP, position);
    }

    private static NativeRowLobPipeline.TerminalRecord terminal(
            BackupFile backup, long position) {
        return new NativeRowLobPipeline.TerminalRecord(
                backup, position, 4096 + position * 64, position + 1);
    }

    private static NativeRowDecoder.DecodedRow row(
            long tid, long position, long locator, long id, boolean deleted) {
        return new NativeRowDecoder.DecodedRow(PLAN, tid, position, locator, 0xfe000001L,
                NativeRecordReader.UT_UPSERT, true, List.of(
                        NativeRowDecoder.ColumnValue.scalar(id),
                        NativeRowDecoder.ColumnValue.scalar(1L),
                        NativeRowDecoder.ColumnValue.scalar(deleted),
                        NativeRowDecoder.ColumnValue.scalar("value-" + id)));
    }

    private static TableDecodePlan plan() {
        return TableDecodePlan.compile(new TableMetadata(
                "CDC", "FULL_ROW", "COLUMN", 999, 0xfe000001L,
                List.of(
                        column(1, 201, "ID", "BIGINT", Types.BIGINT),
                        column(2, 202, "ROW_VERSION", "BIGINT", Types.BIGINT),
                        column(3, 203, "IS_DELETED", "BOOLEAN", Types.BOOLEAN),
                        column(4, 204, "VALUE_TEXT", "NVARCHAR", Types.NVARCHAR)),
                List.of(), List.of("ID")));
    }

    private static TableMetadata.Column column(
            int ordinal, long internalId, String name, String type, int dataTypeId) {
        return new TableMetadata.Column(ordinal, internalId, name, type, dataTypeId,
                64, 34, 0, false, false, false, "");
    }
}

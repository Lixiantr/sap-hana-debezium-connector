package io.hanacdc.decoder;

import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NativeTransactionReducerTest {
    private static final TableDecodePlan PLAN = plan();
    private static final io.hanacdc.decoder.ParallelLogDecoder.BackupFile BACKUP =
            new io.hanacdc.decoder.ParallelLogDecoder.BackupFile(
                    java.nio.file.Path.of("fixture"), 2, 0, 0, 1000, 99);

    @Test
    void coalescesInsertAndUpsertToOneCreateWithFinalImage() throws Exception {
        NativeTransactionReducer reducer = new NativeTransactionReducer();
        var batch = reducer.reduce(commit(71,
                row(71, 1, 42, NativeRecordReader.UT_INSERT_DATA, 1, false),
                row(71, 2, 43, NativeRecordReader.UT_UPSERT, 2, false)))
                .orElseThrow();

        assertEquals(1, batch.changes().size());
        assertEquals(NativeTransactionReducer.Operation.CREATE,
                batch.changes().get(0).operation());
        assertEquals(2L, batch.changes().get(0).row().values().get(1).value());
    }

    @Test
    void coalescesManyUpsertsToOneUpdate() throws Exception {
        NativeTransactionReducer reducer = new NativeTransactionReducer();
        var batch = reducer.reduce(commit(72,
                row(72, 1, 50, NativeRecordReader.UT_UPSERT, 1, false),
                row(72, 2, 51, NativeRecordReader.UT_UPSERT, 2, false)))
                .orElseThrow();

        assertEquals(List.of(NativeTransactionReducer.Operation.UPDATE),
                batch.changes().stream().map(NativeTransactionReducer.LogicalChange::operation)
                        .toList());
    }

    @Test
    void insertThenTombstonePublishesNothingButAuthorizesCleanup() throws Exception {
        NativeTransactionReducer reducer = new NativeTransactionReducer();
        var tombstone = reducer.reduce(commit(73,
                row(73, 1, 60, NativeRecordReader.UT_INSERT_DATA, 1, false),
                row(73, 2, 61, NativeRecordReader.UT_UPSERT, 2, true)))
                .orElseThrow();

        assertTrue(tombstone.changes().isEmpty());
        assertEquals(1, reducer.pendingTombstones());
        var cleanup = reducer.reduce(cleanup(74, 3, 61)).orElseThrow();
        assertEquals(1, cleanup.cleanedPhysicalDeletes());
        assertEquals(0, reducer.pendingTombstones());
    }

    @Test
    void repeatedTombstoneIsIdempotentAndReplacesLocator() throws Exception {
        NativeTransactionReducer reducer = new NativeTransactionReducer();
        var first = reducer.reduce(commit(75,
                row(75, 1, 70, NativeRecordReader.UT_UPSERT, 1, true))).orElseThrow();
        var repeated = reducer.reduce(commit(76,
                row(76, 2, 71, NativeRecordReader.UT_UPSERT, 2, true))).orElseThrow();

        assertEquals(NativeTransactionReducer.Operation.DELETE,
                first.changes().get(0).operation());
        assertTrue(repeated.changes().isEmpty());
        assertEquals(1, reducer.pendingTombstones());
        assertThrows(java.io.IOException.class, () -> reducer.reduce(cleanup(77, 3, 70)));
        assertEquals(1, reducer.reduce(cleanup(78, 4, 71)).orElseThrow()
                .cleanedPhysicalDeletes());
    }

    @Test
    void unmatchedDeleteAndResurrectionFailClosedWithoutPartialState() throws Exception {
        NativeTransactionReducer reducer = new NativeTransactionReducer();
        var unknown = assertThrows(java.io.IOException.class,
                () -> reducer.reduce(cleanup(79, 1, 999)));
        assertTrue(unknown.getMessage().contains("UNKNOWN_DELETE_LOCATOR"));
        assertEquals(0, reducer.pendingTombstones());

        reducer.reduce(commit(80,
                row(80, 2, 80, NativeRecordReader.UT_UPSERT, 1, true)));
        var resurrection = assertThrows(java.io.IOException.class,
                () -> reducer.reduce(commit(81,
                        row(81, 3, 81, NativeRecordReader.UT_UPSERT, 2, false))));
        assertTrue(resurrection.getMessage().contains("RESURRECTION_AFTER_TOMBSTONE"));
        assertEquals(1, reducer.pendingTombstones());
    }

    @Test
    void restoredSnapshotCorrelatesCleanupAndKeepsDeletedKey() throws Exception {
        NativeTransactionReducer first = new NativeTransactionReducer();
        first.reduce(commit(82,
                row(82, 4, 91, NativeRecordReader.UT_UPSERT, 3, true)));

        NativeTransactionReducer restored = new NativeTransactionReducer();
        restored.restore(first.snapshot());
        assertEquals(1, restored.pendingTombstones());
        assertEquals(1, restored.reduce(cleanup(83, 5, 91)).orElseThrow()
                .cleanedPhysicalDeletes());
        assertEquals(0, restored.pendingTombstones());
        assertEquals(1, restored.deletedKeys());
        assertTrue(assertThrows(java.io.IOException.class,
                () -> restored.reduce(commit(84,
                        row(84, 6, 92, NativeRecordReader.UT_UPSERT, 4, false))))
                .getMessage().contains("RESURRECTION_AFTER_TOMBSTONE"));
    }

    @Test
    void sameRowLocatorInDifferentContainersDoesNotCollide() throws Exception {
        NativeTransactionReducer reducer = new NativeTransactionReducer();
        long firstContainer = 0xfe000001L;
        long secondContainer = 0xfe000002L;

        reducer.reduce(commit(85,
                row(85, 7, 100, firstContainer, 7,
                        NativeRecordReader.UT_UPSERT, 1, true),
                row(85, 8, 100, secondContainer, 8,
                        NativeRecordReader.UT_UPSERT, 1, true)));

        assertEquals(2, reducer.pendingTombstones());
        assertEquals(1, reducer.reduce(cleanup(86, 9, firstContainer, 100))
                .orElseThrow().cleanedPhysicalDeletes());
        assertEquals(1, reducer.pendingTombstones());
        assertEquals(1, reducer.reduce(cleanup(87, 10, secondContainer, 100))
                .orElseThrow().cleanedPhysicalDeletes());
        assertEquals(0, reducer.pendingTombstones());
    }

    @Test
    void replaceRestoresAnAtomicPriorSnapshot() throws Exception {
        NativeTransactionReducer reducer = new NativeTransactionReducer();
        reducer.reduce(commit(88,
                row(88, 11, 101, NativeRecordReader.UT_UPSERT, 1, true)));
        var prior = reducer.snapshot();
        reducer.reduce(cleanup(89, 12, 101));

        reducer.replace(prior);

        assertEquals(prior, reducer.snapshot());
        assertEquals(1, reducer.pendingTombstones());
    }

    @Test
    void failedCleanupBatchDoesNotApplyEarlierDelta() throws Exception {
        NativeTransactionReducer reducer = new NativeTransactionReducer();
        reducer.reduce(commit(90,
                row(90, 13, 200, 0xfe000001L, 200,
                        NativeRecordReader.UT_UPSERT, 1, true),
                row(90, 14, 201, 0xfe000001L, 201,
                        NativeRecordReader.UT_UPSERT, 1, true)));

        assertThrows(java.io.IOException.class, () -> reducer.reduce(
                cleanupMany(91, 15, 200, 999)));

        assertEquals(2, reducer.pendingTombstones());
        assertEquals(1, reducer.reduce(cleanup(92, 16, 200))
                .orElseThrow().cleanedPhysicalDeletes());
        assertEquals(1, reducer.pendingTombstones());
    }

    @Test
    void stateLimitFailureDoesNotApplyStagedTombstone() throws Exception {
        NativeTransactionReducer reducer = new NativeTransactionReducer(1, 2);
        reducer.reduce(commit(93,
                row(93, 17, 300, 0xfe000001L, 300,
                        NativeRecordReader.UT_UPSERT, 1, true)));

        assertTrue(assertThrows(java.io.IOException.class, () -> reducer.reduce(commit(94,
                row(94, 18, 301, 0xfe000001L, 301,
                        NativeRecordReader.UT_UPSERT, 1, true))))
                .getMessage().contains("TOMBSTONE_STATE_LIMIT"));

        assertEquals(1, reducer.pendingTombstones());
        assertEquals(1, reducer.deletedKeys());
        assertEquals(1, reducer.reduce(cleanup(95, 19, 300))
                .orElseThrow().cleanedPhysicalDeletes());
    }

    @Test
    void committedDeltaCanBeRevertedAndReappliedExactly() throws Exception {
        NativeTransactionReducer reducer = new NativeTransactionReducer();
        var before = reducer.snapshot();
        var batch = reducer.reduce(commit(96,
                row(96, 20, 400, 0xfe000001L, 400,
                        NativeRecordReader.UT_UPSERT, 1, true)))
                .orElseThrow();
        var after = reducer.snapshot();

        reducer.revert(batch.stateDelta());
        assertEquals(before, reducer.snapshot());
        reducer.apply(batch.stateDelta());
        assertEquals(after, reducer.snapshot());
    }

    private static NativeRowLobPipeline.CompletedTransaction commit(
            long tid, NativeRowDecoder.DecodedRow... rows) {
        long terminal = 100 + tid;
        return new NativeRowLobPipeline.CompletedTransaction(tid,
                new NativeRowLobPipeline.TerminalRecord(BACKUP, terminal,
                        4096 + terminal * 64, terminal + 1),
                NativeRowLobPipeline.Outcome.COMMIT, List.of(rows), List.of());
    }

    private static NativeRowLobPipeline.CompletedTransaction cleanup(
            long tid, long position, long locator) {
        return cleanup(tid, position, 0xfe000001L, locator);
    }

    private static NativeRowLobPipeline.CompletedTransaction cleanup(
            long tid, long position, long containerId, long locator) {
        long terminal = 100 + tid;
        return new NativeRowLobPipeline.CompletedTransaction(tid,
                new NativeRowLobPipeline.TerminalRecord(BACKUP, terminal,
                        4096 + terminal * 64, terminal + 1),
                NativeRowLobPipeline.Outcome.COMMIT, List.of(),
                List.of(new NativeRowLobPipeline.PhysicalDelete(
                        PLAN, containerId, locator, position)));
    }

    private static NativeRowLobPipeline.CompletedTransaction cleanupMany(
            long tid, long position, long... locators) {
        long terminal = 100 + tid;
        List<NativeRowLobPipeline.PhysicalDelete> deletes =
                java.util.Arrays.stream(locators)
                        .mapToObj(locator -> new NativeRowLobPipeline.PhysicalDelete(
                                PLAN, 0xfe000001L, locator, position))
                        .toList();
        return new NativeRowLobPipeline.CompletedTransaction(tid,
                new NativeRowLobPipeline.TerminalRecord(BACKUP, terminal,
                        4096 + terminal * 64, terminal + 1),
                NativeRowLobPipeline.Outcome.COMMIT, List.of(), deletes);
    }

    private static NativeRowDecoder.DecodedRow row(
            long tid, long position, long locator, int operation,
            long version, boolean deleted) {
        return row(tid, position, locator, 0xfe000001L, 7, operation, version, deleted);
    }

    private static NativeRowDecoder.DecodedRow row(
            long tid, long position, long locator, long containerId, long id, int operation,
            long version, boolean deleted) {
        return new NativeRowDecoder.DecodedRow(
                PLAN, tid, position, locator, containerId, operation, true,
                List.of(
                        NativeRowDecoder.ColumnValue.scalar(id),
                        NativeRowDecoder.ColumnValue.scalar(version),
                        NativeRowDecoder.ColumnValue.scalar(deleted),
                        NativeRowDecoder.ColumnValue.scalar("value-" + version)));
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

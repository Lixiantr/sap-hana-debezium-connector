package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeBackupDecoder.NativeRecord;
import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;
import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.sql.Types;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NativeRowLobPipelineTest {
    @Test
    void releasesSelectedFullRowOnlyAfterGlobalCommit() throws Exception {
        try (NativeRowLobPipeline pipeline = pipeline()) {
            assertTrue(pipeline.accept(row(77, 1, 999, NativeRecordReader.UT_UPSERT))
                    .isEmpty());
            pipeline.accept(prepare(77, 2));
            var completed = pipeline.accept(terminal(
                    77, 3, NativeRecordReader.OUTER_COMMIT)).orElseThrow();

            assertEquals(NativeRowLobPipeline.Outcome.COMMIT, completed.outcome());
            assertEquals(77, completed.tid());
            assertEquals(1, completed.rows().size());
            assertEquals(7L, completed.rows().get(0).values().get(0).value());
            assertEquals("hello", completed.rows().get(0).values().get(3).value());
            assertEquals(0, pipeline.openTransactions());
        }
    }

    @Test
    void interleavedTransactionsAreReleasedInCommitOrder() throws Exception {
        try (NativeRowLobPipeline pipeline = pipeline()) {
            pipeline.accept(row(71, 1, 999, NativeRecordReader.UT_UPSERT));
            pipeline.accept(row(72, 2, 999, NativeRecordReader.UT_UPSERT));

            pipeline.accept(prepare(72, 3));
            assertEquals(72, pipeline.accept(terminal(
                    72, 4, NativeRecordReader.OUTER_COMMIT)).orElseThrow().tid());
            pipeline.accept(prepare(71, 5));
            assertEquals(71, pipeline.accept(terminal(
                    71, 6, NativeRecordReader.OUTER_COMMIT)).orElseThrow().tid());
        }
    }

    @Test
    void rollbackAndExcludedOnlyTransactionPublishNoRows() throws Exception {
        try (NativeRowLobPipeline pipeline = pipeline()) {
            pipeline.accept(row(81, 1, 999, NativeRecordReader.UT_UPSERT));
            var rolledBack = pipeline.accept(terminal(
                    81, 2, NativeRecordReader.OUTER_ROLLBACK)).orElseThrow();
            assertEquals(NativeRowLobPipeline.Outcome.ROLLBACK, rolledBack.outcome());
            assertTrue(rolledBack.rows().isEmpty());

            assertTrue(pipeline.accept(row(
                    82, 3, 1000, NativeRecordReader.UT_UPSERT)).isEmpty());
            pipeline.accept(prepare(82, 4));
            var excluded = pipeline.accept(terminal(
                    82, 5, NativeRecordReader.OUTER_COMMIT)).orElseThrow();
            assertTrue(excluded.rows().isEmpty());
            assertTrue(excluded.physicalDeletes().isEmpty());
            assertEquals(5, excluded.terminalPosition());
        }
    }

    @Test
    void ignoresUnselectedCatalogLobTransaction() throws Exception {
        try (NativeRowLobPipeline pipeline = pipeline()) {
            byte[] payload = new byte[40];
            ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN)
                    .putLong(24, 0xfe0000022eL);
            NativeRecordHeader header = new NativeRecordHeader(
                    backup(), 4096 + 1L * 64, 1, 740137,
                    NativeRecordReader.OUTER_LOB_WRITE, payload.length, 128,
                    -1, -1, -1, -1);

            assertTrue(pipeline.accept(new NativeRecord(header, payload)).isEmpty());
            pipeline.accept(prepare(740137, 2));
            assertTrue(pipeline.accept(terminal(
                    740137, 3, NativeRecordReader.OUTER_COMMIT)).isEmpty());
            assertEquals(0, pipeline.openTransactions());
        }
    }

    @Test
    void fullImageUpdateIsAcceptedAsACommittedLogicalUpdate() throws Exception {
        try (NativeRowLobPipeline pipeline = pipeline()) {
            pipeline.accept(row(91, 1, 999, NativeRecordReader.UT_UPDATE));
            pipeline.accept(prepare(91, 2));
            var completed = pipeline.accept(terminal(
                    91, 3, NativeRecordReader.OUTER_COMMIT)).orElseThrow();
            assertEquals(1, completed.rows().size());
            assertEquals(NativeRecordReader.UT_UPDATE,
                    completed.rows().get(0).operationCode());
        }
    }

    @Test
    void commitRequiresPrepareAndPrepareIsAnOrderingBarrier() throws Exception {
        try (NativeRowLobPipeline pipeline = pipeline()) {
            pipeline.accept(row(92, 1, 999, NativeRecordReader.UT_UPSERT));
            var missing = assertThrows(java.io.IOException.class,
                    () -> pipeline.accept(terminal(
                            92, 2, NativeRecordReader.OUTER_COMMIT)));
            assertTrue(missing.getMessage().contains("COMMIT without ordered PREPARE_COMMIT"));
        }

        try (NativeRowLobPipeline pipeline = pipeline()) {
            pipeline.accept(row(93, 1, 999, NativeRecordReader.UT_UPSERT));
            pipeline.accept(prepare(93, 2));
            var after = assertThrows(java.io.IOException.class,
                    () -> pipeline.accept(row(
                            93, 3, 999, NativeRecordReader.UT_UPSERT)));
            assertTrue(after.getMessage().contains("record after PREPARE_COMMIT"));
        }
    }

    @Test
    void partialRollbackRemovesTailAndCommitKeepsPrefix() throws Exception {
        try (NativeRowLobPipeline pipeline = pipeline()) {
            pipeline.accept(row(101, 1, 999, NativeRecordReader.UT_UPSERT));
            pipeline.accept(delete(101, 2));
            pipeline.accept(row(101, 3, 999, NativeRecordReader.UT_UPSERT));
            assertTrue(pipeline.accept(partialRollback(101, 4, 2)).isEmpty());

            pipeline.accept(prepare(101, 5));
            var completed = pipeline.accept(terminal(
                    101, 6, NativeRecordReader.OUTER_COMMIT)).orElseThrow();
            assertEquals(1, completed.rows().size());
            assertTrue(completed.physicalDeletes().isEmpty());
            assertEquals(1, completed.rows().get(0).logPosition());
        }
    }

    @Test
    void zeroStartPartialRollbackClearsStateBeforeFullRollback() throws Exception {
        try (NativeRowLobPipeline pipeline = pipeline()) {
            pipeline.accept(row(102, 1, 999, NativeRecordReader.UT_UPSERT));
            assertTrue(pipeline.accept(partialRollback(102, 2, 0)).isEmpty());
            var rollback = pipeline.accept(terminal(
                    102, 3, NativeRecordReader.OUTER_ROLLBACK)).orElseThrow();
            assertTrue(rollback.rows().isEmpty());
            assertEquals(0, pipeline.openTransactions());
        }
    }

    @Test
    void deleteImmediatelyFollowedByUpsertIsInternalReplacement() throws Exception {
        try (NativeRowLobPipeline pipeline = pipeline()) {
            pipeline.accept(delete(103, 1));
            pipeline.accept(row(103, 2, 999, NativeRecordReader.UT_UPSERT));
            pipeline.accept(prepare(103, 3));
            var completed = pipeline.accept(terminal(
                    103, 4, NativeRecordReader.OUTER_COMMIT)).orElseThrow();
            assertEquals(1, completed.rows().size());
            assertTrue(completed.physicalDeletes().isEmpty());
        }
    }

    @Test
    void unmatchedDeleteSurvivesUntilCommitAsCleanupCandidate() throws Exception {
        try (NativeRowLobPipeline pipeline = pipeline()) {
            pipeline.accept(delete(104, 1));
            pipeline.accept(prepare(104, 2));
            var completed = pipeline.accept(terminal(
                    104, 3, NativeRecordReader.OUTER_COMMIT)).orElseThrow();
            assertEquals(1, completed.physicalDeletes().size());
            assertEquals(42, completed.physicalDeletes().get(0).rowId());
        }
    }

    @Test
    void bulkDeleteThenInsertIsCertifiedAsPartitionRelocation() throws Exception {
        try (NativeRowLobPipeline pipeline = pipeline()) {
            pipeline.accept(delete(105, 1));
            pipeline.accept(delete(105, 2));
            pipeline.accept(row(105, 3, 999, NativeRecordReader.UT_INSERT_DATA));
            pipeline.accept(row(105, 4, 999, NativeRecordReader.UT_INSERT_DATA));
            pipeline.accept(prepare(105, 5));
            var completed = pipeline.accept(terminal(
                    105, 6, NativeRecordReader.OUTER_COMMIT)).orElseThrow();

            assertEquals(2, completed.rows().size());
            assertTrue(completed.rows().stream().allMatch(row ->
                    row.operationCode() == NativeRecordReader.UT_UPSERT));
            assertTrue(completed.physicalDeletes().isEmpty());
        }
    }

    @Test
    void bulkDeleteThenFullUpdateIsCertifiedAsInternalRewrite() throws Exception {
        try (NativeRowLobPipeline pipeline = pipeline()) {
            pipeline.accept(delete(107, 1));
            pipeline.accept(delete(107, 2));
            pipeline.accept(row(107, 3, 999, NativeRecordReader.UT_UPDATE));
            pipeline.accept(row(107, 4, 999, NativeRecordReader.UT_UPDATE));
            pipeline.accept(prepare(107, 5));
            var completed = pipeline.accept(terminal(
                    107, 6, NativeRecordReader.OUTER_COMMIT)).orElseThrow();

            assertEquals(2, completed.rows().size());
            assertTrue(completed.rows().stream().allMatch(row ->
                    row.operationCode() == NativeRecordReader.UT_UPDATE));
            assertTrue(completed.physicalDeletes().isEmpty());
        }
    }

    @Test
    void ambiguousBulkRelocationFailsClosed() throws Exception {
        try (NativeRowLobPipeline pipeline = pipeline()) {
            pipeline.accept(delete(106, 1));
            pipeline.accept(row(106, 2, 999, NativeRecordReader.UT_INSERT_DATA));
            pipeline.accept(row(106, 3, 999, NativeRecordReader.UT_INSERT_DATA));
            pipeline.accept(prepare(106, 4));

            var error = assertThrows(java.io.IOException.class,
                    () -> pipeline.accept(terminal(
                            106, 5, NativeRecordReader.OUTER_COMMIT)));
            assertTrue(error.getMessage().contains("ambiguous DELETE/full-row rewrite"));
        }
    }

    @Test
    void transactionResourceLimitsFailClosedBeforeCommit() throws Exception {
        try (NativeRowLobPipeline pipeline = new NativeRowLobPipeline(
                List.of(plan()), 1, 1, 8, 1024 * 1024)) {
            pipeline.accept(row(201, 1, 999, NativeRecordReader.UT_UPSERT));
            var openLimit = assertThrows(java.io.IOException.class,
                    () -> pipeline.accept(row(
                            202, 2, 999, NativeRecordReader.UT_UPSERT)));
            assertTrue(openLimit.getMessage().contains("FATAL_RESOURCE_LIMIT"));
        }

        try (NativeRowLobPipeline pipeline = new NativeRowLobPipeline(
                List.of(plan()), 1, 8, 1, 1024 * 1024)) {
            pipeline.accept(row(203, 1, 999, NativeRecordReader.UT_UPSERT));
            var rowLimit = assertThrows(java.io.IOException.class,
                    () -> pipeline.accept(row(
                            203, 2, 999, NativeRecordReader.UT_UPSERT)));
            assertTrue(rowLimit.getMessage().contains("FATAL_RESOURCE_LIMIT"));
        }

        try (NativeRowLobPipeline pipeline = new NativeRowLobPipeline(
                List.of(plan()), 1, 8, 8, 100)) {
            var byteLimit = assertThrows(java.io.IOException.class,
                    () -> pipeline.accept(row(
                            204, 1, 999, NativeRecordReader.UT_UPSERT)));
            assertTrue(byteLimit.getMessage().contains("FATAL_RESOURCE_LIMIT"));
        }
    }

    private static NativeRowLobPipeline pipeline() {
        return new NativeRowLobPipeline(List.of(plan()), 2, 8, 16, 1024 * 1024);
    }

    private static NativeRecord row(long tid, long position, long poid, int operation) {
        byte[] payload = operation == NativeRecordReader.UT_INSERT_DATA
                ? insertPayload() : payload();
        NativeRecordHeader header = new NativeRecordHeader(
                backup(), 4096 + position * 64, position, tid,
                NativeRecordReader.OUTER_UNIFIED_TABLE, payload.length, 192,
                poid, container(poid), operation, 42);
        return new NativeRecord(header, payload);
    }

    private static byte[] insertPayload() {
        byte[] upsert = payload();
        byte[] insert = new byte[upsert.length - 16];
        System.arraycopy(upsert, 48, insert, 32, upsert.length - 48);
        return insert;
    }

    private static NativeRecord terminal(long tid, long position, int outer) {
        byte[] payload = new byte[outer == NativeRecordReader.OUTER_ROLLBACK ? 8 : 32];
        NativeRecordHeader header = new NativeRecordHeader(
                backup(), 4096 + position * 64, position, tid, outer,
                payload.length, 64, -1, -1, -1, -1);
        return new NativeRecord(header, payload);
    }

    private static NativeRecord prepare(long tid, long position) {
        byte[] payload = new byte[16];
        NativeRecordHeader header = new NativeRecordHeader(
                backup(), 4096 + position * 64, position, tid,
                NativeRecordReader.OUTER_PREPARE_COMMIT,
                payload.length, 64, -1, -1, -1, -1);
        return new NativeRecord(header, payload);
    }

    private static NativeRecord delete(long tid, long position) {
        byte[] payload = new byte[40];
        NativeRecordHeader header = new NativeRecordHeader(
                backup(), 4096 + position * 64, position, tid,
                NativeRecordReader.OUTER_UNIFIED_TABLE, payload.length, 128,
                999, container(999), NativeRecordReader.UT_DELETE, 42);
        return new NativeRecord(header, payload);
    }

    private static NativeRecord partialRollback(
            long tid, long position, long rollbackStart) {
        ByteBuffer value = ByteBuffer.allocate(48).order(ByteOrder.LITTLE_ENDIAN);
        if (rollbackStart == 0) {
            value.putLong(-1).putLong(-1);
        }
        else {
            value.putLong(0x600000000000L).putLong(0x600000000000L);
        }
        value.putLong(0).putLong(rollbackStart).putLong(2)
                .putLong(rollbackStart == 0 ? -1 : 0x600000000000L);
        NativeRecordHeader header = new NativeRecordHeader(
                backup(), 4096 + position * 64, position, tid,
                NativeRecordReader.OUTER_PARTIAL_ROLLBACK, value.array().length, 128,
                -1, -1, -1, -1);
        return new NativeRecord(header, value.array());
    }

    private static BackupFile backup() {
        return new BackupFile(Path.of("log_backup_2_0_0_100.1"), 2, 0, 0, 100, 1);
    }

    private static byte[] payload() {
        byte[] payload = new byte[139];
        payload[48] = (byte) 0x81;
        payload[50] = 4;
        payload[51] = 1;
        payload[52] = 1;
        ByteBuffer bytes = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        for (int index = 0; index < 4; index++) {
            bytes.putInt(53 + index * 4, 201 + index);
        }
        byte[] descriptor = {
                0x00, (byte) 0xc1, 0x60, 0x04, 0x01, 0x00, 0x27,
                (byte) 0xf0, 0x20, 0x59, 0x04,
                (byte) 0xf0, 0x20, 0x59, 0x0c,
                0x49, 0x20, 0x54, 0x14,
                0x53, 0x22, 0x30, 0x18
        };
        System.arraycopy(descriptor, 0, payload, 69, descriptor.length);
        bytes.putLong(96, 7);
        bytes.putLong(104, 8);
        payload[112] = 0;
        payload[116] = 5;
        System.arraycopy("hello".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                0, payload, 117, 5);
        return payload;
    }

    private static TableDecodePlan plan() {
        return TableDecodePlan.compile(new TableMetadata(
                "CDC", "SIMPLE_ROW", "COLUMN", 999, container(999),
                List.of(
                        column(1, 201, "ID", "BIGINT", Types.BIGINT),
                        column(2, 202, "ROW_VERSION", "BIGINT", Types.BIGINT),
                        column(3, 203, "IS_DELETED", "BOOLEAN", Types.BOOLEAN),
                        column(4, 204, "VALUE_TEXT", "NVARCHAR", Types.NVARCHAR)),
                List.of(), List.of("ID")));
    }

    private static long container(long poid) {
        return 0xfe00000000L + poid;
    }

    private static TableMetadata.Column column(
            int ordinal, long internalId, String name, String type, int dataTypeId) {
        return new TableMetadata.Column(ordinal, internalId, name, type, dataTypeId,
                64, 34, 0, false, false, false, "");
    }
}

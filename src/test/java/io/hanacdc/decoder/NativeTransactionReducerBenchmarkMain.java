package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeTransactionReducer.KeyPart;
import io.hanacdc.decoder.NativeTransactionReducer.RowKey;
import io.hanacdc.decoder.NativeTransactionReducer.Snapshot;
import io.hanacdc.decoder.NativeTransactionReducer.TombstoneState;
import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;

import java.nio.file.Path;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Deterministic delete-heavy reducer benchmark; not part of the unit-test suite. */
public final class NativeTransactionReducerBenchmarkMain {
    private static final long CONTAINER = 0xfe000001L;

    private NativeTransactionReducerBenchmarkMain() {}

    public static void main(String[] args) throws Exception {
        int stateSize = args.length > 0 ? Integer.parseInt(args[0]) : 50_000;
        int cleanups = args.length > 1 ? Integer.parseInt(args[1]) : 1_000;
        if (stateSize <= 0 || cleanups <= 0 || cleanups > stateSize) {
            throw new IllegalArgumentException("invalid state/cleanup count");
        }
        TableDecodePlan plan = plan();
        String tableIdentity = plan.metadata().qualifiedName()
                + "#" + plan.schemaFingerprint();
        List<TombstoneState> tombstones = new ArrayList<>(stateSize);
        List<RowKey> deleted = new ArrayList<>(stateSize);
        for (int index = 0; index < stateSize; index++) {
            RowKey key = new RowKey(tableIdentity, List.of(new KeyPart(
                    "ID", "BIGINT", "java.lang.Long:" + index)));
            tombstones.add(new TombstoneState(tableIdentity, CONTAINER, index, key));
            deleted.add(key);
        }
        NativeTransactionReducer reducer = new NativeTransactionReducer(
                stateSize + 1, stateSize + 1);
        reducer.restore(new Snapshot(Snapshot.CURRENT_VERSION, tombstones, deleted));

        var backup = new ParallelLogDecoder.BackupFile(
                Path.of("delete-heavy-fixture"), 2, 0, 0, 1_000_000, 1);
        long started = System.nanoTime();
        for (int index = 0; index < cleanups; index++) {
            long tid = index + 1L;
            long terminal = 100_000L + index;
            var transaction = new NativeRowLobPipeline.CompletedTransaction(
                    tid,
                    new NativeRowLobPipeline.TerminalRecord(
                            backup, terminal, 4096L + index * 64L, terminal + 1),
                    NativeRowLobPipeline.Outcome.COMMIT,
                    List.of(),
                    List.of(new NativeRowLobPipeline.PhysicalDelete(
                            plan, CONTAINER, index, terminal - 1)));
            var result = reducer.reduce(transaction).orElseThrow();
            if (result.cleanedPhysicalDeletes() != 1) {
                throw new IllegalStateException("cleanup was not applied");
            }
        }
        long elapsed = System.nanoTime() - started;
        if (reducer.pendingTombstones() != stateSize - cleanups
                || reducer.deletedKeys() != stateSize) {
            throw new IllegalStateException("unexpected final reducer state");
        }
        System.out.printf(Locale.ROOT,
                "{\"kind\":\"delete_heavy_reducer_benchmark\","
                        + "\"state_size\":%d,\"cleanups\":%d,"
                        + "\"elapsed_seconds\":%.9f,\"cleanups_per_second\":%.3f,"
                        + "\"pending_tombstones\":%d,\"deleted_keys\":%d}%n",
                stateSize, cleanups, elapsed / 1_000_000_000.0,
                cleanups * 1_000_000_000.0 / elapsed,
                reducer.pendingTombstones(), reducer.deletedKeys());
    }

    private static TableDecodePlan plan() {
        return TableDecodePlan.compile(new TableMetadata(
                "CDC", "DELETE_HEAVY", "COLUMN", 999, CONTAINER,
                List.of(
                        new TableMetadata.Column(
                                1, 201, "ID", "BIGINT", Types.BIGINT,
                                64, 34, 0, false, false, false, ""),
                        new TableMetadata.Column(
                                2, 202, "IS_DELETED", "BOOLEAN", Types.BOOLEAN,
                                1, 0, 0, false, false, false, "")),
                List.of(), List.of("ID")));
    }
}

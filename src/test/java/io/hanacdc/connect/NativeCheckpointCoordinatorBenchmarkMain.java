package io.hanacdc.connect;

import io.hanacdc.decoder.NativeRowLobPipeline;
import io.hanacdc.decoder.NativeTransactionReducer;
import io.hanacdc.decoder.ParallelLogDecoder;
import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Delete-heavy durable-checkpoint benchmark; not part of the unit-test suite. */
public final class NativeCheckpointCoordinatorBenchmarkMain {
    private static final long CONTAINER = 0xfe000001L;

    private NativeCheckpointCoordinatorBenchmarkMain() {}

    public static void main(String[] args) throws Exception {
        int stateSize = args.length > 0 ? Integer.parseInt(args[0]) : 10_000;
        int cleanups = args.length > 1 ? Integer.parseInt(args[1]) : 100;
        Path directory = args.length > 2
                ? Path.of(args[2]) : Files.createTempDirectory("hana-cdc-checkpoint-bench-");
        if (stateSize <= 0 || cleanups <= 0 || cleanups > stateSize) {
            throw new IllegalArgumentException("invalid state/cleanup count");
        }
        TableDecodePlan plan = plan();
        String tableIdentity = plan.metadata().qualifiedName()
                + "#" + plan.schemaFingerprint();
        List<NativeTransactionReducer.TombstoneState> tombstones =
                new ArrayList<>(stateSize);
        List<NativeTransactionReducer.RowKey> deleted = new ArrayList<>(stateSize);
        for (int index = 0; index < stateSize; index++) {
            var key = new NativeTransactionReducer.RowKey(tableIdentity,
                    List.of(new NativeTransactionReducer.KeyPart(
                            "ID", "BIGINT", "java.lang.Long:" + index)));
            tombstones.add(new NativeTransactionReducer.TombstoneState(
                    tableIdentity, CONTAINER, index, key));
            deleted.add(key);
        }
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "DELETE_HEAVY", 2, 0, 0);
        ReplayBoundary initial = new ReplayBoundary(99, 0, 1_000_000, 1, 1);
        DurableReducerStateStore store = new DurableReducerStateStore(directory);
        var checkpoint = store.write(epoch, 1, initial,
                new NativeTransactionReducer.Snapshot(
                        NativeTransactionReducer.Snapshot.CURRENT_VERSION,
                        tombstones, deleted));
        NativeCheckpointCoordinator coordinator = NativeCheckpointCoordinator.restore(
                epoch, store, checkpoint.generation(), checkpoint.sha256());
        var backup = new ParallelLogDecoder.BackupFile(
                Path.of("delete-heavy-fixture"), 2, 0, 0, 1_000_000, 99);

        long started = System.nanoTime();
        for (int index = 0; index < cleanups; index++) {
            long terminalPosition = 1_000L + index;
            var transaction = new NativeRowLobPipeline.CompletedTransaction(
                    index + 1L,
                    new NativeRowLobPipeline.TerminalRecord(
                            backup, terminalPosition, 4096L + index * 64L,
                            terminalPosition + 1),
                    NativeRowLobPipeline.Outcome.COMMIT,
                    List.of(),
                    List.of(new NativeRowLobPipeline.PhysicalDelete(
                            plan, CONTAINER, index, terminalPosition - 1)));
            coordinator.process(transaction,
                    new ReplayBoundary(99, 0, 1_000_000,
                            terminalPosition, terminalPosition));
        }
        long elapsed = System.nanoTime() - started;
        long restoreStarted = System.nanoTime();
        NativeCheckpointCoordinator restored = NativeCheckpointCoordinator.restore(
                epoch, store, coordinator.currentCheckpoint().generation(),
                coordinator.currentCheckpoint().sha256());
        long restoreElapsed = System.nanoTime() - restoreStarted;
        long files;
        long bytes;
        try (var paths = Files.list(directory)) {
            List<Path> stateFiles = paths.filter(Files::isRegularFile).toList();
            files = stateFiles.size();
            bytes = 0;
            for (Path stateFile : stateFiles) bytes = Math.addExact(bytes, Files.size(stateFile));
        }
        var snapshot = coordinator.snapshot();
        if (snapshot.tombstones().size() != stateSize - cleanups
                || snapshot.deletedKeys().size() != stateSize) {
            throw new IllegalStateException("unexpected final checkpoint state");
        }
        if (!snapshot.equals(restored.snapshot())) {
            throw new IllegalStateException("restored checkpoint state differs");
        }
        System.out.printf(Locale.ROOT,
                "{\"kind\":\"delete_heavy_checkpoint_benchmark\","
                        + "\"state_size\":%d,\"cleanups\":%d,"
                        + "\"elapsed_seconds\":%.9f,\"cleanups_per_second\":%.3f,"
                        + "\"restore_seconds\":%.9f,"
                        + "\"state_files\":%d,\"state_bytes\":%d}%n",
                stateSize, cleanups, elapsed / 1_000_000_000.0,
                cleanups * 1_000_000_000.0 / elapsed,
                restoreElapsed / 1_000_000_000.0, files, bytes);
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

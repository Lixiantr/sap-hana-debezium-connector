package io.hanacdc.connect;

import io.hanacdc.decoder.NativeTransactionReducer;
import io.hanacdc.decoder.NativeTransactionReducer.KeyPart;
import io.hanacdc.decoder.NativeTransactionReducer.RowKey;
import io.hanacdc.decoder.NativeTransactionReducer.Snapshot;
import io.hanacdc.decoder.NativeTransactionReducer.TombstoneState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

final class DurableReducerStateStoreTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void atomicallyRoundTripsAndRestoresReducerState() throws Exception {
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        RowKey key = key();
        Snapshot snapshot = new Snapshot(Snapshot.CURRENT_VERSION,
                List.of(new TombstoneState(key.tableIdentity(), 0xfe000001L, 47, key)),
                List.of(key));
        DurableReducerStateStore store = new DurableReducerStateStore(temporaryDirectory);

        ReplayBoundary boundary = new ReplayBoundary(99, 100, 110, 104, 106);
        DurableReducerStateStore.Checkpoint checkpoint = store.write(
                epoch, 1, boundary, snapshot);
        DurableReducerStateStore.Description description = store.describe(
                epoch, 1, boundary, snapshot);
        assertTrue(Files.isRegularFile(checkpoint.path()));
        assertEquals(checkpoint.sha256(), description.sha256());
        assertEquals(checkpoint.path(), description.path());
        assertEquals(Files.size(checkpoint.path()), description.sizeBytes());
        assertEquals(List.of(store.stateFile(checkpoint)), store.inventoryAfter(0));
        assertTrue(store.inventoryAfter(1).isEmpty());
        assertEquals(snapshot, store.read(epoch, checkpoint).snapshot());
        assertEquals(boundary, store.read(epoch, checkpoint).boundary());

        NativeTransactionReducer restored = new NativeTransactionReducer();
        restored.restore(store.read(epoch, checkpoint).snapshot());
        assertEquals(1, restored.pendingTombstones());
        assertEquals(1, restored.deletedKeys());

        SourceOffset offset = new SourceOffset(SourceOffset.CURRENT_VERSION, epoch.id(),
                99, 100, 110, 106, 4096, 7,
                123, 0, 1, 100,
                checkpoint.generation(), checkpoint.sha256());
        assertEquals(checkpoint.sha256(), SourceOffset.fromMap(offset.toMap()).stateSha256());
    }

    @Test
    void deltaCheckpointRestoresThroughParentAndPruningKeepsRequiredChain()
            throws Exception {
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        RowKey key = key();
        TombstoneState tombstone = new TombstoneState(
                key.tableIdentity(), 0xfe000001L, 47, key);
        Snapshot full = new Snapshot(Snapshot.CURRENT_VERSION,
                List.of(tombstone), List.of(key));
        DurableReducerStateStore store = new DurableReducerStateStore(temporaryDirectory);
        ReplayBoundary firstBoundary = new ReplayBoundary(99, 100, 1000, 104, 106);
        var first = store.write(epoch, 1, firstBoundary, full);
        var delta = new NativeTransactionReducer.StateDelta(
                List.of(tombstone), List.of(), List.of());
        ReplayBoundary secondBoundary = new ReplayBoundary(99, 100, 1000, 108, 110);
        var second = store.writeDelta(epoch, 2, secondBoundary, first, delta);

        assertEquals(store.describeDelta(epoch, 2, secondBoundary, first, delta).sha256(),
                second.sha256());
        assertTrue(store.read(epoch, second).snapshot().tombstones().isEmpty());
        assertEquals(List.of(key), store.read(epoch, second).snapshot().deletedKeys());
        assertEquals(secondBoundary, store.read(epoch, second).boundary());

        assertEquals(0, store.pruneBefore(second));
        assertTrue(Files.isRegularFile(first.path()));
        Snapshot compacted = store.read(epoch, second).snapshot();
        var third = store.write(epoch, 3,
                new ReplayBoundary(99, 100, 1000, 112, 114), compacted);
        assertEquals(2, store.pruneBefore(third));
        assertFalse(Files.exists(first.path()));
        assertFalse(Files.exists(second.path()));
        assertEquals(compacted, store.read(epoch, third).snapshot());
    }

    @Test
    void restoresLegacyVersionThreeFullCheckpoint() throws Exception {
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        RowKey key = key();
        Snapshot snapshot = new Snapshot(Snapshot.CURRENT_VERSION,
                List.of(new TombstoneState(
                        key.tableIdentity(), 0xfe000001L, 47, key)),
                List.of(key));
        ReplayBoundary boundary = new ReplayBoundary(99, 100, 110, 104, 106);
        byte[] encoded = legacyV3(epoch, 7, boundary, snapshot);
        String sha256 = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(encoded));
        Path path = temporaryDirectory.resolve(
                "state-7-" + sha256 + ".bin").toAbsolutePath().normalize();
        Files.write(path, encoded);
        DurableReducerStateStore store = new DurableReducerStateStore(temporaryDirectory);

        var restored = store.read(epoch, store.reference(7, sha256));

        assertEquals(snapshot, restored.snapshot());
        assertEquals(boundary, restored.boundary());
    }

    @Test
    void rejectsWrongEpochAndCorruptedBytes() throws Exception {
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        DurableReducerStateStore store = new DurableReducerStateStore(temporaryDirectory);
        DurableReducerStateStore.Checkpoint checkpoint = store.write(epoch, 3,
                new ReplayBoundary(99, 100, 110, 104, 106),
                new Snapshot(Snapshot.CURRENT_VERSION, List.of(), List.of(key())));

        SourceEpoch other = SourceEpoch.prototype("hana01", "OTHER", 2, 0, 0);
        assertThrows(java.io.IOException.class, () -> store.read(other, checkpoint));

        byte[] bytes = Files.readAllBytes(checkpoint.path());
        bytes[bytes.length - 1] ^= 1;
        Files.write(checkpoint.path(), bytes);
        java.io.IOException corrupted = assertThrows(java.io.IOException.class,
                () -> store.read(epoch, checkpoint));
        assertTrue(corrupted.getMessage().contains("CHECKSUM")
                || corrupted.getMessage().contains("CONTENT"));
    }

    @Test
    void classifiesMissingCheckpointObjectAsPermanentStateFailure() throws Exception {
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        DurableReducerStateStore store = new DurableReducerStateStore(temporaryDirectory);
        DurableReducerStateStore.Checkpoint missing = store.reference(34, "a".repeat(64));

        java.io.IOException error = assertThrows(java.io.IOException.class,
                () -> store.read(epoch, missing));

        assertTrue(error.getMessage().contains("FATAL_STATE_OBJECT_MISSING"));
        assertTrue(error.getCause() instanceof java.nio.file.NoSuchFileException);
    }

    @Test
    void reducerRejectsInconsistentOrDuplicateRestoredState() {
        RowKey key = key();
        Snapshot missingDeletedMarker = new Snapshot(Snapshot.CURRENT_VERSION,
                List.of(new TombstoneState(key.tableIdentity(), 0xfe000001L, 47, key)), List.of());
        assertThrows(java.io.IOException.class,
                () -> new NativeTransactionReducer().restore(missingDeletedMarker));

        Snapshot duplicateLocator = new Snapshot(Snapshot.CURRENT_VERSION,
                List.of(new TombstoneState(key.tableIdentity(), 0xfe000001L, 47, key),
                        new TombstoneState(key.tableIdentity(), 0xfe000001L, 47, key)), List.of(key));
        assertThrows(java.io.IOException.class,
                () -> new NativeTransactionReducer().restore(duplicateLocator));
    }

    @Test
    void prunesOnlyOlderCheckpointFilesAndIsIdempotent() throws Exception {
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        DurableReducerStateStore store = new DurableReducerStateStore(temporaryDirectory);
        Snapshot snapshot = new Snapshot(Snapshot.CURRENT_VERSION, List.of(), List.of());
        ReplayBoundary boundary = new ReplayBoundary(99, 100, 110, 104, 106);
        DurableReducerStateStore.Checkpoint first = store.write(epoch, 1, boundary, snapshot);
        DurableReducerStateStore.Checkpoint retained = store.write(epoch, 2, boundary, snapshot);
        DurableReducerStateStore.Checkpoint newer = store.write(epoch, 3, boundary, snapshot);
        Path unrelated = temporaryDirectory.resolve("operator-note.txt");
        Files.writeString(unrelated, "keep");

        assertEquals(1, store.pruneBefore(retained));
        assertFalse(Files.exists(first.path()));
        assertTrue(Files.isRegularFile(retained.path()));
        assertTrue(Files.isRegularFile(newer.path()));
        assertTrue(Files.isRegularFile(unrelated));
        assertEquals(0, store.pruneBefore(retained));
    }

    @Test
    void refusesToPruneWhenRetainedCheckpointIsMissing() throws Exception {
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        DurableReducerStateStore store = new DurableReducerStateStore(temporaryDirectory);
        Snapshot snapshot = new Snapshot(Snapshot.CURRENT_VERSION, List.of(), List.of());
        ReplayBoundary boundary = new ReplayBoundary(99, 100, 110, 104, 106);
        DurableReducerStateStore.Checkpoint first = store.write(epoch, 1, boundary, snapshot);
        DurableReducerStateStore.Checkpoint missing = store.reference(2, "0".repeat(64));

        java.io.IOException error = assertThrows(java.io.IOException.class,
                () -> store.pruneBefore(missing));
        assertTrue(error.getMessage().contains("RETAINED_MISSING"));
        assertTrue(Files.isRegularFile(first.path()));
    }

    private static RowKey key() {
        return new RowKey("CDC.FULL_ROW#fingerprint", List.of(
                new KeyPart("ID", "BIGINT", "java.lang.Long:7")));
    }

    private static byte[] legacyV3(
            SourceEpoch epoch, long generation,
            ReplayBoundary boundary, Snapshot snapshot) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(0x48434453);
            output.writeInt(3);
            writeLegacyString(output, epoch.id());
            output.writeLong(generation);
            output.writeLong(boundary.backupId());
            output.writeLong(boundary.fileFirst());
            output.writeLong(boundary.fileLast());
            output.writeLong(boundary.replayPosition());
            output.writeLong(boundary.committedPosition());
            output.writeInt(snapshot.version());
            output.writeInt(snapshot.tombstones().size());
            for (TombstoneState tombstone : snapshot.tombstones()) {
                writeLegacyString(output, tombstone.tableIdentity());
                output.writeLong(tombstone.containerId());
                output.writeLong(tombstone.rowLocator());
                writeLegacyKey(output, tombstone.key());
            }
            output.writeInt(snapshot.deletedKeys().size());
            for (RowKey rowKey : snapshot.deletedKeys()) writeLegacyKey(output, rowKey);
        }
        return bytes.toByteArray();
    }

    private static void writeLegacyKey(DataOutputStream output, RowKey key)
            throws Exception {
        writeLegacyString(output, key.tableIdentity());
        output.writeInt(key.parts().size());
        for (KeyPart part : key.parts()) {
            writeLegacyString(output, part.columnName());
            writeLegacyString(output, part.typeName());
            writeLegacyString(output, part.canonicalValue());
        }
    }

    private static void writeLegacyString(DataOutputStream output, String value)
            throws Exception {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
    }
}

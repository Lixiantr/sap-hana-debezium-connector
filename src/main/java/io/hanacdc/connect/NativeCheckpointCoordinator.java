package io.hanacdc.connect;

import io.hanacdc.decoder.NativeRowLobPipeline;
import io.hanacdc.decoder.NativeTransactionReducer;
import io.hanacdc.decoder.NativeTransactionReducer.LogicalChange;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

/** Coordinates reducer mutation, immutable state publication and per-change offsets. */
public final class NativeCheckpointCoordinator {
    private static final int FULL_SNAPSHOT_INTERVAL = 256;
    public record Publication(
            LogicalChange change,
            NativeRowLobPipeline.TerminalRecord terminal,
            long transactionId,
            int changeIndex,
            int changeCount,
            DurableReducerStateStore.Checkpoint checkpoint,
            ReplayBoundary stateBoundary) {
        public Publication {
            Objects.requireNonNull(change, "change");
            Objects.requireNonNull(terminal, "terminal");
            Objects.requireNonNull(checkpoint, "checkpoint");
            Objects.requireNonNull(stateBoundary, "stateBoundary");
            if (transactionId <= 0 || changeIndex < 0 || changeCount <= 0
                    || changeIndex >= changeCount) {
                throw new IllegalArgumentException("invalid native publication ordinal");
            }
        }

        public Map<String, Object> sourceOffset(SourceEpoch epoch) {
            return SourceOffset.withDurableState(epoch,
                    terminal.backup().backupId(), terminal.backup().first(),
                    terminal.backup().last(), terminal.logPosition(), terminal.fileOffset(),
                    terminal.logPosition(), transactionId, changeIndex, changeCount,
                    stateBoundary, checkpoint).toMap();
        }
    }

    public record ProcessedTransaction(
            DurableReducerStateStore.Checkpoint preCheckpoint,
            DurableReducerStateStore.Checkpoint postCheckpoint,
            List<Publication> publications) {
        public ProcessedTransaction {
            Objects.requireNonNull(preCheckpoint, "preCheckpoint");
            Objects.requireNonNull(postCheckpoint, "postCheckpoint");
            publications = List.copyOf(publications);
        }
    }

    private final SourceEpoch epoch;
    private final DurableReducerStateStore store;
    private final NativeTransactionReducer reducer;
    private final Object stateMutex = new Object();
    private DurableReducerStateStore.Checkpoint currentCheckpoint;
    private ReplayBoundary currentBoundary;
    private final int maxUncommittedGenerations;
    private final long maxUncommittedBytes;
    private final NavigableMap<Long, DurableReducerStateStore.StateFile>
            uncommittedState = new TreeMap<>();
    private long confirmedGeneration;
    private long uncommittedBytes;

    private NativeCheckpointCoordinator(
            SourceEpoch epoch,
            DurableReducerStateStore store,
            NativeTransactionReducer reducer,
            DurableReducerStateStore.Checkpoint currentCheckpoint,
            ReplayBoundary currentBoundary,
            int maxUncommittedGenerations,
            long maxUncommittedBytes,
            long confirmedGeneration,
            List<DurableReducerStateStore.StateFile> uncommittedFiles)
            throws IOException {
        this.epoch = Objects.requireNonNull(epoch, "epoch");
        this.store = Objects.requireNonNull(store, "store");
        this.reducer = Objects.requireNonNull(reducer, "reducer");
        this.currentCheckpoint = Objects.requireNonNull(currentCheckpoint,
                "currentCheckpoint");
        this.currentBoundary = Objects.requireNonNull(currentBoundary, "currentBoundary");
        if (maxUncommittedGenerations <= 0 || maxUncommittedBytes <= 0
                || confirmedGeneration < 0
                || confirmedGeneration > currentCheckpoint.generation()) {
            throw new IllegalArgumentException("invalid durable state resource limit");
        }
        this.maxUncommittedGenerations = maxUncommittedGenerations;
        this.maxUncommittedBytes = maxUncommittedBytes;
        this.confirmedGeneration = confirmedGeneration;
        for (DurableReducerStateStore.StateFile file
                : Objects.requireNonNull(uncommittedFiles, "uncommittedFiles")) {
            if (file.generation() <= confirmedGeneration) {
                throw new IOException("FATAL_STATE_INVENTORY_CONFIRMED_COLLISION: "
                        + file.path());
            }
            DurableReducerStateStore.StateFile previous = uncommittedState.putIfAbsent(
                    file.generation(), file);
            if (previous != null && !previous.path().equals(file.path())) {
                throw new IOException("FATAL_STATE_GENERATION_COLLISION: generation="
                        + file.generation());
            }
            if (previous == null) {
                try {
                    uncommittedBytes = Math.addExact(
                            uncommittedBytes, file.sizeBytes());
                }
                catch (ArithmeticException error) {
                    throw byteLimit(Long.MAX_VALUE, uncommittedBytes,
                            maxUncommittedBytes);
                }
            }
        }
        if (!uncommittedState.isEmpty()) {
            requireGenerationCapacity(uncommittedState.lastKey());
        }
        requireByteCapacity(0);
    }

    public static NativeCheckpointCoordinator initialize(
            SourceEpoch epoch,
            DurableReducerStateStore store,
            ReplayBoundary initialBoundary) throws IOException {
        return initialize(epoch, store, initialBoundary,
                Integer.MAX_VALUE, Long.MAX_VALUE);
    }

    public static NativeCheckpointCoordinator initialize(
            SourceEpoch epoch,
            DurableReducerStateStore store,
            ReplayBoundary initialBoundary,
            int maxUncommittedGenerations) throws IOException {
        return initialize(epoch, store, initialBoundary,
                maxUncommittedGenerations, Long.MAX_VALUE);
    }

    public static NativeCheckpointCoordinator initialize(
            SourceEpoch epoch,
            DurableReducerStateStore store,
            ReplayBoundary initialBoundary,
            int maxUncommittedGenerations,
            long maxUncommittedBytes) throws IOException {
        NativeTransactionReducer reducer = new NativeTransactionReducer();
        DurableReducerStateStore.Description description = store.describe(
                epoch, 1, initialBoundary, reducer.snapshot());
        if (description.sizeBytes() > maxUncommittedBytes) {
            throw byteLimit(description.sizeBytes(), 0, maxUncommittedBytes);
        }
        DurableReducerStateStore.Checkpoint checkpoint = store.write(
                epoch, 1, initialBoundary, reducer.snapshot());
        DurableReducerStateStore.StateFile stateFile = store.stateFile(checkpoint);
        return new NativeCheckpointCoordinator(
                epoch, store, reducer, checkpoint, initialBoundary,
                maxUncommittedGenerations, maxUncommittedBytes, 0,
                List.of(stateFile));
    }

    public static NativeCheckpointCoordinator restore(
            SourceEpoch epoch,
            DurableReducerStateStore store,
            long generation,
            String sha256) throws IOException {
        return restore(epoch, store, generation, sha256,
                Integer.MAX_VALUE, Long.MAX_VALUE);
    }

    public static NativeCheckpointCoordinator restore(
            SourceEpoch epoch,
            DurableReducerStateStore store,
            long generation,
            String sha256,
            int maxUncommittedGenerations) throws IOException {
        return restore(epoch, store, generation, sha256,
                maxUncommittedGenerations, Long.MAX_VALUE);
    }

    public static NativeCheckpointCoordinator restore(
            SourceEpoch epoch,
            DurableReducerStateStore store,
            long generation,
            String sha256,
            int maxUncommittedGenerations,
            long maxUncommittedBytes) throws IOException {
        DurableReducerStateStore.Checkpoint reference = store.reference(generation, sha256);
        DurableReducerStateStore.RestoredCheckpoint restored = store.read(epoch, reference);
        NativeTransactionReducer reducer = new NativeTransactionReducer();
        reducer.restore(restored.snapshot());
        store.pruneBefore(reference);
        List<DurableReducerStateStore.StateFile> uncommitted =
                store.inventoryAfter(generation);
        return new NativeCheckpointCoordinator(
                epoch, store, reducer, reference, restored.boundary(),
                maxUncommittedGenerations, maxUncommittedBytes, generation,
                uncommitted);
    }

    /**
     * The post checkpoint is published before any SourceRecord can be returned.
     * Intermediate changes reference pre-state; only the final change references post-state.
     */
    public ProcessedTransaction process(
            NativeRowLobPipeline.CompletedTransaction transaction,
            ReplayBoundary postBoundary) throws IOException {
        synchronized (stateMutex) {
            return processLocked(transaction, postBoundary);
        }
    }

    private ProcessedTransaction processLocked(
            NativeRowLobPipeline.CompletedTransaction transaction,
            ReplayBoundary postBoundary) throws IOException {
        Objects.requireNonNull(transaction, "transaction");
        Objects.requireNonNull(postBoundary, "postBoundary");
        if (postBoundary.committedPosition() != transaction.terminalPosition()) {
            throw new IllegalArgumentException("post boundary must end at transaction terminal");
        }
        DurableReducerStateStore.Checkpoint pre = currentCheckpoint;
        List<LogicalChange> changes;
        NativeTransactionReducer.StateDelta stateDelta =
                new NativeTransactionReducer.StateDelta(List.of(), List.of(), List.of());
        DurableReducerStateStore.Checkpoint post;
        boolean reducerApplied = false;
        try {
            var reduced = reducer.reduce(transaction);
            changes = reduced.map(NativeTransactionReducer.CommittedBatch::changes)
                    .orElseGet(List::of);
            stateDelta = reduced.map(NativeTransactionReducer.CommittedBatch::stateDelta)
                    .orElse(stateDelta);
            reducerApplied = true;
            if (stateDelta.isEmpty() && sameBackup(currentBoundary, postBoundary)) {
                post = pre;
            }
            else {
                long generation = Math.addExact(pre.generation(), 1);
                requireGenerationCapacity(generation);
                post = fullSnapshotRequired(generation)
                        ? persist(generation, postBoundary, reducer.snapshot())
                        : persistDelta(generation, postBoundary, pre, stateDelta);
            }
        }
        catch (IOException | RuntimeException error) {
            if (reducerApplied) {
                try {
                    reducer.revert(stateDelta);
                }
                catch (IOException restoreError) {
                    error.addSuppressed(restoreError);
                }
            }
            throw error;
        }

        List<Publication> publications = new ArrayList<>(changes.size());
        for (int index = 0; index < changes.size(); index++) {
            DurableReducerStateStore.Checkpoint referenced = index == changes.size() - 1
                    ? post : pre;
            ReplayBoundary referencedBoundary = index == changes.size() - 1
                    ? (post.equals(pre) ? currentBoundary : postBoundary)
                    : currentBoundary;
            publications.add(new Publication(changes.get(index), transaction.terminal(),
                    transaction.tid(), index, changes.size(), referenced,
                    referencedBoundary));
        }
        if (!post.equals(pre)) {
            currentCheckpoint = post;
            currentBoundary = postBoundary;
        }
        return new ProcessedTransaction(pre, post, publications);
    }

    /** Advances an unchanged reducer snapshot to a globally observed terminal record. */
    public DurableReducerStateStore.Checkpoint advanceProgress(
            NativeRowLobPipeline.CompletedTransaction transaction,
            ReplayBoundary postBoundary) throws IOException {
        synchronized (stateMutex) {
            return advanceProgressLocked(transaction, postBoundary);
        }
    }

    private DurableReducerStateStore.Checkpoint advanceProgressLocked(
            NativeRowLobPipeline.CompletedTransaction transaction,
            ReplayBoundary postBoundary) throws IOException {
        Objects.requireNonNull(transaction, "transaction");
        Objects.requireNonNull(postBoundary, "postBoundary");
        if (!transaction.rows().isEmpty() || !transaction.physicalDeletes().isEmpty()) {
            throw new IllegalArgumentException("progress transaction contains target effects");
        }
        if (postBoundary.committedPosition() != transaction.terminalPosition()) {
            throw new IllegalArgumentException("post boundary must end at transaction terminal");
        }
        if (sameBackup(currentBoundary, postBoundary)) {
            return currentCheckpoint;
        }
        long generation = Math.addExact(currentCheckpoint.generation(), 1);
        requireGenerationCapacity(generation);
        DurableReducerStateStore.Checkpoint post = fullSnapshotRequired(generation)
                ? persist(generation, postBoundary, reducer.snapshot())
                : persistDelta(generation, postBoundary, currentCheckpoint,
                        new NativeTransactionReducer.StateDelta(
                                List.of(), List.of(), List.of()));
        currentCheckpoint = post;
        currentBoundary = postBoundary;
        return post;
    }

    private static boolean sameBackup(ReplayBoundary left, ReplayBoundary right) {
        return left.backupId() == right.backupId()
                && left.fileFirst() == right.fileFirst()
                && left.fileLast() == right.fileLast();
    }

    public DurableReducerStateStore.Checkpoint currentCheckpoint() {
        synchronized (stateMutex) {
            return currentCheckpoint;
        }
    }

    public ReplayBoundary currentBoundary() {
        synchronized (stateMutex) {
            return currentBoundary;
        }
    }

    public NativeTransactionReducer.Snapshot snapshot() {
        synchronized (stateMutex) {
            return reducer.snapshot();
        }
    }

    /** Prunes only state made obsolete by an offset already persisted by Connect. */
    public int pruneConfirmed(SourceOffset confirmed) throws IOException {
        synchronized (stateMutex) {
            return pruneConfirmedLocked(confirmed);
        }
    }

    private int pruneConfirmedLocked(SourceOffset confirmed) throws IOException {
        Objects.requireNonNull(confirmed, "confirmed");
        confirmed.requireEpoch(epoch);
        if (confirmed.stateGeneration() <= 0
                || SourceOffset.NO_STATE_SHA256.equals(confirmed.stateSha256())) {
            throw new IOException("FATAL_STATE_PRUNE_OFFSET_WITHOUT_CHECKPOINT");
        }
        if (confirmed.stateGeneration() < confirmedGeneration) {
            return 0;
        }
        if (confirmed.stateGeneration() > currentCheckpoint.generation()) {
            throw new IOException("FATAL_STATE_PRUNE_OFFSET_AHEAD: confirmed="
                    + confirmed.stateGeneration() + " current="
                    + currentCheckpoint.generation());
        }
        int deleted = store.pruneBefore(store.reference(
                confirmed.stateGeneration(), confirmed.stateSha256()));
        var confirmedFiles = uncommittedState.headMap(
                confirmed.stateGeneration(), true);
        long released = 0;
        for (DurableReducerStateStore.StateFile file : confirmedFiles.values()) {
            released = Math.addExact(released, file.sizeBytes());
        }
        confirmedFiles.clear();
        uncommittedBytes = Math.subtractExact(uncommittedBytes, released);
        confirmedGeneration = confirmed.stateGeneration();
        return deleted;
    }

    private void requireGenerationCapacity(long nextGeneration) throws IOException {
        long uncommitted = Math.subtractExact(nextGeneration, confirmedGeneration);
        if (uncommitted > maxUncommittedGenerations) {
            throw new IOException("FATAL_RESOURCE_LIMIT: durable state uncommitted "
                    + "generations limit exceeded: next=" + nextGeneration
                    + " confirmed=" + confirmedGeneration
                    + " limit=" + maxUncommittedGenerations);
        }
    }

    private DurableReducerStateStore.Checkpoint persist(
            long generation,
            ReplayBoundary boundary,
            NativeTransactionReducer.Snapshot snapshot) throws IOException {
        DurableReducerStateStore.Description description = store.describe(
                epoch, generation, boundary, snapshot);
        DurableReducerStateStore.StateFile existing = uncommittedState.get(generation);
        long additionalBytes = description.sizeBytes();
        if (existing != null) {
            if (!existing.path().equals(description.path())) {
                throw new IOException("FATAL_STATE_GENERATION_COLLISION: generation="
                        + generation);
            }
            additionalBytes = 0;
        }
        requireByteCapacity(additionalBytes);
        DurableReducerStateStore.Checkpoint checkpoint = store.write(
                epoch, generation, boundary, snapshot);
        DurableReducerStateStore.StateFile stateFile = store.stateFile(checkpoint);
        if (!stateFile.path().equals(description.path())
                || stateFile.sizeBytes() != description.sizeBytes()) {
            throw new IOException("FATAL_STATE_DESCRIPTION_MISMATCH: generation="
                    + generation);
        }
        if (existing == null) {
            uncommittedState.put(generation, stateFile);
            uncommittedBytes = Math.addExact(uncommittedBytes, stateFile.sizeBytes());
        }
        return checkpoint;
    }

    private DurableReducerStateStore.Checkpoint persistDelta(
            long generation,
            ReplayBoundary boundary,
            DurableReducerStateStore.Checkpoint parent,
            NativeTransactionReducer.StateDelta delta) throws IOException {
        DurableReducerStateStore.Description description = store.describeDelta(
                epoch, generation, boundary, parent, delta);
        DurableReducerStateStore.StateFile existing = uncommittedState.get(generation);
        long additionalBytes = description.sizeBytes();
        if (existing != null) {
            if (!existing.path().equals(description.path())) {
                throw new IOException("FATAL_STATE_GENERATION_COLLISION: generation="
                        + generation);
            }
            additionalBytes = 0;
        }
        requireByteCapacity(additionalBytes);
        DurableReducerStateStore.Checkpoint checkpoint = store.writeDelta(
                epoch, generation, boundary, parent, delta);
        DurableReducerStateStore.StateFile stateFile = store.stateFile(checkpoint);
        if (!stateFile.path().equals(description.path())
                || stateFile.sizeBytes() != description.sizeBytes()) {
            throw new IOException("FATAL_STATE_DESCRIPTION_MISMATCH: generation="
                    + generation);
        }
        if (existing == null) {
            uncommittedState.put(generation, stateFile);
            uncommittedBytes = Math.addExact(uncommittedBytes, stateFile.sizeBytes());
        }
        return checkpoint;
    }

    private static boolean fullSnapshotRequired(long generation) {
        return generation % FULL_SNAPSHOT_INTERVAL == 0;
    }

    private void requireByteCapacity(long additionalBytes) throws IOException {
        long projected;
        try {
            projected = Math.addExact(uncommittedBytes, additionalBytes);
        }
        catch (ArithmeticException error) {
            throw byteLimit(Long.MAX_VALUE, uncommittedBytes,
                    maxUncommittedBytes);
        }
        if (projected > maxUncommittedBytes) {
            throw byteLimit(projected, uncommittedBytes, maxUncommittedBytes);
        }
    }

    private static IOException byteLimit(
            long projected, long current, long limit) {
        return new IOException("FATAL_RESOURCE_LIMIT: durable state uncommitted bytes "
                + "limit exceeded: projected=" + projected
                + " current=" + current + " limit=" + limit);
    }
}

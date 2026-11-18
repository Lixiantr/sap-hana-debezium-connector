package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeRowDecoder.ColumnValue;
import io.hanacdc.decoder.NativeRowDecoder.DecodedRow;
import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Stateful committed-transaction reducer for the streaming-only DML contract.
 * Durable persistence and replay are intentionally delegated to the restart stage.
 */
public final class NativeTransactionReducer {
    public static final int DEFAULT_MAX_PENDING_TOMBSTONES = 1_000_000;
    public static final int DEFAULT_MAX_DELETED_KEYS = 1_000_000;

    public enum Operation {
        CREATE("c"), UPDATE("u"), DELETE("d");

        private final String debeziumCode;

        Operation(String debeziumCode) {
            this.debeziumCode = debeziumCode;
        }

        public String debeziumCode() {
            return debeziumCode;
        }
    }

    public record KeyPart(String columnName, String typeName, String canonicalValue) {
        public KeyPart {
            columnName = required(columnName, "PK column");
            typeName = required(typeName, "PK type");
            canonicalValue = required(canonicalValue, "PK value");
        }
    }

    public record RowKey(String tableIdentity, List<KeyPart> parts) {
        public RowKey {
            tableIdentity = required(tableIdentity, "table identity");
            parts = List.copyOf(parts);
            if (parts.isEmpty()) {
                throw new IllegalArgumentException("row key has no parts");
            }
        }
    }

    public record LogicalChange(
            RowKey key,
            Operation operation,
            DecodedRow row,
            long finalLogPosition) {
        public LogicalChange {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(operation, "operation");
            Objects.requireNonNull(row, "row");
            if (finalLogPosition != row.logPosition()) {
                throw new IllegalArgumentException("change position must match final row");
            }
        }
    }

    public record CommittedBatch(
            long tid,
            long commitPosition,
            List<LogicalChange> changes,
            int cleanedPhysicalDeletes,
            StateDelta stateDelta) {
        public CommittedBatch {
            changes = List.copyOf(changes);
            Objects.requireNonNull(stateDelta, "stateDelta");
            if (tid <= 0 || commitPosition < 0 || cleanedPhysicalDeletes < 0) {
                throw new IllegalArgumentException("invalid committed batch");
            }
        }
    }

    /** Stable logical state required to correlate cleanup and reject resurrection after restart. */
    public record TombstoneState(
            String tableIdentity, long containerId, long rowLocator, RowKey key) {
        public TombstoneState {
            tableIdentity = required(tableIdentity, "table identity");
            Objects.requireNonNull(key, "key");
            if (containerId <= 0 || rowLocator < 0
                    || !tableIdentity.equals(key.tableIdentity())) {
                throw new IllegalArgumentException("invalid durable tombstone state");
            }
        }
    }

    public record Snapshot(
            int version,
            List<TombstoneState> tombstones,
            List<RowKey> deletedKeys) {
        public static final int CURRENT_VERSION = 2;

        public Snapshot {
            if (version != CURRENT_VERSION) {
                throw new IllegalArgumentException("unsupported reducer snapshot version");
            }
            tombstones = List.copyOf(tombstones);
            deletedKeys = List.copyOf(deletedKeys);
        }
    }

    /** Reversible committed mutation used by incremental durable checkpoints. */
    public record StateDelta(
            List<TombstoneState> beforeTombstones,
            List<TombstoneState> afterTombstones,
            List<RowKey> addedDeletedKeys) {
        public StateDelta {
            beforeTombstones = List.copyOf(beforeTombstones);
            afterTombstones = List.copyOf(afterTombstones);
            addedDeletedKeys = List.copyOf(addedDeletedKeys);
        }

        public boolean isEmpty() {
            return beforeTombstones.isEmpty()
                    && afterTombstones.isEmpty()
                    && addedDeletedKeys.isEmpty();
        }
    }

    private record LocatorKey(String tableIdentity, long containerId, long rowLocator) {}

    private static final class Accumulator {
        private final RowKey key;
        private final int firstOperation;
        private final boolean deletedBeforeTransaction;
        private DecodedRow last;
        private boolean sawDeleted;

        private Accumulator(RowKey key, DecodedRow first, boolean deletedBeforeTransaction) {
            this.key = key;
            this.firstOperation = first.operationCode();
            this.deletedBeforeTransaction = deletedBeforeTransaction;
            this.last = first;
            this.sawDeleted = deleted(first);
        }

        private void accept(DecodedRow row) throws IOException {
            if (sawDeleted && !deleted(row)) {
                throw violation("RESURRECTION_AFTER_TOMBSTONE", key);
            }
            if (row.operationCode() == NativeRecordReader.UT_INSERT_DATA
                    && last != row) {
                throw violation("MULTIPLE_INSERT_IMAGES_FOR_PK", key);
            }
            sawDeleted |= deleted(row);
            last = row;
        }
    }

    /** Transaction-local overlay; the durable in-memory state changes only after validation. */
    private static final class PendingStateDelta {
        private final Map<LocatorKey, RowKey> baseByLocator;
        private final Map<RowKey, LocatorKey> baseByKey;
        private final Set<RowKey> baseDeleted;
        private final Map<LocatorKey, RowKey> locatorPuts = new LinkedHashMap<>();
        private final Set<LocatorKey> locatorRemovals = new LinkedHashSet<>();
        private final Map<RowKey, LocatorKey> keyPuts = new LinkedHashMap<>();
        private final Set<RowKey> keyRemovals = new LinkedHashSet<>();
        private final Set<RowKey> deletedAdds = new LinkedHashSet<>();

        private PendingStateDelta(
                Map<LocatorKey, RowKey> baseByLocator,
                Map<RowKey, LocatorKey> baseByKey,
                Set<RowKey> baseDeleted) {
            this.baseByLocator = baseByLocator;
            this.baseByKey = baseByKey;
            this.baseDeleted = baseDeleted;
        }

        private RowKey keyAt(LocatorKey locator) {
            if (locatorPuts.containsKey(locator)) return locatorPuts.get(locator);
            return locatorRemovals.contains(locator) ? null : baseByLocator.get(locator);
        }

        private LocatorKey locatorFor(RowKey key) {
            if (keyPuts.containsKey(key)) return keyPuts.get(key);
            return keyRemovals.contains(key) ? null : baseByKey.get(key);
        }

        private boolean deleted(RowKey key) {
            return deletedAdds.contains(key) || baseDeleted.contains(key);
        }

        private void put(LocatorKey locator, RowKey key) {
            locatorRemovals.remove(locator);
            locatorPuts.put(locator, key);
            keyRemovals.remove(key);
            keyPuts.put(key, locator);
        }

        private void removeLocator(LocatorKey locator) {
            locatorPuts.remove(locator);
            locatorRemovals.add(locator);
        }

        private void removeKey(RowKey key) {
            keyPuts.remove(key);
            keyRemovals.add(key);
        }

        private void markDeleted(RowKey key) {
            if (!baseDeleted.contains(key)) deletedAdds.add(key);
        }

        private int finalLocatorSize() {
            int size = baseByLocator.size();
            for (LocatorKey locator : locatorRemovals) {
                if (baseByLocator.containsKey(locator)) size--;
            }
            for (LocatorKey locator : locatorPuts.keySet()) {
                if (!baseByLocator.containsKey(locator)) size++;
            }
            return size;
        }

        private int finalDeletedSize() {
            return Math.addExact(baseDeleted.size(), deletedAdds.size());
        }

        private void apply() {
            locatorRemovals.forEach(baseByLocator::remove);
            keyRemovals.forEach(baseByKey::remove);
            baseByLocator.putAll(locatorPuts);
            baseByKey.putAll(keyPuts);
            baseDeleted.addAll(deletedAdds);
        }

        private StateDelta durableDelta() {
            Set<LocatorKey> affected = new LinkedHashSet<>(locatorRemovals);
            affected.addAll(locatorPuts.keySet());
            List<LocatorKey> ordered = affected.stream()
                    .sorted(Comparator.comparing(LocatorKey::tableIdentity)
                            .thenComparingLong(LocatorKey::containerId)
                            .thenComparingLong(LocatorKey::rowLocator))
                    .toList();
            List<TombstoneState> before = new ArrayList<>();
            List<TombstoneState> after = new ArrayList<>();
            for (LocatorKey locator : ordered) {
                RowKey previous = baseByLocator.get(locator);
                if (previous != null) before.add(tombstone(locator, previous));
                RowKey next = keyAt(locator);
                if (next != null) after.add(tombstone(locator, next));
            }
            List<RowKey> deleted = deletedAdds.stream()
                    .sorted(Comparator.comparing(NativeTransactionReducer::rowKeySortValue))
                    .toList();
            return new StateDelta(before, after, deleted);
        }

        private static TombstoneState tombstone(LocatorKey locator, RowKey key) {
            return new TombstoneState(locator.tableIdentity(), locator.containerId(),
                    locator.rowLocator(), key);
        }
    }

    private final int maxPendingTombstones;
    private final int maxDeletedKeys;
    private final Map<LocatorKey, RowKey> tombstoneByLocator = new LinkedHashMap<>();
    private final Map<RowKey, LocatorKey> locatorByKey = new LinkedHashMap<>();
    private final Set<RowKey> deletedKeys = new LinkedHashSet<>();

    public NativeTransactionReducer() {
        this(DEFAULT_MAX_PENDING_TOMBSTONES, DEFAULT_MAX_DELETED_KEYS);
    }

    public NativeTransactionReducer(int maxPendingTombstones, int maxDeletedKeys) {
        if (maxPendingTombstones <= 0 || maxDeletedKeys <= 0) {
            throw new IllegalArgumentException("transaction reducer limits must be positive");
        }
        this.maxPendingTombstones = maxPendingTombstones;
        this.maxDeletedKeys = maxDeletedKeys;
    }

    /** Applies one terminal transaction atomically to reducer state. */
    public Optional<CommittedBatch> reduce(
            NativeRowLobPipeline.CompletedTransaction transaction) throws IOException {
        Objects.requireNonNull(transaction, "transaction");
        if (transaction.outcome() == NativeRowLobPipeline.Outcome.ROLLBACK) {
            return Optional.empty();
        }

        PendingStateDelta delta = new PendingStateDelta(
                tombstoneByLocator, locatorByKey, deletedKeys);

        int cleaned = 0;
        for (NativeRowLobPipeline.PhysicalDelete delete : transaction.physicalDeletes()) {
            LocatorKey locator = new LocatorKey(
                    tableIdentity(delete.plan()), delete.containerId(), delete.rowId());
            RowKey key = delta.keyAt(locator);
            if (key == null) {
                throw violation("UNKNOWN_DELETE_LOCATOR", locator);
            }
            delta.removeLocator(locator);
            if (locator.equals(delta.locatorFor(key))) {
                delta.removeKey(key);
            }
            cleaned++;
        }

        Map<RowKey, Accumulator> accumulated = new LinkedHashMap<>();
        for (DecodedRow row : transaction.rows()) {
            if (!row.fullImage()) {
                throw violation("SPARSE_COMMITTED_ROW", row.logPosition());
            }
            RowKey key = rowKey(row);
            boolean wasDeleted = delta.deleted(key);
            if (wasDeleted && !deleted(row)) {
                throw violation("RESURRECTION_AFTER_TOMBSTONE", key);
            }
            Accumulator current = accumulated.get(key);
            if (current == null) {
                accumulated.put(key, new Accumulator(key, row, wasDeleted));
            }
            else {
                current.accept(row);
            }
        }

        List<Accumulator> ordered = new ArrayList<>(accumulated.values());
        ordered.sort(Comparator.comparingLong(value -> value.last.logPosition()));
        List<LogicalChange> changes = new ArrayList<>(ordered.size());
        for (Accumulator value : ordered) {
            boolean finalDeleted = deleted(value.last);
            if (finalDeleted) {
                registerTombstone(value.key, value.last, delta);
                boolean insertedHere = value.firstOperation
                        == NativeRecordReader.UT_INSERT_DATA;
                if (!insertedHere && !value.deletedBeforeTransaction) {
                    changes.add(new LogicalChange(value.key, Operation.DELETE,
                            value.last, value.last.logPosition()));
                }
                continue;
            }
            Operation operation = value.firstOperation == NativeRecordReader.UT_INSERT_DATA
                    ? Operation.CREATE : Operation.UPDATE;
            changes.add(new LogicalChange(
                    value.key, operation, value.last, value.last.logPosition()));
        }

        int finalLocators = delta.finalLocatorSize();
        int finalDeleted = delta.finalDeletedSize();
        if (finalLocators > maxPendingTombstones
                || finalDeleted > maxDeletedKeys) {
            throw violation("TOMBSTONE_STATE_LIMIT", "locators=" + finalLocators
                    + " deletedKeys=" + finalDeleted);
        }
        StateDelta durableDelta = delta.durableDelta();
        delta.apply();

        return Optional.of(new CommittedBatch(transaction.tid(),
                transaction.terminalPosition(), changes, cleaned, durableDelta));
    }

    public int pendingTombstones() {
        return tombstoneByLocator.size();
    }

    public int deletedKeys() {
        return deletedKeys.size();
    }

    /** Applies a validated forward delta while rebuilding an incremental checkpoint chain. */
    public void apply(StateDelta delta) throws IOException {
        replaceState(delta.beforeTombstones(), delta.afterTombstones(),
                delta.addedDeletedKeys(), true);
    }

    /** Rolls back the most recently applied delta after durable publication fails. */
    public void revert(StateDelta delta) throws IOException {
        replaceState(delta.afterTombstones(), delta.beforeTombstones(),
                delta.addedDeletedKeys(), false);
    }

    private void replaceState(
            List<TombstoneState> expectedStates,
            List<TombstoneState> replacementStates,
            List<RowKey> addedDeleted,
            boolean forward) throws IOException {
        Map<LocatorKey, RowKey> expected = deltaMap(expectedStates);
        Map<LocatorKey, RowKey> replacement = deltaMap(replacementStates);
        Set<LocatorKey> affected = new LinkedHashSet<>(expected.keySet());
        affected.addAll(replacement.keySet());
        for (LocatorKey locator : affected) {
            boolean expectedPresent = expected.containsKey(locator);
            if (tombstoneByLocator.containsKey(locator) != expectedPresent
                    || expectedPresent
                    && !Objects.equals(tombstoneByLocator.get(locator), expected.get(locator))) {
                throw stateFailure("STATE_DELTA_BASE_MISMATCH: " + locator);
            }
        }
        for (RowKey key : addedDeleted) {
            if (deletedKeys.contains(key) != !forward) {
                throw stateFailure("STATE_DELTA_DELETED_KEY_MISMATCH: " + key);
            }
        }
        for (var entry : replacement.entrySet()) {
            LocatorKey existing = locatorByKey.get(entry.getValue());
            if (existing != null && !affected.contains(existing)
                    && !existing.equals(entry.getKey())) {
                throw stateFailure("STATE_DELTA_KEY_COLLISION: " + entry.getValue());
            }
        }

        for (LocatorKey locator : affected) {
            RowKey removed = tombstoneByLocator.remove(locator);
            if (removed != null) {
                locatorByKey.remove(removed, locator);
            }
        }
        for (var entry : replacement.entrySet()) {
            LocatorKey keyCollision = locatorByKey.put(entry.getValue(), entry.getKey());
            if (keyCollision != null && !keyCollision.equals(entry.getKey())) {
                throw stateFailure("STATE_DELTA_KEY_COLLISION: " + entry.getValue());
            }
            RowKey locatorCollision = tombstoneByLocator.put(
                    entry.getKey(), entry.getValue());
            if (locatorCollision != null && !locatorCollision.equals(entry.getValue())) {
                throw stateFailure("STATE_DELTA_LOCATOR_COLLISION: " + entry.getKey());
            }
        }
        if (forward) {
            deletedKeys.addAll(addedDeleted);
        }
        else {
            deletedKeys.removeAll(addedDeleted);
        }
    }

    private static Map<LocatorKey, RowKey> deltaMap(List<TombstoneState> states)
            throws IOException {
        Map<LocatorKey, RowKey> result = new LinkedHashMap<>();
        Set<RowKey> keys = new LinkedHashSet<>();
        for (TombstoneState state : states) {
            LocatorKey locator = new LocatorKey(
                    state.tableIdentity(), state.containerId(), state.rowLocator());
            if (result.put(locator, state.key()) != null) {
                throw stateFailure("STATE_DELTA_DUPLICATE_LOCATOR: " + locator);
            }
            if (!keys.add(state.key())) {
                throw stateFailure("STATE_DELTA_DUPLICATE_KEY: " + state.key());
            }
        }
        return result;
    }

    /** Returns a deterministic immutable image suitable for an atomic durable checkpoint. */
    public Snapshot snapshot() {
        List<TombstoneState> tombstones = tombstoneByLocator.entrySet().stream()
                .map(value -> new TombstoneState(value.getKey().tableIdentity(),
                        value.getKey().containerId(), value.getKey().rowLocator(),
                        value.getValue()))
                .sorted(Comparator.comparing(TombstoneState::tableIdentity)
                        .thenComparingLong(TombstoneState::containerId)
                        .thenComparingLong(TombstoneState::rowLocator)
                        .thenComparing(value -> rowKeySortValue(value.key())))
                .toList();
        List<RowKey> deleted = deletedKeys.stream()
                .sorted(Comparator.comparing(NativeTransactionReducer::rowKeySortValue))
                .toList();
        return new Snapshot(Snapshot.CURRENT_VERSION, tombstones, deleted);
    }

    /** Restores only into a new/empty reducer; partial merge is intentionally forbidden. */
    public void restore(Snapshot snapshot) throws IOException {
        Objects.requireNonNull(snapshot, "snapshot");
        if (!tombstoneByLocator.isEmpty() || !locatorByKey.isEmpty() || !deletedKeys.isEmpty()) {
            throw violation("RESTORE_INTO_NON_EMPTY_STATE", "reducer");
        }
        Map<LocatorKey, RowKey> nextByLocator = new LinkedHashMap<>();
        Map<RowKey, LocatorKey> nextByKey = new LinkedHashMap<>();
        Set<RowKey> nextDeleted = new LinkedHashSet<>(snapshot.deletedKeys());
        for (TombstoneState state : snapshot.tombstones()) {
            LocatorKey locator = new LocatorKey(
                    state.tableIdentity(), state.containerId(), state.rowLocator());
            if (nextByLocator.put(locator, state.key()) != null
                    || nextByKey.put(state.key(), locator) != null) {
                throw violation("DUPLICATE_DURABLE_TOMBSTONE", state);
            }
            if (!nextDeleted.contains(state.key())) {
                throw violation("TOMBSTONE_NOT_MARKED_DELETED", state.key());
            }
        }
        if (nextByLocator.size() > maxPendingTombstones
                || nextDeleted.size() > maxDeletedKeys) {
            throw violation("TOMBSTONE_STATE_LIMIT", "locators=" + nextByLocator.size()
                    + " deletedKeys=" + nextDeleted.size());
        }
        tombstoneByLocator.putAll(nextByLocator);
        locatorByKey.putAll(nextByKey);
        deletedKeys.addAll(nextDeleted);
    }

    /** Replaces runtime state with a previously validated atomic snapshot. */
    public void replace(Snapshot snapshot) throws IOException {
        tombstoneByLocator.clear();
        locatorByKey.clear();
        deletedKeys.clear();
        try {
            restore(snapshot);
        }
        catch (IOException | RuntimeException error) {
            tombstoneByLocator.clear();
            locatorByKey.clear();
            deletedKeys.clear();
            throw error;
        }
    }

    static RowKey rowKey(DecodedRow row) throws IOException {
        TableDecodePlan plan = row.plan();
        List<KeyPart> parts = new ArrayList<>(plan.primaryKeyIndexes().size());
        for (int index : plan.primaryKeyIndexes()) {
            TableMetadata.Column column = plan.metadata().columns().get(index);
            ColumnValue encoded = row.values().get(index);
            if (encoded.kind() != NativeRowDecoder.ValueKind.SCALAR) {
                throw violation("INVALID_PRIMARY_KEY_VALUE", column.name());
            }
            parts.add(new KeyPart(column.name(), column.typeName(),
                    canonical(encoded.value())));
        }
        return new RowKey(tableIdentity(plan), parts);
    }

    private static void registerTombstone(
            RowKey key,
            DecodedRow row,
            PendingStateDelta delta) throws IOException {
        if (row.rowId() < 0) {
            throw violation("TOMBSTONE_WITHOUT_ROW_LOCATOR", row.logPosition());
        }
        LocatorKey next = new LocatorKey(
                key.tableIdentity(), row.containerId(), row.rowId());
        LocatorKey previous = delta.locatorFor(key);
        if (previous != null && !previous.equals(next)) {
            delta.removeLocator(previous);
        }
        RowKey collision = delta.keyAt(next);
        if (collision != null && !collision.equals(key)) {
            throw violation("DELETE_LOCATOR_COLLISION", next);
        }
        delta.put(next, key);
        delta.markDeleted(key);
    }

    private static boolean deleted(DecodedRow row) {
        int index = row.plan().deletedFlagIndex();
        return index >= 0 && Boolean.TRUE.equals(row.values().get(index).value());
    }

    private static String tableIdentity(TableDecodePlan plan) {
        return plan.metadata().qualifiedName() + "#" + plan.schemaFingerprint();
    }

    private static String canonical(Object value) throws IOException {
        if (value instanceof byte[] bytes) {
            return "bytes:" + HexFormat.of().formatHex(bytes);
        }
        if (value instanceof BigDecimal decimal) {
            return "decimal:" + decimal.stripTrailingZeros().toPlainString();
        }
        if (value instanceof Float number) {
            return "float:" + Float.toHexString(number);
        }
        if (value instanceof Double number) {
            return "double:" + Double.toHexString(number);
        }
        if (value instanceof Number || value instanceof Boolean
                || value instanceof CharSequence
                || value instanceof java.time.temporal.TemporalAccessor) {
            return value.getClass().getName() + ":" + value;
        }
        throw violation("UNSUPPORTED_PRIMARY_KEY_VALUE", value.getClass().getName());
    }

    private static String rowKeySortValue(RowKey key) {
        StringBuilder value = new StringBuilder(key.tableIdentity());
        for (KeyPart part : key.parts()) {
            value.append('\u0000').append(part.columnName())
                    .append('\u0000').append(part.typeName())
                    .append('\u0000').append(part.canonicalValue());
        }
        return value.toString();
    }

    private static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    private static IOException violation(String reason, Object detail) {
        return new IOException("FATAL_DML_CONTRACT_VIOLATION / " + reason + ": " + detail);
    }

    private static IOException stateFailure(String detail) {
        return new IOException("FATAL_TRANSACTION_STATE: " + detail);
    }
}

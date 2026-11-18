package io.hanacdc.decoder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Buffers captured-table changes while transaction terminals remain global. */
public final class CapturedTransactionBuffer<T> {
    public enum Outcome { COMMIT, ROLLBACK }

    public record CompletedTransaction<T>(long tid, Outcome outcome, List<T> changes) {
        public CompletedTransaction {
            changes = List.copyOf(changes);
        }
    }

    private final Map<Long, List<T>> captured = new LinkedHashMap<>();

    public void capture(long tid, T change) {
        if (tid <= 0) {
            throw new IllegalArgumentException("TID must be positive");
        }
        captured.computeIfAbsent(tid, ignored -> new ArrayList<>()).add(change);
    }

    /**
     * Applies a global terminal. Empty means that the transaction changed no
     * captured table; this is expected and must not produce a Kafka event.
     */
    public Optional<CompletedTransaction<T>> complete(long tid, Outcome outcome) {
        List<T> changes = captured.remove(tid);
        if (changes == null) {
            return Optional.empty();
        }
        return Optional.of(new CompletedTransaction<>(tid, outcome, changes));
    }

    public int openCapturedTransactions() {
        return captured.size();
    }
}

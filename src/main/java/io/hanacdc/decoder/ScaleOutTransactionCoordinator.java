package io.hanacdc.decoder;

import io.hanacdc.connect.SourceStreamId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Fail-closed barrier for a transaction reconstructed from independent HANA logs.
 * A transaction is released only after an explicit decision names every participant
 * and every named stream has supplied a matching terminal fragment.
 */
public final class ScaleOutTransactionCoordinator<T> {
    public enum Outcome { COMMIT, ROLLBACK }

    public record Fragment<T>(
            long transactionId,
            SourceStreamId stream,
            Outcome outcome,
            List<T> changes) {
        public Fragment {
            Objects.requireNonNull(stream, "stream");
            Objects.requireNonNull(outcome, "outcome");
            changes = List.copyOf(Objects.requireNonNull(changes, "changes"));
            if (transactionId <= 0 || (outcome == Outcome.ROLLBACK && !changes.isEmpty())) {
                throw new IllegalArgumentException("invalid scale-out transaction fragment");
            }
        }
    }

    public record Decision(
            long transactionId,
            Outcome outcome,
            Set<SourceStreamId> participants) {
        public Decision {
            Objects.requireNonNull(outcome, "outcome");
            participants = Set.copyOf(Objects.requireNonNull(participants, "participants"));
            if (transactionId <= 0 || participants.isEmpty()) {
                throw new IllegalArgumentException("invalid scale-out commit decision");
            }
        }
    }

    public record Completed<T>(
            long transactionId,
            Outcome outcome,
            List<SourceStreamId> participants,
            List<T> changes) {
        public Completed {
            participants = List.copyOf(participants);
            changes = List.copyOf(changes);
        }
    }

    private static final class Pending<T> {
        private final Map<SourceStreamId, Fragment<T>> fragments = new LinkedHashMap<>();
        private Decision decision;
    }

    private final int maxOpenTransactions;
    private final int maxBufferedFragments;
    private final Map<Long, Pending<T>> pending = new LinkedHashMap<>();
    private int bufferedFragments;

    public ScaleOutTransactionCoordinator(int maxOpenTransactions, int maxBufferedFragments) {
        if (maxOpenTransactions <= 0 || maxBufferedFragments <= 0) {
            throw new IllegalArgumentException("scale-out coordinator limits must be positive");
        }
        this.maxOpenTransactions = maxOpenTransactions;
        this.maxBufferedFragments = maxBufferedFragments;
    }

    public Optional<Completed<T>> acceptFragment(Fragment<T> fragment) throws IOException {
        Objects.requireNonNull(fragment, "fragment");
        Pending<T> state = state(fragment.transactionId());
        if (state.decision != null
                && !state.decision.participants().contains(fragment.stream())) {
            throw transaction("fragment from undeclared participant: tid="
                    + fragment.transactionId() + " stream=" + fragment.stream().canonical());
        }
        Fragment<T> previous = state.fragments.putIfAbsent(fragment.stream(), fragment);
        if (previous != null) {
            throw transaction("duplicate terminal fragment: tid=" + fragment.transactionId()
                    + " stream=" + fragment.stream().canonical());
        }
        bufferedFragments++;
        if (bufferedFragments > maxBufferedFragments) {
            state.fragments.remove(fragment.stream());
            bufferedFragments--;
            throw resource("terminal fragment limit exceeded");
        }
        try {
            return completeIfReady(fragment.transactionId(), state);
        }
        catch (IOException error) {
            remove(fragment.transactionId(), state);
            throw error;
        }
    }

    public Optional<Completed<T>> acceptDecision(Decision decision) throws IOException {
        Objects.requireNonNull(decision, "decision");
        Pending<T> state = state(decision.transactionId());
        if (state.decision != null) {
            throw transaction("duplicate commit decision: tid=" + decision.transactionId());
        }
        for (SourceStreamId stream : state.fragments.keySet()) {
            if (!decision.participants().contains(stream)) {
                remove(decision.transactionId(), state);
                throw transaction("decision omits observed participant: tid="
                        + decision.transactionId() + " stream=" + stream.canonical());
            }
        }
        state.decision = decision;
        try {
            return completeIfReady(decision.transactionId(), state);
        }
        catch (IOException error) {
            remove(decision.transactionId(), state);
            throw error;
        }
    }

    public int openTransactions() {
        return pending.size();
    }

    public int bufferedFragments() {
        return bufferedFragments;
    }

    public void finish() throws IOException {
        if (!pending.isEmpty()) {
            throw transaction("incomplete distributed transactions=" + pending.keySet());
        }
    }

    private Pending<T> state(long tid) throws IOException {
        Pending<T> existing = pending.get(tid);
        if (existing != null) {
            return existing;
        }
        if (pending.size() >= maxOpenTransactions) {
            throw resource("open transaction limit exceeded");
        }
        Pending<T> created = new Pending<>();
        pending.put(tid, created);
        return created;
    }

    private Optional<Completed<T>> completeIfReady(long tid, Pending<T> state)
            throws IOException {
        if (state.decision == null
                || !state.fragments.keySet().containsAll(state.decision.participants())) {
            return Optional.empty();
        }
        if (state.fragments.size() != state.decision.participants().size()) {
            throw transaction("fragment cardinality differs from decision: tid=" + tid);
        }
        List<T> changes = new ArrayList<>();
        List<SourceStreamId> participants = new ArrayList<>(
                new TreeSet<>(state.decision.participants()));
        for (SourceStreamId stream : participants) {
            Fragment<T> fragment = state.fragments.get(stream);
            if (fragment.outcome() != state.decision.outcome()) {
                throw transaction("terminal outcome mismatch: tid=" + tid
                        + " stream=" + stream.canonical());
            }
            if (state.decision.outcome() == Outcome.COMMIT) {
                changes.addAll(fragment.changes());
            }
        }
        remove(tid, state);
        return Optional.of(new Completed<>(tid, state.decision.outcome(),
                participants, changes));
    }

    private void remove(long tid, Pending<T> state) {
        if (pending.remove(tid, state)) {
            bufferedFragments -= state.fragments.size();
        }
    }

    private static IOException transaction(String detail) {
        return new IOException("FATAL_SCALE_OUT_TRANSACTION: " + detail);
    }

    private static IOException resource(String detail) {
        return new IOException("FATAL_RESOURCE_LIMIT: SCALE_OUT: " + detail);
    }
}

package io.hanacdc.decoder;

import io.hanacdc.connect.SourceStreamId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ScaleOutTransactionCoordinatorTest {
    private static final SourceStreamId A = new SourceStreamId(2, "indexserver");
    private static final SourceStreamId B = new SourceStreamId(3, "indexserver");

    @Test
    void releasesOnlyAfterDecisionAndEveryParticipant() throws Exception {
        ScaleOutTransactionCoordinator<String> coordinator =
                new ScaleOutTransactionCoordinator<>(10, 20);

        assertTrue(coordinator.acceptFragment(fragment(7, B, "b1")).isEmpty());
        assertTrue(coordinator.acceptDecision(decision(7)).isEmpty());
        var completed = coordinator.acceptFragment(fragment(7, A, "a1", "a2"))
                .orElseThrow();

        assertEquals(List.of(A, B), completed.participants());
        assertEquals(List.of("a1", "a2", "b1"), completed.changes());
        assertEquals(0, coordinator.openTransactions());
        coordinator.finish();
    }

    @Test
    void rejectsUnknownParticipantDuplicateAndOutcomeMismatch() throws Exception {
        ScaleOutTransactionCoordinator<String> unknown =
                new ScaleOutTransactionCoordinator<>(10, 20);
        unknown.acceptDecision(new ScaleOutTransactionCoordinator.Decision(
                8, ScaleOutTransactionCoordinator.Outcome.COMMIT, Set.of(A)));
        IOException undeclared = assertThrows(IOException.class,
                () -> unknown.acceptFragment(fragment(8, B, "x")));
        assertTrue(undeclared.getMessage().contains("undeclared participant"));

        ScaleOutTransactionCoordinator<String> duplicate =
                new ScaleOutTransactionCoordinator<>(10, 20);
        duplicate.acceptFragment(fragment(9, A, "x"));
        assertThrows(IOException.class,
                () -> duplicate.acceptFragment(fragment(9, A, "x")));

        ScaleOutTransactionCoordinator<String> mismatch =
                new ScaleOutTransactionCoordinator<>(10, 20);
        mismatch.acceptDecision(decision(10));
        mismatch.acceptFragment(fragment(10, A, "x"));
        assertThrows(IOException.class, () -> mismatch.acceptFragment(
                new ScaleOutTransactionCoordinator.Fragment<>(10, B,
                        ScaleOutTransactionCoordinator.Outcome.ROLLBACK, List.of())));
    }

    @Test
    void enforcesResourceLimitsAndIncompleteFinish() throws Exception {
        ScaleOutTransactionCoordinator<String> coordinator =
                new ScaleOutTransactionCoordinator<>(1, 1);
        coordinator.acceptFragment(fragment(11, A, "x"));

        assertThrows(IOException.class,
                () -> coordinator.acceptFragment(fragment(12, B, "y")));
        assertThrows(IOException.class, coordinator::finish);
    }

    private static ScaleOutTransactionCoordinator.Fragment<String> fragment(
            long tid, SourceStreamId stream, String... changes) {
        return new ScaleOutTransactionCoordinator.Fragment<>(tid, stream,
                ScaleOutTransactionCoordinator.Outcome.COMMIT, List.of(changes));
    }

    private static ScaleOutTransactionCoordinator.Decision decision(long tid) {
        return new ScaleOutTransactionCoordinator.Decision(tid,
                ScaleOutTransactionCoordinator.Outcome.COMMIT, Set.of(A, B));
    }
}

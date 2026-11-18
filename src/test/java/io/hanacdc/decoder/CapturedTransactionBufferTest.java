package io.hanacdc.decoder;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CapturedTransactionBufferTest {
    @Test
    void publishesOnlyCapturedPartOfAMixedTransactionAfterGlobalCommit() {
        CapturedTransactionBuffer<String> buffer = new CapturedTransactionBuffer<>();

        // A non-captured-table record with the same TID is deliberately not
        // decoded or added. The global COMMIT still completes the target part.
        buffer.capture(71, "TARGET:INSERT");
        var completed = buffer.complete(71, CapturedTransactionBuffer.Outcome.COMMIT)
                .orElseThrow();

        assertEquals(CapturedTransactionBuffer.Outcome.COMMIT, completed.outcome());
        assertEquals(java.util.List.of("TARGET:INSERT"), completed.changes());
        assertEquals(0, buffer.openCapturedTransactions());
    }

    @Test
    void globalTerminalForExcludedOnlyTransactionProducesNoEvent() {
        CapturedTransactionBuffer<String> buffer = new CapturedTransactionBuffer<>();

        assertTrue(buffer.complete(72, CapturedTransactionBuffer.Outcome.COMMIT).isEmpty());
    }

    @Test
    void rollbackDiscardsCapturedChangesWithoutPublishingData() {
        CapturedTransactionBuffer<String> buffer = new CapturedTransactionBuffer<>();
        buffer.capture(73, "TARGET:UPSERT");

        var completed = buffer.complete(73, CapturedTransactionBuffer.Outcome.ROLLBACK)
                .orElseThrow();

        assertEquals(CapturedTransactionBuffer.Outcome.ROLLBACK, completed.outcome());
        assertEquals(java.util.List.of("TARGET:UPSERT"), completed.changes());
    }
}

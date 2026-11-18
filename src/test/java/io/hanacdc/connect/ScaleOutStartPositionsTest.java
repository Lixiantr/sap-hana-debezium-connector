package io.hanacdc.connect;

import io.hanacdc.control.PersistenceStreamStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ScaleOutStartPositionsTest {
    private static final SourceStreamId A = new SourceStreamId(2, "indexserver");
    private static final SourceStreamId B = new SourceStreamId(3, "indexserver");

    @Test
    void blankSpecificationStartsAtEachCurrentBackupBoundary() {
        assertEquals(Map.of(A, 110L, B, 220L),
                ScaleOutStartPositions.resolve("", List.of(B, A), statuses()));
    }

    @Test
    void parsesCompleteExplicitVectorAndRejectsGaps() {
        assertEquals(Map.of(A, 105L, B, 205L),
                ScaleOutStartPositions.resolve("3=205,2=105",
                        List.of(A, B), statuses()));

        assertThrows(IllegalArgumentException.class, () ->
                ScaleOutStartPositions.resolve("2=105", List.of(A, B), statuses()));
        IllegalArgumentException gap = assertThrows(IllegalArgumentException.class, () ->
                ScaleOutStartPositions.resolve("2=99,3=205",
                        List.of(A, B), statuses()));
        assertTrue(gap.getMessage().contains("FATAL_GAP"));
    }

    private static Map<SourceStreamId, PersistenceStreamStatus> statuses() {
        Instant now = Instant.parse("2026-08-07T12:00:00Z");
        return Map.of(A, new PersistenceStreamStatus(2, 100, 1000, 110, 112, now),
                B, new PersistenceStreamStatus(3, 200, 1000, 220, 225, now));
    }
}

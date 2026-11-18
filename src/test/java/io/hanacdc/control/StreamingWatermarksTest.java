package io.hanacdc.control;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class StreamingWatermarksTest {
    @Test
    void calculatesLagAndRetentionHeadroomWithoutWallClockOrdering() {
        StreamingWatermarks value = new StreamingWatermarks(
                100, 200, 190, 180, 170, 160, 150,
                50, 90, 10_000, 640,
                Instant.parse("2026-08-03T12:00:00Z"));

        assertTrue(value.retentionSafe());
        assertEquals(640, value.backupLagBytes());
        assertEquals(1280, value.decodeLagBytes());
        assertEquals(2560, value.retentionHeadroomBytes());
        assertEquals(5.0, value.estimatedCatchupSeconds());
        assertTrue(value.toJson().contains("\"phase\":\"STREAMING_UNVERIFIED\""));
    }

    @Test
    void identifiesIrrecoverableGapAndWarmup() {
        StreamingWatermarks gap = new StreamingWatermarks(
                100, 200, 190, 180, 170, 160, 100,
                120, 110, 10_000, 0,
                Instant.parse("2026-08-03T12:00:00Z"));
        assertFalse(gap.retentionSafe());
        assertEquals("FATAL_GAP", gap.phase());
        assertEquals(0, gap.retentionHeadroomBytes());

        StreamingWatermarks warmup = new StreamingWatermarks(
                100, 110, 110, 110, 110, 100, 100,
                50, 90, 10_000, 0,
                Instant.parse("2026-08-03T12:00:00Z"));
        assertEquals("STREAMING_WARMUP", warmup.phase());
    }
}

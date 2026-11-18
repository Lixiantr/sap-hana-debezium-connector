package io.hanacdc.control;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class HanaContractTest {
    private static final long GIB = 1024L * 1024L * 1024L;

    @Test
    void acceptsQualifiedSingleServiceBaseline() {
        CapabilityAssessment result = contract().assess(capabilities(
                900, "immediate", false, false, 0));
        assertTrue(result.ready(), result.summary());
        assertTrue(result.canRead());
    }

    @Test
    void timeoutChangeIsPolicyViolationButDoesNotChangeDecodeCapability() {
        CapabilityAssessment result = contract().assess(capabilities(
                901, "immediate", false, false, 0));
        assertFalse(result.ready());
        assertTrue(result.canRead());
        assertTrue(result.summary().contains("BACKUP_TIMEOUT_EXCEEDS_SLO"));
    }

    @Test
    void unsupportedFormatAndBrokenHistoryFailClosed() {
        CapabilityAssessment encrypted = contract().assess(capabilities(
                30, "immediate", true, false, 0));
        assertFalse(encrypted.canRead());
        assertTrue(encrypted.summary().contains("UNSUPPORTED_BACKUP_ENCRYPTION"));

        CapabilityAssessment service = contract().assess(capabilities(
                30, "service", false, false, 0));
        assertFalse(service.canRead());
        assertTrue(service.summary().contains("UNSUPPORTED_INTERVAL_MODE"));

        CapabilityAssessment gap = contract().assess(capabilities(
                30, "immediate", false, false, 1));
        assertFalse(gap.canRead());
        assertTrue(gap.summary().contains("FATAL_GAP"));
    }

    @Test
    void configuredSizeAboveReaderIsAnEnforcedPolicy() {
        HanaContract twoGiBReader = new HanaContract(
                Set.of("immediate"), Set.of("file"), Set.of("none"),
                false, false, 2 * GIB, 900_000, List.of("2.00.088"));
        CapabilityAssessment result = twoGiBReader.assess(capabilities(
                900, "immediate", false, false, 0));
        assertFalse(result.ready());
        assertTrue(result.canRead());
        assertTrue(result.summary().contains("CONFIGURED_BACKUP_LIMIT_EXCEEDS_READER"));
    }

    private static HanaContract contract() {
        return new HanaContract(
                Set.of("immediate"), Set.of("file"), Set.of("none"),
                false, false, 64 * GIB, 900_000, List.of("2.00.088"));
    }

    private static HanaCapabilities capabilities(
            long timeout,
            String mode,
            boolean encrypted,
            boolean replicated,
            long broken) {
        return new HanaCapabilities(
                "HXE", "HXE", "2.00.088.00.1760424921",
                "normal", true, timeout, mode, 1024, 16,
                false, false, "/hana/mounts/backup/log/DB_HXE/",
                false, "lz4", encrypted, broken, replicated,
                10, 64 * 1024, 100, 110,
                Instant.parse("2026-08-03T12:00:00Z"));
    }
}

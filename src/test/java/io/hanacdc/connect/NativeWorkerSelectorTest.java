package io.hanacdc.connect;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class NativeWorkerSelectorTest {
    @Test
    void autoIsBoundedByFileCpuMaximumAndQualifiedCap() {
        assertEquals(1, NativeWorkerSelector.select(
                "auto", 16, 16, 32L * 1024 * 1024,
                256L * 1024 * 1024, 4));
        assertEquals(2, NativeWorkerSelector.select(
                "auto", 16, 2, 512L * 1024 * 1024,
                256L * 1024 * 1024, 4));
        assertEquals(3, NativeWorkerSelector.select(
                "auto", 16, 16, 512L * 1024 * 1024,
                256L * 1024 * 1024, 4));
    }

    @Test
    void explicitWorkerCountRemainsArbitraryWithinMaximum() {
        assertEquals(5, NativeWorkerSelector.select(
                "5", 16, 2, 1, 1, 1));
        assertThrows(ConnectException.class,
                () -> NativeWorkerSelector.select("17", 16, 16, 1, 1, 1));
    }

    @Test
    void stageSpecificAutoCapIsApplied() {
        assertEquals(6, NativeWorkerSelector.select(
                "auto", 16, 16, Long.MAX_VALUE, 1, 1,
                "log.processor.threads", "log.processor.max.threads", 6));
    }
}

package io.hanacdc.connect;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

final class ScaleOutSourceEpochTest {
    @Test
    void topologyIsPartOfDistributedEpochIdentity() {
        ScaleOutSourceEpoch first = ScaleOutSourceEpoch.prototype(
                "hana", "run", "a".repeat(64));
        ScaleOutSourceEpoch same = ScaleOutSourceEpoch.prototype(
                "hana", "run", "a".repeat(64));
        ScaleOutSourceEpoch moved = ScaleOutSourceEpoch.prototype(
                "hana", "run", "b".repeat(64));

        assertEquals(first, same);
        assertNotEquals(first.id(), moved.id());
    }
}

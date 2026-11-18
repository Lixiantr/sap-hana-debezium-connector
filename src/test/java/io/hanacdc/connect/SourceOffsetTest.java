package io.hanacdc.connect;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class SourceOffsetTest {
    @Test
    void roundTripsStrictVersionedOffset() {
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        SourceOffset value = SourceOffset.withoutDurableState(
                epoch, 99, 100, 110, 106, 4096, 7, 123);

        assertEquals(value, SourceOffset.fromMap(value.toMap()));
        value.requireEpoch(epoch);
    }

    @Test
    void rejectsLegacyOrIncompleteOffsets() {
        Map<String, Object> legacy = new LinkedHashMap<>();
        legacy.put("backup_id", 99L);
        legacy.put("file_first", 100L);
        legacy.put("event_offset", 4096L);
        assertThrows(ConnectException.class, () -> SourceOffset.fromMap(legacy));

        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        Map<String, Object> missingIdentity = new LinkedHashMap<>(
                SourceOffset.withoutDurableState(epoch, 99, 100, 110, 106, 4096, 7, 123).toMap());
        missingIdentity.remove("file_last");
        assertThrows(ConnectException.class, () -> SourceOffset.fromMap(missingIdentity));
    }

    @Test
    void rejectsOffsetFromAnotherSourceEpoch() {
        SourceEpoch first = SourceEpoch.prototype("hana01", "RUN-A", 2, 0, 0);
        SourceEpoch second = SourceEpoch.prototype("hana01", "RUN-B", 2, 0, 0);
        SourceOffset value = SourceOffset.withoutDurableState(
                first, 99, 100, 110, 106, 4096, 7, 123);

        assertThrows(ConnectException.class, () -> value.requireEpoch(second));
    }

    @Test
    void durableStateGenerationAndChecksumAreAtomicPair() {
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        assertThrows(IllegalArgumentException.class, () -> new SourceOffset(
                SourceOffset.CURRENT_VERSION, epoch.id(), 99, 100, 110, 106,
                4096, 7, 123, 0, 1, 100, 1, SourceOffset.NO_STATE_SHA256));
        assertThrows(IllegalArgumentException.class, () -> new SourceOffset(
                SourceOffset.CURRENT_VERSION, epoch.id(), 99, 100, 110, 106,
                4096, 7, 123, 0, 1, 100, 0, "a".repeat(64)));
    }

    @Test
    void acceptsOnlyLowercaseHexChecksums() {
        SourceEpoch epoch = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        assertThrows(IllegalArgumentException.class, () -> new SourceOffset(
                SourceOffset.CURRENT_VERSION, epoch.id().toUpperCase(),
                99, 100, 110, 106, 4096, 7, 123, 0, 1, 100, 0,
                SourceOffset.NO_STATE_SHA256));
        assertThrows(IllegalArgumentException.class, () -> new SourceOffset(
                SourceOffset.CURRENT_VERSION, "g" + epoch.id().substring(1),
                99, 100, 110, 106, 4096, 7, 123, 0, 1, 100, 0,
                SourceOffset.NO_STATE_SHA256));
    }
}

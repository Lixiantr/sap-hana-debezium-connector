package io.hanacdc.connect;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ScaleOutSourceOffsetTest {
    private static final String TOPOLOGY = "b".repeat(64);
    private static final String STATE = "c".repeat(64);

    @Test
    void roundTripsCanonicalSortedVector() {
        ScaleOutSourceEpoch epoch = ScaleOutSourceEpoch.prototype("hana", "p2", TOPOLOGY);
        List<ScaleOutSourceOffset.StreamPosition> positions = new ArrayList<>();
        positions.add(position(3, 20, 200, 210, 205));
        positions.add(position(2, 10, 100, 110, 106));
        DurableReducerStateStore.Checkpoint checkpoint =
                new DurableReducerStateStore.Checkpoint(7, STATE, Path.of("state.bin"));

        ScaleOutSourceOffset created = ScaleOutSourceOffset.create(
                epoch, TOPOLOGY, positions, 42, 1, 2, checkpoint);
        ScaleOutSourceOffset restored = ScaleOutSourceOffset.fromMap(created.toMap());

        assertEquals(created, restored);
        assertEquals(2, restored.streams().get(0).stream().volumeId());
        restored.requireIdentity(epoch);
    }

    @Test
    void detectsVectorTamperingAndTopologyDrift() {
        ScaleOutSourceEpoch epoch = ScaleOutSourceEpoch.prototype("hana", "p2", TOPOLOGY);
        DurableReducerStateStore.Checkpoint checkpoint =
                new DurableReducerStateStore.Checkpoint(1, STATE, Path.of("state.bin"));
        ScaleOutSourceOffset offset = ScaleOutSourceOffset.create(epoch, TOPOLOGY,
                List.of(position(2, 10, 100, 110, 106),
                        position(3, 20, 200, 210, 205)),
                42, 0, 1, checkpoint);
        Map<String, Object> tampered = new java.util.LinkedHashMap<>(offset.toMap());
        tampered.put("stream_vector", ((String) tampered.get("stream_vector"))
                .replace("position=205", "position=206"));

        assertThrows(ConnectException.class,
                () -> ScaleOutSourceOffset.fromMap(tampered));
        assertThrows(ConnectException.class,
                () -> offset.requireIdentity(ScaleOutSourceEpoch.prototype(
                        "hana", "p2", "d".repeat(64))));
    }

    private static ScaleOutSourceOffset.StreamPosition position(
            long volume, long backup, long first, long last, long position) {
        return new ScaleOutSourceOffset.StreamPosition(
                new SourceStreamId(volume, "indexserver"), backup,
                first, last, position, 4096);
    }
}

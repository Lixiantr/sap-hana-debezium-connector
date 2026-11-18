package io.hanacdc.connect;

import io.hanacdc.control.PersistenceService;
import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SourceTopologyTest {
    private static final PersistenceService SERVICE =
            new PersistenceService("hana01", 39040, "indexserver", 2);

    @Test
    void discoversAndPinsSingleServicePartitioning() {
        SourceTopology topology = SourceTopology.discover(
                SourceTopologyMode.AUTO, List.of(SERVICE), List.of(plan(101, 102)), 2);

        assertEquals(SourceTopologyMode.SINGLE_SERVICE, topology.actualMode());
        assertDoesNotThrow(() -> topology.requireUnchanged(SourceTopology.discover(
                SourceTopologyMode.AUTO, List.of(SERVICE), List.of(plan(101, 102)), 2)));
    }

    @Test
    void rejectsModeMismatchSourceIdAndScaleOutForSingleServiceMode() {
        IllegalArgumentException mode = assertThrows(IllegalArgumentException.class,
                () -> SourceTopology.discover(SourceTopologyMode.SINGLE_CONTAINER,
                        List.of(SERVICE), List.of(plan(101, 102)), 2));
        assertTrue(mode.getMessage().contains("FATAL_SOURCE_TOPOLOGY"));

        assertThrows(IllegalArgumentException.class,
                () -> SourceTopology.discover(SourceTopologyMode.SINGLE_SERVICE,
                        List.of(SERVICE), List.of(plan(101, 102)), 3));

        PersistenceService second =
                new PersistenceService("hana02", 39040, "indexserver", 3);
        IllegalArgumentException scaleOut = assertThrows(IllegalArgumentException.class,
                () -> SourceTopology.discover(SourceTopologyMode.SINGLE_SERVICE,
                        List.of(SERVICE, second), List.of(plan(101, 102)), 2));
        assertTrue(scaleOut.getMessage().contains("scale_out"));
    }

    @Test
    void detectsContainerAndServiceTopologyDrift() {
        SourceTopology startup = SourceTopology.discover(
                SourceTopologyMode.AUTO, List.of(SERVICE), List.of(plan(101, 102)), 2);
        SourceTopology changedContainers = SourceTopology.discover(
                SourceTopologyMode.AUTO, List.of(SERVICE), List.of(plan(101, 103)), 2);

        assertNotEquals(startup.fingerprint(), changedContainers.fingerprint());
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> startup.requireUnchanged(changedContainers));
        assertTrue(error.getMessage().contains("FATAL_TOPOLOGY_CHANGE"));
    }

    @Test
    void exposesStableScaleOutStreamsAndRejectsDuplicateVolume() {
        PersistenceService second =
                new PersistenceService("hana02", 39040, "indexserver", 3);
        SourceTopology topology = SourceTopology.discover(
                SourceTopologyMode.SCALE_OUT, List.of(SERVICE, second),
                List.of(scaleOutPlan()), 2);

        assertEquals(List.of(new SourceStreamId(2, "indexserver"),
                new SourceStreamId(3, "indexserver")), topology.streams());

        PersistenceService duplicateVolume =
                new PersistenceService("hana02", 39040, "indexserver", 2);
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> SourceTopology.discover(SourceTopologyMode.SCALE_OUT,
                        List.of(SERVICE, duplicateVolume), List.of(plan(101, 102)), 2));
        assertTrue(error.getMessage().contains("duplicate active persistence volume_id"));
    }

    private static TableDecodePlan plan(long firstContainer, long secondContainer) {
        return TableDecodePlan.compile(new TableMetadata(
                "CDC", "PARTITIONED", "COLUMN", 99,
                List.of(
                        new TableMetadata.Partition(
                                1, firstContainer, "hana01", 39040, 2, "indexserver"),
                        new TableMetadata.Partition(
                                2, secondContainer, "hana01", 39040, 2, "indexserver")),
                false,
                List.of(
                        new TableMetadata.Column(1, 201, "ID", "BIGINT", Types.BIGINT,
                                8, 19, 0, false, false, false, ""),
                        new TableMetadata.Column(2, 202, "IS_DELETED", "BOOLEAN",
                                Types.BOOLEAN, 1, 0, 0, false, false, false, "FALSE")),
                List.of(), List.of("ID")));
    }

    private static TableDecodePlan scaleOutPlan() {
        return TableDecodePlan.compile(new TableMetadata(
                "CDC", "PARTITIONED", "COLUMN", 99,
                List.of(
                        new TableMetadata.Partition(
                                1, 101, "hana01", 39040, 2, "indexserver"),
                        new TableMetadata.Partition(
                                2, 102, "hana02", 39040, 3, "indexserver")),
                false,
                List.of(
                        new TableMetadata.Column(1, 201, "ID", "BIGINT", Types.BIGINT,
                                8, 19, 0, false, false, false, ""),
                        new TableMetadata.Column(2, 202, "IS_DELETED", "BOOLEAN",
                                Types.BOOLEAN, 1, 0, 0, false, false, false, "FALSE")),
                List.of(), List.of("ID")));
    }
}

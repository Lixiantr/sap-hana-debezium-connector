package io.hanacdc.connect;

import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class SourceEpochTest {
    @Test
    void prototypeIdentityIsDeterministicAndSensitiveToHistory() {
        SourceEpoch first = SourceEpoch.prototype("hana01", "RUN-A", 2, 0, 0);
        SourceEpoch same = SourceEpoch.prototype("hana01", "RUN-A", 2, 0, 0);
        SourceEpoch differentRun = SourceEpoch.prototype("hana01", "RUN-B", 2, 0, 0);
        SourceEpoch differentVolume = SourceEpoch.prototype("hana01", "RUN-A", 2, 1, 0);

        assertEquals(first, same);
        assertNotEquals(first.id(), differentRun.id());
        assertNotEquals(first.id(), differentVolume.id());
    }

    @Test
    void rejectsTamperedEpochId() {
        SourceEpoch value = SourceEpoch.prototype("hana01", "RUN", 2, 0, 0);
        assertThrows(IllegalArgumentException.class, () -> new SourceEpoch(
                value.version(), value.systemId(), value.databaseName(), value.sourceId(),
                value.volume(), value.partition(), value.databaseId(),
                value.schemaSetFingerprint(), "0".repeat(64)));
    }

    @Test
    void schemaSetFingerprintChangesWhenPartitionTopologyChanges() {
        TableDecodePlan one = plan(List.of(partition(0, 1001)));
        TableDecodePlan two = plan(List.of(partition(0, 1001), partition(1, 1002)));

        assertNotEquals(SourceEpoch.schemaSetFingerprint(List.of(one)),
                SourceEpoch.schemaSetFingerprint(List.of(two)));
    }

    private static TableDecodePlan plan(List<TableMetadata.Partition> partitions) {
        return TableDecodePlan.compile(new TableMetadata(
                "CDC", "T", "COLUMN", 42, partitions, false,
                List.of(
                        new TableMetadata.Column(1, 201, "ID", "BIGINT", Types.BIGINT,
                                19, 19, 0, false, false, false, ""),
                        new TableMetadata.Column(2, 202, "IS_DELETED", "BOOLEAN", Types.BOOLEAN,
                                1, 1, 0, false, false, false, "FALSE")),
                List.of(), List.of("ID")));
    }

    private static TableMetadata.Partition partition(int partId, long containerId) {
        return new TableMetadata.Partition(partId, containerId,
                "hana01", 39003, 2, "indexserver");
    }
}

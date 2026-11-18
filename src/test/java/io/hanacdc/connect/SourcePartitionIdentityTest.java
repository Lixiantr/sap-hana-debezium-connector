package io.hanacdc.connect;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class SourcePartitionIdentityTest {
    @Test
    void catalogIdentityIsOneFieldAndEscapesDelimiters() {
        Map<String, Object> partition = SourcePartitionIdentity.catalog(
                "hana | primary", 2, 3, 0, "HXE=1", "TENANT/A");

        assertEquals(Map.of("partition_id",
                "v1|mode=catalog|server=hana%20%7C%20primary|source_id=2|volume=3"
                        + "|partition=0|system_id=HXE%3D1|database=TENANT%2FA"), partition);
        assertThrows(UnsupportedOperationException.class,
                () -> partition.put("partition_id", "other"));
    }

    @Test
    void jsonRoundTripPreservesExactOffsetKeyBytes() {
        Map<String, Object> partition = SourcePartitionIdentity.catalog(
                "hana01", 2, 2, 0, "HXE", "HXE");
        JsonConverter converter = new JsonConverter();
        converter.configure(Map.of("schemas.enable", false), true);

        byte[] first = converter.fromConnectData("offsets", null, partition);
        SchemaAndValue decoded = converter.toConnectData("offsets", first);
        byte[] second = converter.fromConnectData(
                "offsets", decoded.schema(), decoded.value());

        assertArrayEquals(first, second);
        assertEquals("{\"partition_id\":\"v1|mode=catalog|server=hana01|source_id=2"
                        + "|volume=2|partition=0|system_id=HXE|database=HXE\"}",
                new String(first, StandardCharsets.UTF_8));
    }

    @Test
    void rejectsInvalidCoordinatesAndBlankIdentityParts() {
        assertThrows(IllegalArgumentException.class,
                () -> SourcePartitionIdentity.filesystem("hana", -1, 2, 0));
        assertThrows(IllegalArgumentException.class,
                () -> SourcePartitionIdentity.catalog(" ", 1, 2, 0, "HXE", "HXE"));
        assertThrows(NullPointerException.class,
                () -> SourcePartitionIdentity.catalog("hana", 1, 2, 0, "HXE", null));
    }

    @Test
    void scaleOutPartitionIsPinnedToTopologyFingerprint() {
        Map<String, Object> partition = SourcePartitionIdentity.catalogScaleOut(
                "hana", "HXE", "HXE", "a".repeat(64));

        assertEquals("v1|mode=catalog_scale_out|server=hana|system_id=HXE"
                        + "|database=HXE|topology=" + "a".repeat(64),
                partition.get(SourcePartitionIdentity.KEY));
    }
}

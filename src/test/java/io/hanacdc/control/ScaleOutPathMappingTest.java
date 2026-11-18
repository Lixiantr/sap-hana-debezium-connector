package io.hanacdc.control;

import io.hanacdc.connect.SourceStreamId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ScaleOutPathMappingTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void resolvesSameHanaPathToDifferentVolumeMounts() {
        Path first = temporaryDirectory.resolve("host-a");
        Path second = temporaryDirectory.resolve("host-b");
        ScaleOutPathMapping mapping = ScaleOutPathMapping.parse(
                "2@/hana/log=" + first + ";3@/hana/log=" + second);
        var streams = List.of(new SourceStreamId(2, "indexserver"),
                new SourceStreamId(3, "indexserver"));
        var resolved = mapping.requireStreams(streams);

        assertEquals(first.toAbsolutePath().resolve("backup.bin"),
                resolved.get(streams.get(0)).resolve("/hana/log/backup.bin"));
        assertEquals(second.toAbsolutePath().resolve("backup.bin"),
                resolved.get(streams.get(1)).resolve("/hana/log/backup.bin"));
    }

    @Test
    void rejectsMissingAndUnknownVolumes() {
        ScaleOutPathMapping missing = ScaleOutPathMapping.parse("2@/hana=/mnt/a");
        assertThrows(IllegalArgumentException.class, () -> missing.requireStreams(
                List.of(new SourceStreamId(2, "indexserver"),
                        new SourceStreamId(3, "indexserver"))));

        ScaleOutPathMapping unknown = ScaleOutPathMapping.parse(
                "2@/hana=/mnt/a;3@/hana=/mnt/b;4@/hana=/mnt/c");
        assertThrows(IllegalArgumentException.class, () -> unknown.requireStreams(
                List.of(new SourceStreamId(2, "indexserver"),
                        new SourceStreamId(3, "indexserver"))));
    }
}

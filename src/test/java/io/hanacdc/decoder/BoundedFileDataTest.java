package io.hanacdc.decoder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class BoundedFileDataTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void addressesSparseFileBeyondTwoGiBWithBoundedWindows() throws Exception {
        Path path = temporaryDirectory.resolve("large-backup.bin");
        long position = (long) Integer.MAX_VALUE + 8193;
        byte[] marker = new byte[] {
                0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
                0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f
        };
        try (FileChannel channel = FileChannel.open(
                path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(marker), position);
        }

        try (BoundedFileData data = new BoundedFileData(path, 4096)) {
            assertEquals(position + marker.length, data.limit());
            byte[] actual = new byte[marker.length];
            data.copy(position, position + marker.length, actual, 0);
            assertArrayEquals(marker, actual);
            assertEquals(
                    ByteBuffer.wrap(marker).order(ByteOrder.LITTLE_ENDIAN).getLong(),
                    data.getLong(position));
        }
    }

    @Test
    void readsPrimitiveAndRangeAcrossWindowBoundary() throws Exception {
        Path path = temporaryDirectory.resolve("boundary.bin");
        byte[] content = new byte[8192];
        for (int index = 0; index < content.length; index++) {
            content[index] = (byte) index;
        }
        java.nio.file.Files.write(path, content);

        try (BoundedFileData data = new BoundedFileData(path, 4096)) {
            long position = 4093;
            assertEquals(
                    ByteBuffer.wrap(content, (int) position, Long.BYTES)
                            .order(ByteOrder.LITTLE_ENDIAN)
                            .getLong(),
                    data.getLong(position));
            byte[] actual = new byte[32];
            data.copy(position, position + actual.length, actual, 0);
            assertArrayEquals(
                    java.util.Arrays.copyOfRange(
                            content, (int) position, (int) position + actual.length),
                    actual);
        }
    }
}

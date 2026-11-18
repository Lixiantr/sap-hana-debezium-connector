package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NativeLobReconstructorTest {
    private static final long LOCATOR = 0xfe000194L;

    @Test
    void virtualFileBecomesVisibleOnlyAfterAllContiguousChunks() throws Exception {
        try (var session = new NativeLobReconstructor.Session()) {
            session.accept(header(1, NativeRecordReader.OUTER_VIRTUAL_FILE,
                    NativeRecordReader.VF_TRUNCATE, LOCATOR, 0), new byte[0]);
            session.accept(header(2, NativeRecordReader.OUTER_VIRTUAL_FILE,
                    NativeRecordReader.VF_OVERWRITE, LOCATOR, 36),
                    overwrite(8, new byte[4]));

            var incomplete = assertThrows(java.io.IOException.class, session::finish);
            assertTrue(incomplete.getMessage().contains("FATAL_INCOMPLETE_LARGE_VALUE"));

            session.accept(header(3, NativeRecordReader.OUTER_VIRTUAL_FILE,
                    NativeRecordReader.VF_OVERWRITE, LOCATOR, 36),
                    overwrite(4, "done".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            var values = session.finish();
            assertEquals(1, values.size());
            assertArrayEquals("done".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    values.get(LOCATOR).data());
        }
    }

    @Test
    void gapAndDuplicateLocatorAreFatalNotRetriableData() throws Exception {
        try (var session = new NativeLobReconstructor.Session()) {
            session.accept(header(1, NativeRecordReader.OUTER_VIRTUAL_FILE,
                    NativeRecordReader.VF_TRUNCATE, LOCATOR, 0), new byte[0]);
            session.accept(header(2, NativeRecordReader.OUTER_VIRTUAL_FILE,
                    NativeRecordReader.VF_OVERWRITE, LOCATOR, 36),
                    overwrite(8, new byte[4]));
            var gap = assertThrows(java.io.IOException.class, () -> session.accept(
                    header(3, NativeRecordReader.OUTER_VIRTUAL_FILE,
                            NativeRecordReader.VF_OVERWRITE, LOCATOR, 36),
                    overwrite(3, new byte[4])));
            assertTrue(gap.getMessage().contains("FATAL_INCOMPLETE_LARGE_VALUE"));
        }

        byte[] inline = inline("value".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        try (var session = new NativeLobReconstructor.Session()) {
            session.accept(header(1, NativeRecordReader.OUTER_LOB_WRITE,
                    -1, -1, inline.length), inline);
            var duplicate = assertThrows(java.io.IOException.class, () -> session.accept(
                    header(2, NativeRecordReader.OUTER_LOB_WRITE,
                            -1, -1, inline.length), inline));
            assertTrue(duplicate.getMessage().contains("FATAL_LOB_RECONSTRUCTION"));
        }
    }

    @Test
    void configuredLobSizeAndOpenLocatorLimitsFailClosed() throws Exception {
        try (var session = new NativeLobReconstructor.Session(
                new NativeLobReconstructor.Limits(64, 8, 1))) {
            session.accept(header(1, NativeRecordReader.OUTER_VIRTUAL_FILE,
                    NativeRecordReader.VF_TRUNCATE, LOCATOR, 0), new byte[0]);
            var locatorLimit = assertThrows(java.io.IOException.class,
                    () -> session.accept(header(2,
                            NativeRecordReader.OUTER_VIRTUAL_FILE,
                            NativeRecordReader.VF_TRUNCATE, LOCATOR + 1, 0), new byte[0]));
            assertTrue(locatorLimit.getMessage().contains("FATAL_RESOURCE_LIMIT"));
        }

        try (var session = new NativeLobReconstructor.Session(
                new NativeLobReconstructor.Limits(64, 8, 1))) {
            var valueLimit = assertThrows(java.io.IOException.class,
                    () -> session.accept(header(1,
                            NativeRecordReader.OUTER_VIRTUAL_FILE,
                            NativeRecordReader.VF_OVERWRITE, LOCATOR, 36),
                            overwrite(9, new byte[4])));
            assertTrue(valueLimit.getMessage().contains("FATAL_EVENT_TOO_LARGE"));
        }
    }

    @Test
    void ignoresCatalogLobGrammarWhenLocatorIsNotRequired() throws Exception {
        byte[] catalogLob = new byte[40];
        ByteBuffer.wrap(catalogLob).order(ByteOrder.LITTLE_ENDIAN)
                .putLong(24, LOCATOR);
        try (var session = new NativeLobReconstructor.Session()) {
            session.acceptRequired(header(1, NativeRecordReader.OUTER_LOB_WRITE,
                    -1, -1, catalogLob.length), catalogLob, Set.of());
            assertTrue(session.finish().isEmpty());
        }
        try (var session = new NativeLobReconstructor.Session()) {
            var required = assertThrows(java.io.IOException.class,
                    () -> session.acceptRequired(
                            header(1, NativeRecordReader.OUTER_LOB_WRITE,
                                    -1, -1, catalogLob.length),
                            catalogLob, Set.of(LOCATOR)));
            assertTrue(required.getMessage().contains("LOB_WRITE header is truncated"));
        }
    }

    private static byte[] overwrite(int expectedRemaining, byte[] chunk) {
        ByteBuffer payload = ByteBuffer.allocate(32 + chunk.length)
                .order(ByteOrder.LITTLE_ENDIAN);
        payload.putInt(24, expectedRemaining);
        payload.putInt(28, 1);
        payload.position(32);
        payload.put(chunk);
        return payload.array();
    }

    private static byte[] inline(byte[] data) {
        ByteBuffer payload = ByteBuffer.allocate(48 + data.length)
                .order(ByteOrder.LITTLE_ENDIAN);
        payload.putLong(24, LOCATOR);
        payload.putLong(32, data.length);
        payload.putLong(40, data.length);
        payload.position(48);
        payload.put(data);
        return payload.array();
    }

    private static NativeRecordHeader header(
            long position, int outer, int operation, long container, int payloadSize) {
        return new NativeRecordHeader(
                new BackupFile(Path.of("log_backup_2_0_0_100.1"), 2, 0, 0, 100, 1),
                4096 + position * 64, position, 77, outer, payloadSize, 128,
                -1, container, operation, -1);
    }
}

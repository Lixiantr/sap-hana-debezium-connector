package io.hanacdc.decoder;

import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class NativeRecordReaderTest {
    @TempDir
    Path temporary;

    @Test
    void readsPayloadSizedNativeRecordsAtStrictLogPositions() throws Exception {
        long first = 1000;
        Path path = temporary.resolve("log_backup_2_0_1000_1004.123");
        ByteBuffer bytes = ByteBuffer.allocate(4096 + 4 * 64).order(ByteOrder.LITTLE_ENDIAN);

        int insertOffset = 4096;
        putOuter(bytes, insertOffset, NativeRecordReader.OUTER_UNIFIED_TABLE, 64, 71, first);
        bytes.putInt(insertOffset + 32, 0x104);
        bytes.putInt(insertOffset + 36, 337217991);
        bytes.putLong(insertOffset + 40, 0xfe0000017dL);
        bytes.putInt(insertOffset + 48, NativeRecordReader.UT_INSERT_DATA);
        bytes.putLong(insertOffset + 56, 7);
        bytes.putLong(insertOffset + 64, 0x1122334455667788L);

        int commitOffset = 4096 + 2 * 64;
        putOuter(bytes, commitOffset, NativeRecordReader.OUTER_COMMIT, 32, 71, first + 2);
        int rollbackOffset = commitOffset + 64;
        putOuter(bytes, rollbackOffset, NativeRecordReader.OUTER_ROLLBACK, 8, 72, first + 3);
        Files.write(path, bytes.array());

        BackupFile backup = BackupFile.parse(path).orElseThrow();
        var records = NativeRecordReader.readHeaders(backup);
        assertEquals(3, records.size());
        var insert = records.get(0);
        assertEquals(128, insert.allocationBytes());
        assertEquals("UNIFIED_TABLE", insert.outerType());
        assertEquals("INSERT_DATA", insert.operationType());
        assertEquals(337217991L, insert.poid());
        assertEquals("0x000000fe0000017d", insert.containerHex());
        assertEquals(7, insert.firstRowId());
        assertEquals(first + 2, records.get(1).position());
        assertEquals("COMMIT", records.get(1).outerType());
        assertEquals("ROLLBACK", records.get(2).outerType());

        byte[] expectedPayload = new byte[64];
        ByteBuffer expected = ByteBuffer.wrap(expectedPayload).order(ByteOrder.LITTLE_ENDIAN);
        expected.putInt(0, 0x104);
        expected.putInt(4, 337217991);
        expected.putLong(8, 0xfe0000017dL);
        expected.putInt(16, NativeRecordReader.UT_INSERT_DATA);
        expected.putLong(24, 7);
        expected.putLong(32, 0x1122334455667788L);
        assertArrayEquals(expectedPayload, NativeRecordReader.readPayload(insert));
    }

    @Test
    void skipsUnoccupiedLogPositionBeforeAValidRecord() throws Exception {
        long first = 2000;
        Path path = temporary.resolve("log_backup_2_0_2000_2002.456");
        ByteBuffer bytes = ByteBuffer.allocate(4096 + 2 * 64).order(ByteOrder.LITTLE_ENDIAN);
        putOuter(bytes, 4096 + 64, NativeRecordReader.OUTER_COMMIT, 32, 81, first + 1);
        Files.write(path, bytes.array());

        var records = NativeRecordReader.readHeaders(BackupFile.parse(path).orElseThrow());
        assertEquals(1, records.size());
        assertEquals(first + 1, records.get(0).position());
    }

    private static void putOuter(
            ByteBuffer target, int offset, int outerCode, int payloadSize, long tid, long position) {
        target.putInt(offset, outerCode);
        target.putInt(offset + 4, payloadSize);
        target.putLong(offset + 8, tid);
        target.putLong(offset + 16, position);
    }
}

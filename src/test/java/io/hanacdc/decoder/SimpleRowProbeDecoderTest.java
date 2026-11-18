package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

final class SimpleRowProbeDecoderTest {
    @TempDir
    Path temporary;

    @Test
    void decodesCharacterizedFourColumnInsertImage() throws Exception {
        Path path = temporary.resolve("log_backup_2_0_3000_3003.789");
        ByteBuffer file = ByteBuffer.allocate(4096 + 3 * 64).order(ByteOrder.LITTLE_ENDIAN);
        int offset = 4096;
        int payloadSize = 105;
        file.putInt(offset, NativeRecordReader.OUTER_UNIFIED_TABLE);
        file.putInt(offset + 4, payloadSize);
        file.putLong(offset + 8, 91);
        file.putLong(offset + 16, 3000);
        int payload = offset + 32;
        file.putInt(payload, 0x104);
        file.putInt(payload + 4, 1234);
        file.putLong(payload + 8, 0xfe0000018fL);
        file.putInt(payload + 16, NativeRecordReader.UT_INSERT_DATA);
        file.putLong(payload + 24, 2);
        file.put(payload + 32, (byte) 0x81);
        file.put(payload + 34, (byte) 4);
        file.put(payload + 35, (byte) 1);
        file.put(payload + 36, (byte) 1);
        for (int index = 0; index < 4; index++) file.putInt(payload + 37 + index * 4, 201 + index);
        int body = payload + 53;
        // This field grows with the variable payload (25 + UTF-8 length); it
        // is deliberately not treated as the data-area offset.
        file.put(body + 6, (byte) 29);
        int data = payload + 80;
        file.putLong(data, 910002);
        file.putLong(data + 8, 202);
        file.putInt(data + 16, 0);
        file.put(data + 20, (byte) 4);
        file.position(data + 21);
        file.put(new byte[] {'B', 'E', 'T', 'A'});
        Files.write(path, file.array());

        BackupFile backup = BackupFile.parse(path).orElseThrow();
        NativeRecordHeader record = NativeRecordReader.readHeaders(backup).get(0);
        var image = SimpleRowProbeDecoder.decode(record).orElseThrow();
        var row = image.fullRow().orElseThrow();
        assertEquals(910002, row.id());
        assertEquals(202, row.rowVersion());
        assertFalse(row.deleted());
        assertEquals("BETA", row.valueText());
    }
}

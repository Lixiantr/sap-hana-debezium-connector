package io.hanacdc.decoder;

import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;
import io.hanacdc.decoder.ParallelLogDecoder.DecodedFile;
import io.hanacdc.decoder.ParallelLogDecoder.DecoderSession;
import io.hanacdc.decoder.ParallelLogDecoder.EventPointer;
import io.hanacdc.decoder.ParallelLogDecoder.ScanConfig;
import io.hanacdc.testutil.TestLogFixture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ParallelLogDecoderTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void oneAndTwoThreadsProduceIdenticalOrderedOutput() throws Exception {
        String runId = "RUN";
        BackupFile backup = TestLogFixture.write(temporaryDirectory, runId);
        ParallelLogDecoder decoder = new ParallelLogDecoder();
        ScanConfig oneThread = new ScanConfig("1", 2, 1, 1, 1, true);
        ScanConfig twoThreads = new ScanConfig("2", 2, 1, 1, 1, true);

        try (DecodedFile one = decoder.decode(backup, runId, oneThread, null);
                DecodedFile two = decoder.decode(backup, runId, twoThreads, null)) {
            assertEquals(2, one.metrics().events());
            assertEquals(2, two.metrics().events());
            assertEquals(1, one.metrics().threads());
            assertEquals(2, two.metrics().threads());
            assertEquals(TestLogFixture.expectedFingerprint(runId), one.metrics().fingerprint());
            assertEquals(one.metrics().fingerprint(), two.metrics().fingerprint());
            assertEquals(eventProjection(one), eventProjection(two));
            assertEquals(List.of(
                    "42:3:false:payload-one:7:100",
                    "43:4:true:payload-two:8:104"), eventProjection(one));
        }
    }

    @Test
    void parsesBackupNameAndDiscoversGapFreeCorpus() throws Exception {
        BackupFile backup = TestLogFixture.write(temporaryDirectory, "RUN");
        assertEquals(2, backup.volume());
        assertEquals(0, backup.partition());
        assertEquals(TestLogFixture.FIRST, backup.first());
        assertEquals(TestLogFixture.LAST, backup.last());
        assertEquals(TestLogFixture.BACKUP_ID, backup.backupId());

        List<BackupFile> corpus = ParallelLogDecoder.discoverCorpus(
                temporaryDirectory, TestLogFixture.FIRST, TestLogFixture.LAST);
        assertEquals(List.of(backup), corpus);
        assertTrue(BackupFile.parse(backup.path()).isPresent());
        assertFalse(BackupFile.parse(temporaryDirectory.resolve("not-a-backup")).isPresent());
    }

    @Test
    void reconstructsEnvelopeAcrossInterleavedRecords() throws Exception {
        BackupFile backup = TestLogFixture.writeFragmented(temporaryDirectory, "RUN");
        ParallelLogDecoder decoder = new ParallelLogDecoder();
        ScanConfig config = new ScanConfig("3", 3, 1, 1, 1, true);

        try (DecodedFile decoded = decoder.decode(backup, "RUN", config, null)) {
            assertEquals(1, decoded.metrics().candidates());
            assertEquals(1, decoded.metrics().events());
            assertEquals(0, decoded.metrics().scanWarnings());
            EventPointer event = decoded.cursor().next();
            assertTrue(event.ownsPayload());
            assertEquals(
                    "payload-fragment-crosses-record-boundary",
                    event.payload());
            assertEquals("2026-08-03 12:34:56.123456",
                    event.timestamp());
        }
    }

    @Test
    void carriesUnresolvedEnvelopeAcrossContiguousBackups() throws Exception {
        List<BackupFile> backups = TestLogFixture.writeAcrossBackups(temporaryDirectory, "RUN");
        ParallelLogDecoder decoder = new ParallelLogDecoder();
        DecoderSession session = new DecoderSession();
        ScanConfig config = new ScanConfig("2", 2, 1, 1, 1, true);

        try (DecodedFile first = decoder.decode(backups.get(0), "RUN", config, null, session)) {
            assertEquals(0, first.metrics().events());
            assertEquals(0, first.metrics().scanWarnings());
            assertEquals(1, first.metrics().deferredFragments());
            assertTrue(session.hasPendingFragments());
        }
        try (DecodedFile second = decoder.decode(backups.get(1), "RUN", config, null, session)) {
            assertEquals(1, second.metrics().events());
            assertEquals(0, second.metrics().scanWarnings());
            assertEquals(0, second.metrics().deferredFragments());
            EventPointer event = second.cursor().next();
            assertEquals(10, event.sourceBackup().backupId());
            assertEquals(123, event.rowId());
            assertEquals("payload-fragment-crosses-backup-boundary",
                    event.payload());
        }
        session.finish();
        assertFalse(session.hasPendingFragments());
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "HANA_CDC_LARGE_TEST", matches = "true")
    void scansOneLogicalBackupLargerThanTwoGiB() throws Exception {
        int recordBytes = ParallelLogDecoder.MAX_RECORD_BYTES;
        int records = 257;
        long first = 100;
        long positionStep = recordBytes / 64L;
        long last = first + records * positionStep;
        Path path = temporaryDirectory.resolve(
                "log_backup_2_0_" + first + "_" + last + ".99");
        try (FileChannel channel = FileChannel.open(
                path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            ByteBuffer header = ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN);
            for (int index = 0; index < records; index++) {
                header.clear();
                header.position(4);
                header.putInt(recordBytes);
                header.putLong(index + 1L);
                header.putLong(first + index * positionStep);
                header.flip();
                long output = ParallelLogDecoder.LOG_RECORD_FILE_BASE
                        + (long) index * recordBytes;
                while (header.hasRemaining()) {
                    output += channel.write(header, output);
                }
            }
            channel.write(ByteBuffer.wrap(new byte[] {0}),
                    ParallelLogDecoder.LOG_RECORD_FILE_BASE
                            + (long) records * recordBytes - 1);
        }

        BackupFile backup = BackupFile.parse(path).orElseThrow();
        ParallelLogDecoder decoder = new ParallelLogDecoder();
        try (DecodedFile decoded = decoder.decode(
                backup,
                "NO_EVENTS",
                new ScanConfig("4", 4, 256L * 1024 * 1024, 256, 4, false),
                null)) {
            assertTrue(decoded.metrics().bytes() > Integer.MAX_VALUE);
            assertEquals(records, decoded.metrics().records());
            assertEquals(0, decoded.metrics().events());
            assertEquals(0, decoded.metrics().scanWarnings());
        }
    }

    private static List<String> eventProjection(DecodedFile decoded) {
        java.util.ArrayList<String> result = new java.util.ArrayList<>();
        var cursor = decoded.cursor();
        while (cursor.hasNext()) {
            EventPointer event = cursor.next();
            result.add(event.rowId() + ":"
                    + event.rowVersion() + ":"
                    + event.deleted() + ":"
                    + event.payload() + ":"
                    + event.transactionId() + ":"
                    + event.logPosition());
        }
        return result;
    }
}

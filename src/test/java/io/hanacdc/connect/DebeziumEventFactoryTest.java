package io.hanacdc.connect;

import io.hanacdc.decoder.ParallelLogDecoder;
import io.hanacdc.decoder.ParallelLogDecoder.DecodedFile;
import io.hanacdc.decoder.ParallelLogDecoder.EventPointer;
import io.hanacdc.decoder.ParallelLogDecoder.ScanConfig;
import io.hanacdc.testutil.TestLogFixture;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;

final class DebeziumEventFactoryTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void emitsDebeziumCompatibleUpsertAndDeleteEnvelopes() throws Exception {
        String runId = "RUN";
        var backup = TestLogFixture.write(temporaryDirectory, runId);
        ParallelLogDecoder decoder = new ParallelLogDecoder();
        ScanConfig config = new ScanConfig("2", 2, 1, 1, 1, false);
        DebeziumEventFactory factory =
                new DebeziumEventFactory("hana.events", "hana01", "HANA", "BENCH", "CDC_LOAD");
        Map<String, Object> partition = Map.of("server", "hana01");

        try (DecodedFile decoded = decoder.decode(backup, runId, config, null)) {
            var cursor = decoded.cursor();
            EventPointer firstEvent = cursor.next();
            SourceRecord update = factory.create(
                    firstEvent, partition, Map.of("event_offset", firstEvent.sourceEventOffset()));
            EventPointer secondEvent = cursor.next();
            SourceRecord delete = factory.create(
                    secondEvent, partition, Map.of("event_offset", secondEvent.sourceEventOffset()));

            assertEquals("hana.events", update.topic());
            assertEquals(42L, ((Struct) update.key()).getInt64("id"));
            Struct updateEnvelope = (Struct) update.value();
            assertEquals("u", updateEnvelope.getString("op"));
            assertNull(updateEnvelope.get("before"));
            Struct after = updateEnvelope.getStruct("after");
            assertEquals(42L, after.getInt64("id"));
            assertEquals("payload-one", after.getString("payload"));
            Struct source = updateEnvelope.getStruct("source");
            assertEquals("sap-hana-log-backup", source.getString("connector"));
            assertEquals(100L, source.getInt64("log_position"));
            assertEquals(7L, source.getInt64("tid"));
            assertNotNull(updateEnvelope.get("transaction"));

            Struct deleteEnvelope = (Struct) delete.value();
            assertEquals("d", deleteEnvelope.getString("op"));
            assertNull(deleteEnvelope.get("after"));
            assertEquals(43L, deleteEnvelope.getStruct("before").getInt64("id"));
            assertEquals(104L, deleteEnvelope.getStruct("source").getInt64("log_position"));
        }
    }
}

package io.hanacdc.connect;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

final class HeartbeatFactoryTest {
    @Test
    void createsDebeziumCompatibleHeartbeatWithoutChangingOffset() {
        Map<String, Object> partition = Map.of("server", "hana01");
        Map<String, Object> offset = Map.of("log_position", 123L);

        SourceRecord record = new HeartbeatFactory(
                "hana01", "__debezium-heartbeat.hana01")
                .create(partition, offset, 1_722_854_400_000L);

        assertEquals("__debezium-heartbeat.hana01", record.topic());
        assertSame(partition, record.sourcePartition());
        assertSame(offset, record.sourceOffset());
        assertEquals("io.debezium.connector.common.ServerNameKey",
                record.keySchema().name());
        assertEquals("hana01", ((Struct) record.key()).getString("serverName"));
        assertEquals("io.debezium.connector.common.Heartbeat",
                record.valueSchema().name());
        assertEquals(1_722_854_400_000L,
                ((Struct) record.value()).getInt64("ts_ms"));
    }
}

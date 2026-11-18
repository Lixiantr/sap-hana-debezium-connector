package io.hanacdc.cli;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/** Persistent low-overhead end-offset watcher for release performance gates. */
public final class KafkaTopicOffsetWatchMain {
    private KafkaTopicOffsetWatchMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException("usage: KafkaTopicOffsetWatchMain "
                    + "<bootstrap> <topic> <expected-records> <timeout-seconds> "
                    + "<poll-millis>");
        }
        String bootstrap = args[0];
        String topic = args[1];
        long expected = Long.parseLong(args[2]);
        Duration timeout = Duration.ofSeconds(Long.parseLong(args[3]));
        long pollMillis = Long.parseLong(args[4]);
        if (expected < 0 || timeout.isZero() || timeout.isNegative() || pollMillis <= 0) {
            throw new IllegalArgumentException("positive timeout/poll and non-negative expected required");
        }

        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "hana-cdc-offset-watch");
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
        long deadline = System.nanoTime() + timeout.toNanos();
        try (Admin admin = Admin.create(properties)) {
            while (System.nanoTime() < deadline) {
                try {
                    long count = endOffset(admin, topic);
                    if (count >= expected) {
                        System.out.printf("topic_offset_watch=PASS topic=%s records=%d "
                                        + "finish_epoch_ms=%d%n",
                                topic, count, System.currentTimeMillis());
                        return;
                    }
                }
                catch (Exception ignored) {
                    // The fresh topic may not exist until the connector starts.
                }
                Thread.sleep(pollMillis);
            }
        }
        throw new IllegalStateException("TOPIC_OFFSET_WATCH_TIMEOUT topic=" + topic
                + " expected=" + expected);
    }

    private static long endOffset(Admin admin, String topic) throws Exception {
        TopicDescription description = admin.describeTopics(List.of(topic))
                .allTopicNames().get(5, TimeUnit.SECONDS).get(topic);
        Map<TopicPartition, OffsetSpec> request = new LinkedHashMap<>();
        description.partitions().forEach(partition -> request.put(
                new TopicPartition(topic, partition.partition()), OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets =
                admin.listOffsets(request).all().get(5, TimeUnit.SECONDS);
        long count = 0;
        for (ListOffsetsResult.ListOffsetsResultInfo offset : offsets.values()) {
            count = Math.addExact(count, offset.offset());
        }
        return count;
    }
}

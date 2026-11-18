package io.hanacdc.cli;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/** Independent streaming audit of ordering, uniqueness and materialized PK state. */
public final class KafkaTopicAuditMain {
    private KafkaTopicAuditMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4 && args.length != 5
                && args.length != 7 && args.length != 8) {
            throw new IllegalArgumentException("usage: KafkaTopicAuditMain <bootstrap> <topic> "
                    + "<expected-unique> <expected-live|-1> "
                    + "[expected-min-duplicates] | "
                    + "[expected-c expected-u expected-d [expected-min-duplicates]]");
        }
        long expectedUnique = Long.parseLong(args[2]);
        long expectedLive = Long.parseLong(args[3]);
        ExpectedOperations expectedOperations = args.length == 7
                ? new ExpectedOperations(Long.parseLong(args[4]),
                        Long.parseLong(args[5]), Long.parseLong(args[6]))
                : null;
        if (args.length == 8) {
            expectedOperations = new ExpectedOperations(Long.parseLong(args[4]),
                    Long.parseLong(args[5]), Long.parseLong(args[6]));
        }
        long expectedMinDuplicates = switch (args.length) {
            case 5 -> Long.parseLong(args[4]);
            case 8 -> Long.parseLong(args[7]);
            default -> 0;
        };
        Audit audit = consume(args[0], args[1], expectedUnique);
        Result result = audit.require(expectedUnique, expectedLive, expectedOperations,
                expectedMinDuplicates);
        System.out.printf("topic_audit=PASS records=%d unique_events=%d duplicates=%d "
                        + "c=%d u=%d d=%d live_keys=%d first_position=%d "
                        + "last_position=%d live_key_sha256=%s%n",
                result.records(), result.uniqueEvents(), result.duplicates(),
                result.creates(), result.updates(), result.deletes(), result.liveKeys(),
                result.firstPosition(), result.lastPosition(), result.liveKeySha256());
    }

    private static Audit consume(String bootstrap, String topic, long expectedUnique) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,
                "hana-cdc-topic-audit-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10000");
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, Integer.toString(64 * 1024 * 1024));

        JsonConverter keyConverter = converter(true);
        JsonConverter valueConverter = converter(false);
        Audit audit = new Audit();
        long deadline = System.nanoTime() + Duration.ofMinutes(15).toNanos();
        int quietPolls = 0;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));
            while (System.nanoTime() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    if (audit.uniqueEvents() >= expectedUnique && ++quietPolls >= 4) {
                        break;
                    }
                    continue;
                }
                quietPolls = 0;
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    SchemaAndValue key = keyConverter.toConnectData(topic, record.key());
                    SchemaAndValue value = valueConverter.toConnectData(topic, record.value());
                    audit.accept((Struct) key.value(), (Struct) value.value());
                }
            }
        }
        return audit;
    }

    private static JsonConverter converter(boolean key) {
        JsonConverter converter = new JsonConverter();
        converter.configure(Map.of("schemas.enable", true), key);
        return converter;
    }

    static final class Audit {
        private final Map<String, String> identities = new HashMap<>();
        private final Set<String> liveKeys = new HashSet<>();
        private long records;
        private long creates;
        private long updates;
        private long deletes;
        private long firstPosition = -1;
        private long lastPosition = -1;

        void accept(Struct key, Struct envelope) {
            if (key == null || envelope == null) {
                throw new IllegalStateException("topic audit received null key/envelope");
            }
            Struct source = envelope.getStruct("source");
            if (source == null) {
                throw new IllegalStateException("topic audit received no source metadata");
            }
            String canonicalKey = canonicalKey(key);
            String operation = envelope.getString("op");
            long position = source.getInt64("log_position");
            long tid = source.getInt64("tid");
            Struct transaction = envelope.getStruct("transaction");
            if (transaction == null) {
                throw new IllegalStateException("topic audit received no transaction metadata");
            }
            long dataCollectionOrder = transaction.getInt64("data_collection_order");
            accept(canonicalKey, operation, position, tid, dataCollectionOrder);
        }

        void accept(String key, String operation, long position, long tid) {
            accept(key, operation, position, tid, 1);
        }

        void accept(String key, String operation, long position, long tid,
                long dataCollectionOrder) {
            records++;
            String identity = position + ":" + tid + ":" + dataCollectionOrder + ":" + key;
            String previousOperation = identities.putIfAbsent(identity, operation);
            if (previousOperation != null) {
                if (!previousOperation.equals(operation)) {
                    throw new IllegalStateException("duplicate event identity changed operation: "
                            + identity + " previous=" + previousOperation
                            + " current=" + operation);
                }
                return;
            }
            if (lastPosition > position) {
                throw new IllegalStateException("unique source order regressed: previous="
                        + lastPosition + " current=" + position);
            }
            if (firstPosition < 0) {
                firstPosition = position;
            }
            lastPosition = position;
            switch (operation) {
                case "c" -> {
                    creates++;
                    liveKeys.add(key);
                }
                case "u" -> {
                    updates++;
                    liveKeys.add(key);
                }
                case "d" -> {
                    deletes++;
                    liveKeys.remove(key);
                }
                default -> throw new IllegalStateException("unexpected CDC op=" + operation);
            }
        }

        long uniqueEvents() {
            return identities.size();
        }

        Result require(long expectedUnique, long expectedLive,
                ExpectedOperations expectedOperations) throws Exception {
            return require(expectedUnique, expectedLive, expectedOperations, 0);
        }

        Result require(long expectedUnique, long expectedLive,
                ExpectedOperations expectedOperations, long expectedMinDuplicates)
                throws Exception {
            if (expectedMinDuplicates < 0) {
                throw new IllegalArgumentException("expectedMinDuplicates must be >= 0");
            }
            if (identities.size() != expectedUnique) {
                throw new IllegalStateException("unique event count mismatch: actual="
                        + identities.size() + " expected=" + expectedUnique);
            }
            long duplicates = records - identities.size();
            if (expectedMinDuplicates == 0 && duplicates != 0) {
                throw new IllegalStateException("fresh topic contains duplicates: records="
                        + records + " unique=" + identities.size());
            }
            if (duplicates < expectedMinDuplicates) {
                throw new IllegalStateException("replay duplicate count is too low: actual="
                        + duplicates + " expected-min=" + expectedMinDuplicates);
            }
            if (expectedLive >= 0 && liveKeys.size() != expectedLive) {
                throw new IllegalStateException("live key count mismatch: actual="
                        + liveKeys.size() + " expected=" + expectedLive);
            }
            if (expectedOperations != null && (creates != expectedOperations.creates()
                    || updates != expectedOperations.updates()
                    || deletes != expectedOperations.deletes())) {
                throw new IllegalStateException("operation count mismatch: c=" + creates
                        + " u=" + updates + " d=" + deletes + " expected="
                        + expectedOperations);
            }
            List<String> ordered = new ArrayList<>(liveKeys);
            ordered.sort(Comparator.naturalOrder());
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String key : ordered) {
                digest.update(key.getBytes(StandardCharsets.UTF_8));
                digest.update((byte) '\n');
            }
            return new Result(records, identities.size(), duplicates,
                    creates, updates, deletes, liveKeys.size(), firstPosition, lastPosition,
                    HexFormat.of().formatHex(digest.digest()));
        }

        private static String canonicalKey(Struct key) {
            StringBuilder result = new StringBuilder();
            for (Field field : key.schema().fields()) {
                if (!result.isEmpty()) {
                    result.append('|');
                }
                result.append(field.name()).append('=').append(key.get(field));
            }
            if (result.isEmpty()) {
                throw new IllegalStateException("topic audit received empty key");
            }
            return result.toString();
        }
    }

    record ExpectedOperations(long creates, long updates, long deletes) {
    }

    record Result(
            long records,
            long uniqueEvents,
            long duplicates,
            long creates,
            long updates,
            long deletes,
            int liveKeys,
            long firstPosition,
            long lastPosition,
            String liveKeySha256) {
    }
}

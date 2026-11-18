package io.hanacdc.cli;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

import java.nio.charset.StandardCharsets;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/** Independent materialized-sink oracle for the seven-event DML corpus. */
public final class ContractCorpusSinkTruthMain {
    private ContractCorpusSinkTruthMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            throw new IllegalArgumentException(
                    "usage: ContractCorpusSinkTruthMain <bootstrap> <topic> [full|prefix3]");
        }
        String profile = args.length == 3 ? args[2] : "full";
        int expectedEvents = switch (profile) {
            case "full" -> 7;
            case "prefix3" -> 3;
            default -> throw new IllegalArgumentException("unknown sink truth profile=" + profile);
        };
        Materializer materializer = consume(args[0], args[1], expectedEvents);
        Result result = "prefix3".equals(profile)
                ? materializer.requireContractPrefix3()
                : materializer.requireContractTruth();
        System.out.printf("sink_truth=PASS profile=%s records=%d unique_events=%d c=%d u=%d d=%d "
                        + "live_rows=%d sha256=%s%n",
                profile, result.records(), result.uniqueEvents(), result.creates(),
                result.updates(), result.deletes(), result.liveRows(), result.sha256());
    }

    private static Materializer consume(String bootstrap, String topic, int expectedEvents) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,
                "hana-cdc-sink-truth-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        JsonConverter keyConverter = converter(true);
        JsonConverter valueConverter = converter(false);
        Materializer materializer = new Materializer();
        long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
        int quietPolls = 0;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));
            while (System.nanoTime() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    if (materializer.uniqueEvents() >= expectedEvents && ++quietPolls >= 4) {
                        break;
                    }
                    continue;
                }
                quietPolls = 0;
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    SchemaAndValue key = keyConverter.toConnectData(topic, record.key());
                    SchemaAndValue value = valueConverter.toConnectData(topic, record.value());
                    materializer.accept((Struct) key.value(), (Struct) value.value());
                }
            }
        }
        return materializer;
    }

    private static JsonConverter converter(boolean key) {
        JsonConverter converter = new JsonConverter();
        converter.configure(Map.of("schemas.enable", true), key);
        return converter;
    }

    static final class Materializer {
        private final Map<Long, Row> rows = new LinkedHashMap<>();
        private final Set<String> identities = new LinkedHashSet<>();
        private long records;
        private long creates;
        private long updates;
        private long deletes;

        void accept(Struct key, Struct envelope) {
            if (key == null || envelope == null) {
                throw new IllegalStateException("sink truth received null key/envelope");
            }
            Struct before = envelope.getStruct("before");
            Struct after = envelope.getStruct("after");
            Struct source = envelope.getStruct("source");
            String operation = envelope.getString("op");
            Struct image = after == null ? before : after;
            if (image == null || source == null) {
                throw new IllegalStateException("sink truth received incomplete envelope");
            }
            long id = number(image.get("ID"), "ID");
            Object keyId = key.get("ID");
            if (number(keyId, "key.ID") != id) {
                throw new IllegalStateException("event key differs from row ID");
            }
            accept(id, operation, row(image), source.getInt64("log_position"),
                    source.getInt64("tid"));
        }

        void accept(long id, String operation, Row row, long logPosition, long tid) {
            records++;
            String identity = logPosition + ":" + tid + ":" + id;
            boolean first = identities.add(identity);
            if (!first) {
                return;
            }
            switch (operation) {
                case "c" -> {
                    creates++;
                    rows.put(id, row);
                }
                case "u" -> {
                    updates++;
                    rows.put(id, row);
                }
                case "d" -> {
                    deletes++;
                    rows.remove(id);
                }
                default -> throw new IllegalStateException("unexpected sink op=" + operation);
            }
        }

        int uniqueEvents() {
            return identities.size();
        }

        Result requireContractTruth() throws Exception {
            if (identities.size() != 7 || creates != 3 || updates != 2 || deletes != 2) {
                throw new IllegalStateException("event truth mismatch: unique="
                        + identities.size() + " c=" + creates + " u=" + updates
                        + " d=" + deletes);
            }
            if (!rows.keySet().equals(Set.of(941804003L))) {
                throw new IllegalStateException("materialized PK truth mismatch: "
                        + rows.keySet());
            }
            Row row = rows.get(941804003L);
            Row expected = expectedLiveRow();
            if (!expected.equals(row)) {
                throw new IllegalStateException("materialized row truth mismatch: " + row);
            }
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(row.canonical().getBytes(StandardCharsets.UTF_8));
            return new Result(records, identities.size(), creates, updates, deletes,
                    rows.size(), HexFormat.of().formatHex(digest.digest()));
        }

        Result requireContractPrefix3() throws Exception {
            if (identities.size() != 3 || creates != 1 || updates != 1 || deletes != 1) {
                throw new IllegalStateException("prefix event truth mismatch: unique="
                        + identities.size() + " c=" + creates + " u=" + updates
                        + " d=" + deletes);
            }
            if (!rows.isEmpty()) {
                throw new IllegalStateException("prefix materialized PK truth mismatch: "
                        + rows.keySet());
            }
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return new Result(records, identities.size(), creates, updates, deletes,
                    0, HexFormat.of().formatHex(digest.digest()));
        }

        private static Row row(Struct image) {
            return new Row(number(image.get("ID"), "ID"),
                    (Boolean) image.get("IS_DELETED"),
                    number(image.get("ROW_VERSION"), "ROW_VERSION"),
                    (String) image.get("VALUE_TEXT"),
                    (BigDecimal) image.get("AMOUNT"),
                    (String) image.get("EVENT_TS"),
                    (String) image.get("C_CLOB"),
                    (String) image.get("C_NCLOB"),
                    new String((byte[]) image.get("C_BLOB"), StandardCharsets.UTF_8));
        }
    }

    record Row(
            long id,
            boolean deleted,
            long version,
            String text,
            BigDecimal amount,
            String eventTimestamp,
            String clob,
            String nclob,
            String blob) {
        String canonical() {
            List<Object> values = new ArrayList<>();
            values.add(id);
            values.add(deleted);
            values.add(version);
            values.add(text);
            values.add(amount.toPlainString());
            values.add(eventTimestamp);
            values.add(clob);
            values.add(nclob);
            values.add(blob);
            return values.toString();
        }
    }

    record Result(
            long records,
            int uniqueEvents,
            long creates,
            long updates,
            long deletes,
            int liveRows,
            String sha256) {
    }

    static Row expectedLiveRow() {
        return new Row(941804003L, false, 3L, "ID3_LIVE_AFTER_ROLLBACK",
                new BigDecimal("807.0400"),
                "2026-08-04T23:00:03.000000100", "CLOB-ID3_LIVE_AFTER_ROLLBACK",
                "NCLOB-данные-ID3_LIVE_AFTER_ROLLBACK",
                "BLOB-ID3_LIVE_AFTER_ROLLBACK");
    }

    private static long number(Object value, String name) {
        if (!(value instanceof Number number)) {
            throw new IllegalStateException(name + " is not numeric: " + value);
        }
        return number.longValue();
    }
}

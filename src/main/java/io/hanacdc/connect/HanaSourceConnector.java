package io.hanacdc.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.io.IOException;
import java.io.InputStream;

/**
 * Kafka Connect entry point. The connector creates one ordered
 * SourceTask; parallelism lives inside the decoder so transaction/source order
 * is not delegated to independent Connect tasks.
 */
public final class HanaSourceConnector extends SourceConnector {
    public static final String VERSION = loadVersion();
    public static final long MAX_SUPPORTED_BACKUP_BYTES = 65L * 1024L * 1024L * 1024L;
    private static final List<String> REMOVED_NATIVE_READER_SETTINGS = List.of(
            "log.reader.threads",
            "log.reader.max.threads",
            "log.reader.auto.target.bytes.per.thread",
            "log.reader.queue.max.records.per.worker",
            "log.reader.queue.capacity");

    public static final String SERVER_NAME = "server.name";
    public static final String BACKUP_DIR = "backup.dir";
    public static final String RUN_ID = "run.id";
    public static final String TOPIC = "topic";
    public static final String FIRST_POSITION = "first.position";
    public static final String LAST_POSITION = "last.position";
    public static final String SCAN_THREADS = "scan.threads";
    public static final String SCAN_MAX_THREADS = "scan.max.threads";
    public static final String TARGET_CHUNK_MIB = "scan.target.chunk.mib";
    public static final String CHUNKS_PER_THREAD = "scan.chunks.per.thread";
    public static final String LOG_PROCESSOR_THREADS = "log.processor.threads";
    public static final String LOG_PROCESSOR_MAX_THREADS = "log.processor.max.threads";
    public static final String LOG_PROCESSOR_CHUNKS_PER_THREAD =
            "log.processor.chunks.per.thread";
    public static final String POLL_MAX_BATCH_SIZE = "poll.max.batch.size";
    public static final String POLL_INTERVAL_MS = "poll.interval.ms";
    public static final String DATABASE_NAME = "database.name";
    public static final String SCHEMA_NAME = "schema.name";
    public static final String TABLE_NAME = "table.name";
    public static final String TABLE_INCLUDE_LIST = "table.include.list";
    public static final String PROTOTYPE_MODE = "prototype.mode";
    public static final String SNAPSHOT_MODE = "snapshot.mode";
    public static final String DISCOVERY_MODE = "log.backup.discovery.mode";
    public static final String PATH_MAPPING = "log.backup.path.mapping";
    public static final String DATABASE_URL = "database.connection.url";
    public static final String DATABASE_DRIVER = "database.jdbc.driver";
    public static final String DATABASE_USER = "database.user";
    public static final String DATABASE_PASSWORD = "database.password";
    public static final String SOURCE_ID = "source.id";
    public static final String SOURCE_VOLUME = "source.volume.id";
    public static final String SOURCE_PARTITION = "source.partition.id";
    public static final String SOURCE_TOPOLOGY_MODE = "source.topology.mode";
    public static final String SCALE_OUT_START_POSITIONS =
            "source.scale.out.start.positions";
    public static final String SCALE_OUT_CATALOG_THREADS =
            "source.scale.out.catalog.threads";
    public static final String SCALE_OUT_PATH_MAPPING =
            "log.backup.scale.out.path.mapping";
    public static final String TOPOLOGY_CHANGE_POLICY = "source.topology.change.policy";
    public static final String CONTROL_QUERY_TIMEOUT_SECONDS = "control.query.timeout.seconds";
    public static final String POLICY_REFRESH_INTERVAL_MS = "control.policy.refresh.interval.ms";
    public static final String POLICY_MAX_STALENESS_MS = "control.policy.max.staleness.ms";
    public static final String MAX_PUBLICATION_DELAY_MS = "log.backup.max.publication.delay.ms";
    public static final String MAX_BACKUP_BYTES = "log.backup.max.bytes";
    public static final String SUPPORTED_HANA_VERSIONS = "hana.version.supported.prefixes";
    public static final String POLICY_VIOLATION_MODE = "errors.hana.policy.mode";
    public static final String STATE_DIR = "state.dir";
    public static final String HEARTBEAT_INTERVAL_MS = "heartbeat.interval.ms";
    public static final String HEARTBEAT_TOPIC_PREFIX = "topic.heartbeat.prefix";
    public static final String HEARTBEAT_TOPIC_NAME = "topic.heartbeat.name";
    public static final String RETENTION_MIN_SECONDS = "log.backup.retention.min.seconds";
    public static final String RETENTION_SAFETY_MARGIN_SECONDS =
            "log.backup.retention.safety.margin.seconds";
    public static final String TRANSACTION_MAX_OPEN = "transaction.buffer.max.open";
    public static final String TRANSACTION_MAX_ROWS = "transaction.buffer.max.rows";
    public static final String TRANSACTION_MAX_BYTES = "transaction.buffer.max.bytes";
    public static final String LOB_MAX_BUFFERED_BYTES = "lob.buffer.max.bytes";
    public static final String LOB_MAX_BYTES = "lob.max.bytes";
    public static final String LOB_MAX_OPEN_LOCATORS = "lob.max.open.locators";
    public static final String EVENT_MAX_BYTES = "event.max.bytes";
    public static final String STATE_MAX_UNCOMMITTED_GENERATIONS =
            "state.max.uncommitted.generations";
    public static final String STATE_MAX_UNCOMMITTED_BYTES =
            "state.max.uncommitted.bytes";

    private static final String GROUP_SOURCE = "Source";
    private static final String GROUP_DECODER = "Parallel decoder";
    private static final String GROUP_RECORDS = "Records";
    private static final String GROUP_CONTROL = "HANA control plane";
    private static final String GROUP_LIMITS = "Resource limits";

    private static String loadVersion() {
        Properties values = new Properties();
        try (InputStream input = HanaSourceConnector.class.getResourceAsStream(
                "/hana-cdc-version.properties")) {
            if (input == null) {
                throw new IllegalStateException("connector version resource is missing");
            }
            values.load(input);
        }
        catch (IOException error) {
            throw new ExceptionInInitializerError(error);
        }
        String version = values.getProperty("version", "").strip();
        if (!version.matches("[0-9]+\\.[0-9]+\\.[0-9]+(?:[-.][0-9A-Za-z]+)*")) {
            throw new IllegalStateException("invalid connector version resource: " + version);
        }
        return version;
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(SERVER_NAME, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH,
                    "Logical source name used in Debezium-compatible source metadata.", GROUP_SOURCE, 1,
                    ConfigDef.Width.MEDIUM, "Server name")
            .define(BACKUP_DIR, ConfigDef.Type.STRING, "",
                    ConfigDef.Importance.HIGH,
                    "Directory containing immutable log backups in filesystem benchmark mode.", GROUP_SOURCE, 2,
                    ConfigDef.Width.LONG, "Backup directory")
            .define(RUN_ID, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH,
                    "HCDC1 benchmark run identifier.", GROUP_SOURCE, 3,
                    ConfigDef.Width.MEDIUM, "Run ID")
            .define(TOPIC, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH,
                    "Kafka topic receiving change events.", GROUP_SOURCE, 4,
                    ConfigDef.Width.LONG, "Topic")
            .define(FIRST_POSITION, ConfigDef.Type.LONG, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Range.atLeast(0L), ConfigDef.Importance.HIGH,
                    "Inclusive first HANA log position in the benchmark corpus.", GROUP_SOURCE, 5,
                    ConfigDef.Width.MEDIUM, "First position")
            .define(LAST_POSITION, ConfigDef.Type.LONG, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Range.atLeast(1L), ConfigDef.Importance.HIGH,
                    "Exclusive final HANA log position in the benchmark corpus.", GROUP_SOURCE, 6,
                    ConfigDef.Width.MEDIUM, "Last position")
            .define(PROTOTYPE_MODE, ConfigDef.Type.BOOLEAN, true,
                    ConfigDef.Importance.HIGH,
                    "Acknowledges that HCDC1 row decoding is benchmark-only, not a production HANA decoder.",
                    GROUP_SOURCE, 7, ConfigDef.Width.SHORT, "Prototype mode")
            .define(SNAPSHOT_MODE, ConfigDef.Type.STRING, "no_data",
                    ConfigDef.ValidString.in("no_data"), ConfigDef.Importance.HIGH,
                    "Streaming-only contract. Data snapshots and READ events are not supported.",
                    GROUP_SOURCE, 8, ConfigDef.Width.SHORT, "Snapshot mode")
            .define(SCAN_THREADS, ConfigDef.Type.STRING, "auto",
                    new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH,
                    "HCDC1 benchmark threads; deprecated alias for log.processor.threads.",
                    GROUP_DECODER, 91, ConfigDef.Width.SHORT, "Deprecated threads")
            .define(SCAN_MAX_THREADS, ConfigDef.Type.INT,
                    Math.max(1, Runtime.getRuntime().availableProcessors()),
                    ConfigDef.Range.atLeast(1), ConfigDef.Importance.MEDIUM,
                    "HCDC1 benchmark maximum; deprecated alias for log.processor.max.threads.",
                    GROUP_DECODER, 92,
                    ConfigDef.Width.SHORT, "Deprecated maximum threads")
            .define(TARGET_CHUNK_MIB, ConfigDef.Type.INT, 256,
                    ConfigDef.Range.atLeast(1), ConfigDef.Importance.MEDIUM,
                    "HCDC1 benchmark chunk target in MiB.", GROUP_DECODER, 93,
                    ConfigDef.Width.SHORT, "Benchmark target MiB")
            .define(CHUNKS_PER_THREAD, ConfigDef.Type.INT, 4,
                    ConfigDef.Range.atLeast(1), ConfigDef.Importance.MEDIUM,
                    "Deprecated alias for log.processor.chunks.per.thread.",
                    GROUP_DECODER, 94, ConfigDef.Width.SHORT, "Deprecated chunks")
            .define(LOG_PROCESSOR_THREADS, ConfigDef.Type.STRING, "auto",
                    new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH,
                    "Row/LOB decoding and ordered event-building threads, or auto.",
                    GROUP_DECODER, 1, ConfigDef.Width.SHORT, "Processing threads")
            .define(LOG_PROCESSOR_MAX_THREADS, ConfigDef.Type.INT, 6,
                    ConfigDef.Range.atLeast(1), ConfigDef.Importance.HIGH,
                    "Upper bound for row/LOB decoding and event-building threads.",
                    GROUP_DECODER, 2, ConfigDef.Width.SHORT,
                    "Maximum processing threads")
            .define(LOG_PROCESSOR_CHUNKS_PER_THREAD, ConfigDef.Type.INT, 4,
                    ConfigDef.Range.atLeast(1), ConfigDef.Importance.MEDIUM,
                    "Ordered processing work chunks made available per processing thread.",
                    GROUP_DECODER, 3, ConfigDef.Width.SHORT,
                    "Processing chunks per thread")
            .define(POLL_MAX_BATCH_SIZE, ConfigDef.Type.INT, 4096,
                    ConfigDef.Range.between(1, 100_000), ConfigDef.Importance.MEDIUM,
                    "Maximum SourceRecords returned from one poll.", GROUP_RECORDS, 1,
                    ConfigDef.Width.SHORT, "Poll batch size")
            .define(POLL_INTERVAL_MS, ConfigDef.Type.LONG, 250L,
                    ConfigDef.Range.atLeast(1L), ConfigDef.Importance.LOW,
                    "Wait after reaching the current end of the immutable corpus.", GROUP_RECORDS, 2,
                    ConfigDef.Width.SHORT, "Poll interval ms")
            .define(DATABASE_NAME, ConfigDef.Type.STRING, "HANA",
                    ConfigDef.Importance.LOW, "Database name in source metadata.", GROUP_RECORDS, 3,
                    ConfigDef.Width.MEDIUM, "Database")
            .define(SCHEMA_NAME, ConfigDef.Type.STRING, "BENCH",
                    ConfigDef.Importance.LOW, "Schema name in source metadata.", GROUP_RECORDS, 4,
                    ConfigDef.Width.MEDIUM, "Schema")
            .define(TABLE_NAME, ConfigDef.Type.STRING, "CDC_LOAD",
                    ConfigDef.Importance.LOW, "Table name in source metadata.", GROUP_RECORDS, 5,
                    ConfigDef.Width.MEDIUM, "Table")
            .define(TABLE_INCLUDE_LIST, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Anchored regular expressions matching captured schema.table identifiers. "
                            + "Non-matching table records are classified but not row-decoded.",
                    GROUP_RECORDS, 6, ConfigDef.Width.LONG, "Included tables")
            .define(DISCOVERY_MODE, ConfigDef.Type.STRING, "catalog",
                    ConfigDef.ValidString.in("catalog", "filesystem"), ConfigDef.Importance.HIGH,
                    "catalog uses successful HANA backup-catalog entries; filesystem is benchmark-only.",
                    GROUP_CONTROL, 1, ConfigDef.Width.SHORT, "Discovery mode")
            .define(PATH_MAPPING, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                    "Semicolon-separated allowlisted HANA-root=local-root mappings.",
                    GROUP_CONTROL, 2, ConfigDef.Width.LONG, "Backup path mapping")
            .define(DATABASE_URL, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                    "Direct private-LAN SAP HANA JDBC URL.", GROUP_CONTROL, 3,
                    ConfigDef.Width.LONG, "JDBC URL")
            .define(DATABASE_DRIVER, ConfigDef.Type.STRING, "com.sap.db.jdbc.Driver",
                    ConfigDef.Importance.MEDIUM, "SAP HANA JDBC driver class available on plugin.path.",
                    GROUP_CONTROL, 4, ConfigDef.Width.LONG, "JDBC driver")
            .define(DATABASE_USER, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                    "Dedicated read-only HANA catalog user.", GROUP_CONTROL, 5,
                    ConfigDef.Width.MEDIUM, "Database user")
            .define(DATABASE_PASSWORD, ConfigDef.Type.PASSWORD, "", ConfigDef.Importance.HIGH,
                    "Password for the read-only HANA catalog user.", GROUP_CONTROL, 6,
                    ConfigDef.Width.MEDIUM, "Database password")
            .define(SOURCE_ID, ConfigDef.Type.LONG, 2L, ConfigDef.Range.atLeast(0L),
                    ConfigDef.Importance.HIGH, "HANA persistence source/volume ID.",
                    GROUP_CONTROL, 7, ConfigDef.Width.SHORT, "Source ID")
            .define(SOURCE_VOLUME, ConfigDef.Type.LONG, 2L, ConfigDef.Range.atLeast(0L),
                    ConfigDef.Importance.HIGH, "Volume encoded in log-backup file names.",
                    GROUP_CONTROL, 8, ConfigDef.Width.SHORT, "Volume ID")
            .define(SOURCE_PARTITION, ConfigDef.Type.LONG, 0L, ConfigDef.Range.atLeast(0L),
                    ConfigDef.Importance.HIGH, "Partition encoded in log-backup file names.",
                    GROUP_CONTROL, 9, ConfigDef.Width.SHORT, "Partition ID")
            .define(SOURCE_TOPOLOGY_MODE, ConfigDef.Type.STRING, "single_container",
                    ConfigDef.ValidString.in("single_container", "single_service", "scale_out", "auto"),
                    ConfigDef.Importance.HIGH,
                    "Maximum HANA source topology. auto discovers and pins one topology per epoch.",
                    GROUP_CONTROL, 10, ConfigDef.Width.SHORT, "Source topology")
            .define(TOPOLOGY_CHANGE_POLICY, ConfigDef.Type.STRING, "fail",
                    ConfigDef.ValidString.in("fail"), ConfigDef.Importance.HIGH,
                    "Fail closed when persistence services or selected-table leaf partitions change.",
                    GROUP_CONTROL, 11, ConfigDef.Width.SHORT, "Topology change policy")
            .define(SCALE_OUT_START_POSITIONS, ConfigDef.Type.STRING, "",
                    ConfigDef.Importance.HIGH,
                    "Optional comma-separated volume=position vector. Blank starts every "
                            + "stream at its current successful-backup HWM.",
                    GROUP_CONTROL, 12, ConfigDef.Width.LONG,
                    "Scale-out start positions")
            .define(SCALE_OUT_CATALOG_THREADS, ConfigDef.Type.INT, 4,
                    ConfigDef.Range.atLeast(1), ConfigDef.Importance.MEDIUM,
                    "Maximum concurrent per-volume catalog and watermark queries.",
                    GROUP_CONTROL, 13, ConfigDef.Width.SHORT,
                    "Scale-out catalog threads")
            .define(SCALE_OUT_PATH_MAPPING, ConfigDef.Type.STRING, "",
                    ConfigDef.Importance.HIGH,
                    "Optional per-volume mappings in volume@hana-root=local-root format; "
                            + "blank reuses log.backup.path.mapping for every volume.",
                    GROUP_CONTROL, 14, ConfigDef.Width.LONG,
                    "Scale-out backup path mapping")
            .define(CONTROL_QUERY_TIMEOUT_SECONDS, ConfigDef.Type.INT, 30,
                    ConfigDef.Range.between(1, 300), ConfigDef.Importance.MEDIUM,
                    "Timeout for one read-only HANA control-plane query.",
                    GROUP_CONTROL, 15, ConfigDef.Width.SHORT, "Query timeout")
            .define(POLICY_REFRESH_INTERVAL_MS, ConfigDef.Type.LONG, 10_000L,
                    ConfigDef.Range.atLeast(1_000L), ConfigDef.Importance.MEDIUM,
                    "Independent refresh interval for effective HANA policy and health.",
                    GROUP_CONTROL, 16, ConfigDef.Width.SHORT, "Policy refresh")
            .define(POLICY_MAX_STALENESS_MS, ConfigDef.Type.LONG, 60_000L,
                    ConfigDef.Range.atLeast(1_000L), ConfigDef.Importance.HIGH,
                    "Maximum age of the last successful background policy refresh.",
                    GROUP_CONTROL, 17, ConfigDef.Width.SHORT, "Maximum policy staleness")
            .define(MAX_PUBLICATION_DELAY_MS, ConfigDef.Type.LONG, 900_000L,
                    ConfigDef.Range.atLeast(1_000L), ConfigDef.Importance.HIGH,
                    "Maximum acceptable HANA log-backup timeout budget.",
                    GROUP_CONTROL, 18, ConfigDef.Width.SHORT, "Maximum HANA delay")
            .define(MAX_BACKUP_BYTES, ConfigDef.Type.LONG, MAX_SUPPORTED_BACKUP_BYTES,
                    ConfigDef.Range.atLeast(1L), ConfigDef.Importance.HIGH,
                    "Maximum log-backup size supported by this reader build.",
                    GROUP_CONTROL, 19, ConfigDef.Width.MEDIUM, "Maximum backup bytes")
            .define(SUPPORTED_HANA_VERSIONS, ConfigDef.Type.LIST, List.of("2.00.088"),
                    ConfigDef.Importance.HIGH, "Tested HANA version prefixes.",
                    GROUP_CONTROL, 20, ConfigDef.Width.LONG, "Supported HANA versions")
            .define(POLICY_VIOLATION_MODE, ConfigDef.Type.STRING, "fail",
                    ConfigDef.ValidString.in("fail", "warn"), ConfigDef.Importance.HIGH,
                    "fail rejects startup on latency policy violations; warn is test-only.",
                    GROUP_CONTROL, 21, ConfigDef.Width.SHORT, "Policy violation mode")
            .define(STATE_DIR, ConfigDef.Type.STRING, "",
                    ConfigDef.Importance.HIGH,
                    "Durable shared directory for immutable native reducer checkpoints.",
                    GROUP_CONTROL, 22, ConfigDef.Width.LONG, "State directory")
            .define(RETENTION_MIN_SECONDS, ConfigDef.Type.LONG, 7_200L,
                    ConfigDef.Range.atLeast(60L), ConfigDef.Importance.HIGH,
                    "Minimum readable log-backup lifetime guaranteed by the DBA contract.",
                    GROUP_CONTROL, 23, ConfigDef.Width.SHORT, "Minimum retention")
            .define(RETENTION_SAFETY_MARGIN_SECONDS, ConfigDef.Type.LONG, 900L,
                    ConfigDef.Range.atLeast(0L), ConfigDef.Importance.HIGH,
                    "Fail/warn threshold before the guaranteed retention window is exhausted.",
                    GROUP_CONTROL, 24, ConfigDef.Width.SHORT, "Retention safety margin")
            .define(HEARTBEAT_INTERVAL_MS, ConfigDef.Type.LONG, 30_000L,
                    ConfigDef.Range.atLeast(1_000L), ConfigDef.Importance.HIGH,
                    "Debezium-compatible heartbeat interval used to commit idle source progress.",
                    GROUP_RECORDS, 7, ConfigDef.Width.SHORT, "Heartbeat interval")
            .define(HEARTBEAT_TOPIC_PREFIX, ConfigDef.Type.STRING, "__debezium-heartbeat",
                    new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM,
                    "Prefix for the derived heartbeat topic.",
                    GROUP_RECORDS, 8, ConfigDef.Width.MEDIUM, "Heartbeat topic prefix")
            .define(HEARTBEAT_TOPIC_NAME, ConfigDef.Type.STRING, "",
                    ConfigDef.Importance.MEDIUM,
                    "Explicit heartbeat topic; overrides topic.heartbeat.prefix when non-empty.",
                    GROUP_RECORDS, 9, ConfigDef.Width.LONG, "Heartbeat topic name")
            .define(TRANSACTION_MAX_OPEN, ConfigDef.Type.INT, 4_096,
                    ConfigDef.Range.atLeast(1), ConfigDef.Importance.HIGH,
                    "Maximum simultaneously open HANA transactions kept by the decoder.",
                    GROUP_LIMITS, 2, ConfigDef.Width.SHORT, "Open transactions")
            .define(TRANSACTION_MAX_ROWS, ConfigDef.Type.INT, 100_000,
                    ConfigDef.Range.atLeast(1), ConfigDef.Importance.HIGH,
                    "Maximum decoded rows buffered across open transactions.",
                    GROUP_LIMITS, 3, ConfigDef.Width.MEDIUM, "Buffered rows")
            .define(TRANSACTION_MAX_BYTES, ConfigDef.Type.LONG, 64L * 1024 * 1024,
                    ConfigDef.Range.atLeast(1L), ConfigDef.Importance.HIGH,
                    "Maximum native row payload bytes buffered across open transactions.",
                    GROUP_LIMITS, 4, ConfigDef.Width.MEDIUM, "Transaction bytes")
            .define(LOB_MAX_BUFFERED_BYTES, ConfigDef.Type.LONG, 64L * 1024 * 1024,
                    ConfigDef.Range.atLeast(1L), ConfigDef.Importance.HIGH,
                    "Maximum reconstructed LOB bytes buffered by one transaction.",
                    GROUP_LIMITS, 5, ConfigDef.Width.MEDIUM, "LOB buffer bytes")
            .define(LOB_MAX_BYTES, ConfigDef.Type.LONG, 16L * 1024 * 1024,
                    ConfigDef.Range.atLeast(1L), ConfigDef.Importance.HIGH,
                    "Maximum supported size of one reconstructed LOB value.",
                    GROUP_LIMITS, 6, ConfigDef.Width.MEDIUM, "LOB value bytes")
            .define(LOB_MAX_OPEN_LOCATORS, ConfigDef.Type.INT, 1_024,
                    ConfigDef.Range.atLeast(1), ConfigDef.Importance.HIGH,
                    "Maximum incomplete LOB locators buffered by one transaction.",
                    GROUP_LIMITS, 7, ConfigDef.Width.SHORT, "Open LOB locators")
            .define(EVENT_MAX_BYTES, ConfigDef.Type.LONG, 1_048_576L,
                    ConfigDef.Range.atLeast(1L), ConfigDef.Importance.HIGH,
                    "Maximum combined JsonConverter key/value bytes before publication.",
                    GROUP_LIMITS, 8, ConfigDef.Width.MEDIUM, "Serialized event bytes")
            .define(STATE_MAX_UNCOMMITTED_GENERATIONS, ConfigDef.Type.INT, 65_536,
                    ConfigDef.Range.atLeast(1), ConfigDef.Importance.HIGH,
                    "Maximum durable checkpoints newer than the last Connect-committed offset.",
                    GROUP_LIMITS, 9, ConfigDef.Width.MEDIUM,
                    "Uncommitted state generations")
            .define(STATE_MAX_UNCOMMITTED_BYTES, ConfigDef.Type.LONG,
                    4L * 1024 * 1024 * 1024, ConfigDef.Range.atLeast(1L),
                    ConfigDef.Importance.HIGH,
                    "Maximum encoded bytes of checkpoints newer than the committed offset.",
                    GROUP_LIMITS, 10, ConfigDef.Width.MEDIUM,
                    "Uncommitted state bytes");

    private Map<String, String> properties = Map.of();

    @Override
    public void start(Map<String, String> props) {
        Map<String, String> normalized = normalizeParallelismProperties(props);
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, normalized);
        TableIncludeList.compile(config.getList(TABLE_INCLUDE_LIST));
        if (config.getLong(LOB_MAX_BYTES) > config.getLong(LOB_MAX_BUFFERED_BYTES)) {
            throw new ConfigException(LOB_MAX_BYTES, config.getLong(LOB_MAX_BYTES),
                    "must not exceed " + LOB_MAX_BUFFERED_BYTES);
        }
        Map<String, String> copy = new HashMap<>(normalized);
        copy.putIfAbsent(PROTOTYPE_MODE, "true");
        properties = Map.copyOf(copy);
    }

    static Map<String, String> normalizeParallelismProperties(Map<String, String> props) {
        Map<String, String> result = new HashMap<>(props);
        for (String removed : REMOVED_NATIVE_READER_SETTINGS) {
            if (result.containsKey(removed)) {
                throw new ConfigException(removed, result.get(removed),
                        "was removed; native backups now use one sequential reader");
            }
        }
        copyAlias(result, LOG_PROCESSOR_THREADS, SCAN_THREADS);
        copyAlias(result, LOG_PROCESSOR_MAX_THREADS, SCAN_MAX_THREADS);
        copyAlias(result, LOG_PROCESSOR_CHUNKS_PER_THREAD, CHUNKS_PER_THREAD);
        return result;
    }

    private static void copyAlias(
            Map<String, String> values, String canonical, String legacy) {
        String legacyValue = values.get(legacy);
        if (legacyValue == null) {
            return;
        }
        String canonicalValue = values.get(canonical);
        if (canonicalValue != null && !canonicalValue.equalsIgnoreCase(legacyValue)) {
            throw new ConfigException(canonical, canonicalValue,
                    "conflicts with deprecated " + legacy);
        }
        values.putIfAbsent(canonical, legacyValue);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HanaSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks < 1) {
            return List.of();
        }
        List<Map<String, String>> result = new ArrayList<>(1);
        result.add(new HashMap<>(properties));
        return result;
    }

    @Override
    public void stop() {
        properties = Map.of();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> props) {
        // Durable native reducer state alone does not prove Kafka Connect's
        // framework-level exactly-once contract. Keep the conservative
        // at-least-once declaration until the real-cluster E2E gate (M4).
        return ExactlyOnceSupport.UNSUPPORTED;
    }

    @Override
    public String version() {
        return VERSION;
    }
}

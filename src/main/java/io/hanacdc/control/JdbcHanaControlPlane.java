package io.hanacdc.control;

import io.hanacdc.schema.TableMetadata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiPredicate;

/** Read-only JDBC control plane. All decisions use effective runtime values. */
public final class JdbcHanaControlPlane implements HanaControlPlane {
    @FunctionalInterface
    public interface ConnectionFactory {
        Connection open() throws SQLException;
    }

    static final String IDENTITY_SQL = """
            SELECT SYSTEM_ID, DATABASE_NAME, VERSION
              FROM SYS.M_DATABASE
            """;

    static final String BACKUP_CONFIGURATION_SQL = """
            SELECT FILE_LOG_BACKUP_PATH,
                   LOG_BACKUP_TIMEOUT,
                   LOG_BACKUP_INTERVAL_MODE,
                   IS_LOG_BACKUP_COMPRESSION_ENABLED,
                   LOG_BACKUP_COMPRESSION_ALGORITHM
              FROM SYS.M_BACKUP_CONFIGURATION
            """;

    static final String EFFECTIVE_CONFIGURATION_SQL = """
            SELECT SECTION, KEY, VALUE
              FROM SYS.M_CONFIGURATION_PARAMETER_VALUES
             WHERE FILE_NAME = 'global.ini'
               AND ((SECTION = 'persistence' AND KEY IN (
                       'log_mode', 'enable_auto_log_backup',
                       'log_segment_size_mb'))
                 OR (SECTION = 'backup' AND KEY IN (
                       'max_log_backup_size', 'log_backup_using_backint',
                       'log_backup_backint_fallback')))
            """;

    static final String ENCRYPTION_SQL = """
            SELECT IS_ENCRYPTION_ACTIVE
              FROM SYS.M_ENCRYPTION_OVERVIEW
             WHERE UPPER(SCOPE) = 'BACKUP'
            """;

    static final String BROKEN_HISTORY_SQL = """
            SELECT COUNT(*) AS BROKEN_COUNT
              FROM SYS.M_BACKUP_HISTORY_BROKEN
            """;

    static final String SYSTEM_REPLICATION_SQL = """
            SELECT COUNT(*) AS REPLICATION_COUNT
              FROM SYS.M_SYSTEM_REPLICATION
            """;

    static final String REDO_HEAD_SQL = """
            SELECT COALESCE(MAX(MAX_POSITION), 0) AS REDO_HEAD
              FROM SYS.M_LOG_SEGMENTS
             WHERE VOLUME_ID = ?
            """;

    static final String BACKUP_HWM_SQL = """
            SELECT COALESCE(MAX(F.LAST_REDO_LOG_POSITION), 0) AS BACKUP_HWM
              FROM SYS.M_BACKUP_CATALOG C
              JOIN SYS.M_BACKUP_CATALOG_FILES F
                ON C.BACKUP_ID = F.BACKUP_ID
             WHERE C.ENTRY_TYPE_NAME = 'log backup'
               AND C.STATE_NAME = 'successful'
               AND F.SOURCE_TYPE_NAME = 'volume'
               AND F.SOURCE_ID = ?
               AND F.FIRST_REDO_LOG_POSITION > 0
            """;

    static final String BACKUP_WINDOW_SQL = """
            SELECT COALESCE(MIN(F.FIRST_REDO_LOG_POSITION), 0) AS EARLIEST_POSITION,
                   COALESCE(SUM(F.BACKUP_SIZE), 0) AS RETAINED_BYTES
              FROM SYS.M_BACKUP_CATALOG C
              JOIN SYS.M_BACKUP_CATALOG_FILES F
                ON C.BACKUP_ID = F.BACKUP_ID
             WHERE C.ENTRY_TYPE_NAME = 'log backup'
               AND C.STATE_NAME = 'successful'
               AND F.SOURCE_TYPE_NAME = 'volume'
               AND F.SOURCE_ID = ?
               AND F.FIRST_REDO_LOG_POSITION > 0
            """;

    static final String MANIFEST_SQL = """
            SELECT C.ENTRY_ID,
                   C.BACKUP_ID,
                   C.UTC_END_TIME,
                   F.SOURCE_ID,
                   F.HOST,
                   F.SERVICE_TYPE_NAME,
                   F.FIRST_REDO_LOG_POSITION,
                   F.LAST_REDO_LOG_POSITION,
                   F.BACKUP_SIZE,
                   F.DESTINATION_TYPE_NAME,
                   F.DESTINATION_PATH,
                   F.EXTERNAL_BACKUP_ID,
                   F.LOG_SEGMENT_COUNT,
                   F.BACKINT_FALLBACK_USED,
                   F.COMPRESSED_SIZE,
                   F.COMPRESSION_ALGORITHM
              FROM SYS.M_BACKUP_CATALOG C
              JOIN SYS.M_BACKUP_CATALOG_FILES F
                ON C.BACKUP_ID = F.BACKUP_ID
             WHERE C.ENTRY_TYPE_NAME = 'log backup'
               AND C.STATE_NAME = 'successful'
               AND F.SOURCE_TYPE_NAME = 'volume'
               AND F.SOURCE_ID = ?
               AND F.LAST_REDO_LOG_POSITION > ?
               AND F.FIRST_REDO_LOG_POSITION < ?
             ORDER BY F.FIRST_REDO_LOG_POSITION,
                      F.LAST_REDO_LOG_POSITION,
                      C.BACKUP_ID
            """;

    static final String COLUMN_TABLES_SQL = """
            SELECT T.SCHEMA_NAME,
                   T.TABLE_NAME,
                   T.TABLE_OID,
                   T.TABLE_TYPE,
                   C.POSITION,
                   C.COLUMN_ID,
                   C.COLUMN_NAME,
                   C.DATA_TYPE_ID,
                   C.DATA_TYPE_NAME,
                   C.LENGTH,
                   C.SCALE,
                   C.IS_NULLABLE,
                   C.DEFAULT_VALUE,
                   C.GENERATED_ALWAYS_AS,
                   C.GENERATION_TYPE
              FROM SYS.TABLES T
              JOIN SYS.TABLE_COLUMNS C
                ON C.SCHEMA_NAME = T.SCHEMA_NAME
               AND C.TABLE_NAME = T.TABLE_NAME
             WHERE T.IS_COLUMN_TABLE = 'TRUE'
               AND T.IS_TEMPORARY = 'FALSE'
            ORDER BY T.SCHEMA_NAME, T.TABLE_NAME, C.POSITION
            """;

    static final String INTERNAL_COLUMNS_SQL = """
            SELECT SCHEMA_NAME,
                   TABLE_NAME,
                   POSITION,
                   COLUMN_NAME,
                   INTERNAL_ATTRIBUTE_TYPE,
                   INTERNAL_COLUMN_ID
              FROM SYS.CS_ALL_COLUMNS
             ORDER BY SCHEMA_NAME, TABLE_NAME, INTERNAL_COLUMN_ID
            """;

    static final String PRIMARY_KEYS_SQL = """
            SELECT SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, POSITION
              FROM SYS.CONSTRAINTS
             WHERE IS_PRIMARY_KEY = 'TRUE'
             ORDER BY SCHEMA_NAME, TABLE_NAME, POSITION
            """;

    static final String DATA_CONTAINERS_SQL = """
            SELECT SCHEMA_NAME,
                   TABLE_NAME,
                   PART_ID,
                   HOST,
                   PORT,
                   CONTAINER_ID
              FROM SYS.M_TABLE_VIRTUAL_FILES
             WHERE IS_HISTORY = 'FALSE'
               AND NAME = '$container$'
             ORDER BY SCHEMA_NAME, TABLE_NAME, PART_ID, CONTAINER_ID
            """;

    static final String TABLE_PARTITIONS_SQL = """
            SELECT SCHEMA_NAME,
                   TABLE_NAME,
                   PART_ID,
                   IS_REPLICA
              FROM SYS.M_TABLE_PARTITIONS
             ORDER BY SCHEMA_NAME, TABLE_NAME, PART_ID
            """;

    static final String PERSISTENCE_SERVICES_SQL = """
            SELECT DISTINCT V.HOST, V.PORT, V.SERVICE_NAME, V.VOLUME_ID
              FROM SYS.M_VOLUMES V
              JOIN SYS.M_SERVICES S
                ON S.HOST = V.HOST
               AND S.PORT = V.PORT
             WHERE LOWER(V.SERVICE_NAME) = 'indexserver'
               AND UPPER(S.ACTIVE_STATUS) = 'YES'
               AND UPPER(COALESCE(S.COORDINATOR_TYPE, 'NONE')) <> 'STANDBY'
             ORDER BY V.HOST, V.PORT, V.VOLUME_ID
            """;

    private final ConnectionFactory connections;
    private final long sourceId;
    private final int queryTimeoutSeconds;

    public JdbcHanaControlPlane(
            ConnectionFactory connections,
            long sourceId,
            int queryTimeoutSeconds) {
        this.connections = connections;
        this.sourceId = sourceId;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
        if (sourceId < 0 || queryTimeoutSeconds <= 0) {
            throw new IllegalArgumentException("sourceId and query timeout must be positive");
        }
    }

    public static JdbcHanaControlPlane connect(
            String driverClass,
            String jdbcUrl,
            String user,
            String password,
            long sourceId,
            int queryTimeoutSeconds) {
        if (jdbcUrl == null || jdbcUrl.isBlank()
                || user == null || user.isBlank()
                || password == null) {
            throw new IllegalArgumentException("JDBC URL, user, and password are required");
        }
        if (driverClass != null && !driverClass.isBlank()) {
            try {
                Class.forName(driverClass);
            }
            catch (ClassNotFoundException error) {
                throw new IllegalStateException("SAP HANA JDBC driver is not on plugin.path: "
                        + driverClass, error);
            }
        }
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        return new JdbcHanaControlPlane(
                () -> DriverManager.getConnection(jdbcUrl, properties),
                sourceId,
                queryTimeoutSeconds);
    }

    @Override
    public HanaCapabilities inspect() throws SQLException {
        try (Connection connection = readOnlyConnection()) {
            Identity identity = identity(connection);
            BackupConfiguration backup = backupConfiguration(connection);
            Map<String, Set<String>> effective = effectiveConfiguration(connection);
            String logMode = uniform(effective, "persistence.log_mode");
            boolean automaticBackup = booleanValue(
                    uniform(effective, "persistence.enable_auto_log_backup"));
            long segmentMiB = longValue(
                    uniform(effective, "persistence.log_segment_size_mb"));
            long maximumBackupGiB = longValue(
                    uniform(effective, "backup.max_log_backup_size"));
            boolean backint = booleanValue(
                    uniform(effective, "backup.log_backup_using_backint"));
            boolean fallback = booleanValue(
                    uniform(effective, "backup.log_backup_backint_fallback"));
            boolean encryption = singleBoolean(connection, ENCRYPTION_SQL);
            long broken = singleLong(connection, BROKEN_HISTORY_SQL, "BROKEN_COUNT");
            boolean replication = singleLong(
                    connection, SYSTEM_REPLICATION_SQL, "REPLICATION_COUNT") != 0;
            PersistenceStreamStatus stream = persistenceStreamStatus(connection, sourceId);
            return new HanaCapabilities(
                    identity.systemId(),
                    identity.databaseName(),
                    identity.version(),
                    logMode,
                    automaticBackup,
                    backup.timeoutSeconds(),
                    backup.intervalMode(),
                    segmentMiB,
                    maximumBackupGiB,
                    backint,
                    fallback,
                    backup.fileLogBackupPath(),
                    backup.compressionEnabled(),
                    backup.compressionAlgorithm(),
                    encryption,
                    broken,
                    replication,
                    stream.earliestSuccessfulBackupPosition(),
                    stream.retainedSuccessfulBackupBytes(),
                    stream.successfulBackupHighWatermark(),
                    stream.redoHead(),
                    Instant.now());
        }
    }

    @Override
    public List<LogBackupEntry> successfulLogBackups(
            long requestedSourceId,
            long afterOrAtRedoPosition,
            long beforeOrAtRedoPosition) throws SQLException {
        if (requestedSourceId <= 0 || afterOrAtRedoPosition < 0
                || beforeOrAtRedoPosition <= afterOrAtRedoPosition) {
            throw new IllegalArgumentException("invalid manifest redo range");
        }
        try (Connection connection = readOnlyConnection();
                PreparedStatement statement = connection.prepareStatement(MANIFEST_SQL)) {
            timeout(statement);
            statement.setLong(1, requestedSourceId);
            statement.setLong(2, afterOrAtRedoPosition);
            statement.setLong(3, beforeOrAtRedoPosition);
            try (ResultSet rows = statement.executeQuery()) {
                List<LogBackupEntry> result = new ArrayList<>();
                while (rows.next()) {
                    Timestamp completed = rows.getTimestamp("UTC_END_TIME");
                    result.add(new LogBackupEntry(
                            rows.getLong("ENTRY_ID"),
                            rows.getLong("BACKUP_ID"),
                            rows.getLong("SOURCE_ID"),
                            rows.getString("HOST"),
                            rows.getString("SERVICE_TYPE_NAME"),
                            rows.getLong("FIRST_REDO_LOG_POSITION"),
                            rows.getLong("LAST_REDO_LOG_POSITION"),
                            rows.getLong("BACKUP_SIZE"),
                            rows.getString("DESTINATION_TYPE_NAME"),
                            rows.getString("DESTINATION_PATH"),
                            rows.getString("EXTERNAL_BACKUP_ID"),
                            rows.getInt("LOG_SEGMENT_COUNT"),
                            rows.getBoolean("BACKINT_FALLBACK_USED"),
                            rows.getLong("COMPRESSED_SIZE"),
                            rows.getString("COMPRESSION_ALGORITHM"),
                            completed == null ? Instant.EPOCH : completed.toInstant()));
                }
                return List.copyOf(result);
            }
        }
    }

    @Override
    public PersistenceStreamStatus persistenceStreamStatus(long requestedSourceId)
            throws SQLException {
        if (requestedSourceId <= 0) {
            throw new IllegalArgumentException("source_id must be positive");
        }
        try (Connection connection = readOnlyConnection()) {
            return persistenceStreamStatus(connection, requestedSourceId);
        }
    }

    @Override
    public List<TableMetadata> columnTableMetadata() throws SQLException {
        return columnTableMetadata((schema, table) -> true);
    }

    @Override
    public List<TableMetadata> columnTableMetadata(
            BiPredicate<String, String> tableFilter) throws SQLException {
        try (Connection connection = readOnlyConnection()) {
            Map<TableKey, List<String>> primaryKeys = primaryKeys(connection, tableFilter);
            Map<TableKey, List<NativeColumn>> internalColumns =
                    internalColumns(connection, tableFilter);
            List<PersistenceService> services = persistenceServices(connection);
            Map<TableKey, List<TableMetadata.Partition>> dataContainers =
                    dataContainers(connection, services, tableFilter);
            Map<TableKey, PartitionState> partitionState =
                    partitionState(connection, tableFilter);
            List<TableMetadata> result = new ArrayList<>();
            TableKey current = null;
            long objectId = 0;
            String tableType = "";
            List<TableMetadata.Column> columns = new ArrayList<>();
            try (Statement statement = connection.createStatement()) {
                timeout(statement);
                try (ResultSet rows = statement.executeQuery(COLUMN_TABLES_SQL)) {
                    while (rows.next()) {
                        TableKey key = new TableKey(
                                rows.getString("SCHEMA_NAME"), rows.getString("TABLE_NAME"));
                        if (current != null && !current.equals(key)) {
                            if (tableFilter.test(current.schema(), current.table())) {
                                result.add(tableMetadata(
                                        current, tableType, objectId, columns,
                                        internalColumns, primaryKeys,
                                        dataContainers, partitionState));
                            }
                            columns = new ArrayList<>();
                        }
                        current = key;
                        objectId = rows.getLong("TABLE_OID");
                        tableType = rows.getString("TABLE_TYPE");
                        String generationType = rows.getString("GENERATION_TYPE");
                        String generatedExpression = rows.getString("GENERATED_ALWAYS_AS");
                        boolean identity = generationType != null
                                && generationType.toUpperCase(Locale.ROOT).contains("IDENTITY");
                        boolean generated = identity
                                || (generationType != null && !generationType.isBlank())
                                || (generatedExpression != null && !generatedExpression.isBlank());
                        int length = rows.getInt("LENGTH");
                        columns.add(new TableMetadata.Column(
                                rows.getInt("POSITION"),
                                rows.getLong("COLUMN_ID"),
                                rows.getString("COLUMN_NAME"),
                                rows.getString("DATA_TYPE_NAME"),
                                rows.getInt("DATA_TYPE_ID"),
                                Math.max(0, length),
                                Math.max(0, length),
                                Math.max(0, rows.getInt("SCALE")),
                                booleanValue(rows.getString("IS_NULLABLE")),
                                generated,
                                identity,
                                rows.getString("DEFAULT_VALUE")));
                    }
                }
            }
            if (current != null && tableFilter.test(current.schema(), current.table())) {
                result.add(tableMetadata(current, tableType, objectId, columns,
                        internalColumns, primaryKeys, dataContainers, partitionState));
            }
            return List.copyOf(result);
        }
    }

    @Override
    public List<PersistenceService> persistenceServices() throws SQLException {
        try (Connection connection = readOnlyConnection()) {
            return persistenceServices(connection);
        }
    }

    private Map<TableKey, List<String>> primaryKeys(
            Connection connection,
            BiPredicate<String, String> tableFilter) throws SQLException {
        Map<TableKey, List<String>> result = new LinkedHashMap<>();
        try (Statement statement = connection.createStatement()) {
            timeout(statement);
            try (ResultSet rows = statement.executeQuery(PRIMARY_KEYS_SQL)) {
                while (rows.next()) {
                    TableKey key = new TableKey(
                            rows.getString("SCHEMA_NAME"), rows.getString("TABLE_NAME"));
                    if (!tableFilter.test(key.schema(), key.table())) {
                        continue;
                    }
                    result.computeIfAbsent(key, ignored -> new ArrayList<>())
                            .add(rows.getString("COLUMN_NAME"));
                }
            }
        }
        return result;
    }

    private Map<TableKey, List<NativeColumn>> internalColumns(
            Connection connection,
            BiPredicate<String, String> tableFilter)
            throws SQLException {
        Map<TableKey, List<NativeColumn>> result = new LinkedHashMap<>();
        try (Statement statement = connection.createStatement()) {
            timeout(statement);
            try (ResultSet rows = statement.executeQuery(INTERNAL_COLUMNS_SQL)) {
                while (rows.next()) {
                    TableKey key = new TableKey(
                            rows.getString("SCHEMA_NAME"), rows.getString("TABLE_NAME"));
                    if (!tableFilter.test(key.schema(), key.table())) {
                        continue;
                    }
                    result.computeIfAbsent(key, ignored -> new ArrayList<>()).add(
                            new NativeColumn(
                                    rows.getLong("INTERNAL_COLUMN_ID"),
                                    rows.getInt("POSITION"),
                                    rows.getString("COLUMN_NAME"),
                                    rows.getString("INTERNAL_ATTRIBUTE_TYPE")));
                }
            }
        }
        return result;
    }

    private Map<TableKey, List<TableMetadata.Partition>> dataContainers(
            Connection connection,
            List<PersistenceService> services,
            BiPredicate<String, String> tableFilter) throws SQLException {
        Map<ServiceKey, PersistenceService> byLocation = new LinkedHashMap<>();
        for (PersistenceService service : services) {
            PersistenceService previous = byLocation.putIfAbsent(
                    new ServiceKey(service.host(), service.port()), service);
            if (previous != null && previous.volumeId() != service.volumeId()) {
                throw new SQLException("multiple persistence volumes for service "
                        + service.host() + ":" + service.port());
            }
        }
        Map<TableKey, List<TableMetadata.Partition>> result = new LinkedHashMap<>();
        try (Statement statement = connection.createStatement()) {
            timeout(statement);
            try (ResultSet rows = statement.executeQuery(DATA_CONTAINERS_SQL)) {
                while (rows.next()) {
                    TableKey key = new TableKey(
                            rows.getString("SCHEMA_NAME"), rows.getString("TABLE_NAME"));
                    if (!tableFilter.test(key.schema(), key.table())) {
                        continue;
                    }
                    int partId = rows.getInt("PART_ID");
                    if (partId < 0) {
                        throw new SQLException("FATAL_SOURCE_TOPOLOGY: table schema is being "
                                + "modified: " + key.schema() + "." + key.table());
                    }
                    String host = rows.getString("HOST");
                    int port = rows.getInt("PORT");
                    PersistenceService service = byLocation.get(new ServiceKey(host, port));
                    if (service == null) {
                        throw new SQLException("table container is not bound to an indexserver "
                                + "persistence volume: " + key.schema() + "." + key.table()
                                + " host=" + host + " port=" + port);
                    }
                    result.computeIfAbsent(key, ignored -> new ArrayList<>()).add(
                            new TableMetadata.Partition(
                                    partId,
                                    rows.getLong("CONTAINER_ID"),
                                    host,
                                    port,
                                    service.volumeId(),
                                    service.serviceName()));
                }
            }
        }
        return result;
    }

    private Map<TableKey, PartitionState> partitionState(
            Connection connection,
            BiPredicate<String, String> tableFilter)
            throws SQLException {
        Map<TableKey, PartitionState> result = new LinkedHashMap<>();
        try (Statement statement = connection.createStatement()) {
            timeout(statement);
            try (ResultSet rows = statement.executeQuery(TABLE_PARTITIONS_SQL)) {
                while (rows.next()) {
                    TableKey key = new TableKey(
                            rows.getString("SCHEMA_NAME"), rows.getString("TABLE_NAME"));
                    if (!tableFilter.test(key.schema(), key.table())) {
                        continue;
                    }
                    int partId = rows.getInt("PART_ID");
                    if (partId < 0) {
                        throw new SQLException("FATAL_SOURCE_TOPOLOGY: table schema is being "
                                + "modified: " + key.schema() + "." + key.table());
                    }
                    PartitionState previous = result.getOrDefault(
                            key, new PartitionState(new LinkedHashSet<>(), false));
                    Set<Integer> partIds = new LinkedHashSet<>(previous.partIds());
                    partIds.add(partId);
                    result.put(key, new PartitionState(partIds,
                            previous.replicated()
                                    || booleanValue(rows.getString("IS_REPLICA"))));
                }
            }
        }
        return Map.copyOf(result);
    }

    private List<PersistenceService> persistenceServices(Connection connection)
            throws SQLException {
        List<PersistenceService> result = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            timeout(statement);
            try (ResultSet rows = statement.executeQuery(PERSISTENCE_SERVICES_SQL)) {
                while (rows.next()) {
                    result.add(new PersistenceService(
                            rows.getString("HOST"),
                            rows.getInt("PORT"),
                            rows.getString("SERVICE_NAME"),
                            rows.getLong("VOLUME_ID")));
                }
            }
        }
        return result.stream().distinct().sorted().toList();
    }

    private static TableMetadata tableMetadata(
            TableKey key,
            String tableType,
            long objectId,
            List<TableMetadata.Column> columns,
            Map<TableKey, List<NativeColumn>> internalColumns,
            Map<TableKey, List<String>> primaryKeys,
            Map<TableKey, List<TableMetadata.Partition>> dataContainers,
            Map<TableKey, PartitionState> partitionState) {
        PartitionState state = partitionState.getOrDefault(
                key, new PartitionState(Set.of(), false));
        return bindNativeColumns(key.schema(), key.table(), tableType, objectId,
                dataContainers.getOrDefault(key, List.of()),
                state.replicated(), columns,
                internalColumns.getOrDefault(key, List.of()),
                primaryKeys.getOrDefault(key, List.of()));
    }

    static TableMetadata bindNativeColumns(
            String schema,
            String table,
            String tableType,
            long objectId,
            long containerId,
            List<TableMetadata.Column> catalogColumns,
            List<NativeColumn> physical,
            List<String> primaryKeys) {
        List<TableMetadata.Partition> partitions = containerId > 0
                ? List.of(new TableMetadata.Partition(0, containerId, "", 0, 0, ""))
                : List.of();
        return bindNativeColumns(schema, table, tableType, objectId, partitions, false,
                catalogColumns, physical, primaryKeys);
    }

    static TableMetadata bindNativeColumns(
            String schema,
            String table,
            String tableType,
            long objectId,
            List<TableMetadata.Partition> partitions,
            boolean replicated,
            List<TableMetadata.Column> catalogColumns,
            List<NativeColumn> physical,
            List<String> primaryKeys) {
        List<TableMetadata.Column> mapped = new ArrayList<>(catalogColumns.size());
        Set<Long> visibleIds = new LinkedHashSet<>();
        for (TableMetadata.Column catalog : catalogColumns) {
            List<NativeColumn> matches = physical.stream()
                    .filter(column -> column.position() == catalog.ordinal()
                            && column.name().equals(catalog.name())
                            && (column.attributeType() == null
                            || column.attributeType().isBlank()))
                    .toList();
            if (matches.size() != 1) {
                throw new IllegalArgumentException("FATAL_SCHEMA_CONTRACT: expected one native "
                        + "column for " + schema + "." + table + "." + catalog.name()
                        + " at position " + catalog.ordinal() + ", found " + matches.size());
            }
            long internalId = matches.get(0).id();
            if (!visibleIds.add(internalId)) {
                throw new IllegalArgumentException("FATAL_SCHEMA_CONTRACT: duplicate native "
                        + "column ID " + internalId + " for " + schema + "." + table);
            }
            mapped.add(new TableMetadata.Column(
                    catalog.ordinal(), internalId, catalog.name(), catalog.typeName(),
                    catalog.dataTypeId(), catalog.length(), catalog.precision(), catalog.scale(),
                    catalog.nullable(), catalog.generated(), catalog.identity(),
                    catalog.defaultValue()));
        }
        Set<String> ignoredAttributes = Set.of("ROWID", "TREX_UDIV");
        List<TableMetadata.AuxiliaryColumn> auxiliary = physical.stream()
                .filter(column -> !visibleIds.contains(column.id()))
                .filter(column -> column.attributeType() == null
                        || !ignoredAttributes.contains(
                        column.attributeType().toUpperCase(Locale.ROOT)))
                .map(column -> new TableMetadata.AuxiliaryColumn(
                        column.id(), column.position(), column.name(), column.attributeType()))
                .toList();
        return new TableMetadata(schema, table, tableType, objectId, partitions,
                replicated, mapped, auxiliary, primaryKeys);
    }

    private Connection readOnlyConnection() throws SQLException {
        Connection connection = connections.open();
        try {
            connection.setReadOnly(true);
            return connection;
        }
        catch (SQLException error) {
            connection.close();
            throw error;
        }
    }

    private Identity identity(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            timeout(statement);
            try (ResultSet rows = statement.executeQuery(IDENTITY_SQL)) {
                if (!rows.next()) {
                    throw new SQLException("M_DATABASE returned no database identity");
                }
                Identity identity = new Identity(
                        rows.getString("SYSTEM_ID"),
                        rows.getString("DATABASE_NAME"),
                        rows.getString("VERSION"));
                if (rows.next()) {
                    throw new SQLException("M_DATABASE returned multiple database identities");
                }
                return identity;
            }
        }
    }

    private BackupConfiguration backupConfiguration(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            timeout(statement);
            try (ResultSet rows = statement.executeQuery(BACKUP_CONFIGURATION_SQL)) {
                if (!rows.next()) {
                    throw new SQLException("M_BACKUP_CONFIGURATION returned no row");
                }
                BackupConfiguration value = new BackupConfiguration(
                        rows.getString("FILE_LOG_BACKUP_PATH"),
                        rows.getLong("LOG_BACKUP_TIMEOUT"),
                        rows.getString("LOG_BACKUP_INTERVAL_MODE"),
                        rows.getBoolean("IS_LOG_BACKUP_COMPRESSION_ENABLED"),
                        rows.getString("LOG_BACKUP_COMPRESSION_ALGORITHM"));
                if (rows.next()) {
                    throw new SQLException("M_BACKUP_CONFIGURATION returned multiple rows");
                }
                return value;
            }
        }
    }

    private Map<String, Set<String>> effectiveConfiguration(Connection connection)
            throws SQLException {
        Map<String, Set<String>> result = new LinkedHashMap<>();
        try (Statement statement = connection.createStatement()) {
            timeout(statement);
            try (ResultSet rows = statement.executeQuery(EFFECTIVE_CONFIGURATION_SQL)) {
                while (rows.next()) {
                    String key = rows.getString("SECTION") + "." + rows.getString("KEY");
                    result.computeIfAbsent(key, ignored -> new LinkedHashSet<>())
                            .add(normalize(rows.getString("VALUE")));
                }
            }
        }
        return result;
    }

    private boolean singleBoolean(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            timeout(statement);
            try (ResultSet rows = statement.executeQuery(sql)) {
                if (!rows.next()) {
                    throw new SQLException("required monitoring query returned no row");
                }
                boolean result = rows.getBoolean(1);
                if (rows.next()) {
                    throw new SQLException("required monitoring query returned multiple rows");
                }
                return result;
            }
        }
    }

    private long singleLong(Connection connection, String sql, String column) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            timeout(statement);
            try (ResultSet rows = statement.executeQuery(sql)) {
                if (!rows.next()) {
                    throw new SQLException("required monitoring query returned no row");
                }
                return rows.getLong(column);
            }
        }
    }

    private long parameterLong(
            Connection connection,
            String sql,
            String column,
            long parameter) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            timeout(statement);
            statement.setLong(1, parameter);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new SQLException("required monitoring query returned no row");
                }
                return rows.getLong(column);
            }
        }
    }

    private BackupWindow backupWindow(Connection connection, long requestedSourceId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(BACKUP_WINDOW_SQL)) {
            timeout(statement);
            statement.setLong(1, requestedSourceId);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new SQLException("backup retention window query returned no row");
                }
                return new BackupWindow(
                        rows.getLong("EARLIEST_POSITION"),
                        rows.getLong("RETAINED_BYTES"));
            }
        }
    }

    private PersistenceStreamStatus persistenceStreamStatus(
            Connection connection, long requestedSourceId) throws SQLException {
        long redoHead = parameterLong(
                connection, REDO_HEAD_SQL, "REDO_HEAD", requestedSourceId);
        long backupHwm = parameterLong(
                connection, BACKUP_HWM_SQL, "BACKUP_HWM", requestedSourceId);
        BackupWindow window = backupWindow(connection, requestedSourceId);
        return new PersistenceStreamStatus(requestedSourceId,
                window.earliestPosition(), window.retainedBytes(),
                backupHwm, redoHead, Instant.now());
    }

    private void timeout(Statement statement) throws SQLException {
        statement.setQueryTimeout(queryTimeoutSeconds);
    }

    private static String uniform(Map<String, Set<String>> values, String key) throws SQLException {
        Set<String> found = values.getOrDefault(key, Set.of());
        if (found.isEmpty()) {
            throw new SQLException("effective configuration is missing " + key);
        }
        if (found.size() != 1) {
            throw new SQLException("effective configuration differs across services for "
                    + key + ": " + found);
        }
        return found.iterator().next();
    }

    private static boolean booleanValue(String value) throws SQLException {
        return switch (normalize(value)) {
            case "true", "yes", "on", "1" -> true;
            case "false", "no", "off", "0" -> false;
            default -> throw new SQLException("not a HANA boolean: " + value);
        };
    }

    private static long longValue(String value) throws SQLException {
        try {
            return Long.parseLong(value);
        }
        catch (NumberFormatException error) {
            throw new SQLException("not a HANA integer: " + value, error);
        }
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }

    private record Identity(String systemId, String databaseName, String version) {}

    private record BackupConfiguration(
            String fileLogBackupPath,
            long timeoutSeconds,
            String intervalMode,
            boolean compressionEnabled,
            String compressionAlgorithm) {}

    private record BackupWindow(long earliestPosition, long retainedBytes) {}

    private record TableKey(String schema, String table) {}

    private record ServiceKey(String host, int port) {}

    private record PartitionState(Set<Integer> partIds, boolean replicated) {
        private PartitionState {
            partIds = Set.copyOf(partIds);
        }
    }

    static record NativeColumn(long id, int position, String name, String attributeType) {}
}

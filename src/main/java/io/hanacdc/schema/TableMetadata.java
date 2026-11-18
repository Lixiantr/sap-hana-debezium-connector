package io.hanacdc.schema;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Immutable JDBC/native identity snapshot for one captured HANA column table. */
public record TableMetadata(
        String schemaName,
        String tableName,
        String storageType,
        long objectId,
        List<Partition> partitions,
        boolean replicated,
        List<Column> columns,
        List<AuxiliaryColumn> auxiliaryColumns,
        List<String> primaryKeyColumns) {

    /** One physical leaf partition/container and its persistence-service binding. */
    public record Partition(
            int partId,
            long containerId,
            String host,
            int port,
            long volumeId,
            String serviceName) {
        public Partition {
            host = host == null ? "" : host.trim();
            serviceName = serviceName == null ? "" : serviceName.trim().toLowerCase(
                    java.util.Locale.ROOT);
            if (partId < 0 || containerId <= 0 || port < 0 || volumeId < 0) {
                throw new IllegalArgumentException("invalid table partition/container binding");
            }
        }

        public boolean catalogBound() {
            return !host.isEmpty() && port > 0 && volumeId > 0 && !serviceName.isEmpty();
        }
    }

    public record Column(
            int ordinal,
            long internalColumnId,
            String name,
            String typeName,
            int dataTypeId,
            long length,
            int precision,
            int scale,
            boolean nullable,
            boolean generated,
            boolean identity,
            String defaultValue) {
        public Column {
            name = required(name, "column name");
            typeName = required(typeName, "type name").toUpperCase(java.util.Locale.ROOT);
            defaultValue = defaultValue == null ? "" : defaultValue;
            if (ordinal <= 0 || internalColumnId <= 0
                    || length < 0 || precision < 0 || scale < 0) {
                throw new IllegalArgumentException("invalid column metadata for " + name);
            }
        }
    }

    public record AuxiliaryColumn(
            long internalColumnId,
            int position,
            String name,
            String attributeType) {
        public AuxiliaryColumn {
            name = required(name, "auxiliary column name");
            attributeType = attributeType == null ? "" : attributeType;
            if (internalColumnId <= 0 || position <= 0) {
                throw new IllegalArgumentException("invalid auxiliary column metadata for " + name);
            }
        }
    }

    public TableMetadata {
        schemaName = required(schemaName, "schema name");
        tableName = required(tableName, "table name");
        storageType = required(storageType, "storage type").toUpperCase(java.util.Locale.ROOT);
        partitions = List.copyOf(Objects.requireNonNull(partitions, "partitions"));
        columns = List.copyOf(Objects.requireNonNull(columns, "columns"));
        auxiliaryColumns = List.copyOf(
                Objects.requireNonNull(auxiliaryColumns, "auxiliaryColumns"));
        primaryKeyColumns = List.copyOf(
                Objects.requireNonNull(primaryKeyColumns, "primaryKeyColumns"));
        if (objectId <= 0 || columns.isEmpty()) {
            throw new IllegalArgumentException("table identity and columns are required");
        }
        Set<Long> containers = partitions.stream().map(Partition::containerId)
                .collect(Collectors.toSet());
        Set<Integer> partIds = partitions.stream().map(Partition::partId)
                .collect(Collectors.toSet());
        if (containers.size() != partitions.size() || partIds.size() != partitions.size()) {
            throw new IllegalArgumentException("duplicate table partition/container binding");
        }
    }

    /** Compatibility constructor for benchmark fixtures and existing single-container tests. */
    public TableMetadata(
            String schemaName,
            String tableName,
            String storageType,
            long objectId,
            long containerId,
            List<Column> columns,
            List<AuxiliaryColumn> auxiliaryColumns,
            List<String> primaryKeyColumns) {
        this(schemaName, tableName, storageType, objectId,
                containerId > 0
                        ? List.of(new Partition(0, containerId, "", 0, 0, ""))
                        : List.of(),
                false, columns, auxiliaryColumns, primaryKeyColumns);
    }

    /** Legacy single-container identity; returns zero for zero or multiple containers. */
    public long containerId() {
        return partitions.size() == 1 ? partitions.get(0).containerId() : 0;
    }

    public Set<Long> containerIds() {
        return partitions.stream().map(Partition::containerId)
                .collect(Collectors.toUnmodifiableSet());
    }

    public boolean partitioned() {
        return partitions.size() > 1 || partitions.stream().anyMatch(value -> value.partId() > 0);
    }

    public String qualifiedName() {
        return schemaName + "." + tableName;
    }

    private static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }
}

package io.hanacdc.schema;

import io.hanacdc.schema.TableMetadata.Column;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Validated, fingerprinted decode plan for a selected table. */
public record TableDecodePlan(
        TableMetadata metadata,
        String schemaFingerprint,
        String topologyFingerprint,
        List<Integer> primaryKeyIndexes,
        int deletedFlagIndex,
        Map<Long, Integer> columnIndexesByInternalId,
        Set<Long> auxiliaryInternalIds) {

    public record RowProjection(List<Integer> descriptorToColumnIndex, boolean fullImage) {
        public RowProjection {
            descriptorToColumnIndex = List.copyOf(descriptorToColumnIndex);
        }
    }

    public TableDecodePlan {
        primaryKeyIndexes = List.copyOf(primaryKeyIndexes);
        columnIndexesByInternalId = Map.copyOf(columnIndexesByInternalId);
        auxiliaryInternalIds = Set.copyOf(auxiliaryInternalIds);
    }

    public static TableDecodePlan compile(TableMetadata metadata) {
        return compile(metadata, true);
    }

    /** Format-corpus only: allows a schema without an application tombstone column. */
    public static TableDecodePlan compileSchema(TableMetadata metadata) {
        return compile(metadata, false);
    }

    private static TableDecodePlan compile(TableMetadata metadata, boolean requireDeletedFlag) {
        if (!metadata.storageType().contains("COLUMN")) {
            throw violation("captured table is not a column table: " + metadata.qualifiedName());
        }
        Map<String, Integer> indexes = new HashMap<>();
        Set<String> duplicates = new HashSet<>();
        Map<Long, Integer> internalIndexes = new HashMap<>();
        for (int index = 0; index < metadata.columns().size(); index++) {
            Column column = metadata.columns().get(index);
            if (column.ordinal() != index + 1) {
                throw violation("column ordinals are not contiguous at " + column.name());
            }
            if (indexes.put(column.name(), index) != null) {
                duplicates.add(column.name());
            }
            if (internalIndexes.put(column.internalColumnId(), index) != null) {
                throw violation("duplicate visible internal column id: "
                        + column.internalColumnId());
            }
            if (column.generated() || column.identity()) {
                throw violation("generated/identity column is unsupported: " + column.name());
            }
        }
        if (!duplicates.isEmpty()) {
            throw violation("duplicate column names: " + duplicates);
        }
        if (metadata.primaryKeyColumns().isEmpty()) {
            throw violation("stable primary key is required");
        }
        List<Integer> keyIndexes = new ArrayList<>();
        Set<String> keyNames = new HashSet<>();
        for (String key : metadata.primaryKeyColumns()) {
            Integer index = indexes.get(key);
            if (index == null || !keyNames.add(key)) {
                throw violation("invalid primary-key column: " + key);
            }
            if (metadata.columns().get(index).nullable()) {
                throw violation("primary-key column is nullable: " + key);
            }
            keyIndexes.add(index);
        }
        Integer deleted = indexes.get("IS_DELETED");
        if (deleted == null && requireDeletedFlag) {
            throw violation("required IS_DELETED column is missing");
        }
        if (deleted != null) {
            Column deletedColumn = metadata.columns().get(deleted);
            if (!"BOOLEAN".equals(deletedColumn.typeName()) || deletedColumn.nullable()) {
                throw violation("IS_DELETED must be BOOLEAN NOT NULL");
            }
        }
        Set<Long> auxiliaryIds = new HashSet<>();
        for (TableMetadata.AuxiliaryColumn column : metadata.auxiliaryColumns()) {
            if (internalIndexes.containsKey(column.internalColumnId())
                    || !auxiliaryIds.add(column.internalColumnId())) {
                throw violation("duplicate auxiliary internal column id: "
                        + column.internalColumnId());
            }
        }
        return new TableDecodePlan(metadata, schemaFingerprint(metadata),
                topologyFingerprint(metadata), keyIndexes,
                deleted == null ? -1 : deleted,
                internalIndexes, auxiliaryIds);
    }

    /** Maps a native vector to visible columns; -1 denotes a catalogued auxiliary column. */
    public RowProjection project(List<Long> internalColumnIds) {
        List<Integer> projection = new ArrayList<>(internalColumnIds.size());
        Set<Long> seen = new HashSet<>();
        Set<Integer> visible = new HashSet<>();
        for (long internalId : internalColumnIds) {
            if (!seen.add(internalId)) {
                throw violation("duplicate internal column id in row vector: " + internalId);
            }
            Integer index = columnIndexesByInternalId.get(internalId);
            if (index != null) {
                projection.add(index);
                visible.add(index);
            }
            else if (auxiliaryInternalIds.contains(internalId)) {
                projection.add(-1);
            }
            else {
                throw violation("unknown internal column id in row vector: " + internalId);
            }
        }
        return new RowProjection(projection, visible.size() == metadata.columns().size());
    }

    private static String schemaFingerprint(TableMetadata metadata) {
        StringBuilder canonical = new StringBuilder(1024);
        field(canonical, metadata.schemaName());
        field(canonical, metadata.tableName());
        field(canonical, metadata.storageType());
        canonical.append(metadata.objectId()).append('\n');
        for (Column column : metadata.columns()) {
            canonical.append(column.ordinal()).append('|');
            canonical.append(column.internalColumnId()).append('|');
            field(canonical, column.name());
            field(canonical, column.typeName());
            canonical.append(column.dataTypeId()).append('|')
                    .append(column.length()).append('|')
                    .append(column.precision()).append('|')
                    .append(column.scale()).append('|')
                    .append(column.nullable()).append('|')
                    .append(column.generated()).append('|')
                    .append(column.identity()).append('|');
            field(canonical, column.defaultValue());
        }
        canonical.append("AUX|");
        for (TableMetadata.AuxiliaryColumn column : metadata.auxiliaryColumns()) {
            canonical.append(column.internalColumnId()).append('|')
                    .append(column.position()).append('|');
            field(canonical, column.name());
            field(canonical, column.attributeType());
        }
        canonical.append("PK|");
        for (String key : metadata.primaryKeyColumns()) {
            field(canonical, key);
        }
        return sha256(canonical);
    }

    private static String topologyFingerprint(TableMetadata metadata) {
        StringBuilder canonical = new StringBuilder(512);
        field(canonical, metadata.qualifiedName());
        canonical.append(metadata.replicated()).append('|');
        metadata.partitions().stream()
                .sorted(java.util.Comparator.comparingInt(TableMetadata.Partition::partId)
                        .thenComparingLong(TableMetadata.Partition::containerId))
                .forEach(value -> {
                    canonical.append(value.partId()).append('|')
                            .append(value.containerId()).append('|');
                    field(canonical, value.host());
                    canonical.append(value.port()).append('|')
                            .append(value.volumeId()).append('|');
                    field(canonical, value.serviceName());
                });
        return sha256(canonical);
    }

    private static String sha256(StringBuilder canonical) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(canonical.toString().getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(64);
            for (byte value : digest) {
                hex.append(String.format(java.util.Locale.ROOT, "%02x", value & 0xff));
            }
            return hex.toString();
        }
        catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static void field(StringBuilder target, String value) {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        target.append(encoded.length).append(':').append(value).append('|');
    }

    private static IllegalArgumentException violation(String detail) {
        return new IllegalArgumentException("FATAL_SCHEMA_CONTRACT: " + detail);
    }
}

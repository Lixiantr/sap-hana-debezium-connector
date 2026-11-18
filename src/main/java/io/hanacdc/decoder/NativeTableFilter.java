package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Resolves native container ids before row decoding.
 *
 * <p>The filter never hides transaction terminals: the caller must consume
 * the complete ordered stream and apply COMMIT/ROLLBACK to buffered captured
 * changes. Records for excluded tables can skip the expensive row decoder.</p>
 */
public final class NativeTableFilter {
    public enum Disposition {
        CAPTURED_TABLE,
        EXCLUDED_TABLE,
        TRANSACTION_CONTROL,
        AUXILIARY,
        OTHER
    }

    public record TableBinding(
            String schema,
            String table,
            long objectId,
            long containerId,
            String schemaFingerprint,
            boolean captured) {
        public TableBinding {
            if (schema == null || schema.isBlank() || table == null || table.isBlank()) {
                throw new IllegalArgumentException("schema and table must be non-empty");
            }
            if (schemaFingerprint == null || schemaFingerprint.isBlank()) {
                throw new IllegalArgumentException("schema fingerprint must be non-empty");
            }
        }

        public String qualifiedName() {
            return schema + "." + table;
        }
    }

    private final Map<Long, TableBinding> bindings;

    public NativeTableFilter(List<TableBinding> bindings) {
        Map<Long, TableBinding> values = new LinkedHashMap<>();
        for (TableBinding binding : bindings) {
            TableBinding previous = values.putIfAbsent(binding.containerId(), binding);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate container id 0x"
                        + Long.toUnsignedString(binding.containerId(), 16));
            }
        }
        this.bindings = Map.copyOf(values);
    }

    public Disposition classify(NativeRecordHeader record) {
        if (record.outerCode() == NativeRecordReader.OUTER_COMMIT
                || record.outerCode() == NativeRecordReader.OUTER_ROLLBACK
                || record.outerCode() == NativeRecordReader.OUTER_PARTIAL_ROLLBACK
                || record.outerCode() == NativeRecordReader.OUTER_PREPARE_COMMIT) {
            return Disposition.TRANSACTION_CONTROL;
        }
        if (record.outerCode() == NativeRecordReader.OUTER_UNIFIED_TABLE) {
            TableBinding binding = bindings.get(record.containerId());
            return binding != null && binding.captured()
                    ? Disposition.CAPTURED_TABLE
                    : Disposition.EXCLUDED_TABLE;
        }
        if (record.outerCode() == NativeRecordReader.OUTER_CONTAINER_DIRECTORY
                || record.outerCode() == NativeRecordReader.OUTER_VIRTUAL_FILE) {
            // LOB/container records require correlation by TID/locator before
            // they can safely be assigned to a captured or excluded row.
            return Disposition.AUXILIARY;
        }
        return Disposition.OTHER;
    }

    public Optional<TableBinding> binding(NativeRecordHeader record) {
        return Optional.ofNullable(bindings.get(record.containerId()));
    }
}

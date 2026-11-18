package io.hanacdc.control;

import io.hanacdc.schema.TableMetadata;

import java.sql.SQLException;
import java.util.List;
import java.util.function.BiPredicate;

public interface HanaControlPlane extends AutoCloseable {
    HanaCapabilities inspect() throws SQLException;

    List<LogBackupEntry> successfulLogBackups(
            long sourceId,
            long afterOrAtRedoPosition,
            long beforeOrAtRedoPosition) throws SQLException;

    PersistenceStreamStatus persistenceStreamStatus(long sourceId) throws SQLException;

    List<TableMetadata> columnTableMetadata() throws SQLException;

    default List<TableMetadata> columnTableMetadata(
            BiPredicate<String, String> tableFilter) throws SQLException {
        return columnTableMetadata().stream()
                .filter(table -> tableFilter.test(table.schemaName(), table.tableName()))
                .toList();
    }

    List<PersistenceService> persistenceServices() throws SQLException;

    @Override
    default void close() throws SQLException {}
}

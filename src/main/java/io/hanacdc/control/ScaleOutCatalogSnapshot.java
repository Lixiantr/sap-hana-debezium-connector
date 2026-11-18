package io.hanacdc.control;

import io.hanacdc.connect.SourceStreamId;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Concurrent bounded catalog refresh across all pinned persistence streams. */
public record ScaleOutCatalogSnapshot(
        Map<SourceStreamId, PersistenceStreamStatus> statuses,
        Map<SourceStreamId, List<BackupFile>> manifests) {

    public ScaleOutCatalogSnapshot {
        statuses = immutableMap(statuses);
        manifests = immutableLists(manifests);
        if (!statuses.keySet().equals(manifests.keySet()) || statuses.size() < 2) {
            throw new IllegalArgumentException("scale-out snapshot stream sets differ");
        }
    }

    public static ScaleOutCatalogSnapshot capture(
            HanaControlPlane controlPlane,
            List<PersistenceService> services,
            Map<SourceStreamId, Long> expectedPositions,
            long partition,
            int workers,
            HanaContract contract,
            PathMapper mapper) throws SQLException {
        Objects.requireNonNull(services, "services");
        Objects.requireNonNull(mapper, "mapper");
        Map<SourceStreamId, PathMapper> mappers = new LinkedHashMap<>();
        services.forEach(service -> mappers.put(SourceStreamId.from(service), mapper));
        return capture(controlPlane, services, expectedPositions, partition,
                workers, contract, mappers);
    }

    public static ScaleOutCatalogSnapshot capture(
            HanaControlPlane controlPlane,
            List<PersistenceService> services,
            Map<SourceStreamId, Long> expectedPositions,
            long partition,
            int workers,
            HanaContract contract,
            Map<SourceStreamId, PathMapper> mappers) throws SQLException {
        Objects.requireNonNull(controlPlane, "controlPlane");
        Objects.requireNonNull(services, "services");
        Objects.requireNonNull(expectedPositions, "expectedPositions");
        Objects.requireNonNull(mappers, "mappers");
        if (services.size() < 2 || partition < 0 || workers <= 0) {
            throw new IllegalArgumentException("invalid scale-out catalog request");
        }
        List<PersistenceService> sorted = services.stream().distinct().sorted().toList();
        List<SourceStreamId> streams = sorted.stream().map(SourceStreamId::from).toList();
        if (!expectedPositions.keySet().equals(java.util.Set.copyOf(streams))) {
            throw new IllegalArgumentException("expected position vector does not match topology");
        }

        ExecutorService executor = Executors.newFixedThreadPool(
                Math.min(workers, sorted.size()));
        try {
            Map<SourceStreamId, Future<CapturedStream>> futures = new LinkedHashMap<>();
            for (PersistenceService service : sorted) {
                SourceStreamId stream = SourceStreamId.from(service);
                long expected = expectedPositions.get(stream);
                if (expected < 0) {
                    throw new IllegalArgumentException("negative expected position for " + stream);
                }
                futures.put(stream, executor.submit(() -> captureStream(
                        controlPlane, service, expected)));
            }

            Map<SourceStreamId, PersistenceStreamStatus> statuses = new LinkedHashMap<>();
            List<LogBackupEntry> entries = new ArrayList<>();
            Map<SourceStreamId, ScaleOutManifestPlanner.StreamWindow> windows =
                    new LinkedHashMap<>();
            for (var item : futures.entrySet()) {
                CapturedStream captured = await(item.getValue(), item.getKey());
                statuses.put(item.getKey(), captured.status());
                entries.addAll(captured.entries());
                windows.put(item.getKey(), new ScaleOutManifestPlanner.StreamWindow(
                        expectedPositions.get(item.getKey()),
                        captured.status().successfulBackupHighWatermark(), partition));
            }
            Map<SourceStreamId, List<BackupFile>> manifests =
                    ScaleOutManifestPlanner.contiguousFiles(
                            sorted, entries, windows, contract, mappers);
            return new ScaleOutCatalogSnapshot(statuses, manifests);
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static CapturedStream captureStream(
            HanaControlPlane controlPlane,
            PersistenceService service,
            long expected) throws SQLException {
        PersistenceStreamStatus status = controlPlane.persistenceStreamStatus(
                service.volumeId());
        if (status.sourceId() != service.volumeId()) {
            throw new SQLException("FATAL_SCALE_OUT_STATUS_IDENTITY: expected source_id="
                    + service.volumeId() + " actual=" + status.sourceId());
        }
        if (status.successfulBackupHighWatermark() < expected) {
            throw new SQLException("FATAL_SCALE_OUT_WATERMARK_REGRESSION: stream="
                    + SourceStreamId.from(service).canonical() + " expected=" + expected
                    + " hwm=" + status.successfulBackupHighWatermark());
        }
        if (status.earliestSuccessfulBackupPosition() > 0
                && status.earliestSuccessfulBackupPosition() > expected) {
            throw new SQLException("FATAL_GAP: stream="
                    + SourceStreamId.from(service).canonical() + " expected=" + expected
                    + " earliest=" + status.earliestSuccessfulBackupPosition());
        }
        List<LogBackupEntry> entries = status.successfulBackupHighWatermark() == expected
                ? List.of()
                : controlPlane.successfulLogBackups(service.volumeId(), expected,
                        status.successfulBackupHighWatermark());
        return new CapturedStream(status, entries);
    }

    private static CapturedStream await(
            Future<CapturedStream> future, SourceStreamId stream) throws SQLException {
        try {
            return future.get();
        }
        catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new SQLException("scale-out catalog refresh interrupted: " + stream, error);
        }
        catch (ExecutionException error) {
            Throwable cause = error.getCause();
            if (cause instanceof SQLException sql) throw sql;
            if (cause instanceof RuntimeException runtime) throw runtime;
            throw new SQLException("scale-out catalog refresh failed: " + stream, cause);
        }
    }

    private static <K, V> Map<K, V> immutableMap(Map<K, V> values) {
        return java.util.Collections.unmodifiableMap(new LinkedHashMap<>(
                Objects.requireNonNull(values, "values")));
    }

    private static <K, V> Map<K, List<V>> immutableLists(Map<K, List<V>> values) {
        Map<K, List<V>> result = new LinkedHashMap<>();
        Objects.requireNonNull(values, "values").forEach(
                (key, value) -> result.put(key, List.copyOf(value)));
        return java.util.Collections.unmodifiableMap(result);
    }

    private record CapturedStream(
            PersistenceStreamStatus status, List<LogBackupEntry> entries) {}
}

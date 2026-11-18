package io.hanacdc.control;

import io.hanacdc.connect.SourceStreamId;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/** Builds one independently contiguous catalog manifest per persistence service. */
public final class ScaleOutManifestPlanner {
    private ScaleOutManifestPlanner() {}

    public record StreamWindow(long firstPosition, long lastPositionExclusive, long partition) {
        public StreamWindow {
            if (firstPosition < 0 || lastPositionExclusive < firstPosition || partition < 0) {
                throw new IllegalArgumentException("invalid scale-out stream window");
            }
        }
    }

    public static Map<SourceStreamId, List<BackupFile>> contiguousFiles(
            List<PersistenceService> services,
            List<LogBackupEntry> entries,
            Map<SourceStreamId, StreamWindow> windows,
            HanaContract contract,
            PathMapper mapper) {
        Objects.requireNonNull(mapper, "mapper");
        Map<SourceStreamId, PathMapper> mappers = new LinkedHashMap<>();
        services.forEach(service -> mappers.put(SourceStreamId.from(service), mapper));
        return contiguousFiles(services, entries, windows, contract, mappers);
    }

    public static Map<SourceStreamId, List<BackupFile>> contiguousFiles(
            List<PersistenceService> services,
            List<LogBackupEntry> entries,
            Map<SourceStreamId, StreamWindow> windows,
            HanaContract contract,
            Map<SourceStreamId, PathMapper> mappers) {
        Objects.requireNonNull(services, "services");
        Objects.requireNonNull(entries, "entries");
        Objects.requireNonNull(windows, "windows");
        Objects.requireNonNull(contract, "contract");
        Objects.requireNonNull(mappers, "mappers");
        if (services.size() < 2) {
            throw fatal("scale-out manifest requires at least two services");
        }

        Map<SourceStreamId, PersistenceService> indexed = new TreeMap<>();
        for (PersistenceService service : services) {
            SourceStreamId stream = SourceStreamId.from(service);
            PersistenceService previous = indexed.putIfAbsent(stream, service);
            if (previous != null) {
                throw fatal("duplicate stable stream " + stream.canonical());
            }
        }
        if (!windows.keySet().equals(indexed.keySet())) {
            Set<SourceStreamId> missing = new java.util.TreeSet<>(indexed.keySet());
            missing.removeAll(windows.keySet());
            Set<SourceStreamId> unknown = new java.util.TreeSet<>(windows.keySet());
            unknown.removeAll(indexed.keySet());
            throw fatal("stream windows mismatch: missing=" + missing + " unknown=" + unknown);
        }
        if (!mappers.keySet().equals(indexed.keySet())) {
            throw fatal("path mapper stream set does not match topology");
        }

        Map<SourceStreamId, List<LogBackupEntry>> grouped = new TreeMap<>();
        for (LogBackupEntry entry : entries) {
            List<Map.Entry<SourceStreamId, PersistenceService>> candidates = indexed.entrySet()
                    .stream().filter(value -> value.getKey().volumeId() == entry.sourceId())
                    .toList();
            if (candidates.isEmpty()) {
                throw fatal("catalog contains unknown source_id=" + entry.sourceId());
            }
            if (candidates.size() != 1) {
                throw fatal("source_id is not unique: " + entry.sourceId());
            }
            var candidate = candidates.get(0);
            PersistenceService service = candidate.getValue();
            if (!service.host().equals(entry.host())
                    || !service.serviceName().equals(entry.serviceType())) {
                throw fatal("catalog service identity mismatch for source_id="
                        + entry.sourceId() + ": expected=" + service + " actual="
                        + entry.host() + ":" + entry.serviceType());
            }
            grouped.computeIfAbsent(candidate.getKey(), ignored -> new ArrayList<>())
                    .add(entry);
        }

        Map<SourceStreamId, List<BackupFile>> result = new LinkedHashMap<>();
        for (var serviceEntry : indexed.entrySet()) {
            SourceStreamId stream = serviceEntry.getKey();
            StreamWindow window = windows.get(stream);
            List<BackupFile> files = window.firstPosition() == window.lastPositionExclusive()
                    ? List.of()
                    : ManifestPlanner.contiguousFiles(
                            grouped.getOrDefault(stream, List.of()), window.firstPosition(),
                            window.lastPositionExclusive(), stream.volumeId(), stream.volumeId(),
                            window.partition(), contract, mappers.get(stream));
            result.put(stream, files);
        }
        return java.util.Collections.unmodifiableMap(result);
    }

    private static IllegalStateException fatal(String detail) {
        return new IllegalStateException("FATAL_SCALE_OUT_MANIFEST: " + detail);
    }
}

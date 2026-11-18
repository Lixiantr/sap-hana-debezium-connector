package io.hanacdc.control;

import java.util.Locale;
import java.util.Objects;

/** Stable JDBC identity of one persistence-bearing HANA service. */
public record PersistenceService(
        String host,
        int port,
        String serviceName,
        long volumeId) implements Comparable<PersistenceService> {

    public PersistenceService {
        host = Objects.requireNonNull(host, "host").trim();
        serviceName = Objects.requireNonNull(serviceName, "serviceName")
                .trim().toLowerCase(Locale.ROOT);
        if (host.isEmpty() || serviceName.isEmpty() || port <= 0 || volumeId <= 0) {
            throw new IllegalArgumentException("invalid HANA persistence service identity");
        }
    }

    @Override
    public int compareTo(PersistenceService other) {
        int value = host.compareTo(other.host);
        if (value != 0) return value;
        value = Integer.compare(port, other.port);
        if (value != 0) return value;
        value = serviceName.compareTo(other.serviceName);
        if (value != 0) return value;
        return Long.compare(volumeId, other.volumeId);
    }
}

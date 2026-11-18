package io.hanacdc.connect;

import io.hanacdc.control.PersistenceService;

import java.util.Locale;
import java.util.Objects;

/** Stable identity of one persistence log stream inside a pinned topology epoch. */
public record SourceStreamId(long volumeId, String serviceName)
        implements Comparable<SourceStreamId> {

    public SourceStreamId {
        serviceName = Objects.requireNonNull(serviceName, "serviceName")
                .strip().toLowerCase(Locale.ROOT);
        if (volumeId <= 0 || serviceName.isEmpty()
                || !serviceName.matches("[a-z0-9_]+")) {
            throw new IllegalArgumentException("invalid source stream identity");
        }
    }

    public static SourceStreamId from(PersistenceService service) {
        Objects.requireNonNull(service, "service");
        return new SourceStreamId(service.volumeId(), service.serviceName());
    }

    public String canonical() {
        return "volume=" + volumeId + ";service=" + serviceName;
    }

    public static SourceStreamId parse(String value) {
        Objects.requireNonNull(value, "value");
        String[] fields = value.split(";", -1);
        if (fields.length != 2 || !fields[0].startsWith("volume=")
                || !fields[1].startsWith("service=")) {
            throw new IllegalArgumentException("invalid source stream encoding");
        }
        try {
            return new SourceStreamId(Long.parseLong(fields[0].substring(7)),
                    fields[1].substring(8));
        }
        catch (NumberFormatException error) {
            throw new IllegalArgumentException("invalid source stream volume", error);
        }
    }

    @Override
    public int compareTo(SourceStreamId other) {
        int value = Long.compare(volumeId, other.volumeId);
        return value != 0 ? value : serviceName.compareTo(other.serviceName);
    }
}

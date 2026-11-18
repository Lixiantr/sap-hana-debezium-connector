package io.hanacdc.connect;

import java.util.Locale;

/** User-selected maximum HANA source topology. */
enum SourceTopologyMode {
    SINGLE_CONTAINER("single_container", 0),
    SINGLE_SERVICE("single_service", 1),
    SCALE_OUT("scale_out", 2),
    AUTO("auto", Integer.MAX_VALUE);

    private final String configValue;
    private final int capacity;

    SourceTopologyMode(String configValue, int capacity) {
        this.configValue = configValue;
        this.capacity = capacity;
    }

    String configValue() {
        return configValue;
    }

    boolean permits(SourceTopologyMode actual) {
        return this == AUTO || capacity >= actual.capacity;
    }

    static SourceTopologyMode parse(String value) {
        String normalized = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
        for (SourceTopologyMode mode : values()) {
            if (mode.configValue.equals(normalized)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("unknown source topology mode: " + value);
    }
}

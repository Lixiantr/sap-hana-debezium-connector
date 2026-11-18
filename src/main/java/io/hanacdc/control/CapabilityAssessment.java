package io.hanacdc.control;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Fail-closed assessment of a HANA snapshot against the connector contract. */
public record CapabilityAssessment(List<Violation> violations) {
    public enum Severity {
        POLICY,
        BLOCKED,
        FATAL
    }

    public record Violation(Severity severity, String code, String message) {
        public Violation {
            Objects.requireNonNull(severity, "severity");
            if (code == null || code.isBlank() || message == null || message.isBlank()) {
                throw new IllegalArgumentException("violation code and message are required");
            }
        }
    }

    public CapabilityAssessment {
        violations = List.copyOf(violations);
    }

    public boolean canRead() {
        return violations.stream().noneMatch(value -> value.severity() != Severity.POLICY);
    }

    public boolean ready() {
        return violations.isEmpty();
    }

    public String summary() {
        return violations.stream()
                .map(value -> value.severity() + ":" + value.code() + ":" + value.message())
                .collect(Collectors.joining("; "));
    }
}

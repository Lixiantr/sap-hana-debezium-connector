package io.hanacdc.control;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Explicit allowlisted mapping from catalog paths to read-only connector mounts. */
public final class PathMapper {
    private record Mapping(String hanaRoot, Path localRoot) {}

    private final List<Mapping> mappings;

    private PathMapper(List<Mapping> mappings) {
        this.mappings = mappings.stream()
                .sorted(Comparator.comparingInt((Mapping value) -> value.hanaRoot().length())
                        .reversed())
                .toList();
    }

    public static PathMapper parse(String specification) {
        if (specification == null || specification.isBlank()) {
            throw new IllegalArgumentException("log.backup.path.mapping is required in catalog mode");
        }
        List<Mapping> result = new ArrayList<>();
        for (String item : specification.split(";")) {
            int separator = item.indexOf('=');
            if (separator <= 0 || separator == item.length() - 1) {
                throw new IllegalArgumentException("invalid path mapping: " + item);
            }
            String hanaRoot = normalizeHana(item.substring(0, separator));
            Path localRoot = Path.of(item.substring(separator + 1).trim())
                    .toAbsolutePath().normalize();
            result.add(new Mapping(hanaRoot, localRoot));
        }
        return new PathMapper(List.copyOf(result));
    }

    public Path resolve(String destinationPath) {
        String normalized = normalizeHana(destinationPath);
        for (Mapping mapping : mappings) {
            if (!(normalized.equals(mapping.hanaRoot())
                    || normalized.startsWith(mapping.hanaRoot() + "/"))) {
                continue;
            }
            String relative = normalized.substring(mapping.hanaRoot().length());
            while (relative.startsWith("/")) {
                relative = relative.substring(1);
            }
            Path resolved = mapping.localRoot().resolve(relative.replace('/', java.io.File.separatorChar))
                    .normalize();
            if (!resolved.startsWith(mapping.localRoot())) {
                throw new IllegalStateException("catalog path escapes allowlisted root: "
                        + destinationPath);
            }
            return resolved;
        }
        throw new IllegalStateException("catalog path has no allowlisted mapping: "
                + destinationPath);
    }

    private static String normalizeHana(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("HANA path cannot be empty");
        }
        String result = value.trim().replace('\\', '/');
        while (result.length() > 1 && result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }
}

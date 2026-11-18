package io.hanacdc.connect;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class SupportMatrixFreshnessTest {
    private static final Pattern CANDIDATE_VERSION = Pattern.compile("(?i)\\bdev\\.\\d+\\b");
    private static final Pattern CYRILLIC = Pattern.compile("\\p{IsCyrillic}");
    private static final String KAFKA_IMAGE_DIGEST =
            "apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837";

    @Test
    void supportMatrixDefinesAStableBoundaryWithoutCandidateProgress() throws IOException {
        String matrix = Files.readString(Path.of("docs", "compatibility.md"));

        assertFalse(CANDIDATE_VERSION.matcher(matrix).find());
        assertFalse(matrix.toLowerCase(Locale.ROOT).contains("requalification pending"));
        assertTrue(matrix.contains(KAFKA_IMAGE_DIGEST));
    }

    @Test
    void documentationHasOneEnglishVersionPerDocument() throws IOException {
        List<Path> documents;
        try (var paths = Files.list(Path.of("docs"))) {
            documents = paths.filter(Files::isRegularFile).toList();
        }
        assertFalse(documents.isEmpty());
        for (Path document : documents) {
            String name = document.getFileName().toString();
            assertTrue(name.endsWith(".md"), name);
            assertFalse(name.endsWith(".ru.md") || name.endsWith(".en.md"), name);
            assertFalse(CYRILLIC.matcher(Files.readString(document)).find(), name);
        }
    }
}

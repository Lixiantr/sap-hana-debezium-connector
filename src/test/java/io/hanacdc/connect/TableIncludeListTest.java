package io.hanacdc.connect;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class TableIncludeListTest {
    @Test
    void usesAnchoredDebeziumStyleExpressions() {
        TableIncludeList selection = TableIncludeList.compile(
                List.of("CDC_FORMAT_F0804A\\.SIMPLE_ROW", "SALES\\..*"));

        assertTrue(selection.includes("CDC_FORMAT_F0804A", "SIMPLE_ROW"));
        assertTrue(selection.includes("SALES", "ORDERS"));
        assertFalse(selection.includes("OTHER", "SIMPLE_ROW"));
        assertFalse(selection.includes("XSALES", "ORDERS"));
    }

    @Test
    void rejectsEmptyAndInvalidSelections() {
        assertThrows(ConfigException.class, () -> TableIncludeList.compile(List.of()));
        assertThrows(ConfigException.class, () -> TableIncludeList.compile(List.of("[")));
    }
}

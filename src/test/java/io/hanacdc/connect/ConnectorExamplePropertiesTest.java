package io.hanacdc.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

final class ConnectorExamplePropertiesTest {
    @Test
    void productionExampleMatchesReleasedConfigDef() throws Exception {
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(Path.of("connector.example.properties"))) {
            properties.load(input);
        }
        Map<String, String> values = new LinkedHashMap<>();
        properties.forEach((key, value) -> values.put(key.toString(), value.toString()));

        AbstractConfig parsed = new AbstractConfig(HanaSourceConnector.CONFIG_DEF, values);
        TableIncludeList.compile(parsed.getList(HanaSourceConnector.TABLE_INCLUDE_LIST));

        assertEquals(false, parsed.getBoolean(HanaSourceConnector.PROTOTYPE_MODE));
        assertEquals("no_data", parsed.getString(HanaSourceConnector.SNAPSHOT_MODE));
        assertEquals("catalog", parsed.getString(HanaSourceConnector.DISCOVERY_MODE));
        assertEquals(4_096, parsed.getInt(HanaSourceConnector.POLL_MAX_BATCH_SIZE));
        assertEquals(7_200L, parsed.getLong(HanaSourceConnector.RETENTION_MIN_SECONDS));
        assertFalse(parsed.getString(HanaSourceConnector.STATE_DIR).isBlank());
    }
}

package io.hanacdc.cli;

import io.hanacdc.control.JdbcHanaControlPlane;
import io.hanacdc.schema.TableDecodePlan;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.regex.Pattern;

/** Read-only verification of catalog metadata and schema fingerprint on HANA. */
public final class TableMetadataProbeMain {
    private TableMetadataProbeMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            throw new IllegalArgumentException(
                    "usage: TableMetadataProbeMain <connector.properties> <table-regex> [password-file]");
        }
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(Path.of(args[0]))) {
            properties.load(input);
        }
        if (args.length == 3) {
            properties.setProperty("database.password", Files.readString(Path.of(args[2])).strip());
        }
        Pattern include = Pattern.compile(args[1]);
        try (JdbcHanaControlPlane control = JdbcHanaControlPlane.connect(
                properties.getProperty("database.jdbc.driver", "com.sap.db.jdbc.Driver"),
                required(properties, "database.connection.url"),
                required(properties, "database.user"),
                required(properties, "database.password"),
                Long.parseLong(properties.getProperty("source.id", "2")),
                Integer.parseInt(properties.getProperty("control.query.timeout.seconds", "30")))) {
            long matched = 0;
            for (var table : control.columnTableMetadata()) {
                if (!include.matcher(table.qualifiedName()).matches()) {
                    continue;
                }
                TableDecodePlan plan = TableDecodePlan.compile(table);
                System.out.printf("table=%s oid=%d partitions=%s columns=%d pk=%s "
                                + "schema_fingerprint=%s topology_fingerprint=%s%n",
                        table.qualifiedName(), table.objectId(), table.partitions(),
                        table.columns().size(), table.primaryKeyColumns(),
                        plan.schemaFingerprint(), plan.topologyFingerprint());
                matched++;
            }
            if (matched == 0) {
                throw new IllegalStateException("FATAL_SCHEMA_CONTRACT: no table matched");
            }
        }
    }

    private static String required(Properties properties, String name) {
        String value = properties.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }
}

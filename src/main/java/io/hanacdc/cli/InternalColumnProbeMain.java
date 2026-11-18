package io.hanacdc.cli;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.Properties;

/** Read-only mapping of SQL columns to internal column-store identifiers. */
public final class InternalColumnProbeMain {
    private static final String VISIBLE_SQL = """
            SELECT POSITION, COLUMN_NAME, COLUMN_ID, DATA_TYPE_NAME
              FROM SYS.TABLE_COLUMNS
             WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
             ORDER BY POSITION
            """;
    private static final String INTERNAL_SQL = """
            SELECT POSITION, COLUMN_NAME, INTERNAL_ATTRIBUTE_TYPE, INTERNAL_COLUMN_ID
              FROM SYS.CS_ALL_COLUMNS
             WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
             ORDER BY INTERNAL_COLUMN_ID
            """;

    private InternalColumnProbeMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException(
                    "usage: InternalColumnProbeMain <properties> <schema> <table> <password-file>");
        }
        Properties settings = new Properties();
        try (InputStream input = Files.newInputStream(Path.of(args[0]))) {
            settings.load(input);
        }
        Class.forName(settings.getProperty("database.jdbc.driver", "com.sap.db.jdbc.Driver"));
        Properties credentials = new Properties();
        credentials.setProperty("user", required(settings, "database.user"));
        credentials.setProperty("password", Files.readString(Path.of(args[3])).strip());
        try (var connection = DriverManager.getConnection(
                required(settings, "database.connection.url"), credentials)) {
            connection.setReadOnly(true);
            probe(connection, VISIBLE_SQL, args[1], args[2], false);
            probe(connection, INTERNAL_SQL, args[1], args[2], true);
        }
    }

    private static void probe(
            java.sql.Connection connection,
            String sql,
            String schema,
            String table,
            boolean internal) throws Exception {
        try (var statement = connection.prepareStatement(sql)) {
            statement.setString(1, schema);
            statement.setString(2, table);
            try (var rows = statement.executeQuery()) {
                while (rows.next()) {
                    if (internal) {
                        System.out.printf("internal id=%d position=%d column=%s attribute=%s%n",
                                rows.getLong("INTERNAL_COLUMN_ID"), rows.getInt("POSITION"),
                                rows.getString("COLUMN_NAME"),
                                rows.getString("INTERNAL_ATTRIBUTE_TYPE"));
                    }
                    else {
                        System.out.printf("visible id=%d position=%d column=%s type=%s%n",
                                rows.getLong("COLUMN_ID"), rows.getInt("POSITION"),
                                rows.getString("COLUMN_NAME"), rows.getString("DATA_TYPE_NAME"));
                    }
                }
            }
        }
    }

    private static String required(Properties settings, String name) {
        String value = settings.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }
}

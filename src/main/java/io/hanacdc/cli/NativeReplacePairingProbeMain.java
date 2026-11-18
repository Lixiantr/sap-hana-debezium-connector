package io.hanacdc.cli;

import io.hanacdc.connect.TableIncludeList;
import io.hanacdc.connect.TablePlanSelector;
import io.hanacdc.control.JdbcHanaControlPlane;
import io.hanacdc.decoder.NativeRecordReader;
import io.hanacdc.decoder.NativeRowLobPipeline;
import io.hanacdc.decoder.ParallelLogDecoder;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Diagnoses pairing of a physical delete and row image within a REPLACE transaction. */
public final class NativeReplacePairingProbeMain {
    private NativeReplacePairingProbeMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 8) {
            throw new IllegalArgumentException("usage: NativeReplacePairingProbeMain "
                    + "<connector.properties> <password-file> <table-regex> "
                    + "<backup-dir> <first> <last> <workers> <max-transactions>");
        }
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(Path.of(args[0]))) {
            properties.load(input);
        }
        List<io.hanacdc.schema.TableDecodePlan> plans;
        try (JdbcHanaControlPlane control = JdbcHanaControlPlane.connect(
                properties.getProperty("database.jdbc.driver", "com.sap.db.jdbc.Driver"),
                required(properties, "database.connection.url"),
                required(properties, "database.user"),
                Files.readString(Path.of(args[1])).strip(),
                Long.parseLong(properties.getProperty("source.id", "2")),
                Integer.parseInt(properties.getProperty(
                        "control.query.timeout.seconds", "30")))) {
            plans = TablePlanSelector.select(
                    TableIncludeList.compile(List.of(args[2])),
                    control.columnTableMetadata());
        }

        long first = Long.parseLong(args[4]);
        long last = Long.parseLong(args[5]);
        int workers = Integer.parseInt(args[6]);
        int remaining = Integer.parseInt(args[7]);
        long observed = 0;
        Map<Long, String> keyByLocator = new LinkedHashMap<>();
        Map<String, Long> currentLocatorByKey = new LinkedHashMap<>();
        try (NativeRowLobPipeline pipeline = new NativeRowLobPipeline(plans, workers)) {
            outer:
            for (var backup : ParallelLogDecoder.discoverCorpus(Path.of(args[3]), first, last)) {
                try (var cursor = pipeline.open(backup)) {
                    while (cursor.hasNext()) {
                        var transaction = cursor.next();
                        if (transaction.outcome() != NativeRowLobPipeline.Outcome.COMMIT) {
                            continue;
                        }
                        for (var row : transaction.rows()) {
                            String key = row.plan().metadata().qualifiedName() + ':'
                                    + row.values().get(row.plan().primaryKeyIndexes().get(0)).value();
                            keyByLocator.put(row.rowId(), key);
                            currentLocatorByKey.put(key, row.rowId());
                        }
                        if (transaction.physicalDeletes().isEmpty()) {
                            continue;
                        }
                        observed++;
                        System.out.printf("tid=%d terminal=%d rows=%d unmatched_deletes=%d%n",
                                transaction.tid(), transaction.terminalPosition(),
                                transaction.rows().size(), transaction.physicalDeletes().size());
                        for (var delete : transaction.physicalDeletes()) {
                            String knownKey = keyByLocator.remove(delete.rowId());
                            Long currentLocator = knownKey == null
                                    ? null : currentLocatorByKey.get(knownKey);
                            var sameLocator = transaction.rows().stream()
                                    .filter(row -> row.plan().metadata().objectId()
                                            == delete.plan().metadata().objectId())
                                    .filter(row -> row.rowId() == delete.rowId())
                                    .map(row -> row.logPosition() + ":" + row.operationCode())
                                    .toList();
                            var tableUpserts = transaction.rows().stream()
                                    .filter(row -> row.plan().metadata().objectId()
                                            == delete.plan().metadata().objectId())
                                    .filter(row -> row.operationCode()
                                            == NativeRecordReader.UT_UPSERT)
                                    .map(row -> row.logPosition() + ":locator=" + row.rowId())
                                    .limit(8)
                                    .toList();
                            System.out.printf(" delete_position=%d locator=%d "
                                            + "known_key=%s current_locator=%s superseded=%s "
                                            + "same_locator_rows=%s first_table_upserts=%s%n",
                                    delete.logPosition(), delete.rowId(), knownKey, currentLocator,
                                    currentLocator != null && currentLocator != delete.rowId(),
                                    sameLocator, tableUpserts);
                        }
                        if (--remaining == 0) {
                            break outer;
                        }
                    }
                }
            }
        }
        System.out.printf("replace_pairing_probe=PASS observed=%d%n", observed);
    }

    private static String required(Properties properties, String name) {
        String value = properties.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }
}

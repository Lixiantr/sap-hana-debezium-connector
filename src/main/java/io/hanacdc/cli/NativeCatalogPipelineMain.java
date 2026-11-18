package io.hanacdc.cli;

import io.hanacdc.connect.TableIncludeList;
import io.hanacdc.connect.TablePlanSelector;
import io.hanacdc.control.JdbcHanaControlPlane;
import io.hanacdc.decoder.NativeRowDecoder;
import io.hanacdc.decoder.NativeRowLobPipeline;
import io.hanacdc.decoder.NativeTransactionReducer;
import io.hanacdc.decoder.ParallelLogDecoder;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

/** Live-catalog gate: JDBC metadata -> native plans -> real backup corpus. */
public final class NativeCatalogPipelineMain {
    private NativeCatalogPipelineMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 10) {
            throw new IllegalArgumentException("usage: NativeCatalogPipelineMain "
                    + "<connector.properties> <password-file> <table-regex> "
                    + "<backup-dir> <first> <last> <expected-rows> "
                    + "<expected-changes> <expected-physical-deletes> <workers>");
        }
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(Path.of(args[0]))) {
            properties.load(input);
        }
        String password = Files.readString(Path.of(args[1])).strip();
        long first = Long.parseLong(args[4]);
        long last = Long.parseLong(args[5]);
        long expectedRows = Long.parseLong(args[6]);
        long expectedChanges = Long.parseLong(args[7]);
        long expectedPhysicalDeletes = Long.parseLong(args[8]);
        int workers = Integer.parseInt(args[9]);

        List<io.hanacdc.schema.TableDecodePlan> plans;
        try (JdbcHanaControlPlane control = JdbcHanaControlPlane.connect(
                properties.getProperty("database.jdbc.driver", "com.sap.db.jdbc.Driver"),
                required(properties, "database.connection.url"),
                required(properties, "database.user"), password,
                Long.parseLong(properties.getProperty("source.id", "2")),
                Integer.parseInt(properties.getProperty(
                        "control.query.timeout.seconds", "30")))) {
            plans = TablePlanSelector.select(
                    TableIncludeList.compile(List.of(args[2])),
                    control.columnTableMetadata());
        }

        long transactions = 0;
        long rows = 0;
        long deletes = 0;
        long logicalChanges = 0;
        long creates = 0;
        long updates = 0;
        long logicalDeletes = 0;
        NativeTransactionReducer reducer = new NativeTransactionReducer();
        try (NativeRowLobPipeline pipeline = new NativeRowLobPipeline(plans, workers)) {
            for (var backup : ParallelLogDecoder.discoverCorpus(
                    Path.of(args[3]), first, last)) {
                try (var cursor = pipeline.open(backup)) {
                    while (cursor.hasNext()) {
                        var transaction = cursor.next();
                        if (transaction.outcome() != NativeRowLobPipeline.Outcome.COMMIT) {
                            continue;
                        }
                        transactions++;
                        rows += transaction.rows().size();
                        deletes += transaction.physicalDeletes().size();
                        for (var row : transaction.rows()) {
                            if (!row.fullImage() || row.values().stream().anyMatch(value ->
                                    value.kind() == NativeRowDecoder.ValueKind.MISSING
                                            || value.kind()
                                            == NativeRowDecoder.ValueKind.LOB_REFERENCE)) {
                                throw new IllegalStateException(
                                        "pipeline emitted incomplete row at "
                                                + row.logPosition());
                            }
                        }
                        if (expectedChanges >= 0) {
                            var reduced = reducer.reduce(transaction).orElseThrow();
                            logicalChanges += reduced.changes().size();
                            creates += reduced.changes().stream().filter(change ->
                                    change.operation() == NativeTransactionReducer.Operation.CREATE)
                                    .count();
                            updates += reduced.changes().stream().filter(change ->
                                    change.operation() == NativeTransactionReducer.Operation.UPDATE)
                                    .count();
                            logicalDeletes += reduced.changes().stream().filter(change ->
                                    change.operation() == NativeTransactionReducer.Operation.DELETE)
                                    .count();
                        }
                    }
                }
            }
            pipeline.finish();
        }
        if (rows != expectedRows || (expectedChanges >= 0 && logicalChanges != expectedChanges)
                || deletes != expectedPhysicalDeletes) {
            throw new IllegalStateException("catalog pipeline expected rows=" + expectedRows
                    + " changes=" + expectedChanges + " deletes=" + expectedPhysicalDeletes
                    + ", actual rows=" + rows
                    + " changes=" + logicalChanges + " deletes=" + deletes);
        }
        System.out.printf("native_catalog_pipeline=PASS decode_only=%s plans=%d transactions=%d rows=%d "
                        + "logical_changes=%d c=%d u=%d d=%d physical_deletes=%d workers=%d%n",
                expectedChanges < 0, plans.size(), transactions, rows, logicalChanges, creates, updates,
                logicalDeletes, deletes, workers);
    }

    private static String required(Properties properties, String name) {
        String value = properties.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }
}

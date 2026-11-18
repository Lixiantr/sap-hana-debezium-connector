package io.hanacdc.cli;

import io.hanacdc.decoder.NativeRowLobPipeline;
import io.hanacdc.decoder.ParallelLogDecoder;
import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;

import java.nio.file.Path;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/** Regression gate for characterized HANA savepoint/full rollback grammar. */
public final class NativePipelineSavepointMain {
    private NativePipelineSavepointMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "usage: NativePipelineSavepointMain <dir> <first> <last>");
        }
        TableDecodePlan plan = plan();
        List<String> committed = new ArrayList<>();
        long deletes = 0;
        try (NativeRowLobPipeline pipeline = new NativeRowLobPipeline(List.of(plan), 4)) {
            for (var backup : ParallelLogDecoder.discoverCorpus(
                    Path.of(args[0]), Long.parseLong(args[1]), Long.parseLong(args[2]))) {
                try (var cursor = pipeline.open(backup)) {
                    while (cursor.hasNext()) {
                        var transaction = cursor.next();
                        if (transaction.outcome() != NativeRowLobPipeline.Outcome.COMMIT) {
                            continue;
                        }
                        deletes += transaction.physicalDeletes().size();
                        for (var row : transaction.rows()) {
                            committed.add(row.tid() + ":" + row.values().get(0).value()
                                    + ":" + row.values().get(2).value()
                                    + ":" + row.values().get(3).value());
                        }
                    }
                }
            }
            pipeline.finish();
        }
        List<String> expected = List.of(
                "204556:940804901:1:SP-A-BEFORE",
                "204558:940804903:1:SP-NESTED-BASE",
                "204558:940804903:2:SP-NESTED-KEEP");
        if (!expected.equals(committed) || deletes != 0) {
            throw new IllegalStateException("savepoint truth mismatch: rows="
                    + committed + " deletes=" + deletes);
        }
        System.out.printf("native_pipeline_savepoint=PASS committed_rows=%d "
                        + "physical_deletes=%d fingerprint=%s%n",
                committed.size(), deletes, plan.schemaFingerprint());
    }

    private static TableDecodePlan plan() {
        return TableDecodePlan.compile(new TableMetadata(
                "CDC_CONTRACT_C0803B", "FULL_ROW", "COLUMN", 337217991L, 0,
                List.of(
                        column(1, "ID", "BIGINT", Types.BIGINT, 19, 0),
                        column(2, "IS_DELETED", "BOOLEAN", Types.BOOLEAN, 1, 0),
                        column(3, "ROW_VERSION", "BIGINT", Types.BIGINT, 19, 0),
                        column(4, "VALUE_TEXT", "NVARCHAR", Types.NVARCHAR, 512, 0),
                        column(5, "AMOUNT", "DECIMAL", Types.DECIMAL, 20, 4),
                        column(6, "EVENT_TS", "TIMESTAMP", Types.TIMESTAMP, 27, 7),
                        column(7, "C_CLOB", "CLOB", Types.CLOB, 0, 0),
                        column(8, "C_NCLOB", "NCLOB", Types.NCLOB, 0, 0),
                        column(9, "C_BLOB", "BLOB", Types.BLOB, 0, 0)),
                List.of(), List.of("ID")));
    }

    private static TableMetadata.Column column(
            int ordinal, String name, String type, int dataTypeId, int precision, int scale) {
        return new TableMetadata.Column(ordinal, 200L + ordinal, name, type, dataTypeId,
                Math.max(0, precision), Math.max(0, precision), Math.max(0, scale),
                false, false, false, "");
    }
}

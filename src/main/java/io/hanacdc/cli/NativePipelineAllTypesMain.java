package io.hanacdc.cli;

import io.hanacdc.decoder.NativeRowDecoder;
import io.hanacdc.decoder.NativeRowLobPipeline;
import io.hanacdc.decoder.ParallelLogDecoder;

import java.nio.file.Path;

/** Real-corpus gate for the unified production row/LOB pipeline API. */
public final class NativePipelineAllTypesMain {
    private NativePipelineAllTypesMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "usage: NativePipelineAllTypesMain <dir> <first> <last>");
        }
        var plan = NativeGenericAllTypesMain.plan();
        long committedTransactions = 0;
        long rows = 0;
        long deletes = 0;
        try (NativeRowLobPipeline pipeline = new NativeRowLobPipeline(
                java.util.List.of(plan), 4)) {
            for (var backup : ParallelLogDecoder.discoverCorpus(
                    Path.of(args[0]), Long.parseLong(args[1]), Long.parseLong(args[2]))) {
                try (var cursor = pipeline.open(backup)) {
                    while (cursor.hasNext()) {
                        var transaction = cursor.next();
                        if (transaction.outcome() != NativeRowLobPipeline.Outcome.COMMIT) {
                            continue;
                        }
                        committedTransactions++;
                        rows += transaction.rows().size();
                        deletes += transaction.physicalDeletes().size();
                        for (var row : transaction.rows()) {
                            if (!row.fullImage()
                                    || row.values().stream().anyMatch(value ->
                                    value.kind() == NativeRowDecoder.ValueKind.MISSING
                                            || value.kind()
                                            == NativeRowDecoder.ValueKind.LOB_REFERENCE)) {
                                throw new IllegalStateException(
                                        "pipeline emitted incomplete row at " + row.logPosition());
                            }
                        }
                    }
                }
            }
            pipeline.finish();
        }
        if (rows != 65 || deletes != 0) {
            throw new IllegalStateException("expected 65 rows and no physical deletes, actual rows="
                    + rows + " deletes=" + deletes);
        }
        System.out.printf("native_pipeline_all_types=PASS transactions=%d rows=%d "
                        + "physical_deletes=%d fingerprint=%s%n",
                committedTransactions, rows, deletes, plan.schemaFingerprint());
    }
}

package io.hanacdc.cli;

import io.hanacdc.decoder.NativeLobReconstructor;
import io.hanacdc.decoder.NativeRecordReader;
import io.hanacdc.decoder.NativeRowDecoder;
import io.hanacdc.decoder.ParallelLogDecoder;
import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;

import java.nio.file.Path;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/** Real-corpus gate for nullable rows and locator resolution. */
public final class NativeGenericEdgeMain {
    private NativeGenericEdgeMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException(
                    "usage: NativeGenericEdgeMain <dir> <first> <last> <tid>");
        }
        long tid = Long.parseLong(args[3]);
        TableDecodePlan plan = plan();
        NativeRowDecoder decoder = new NativeRowDecoder();
        List<NativeRowDecoder.DecodedRow> rows = new ArrayList<>();
        try (NativeLobReconstructor.Session lobs = new NativeLobReconstructor.Session()) {
            for (var backup : ParallelLogDecoder.discoverCorpus(
                    Path.of(args[0]), Long.parseLong(args[1]), Long.parseLong(args[2]))) {
                try (var cursor = NativeRecordReader.open(backup)) {
                    while (cursor.hasNext()) {
                        var record = cursor.next();
                        if (record.tid() != tid) continue;
                        lobs.accept(record);
                        if (record.operationCode() == NativeRecordReader.UT_UPSERT) {
                            rows.add(decoder.decode(record, plan, cursor.readPayload(record)));
                        }
                    }
                }
            }
            var reconstructed = lobs.finish();
            rows = rows.stream().map(row -> {
                try {
                    return decoder.resolveLobs(row, reconstructed);
                }
                catch (java.io.IOException error) {
                    throw new IllegalStateException(error);
                }
            }).toList();
            if (rows.size() != 60 || rows.stream().anyMatch(row -> !row.fullImage())) {
                throw new IllegalStateException("expected 60 full generic edge rows");
            }
            verify(rows);
            System.out.printf("generic_edge=PASS images=%d reconstructed_lobs=%d "
                            + "fingerprint=%s%n",
                    rows.size(), reconstructed.size(), plan.schemaFingerprint());
        }
    }

    private static void verify(List<NativeRowDecoder.DecodedRow> rows) {
        equal(rows, 1, 2, "-987654321098765432109876.0987654321");
        equal(rows, 3, 2, "0.0000000000");
        equal(rows, 5, 3, "-98765.4321");
        equal(rows, 7, 3, "0.0000");
        kind(rows, 9, 2, NativeRowDecoder.ValueKind.NULL);
        kind(rows, 11, 3, NativeRowDecoder.ValueKind.NULL);
        kind(rows, 13, 4, NativeRowDecoder.ValueKind.NULL);
        kind(rows, 15, 5, NativeRowDecoder.ValueKind.NULL);
        kind(rows, 17, 6, NativeRowDecoder.ValueKind.NULL);
        kind(rows, 19, 7, NativeRowDecoder.ValueKind.NULL);
        kind(rows, 21, 8, NativeRowDecoder.ValueKind.NULL);
        kind(rows, 23, 9, NativeRowDecoder.ValueKind.NULL);
        equal(rows, 25, 4, "");
        length(rows, 27, 4, 300);
        equal(rows, 29, 5, "");
        length(rows, 31, 5, 240);
        length(rows, 33, 5, 255);
        length(rows, 35, 5, 256);
        length(rows, 37, 5, 4096);
        length(rows, 39, 5, 131072);
        equal(rows, 41, 6, "");
        length(rows, 43, 6, 4096);
        byteLength(rows, 45, 7, 0);
        byteLength(rows, 47, 7, 240);
        byteLength(rows, 49, 7, 255);
        byteLength(rows, 51, 7, 256);
        byteLength(rows, 53, 7, 4096);
        byteLength(rows, 55, 7, 131072);
        equal(rows, 57, 8, "One,  TWO!  café E0804C\nline-2");
        equal(rows, 59, 9, "Bin, TEXT! café E0804C\nline-2");
    }

    private static void equal(
            List<NativeRowDecoder.DecodedRow> rows, int sequence, int column, String expected) {
        Object value = rows.get(sequence - 1).values().get(column).value();
        String actual = value instanceof java.math.BigDecimal decimal
                ? decimal.toPlainString() : String.valueOf(value);
        if (!expected.equals(actual)) {
            throw new IllegalStateException("sequence=" + sequence + " column=" + column
                    + " expected=" + expected + " actual=" + actual);
        }
    }

    private static void kind(
            List<NativeRowDecoder.DecodedRow> rows,
            int sequence,
            int column,
            NativeRowDecoder.ValueKind expected) {
        if (rows.get(sequence - 1).values().get(column).kind() != expected) {
            throw new IllegalStateException("unexpected NULL state at sequence=" + sequence);
        }
    }

    private static void length(
            List<NativeRowDecoder.DecodedRow> rows, int sequence, int column, int expected) {
        Object value = rows.get(sequence - 1).values().get(column).value();
        if (!(value instanceof String text) || text.length() != expected) {
            throw new IllegalStateException("unexpected text length at sequence=" + sequence);
        }
    }

    private static void byteLength(
            List<NativeRowDecoder.DecodedRow> rows, int sequence, int column, int expected) {
        Object value = rows.get(sequence - 1).values().get(column).value();
        if (!(value instanceof byte[] bytes) || bytes.length != expected) {
            throw new IllegalStateException("unexpected binary length at sequence=" + sequence);
        }
    }

    private static TableDecodePlan plan() {
        List<TableMetadata.Column> columns = new ArrayList<>();
        add(columns, "ID", "BIGINT", Types.BIGINT, 19, 0, false);
        add(columns, "ROW_VERSION", "BIGINT", Types.BIGINT, 19, 0, false);
        add(columns, "C_DECIMAL", "DECIMAL", Types.DECIMAL, 34, 10, true);
        add(columns, "C_SMALLDECIMAL", "SMALLDECIMAL", Types.DECIMAL, 16, 0, true);
        add(columns, "C_NVARCHAR", "NVARCHAR", Types.NVARCHAR, 2048, 0, true);
        add(columns, "C_CLOB", "CLOB", Types.CLOB, 16, 1000, true);
        add(columns, "C_NCLOB", "NCLOB", Types.NCLOB, 24, 1000, true);
        add(columns, "C_BLOB", "BLOB", Types.BLOB, 16, 1000, true);
        add(columns, "C_TEXT", "TEXT", Types.CLOB, 0, 0, true);
        add(columns, "C_BINTEXT", "BINTEXT", Types.CLOB, 0, 0, true);
        return TableDecodePlan.compileSchema(new TableMetadata(
                "CDC_CONTRACT_C0803B", "EDGE_TYPES_E0804C", "COLUMN",
                3385730955L, 0, columns, List.of(), List.of("ID")));
    }

    private static void add(
            List<TableMetadata.Column> columns,
            String name,
            String type,
            int dataTypeId,
            int precision,
            int scale,
            boolean nullable) {
        int ordinal = columns.size() + 1;
        columns.add(new TableMetadata.Column(
                ordinal, 200L + ordinal, name, type, dataTypeId,
                Math.max(0, precision), Math.max(0, precision), Math.max(0, scale),
                nullable, false, false, ""));
    }
}

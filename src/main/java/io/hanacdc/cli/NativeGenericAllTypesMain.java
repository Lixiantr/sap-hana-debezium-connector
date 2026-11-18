package io.hanacdc.cli;

import io.hanacdc.decoder.NativeRecordReader;
import io.hanacdc.decoder.NativeRowDecoder;
import io.hanacdc.decoder.ParallelLogDecoder;
import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;

import java.nio.file.Path;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/** Real-corpus gate for generic decoding of the 33-column all-types table. */
public final class NativeGenericAllTypesMain {
    private NativeGenericAllTypesMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException(
                    "usage: NativeGenericAllTypesMain <dir> <first> <last> <tid>");
        }
        TableDecodePlan plan = plan();
        NativeRowDecoder decoder = new NativeRowDecoder();
        List<String> expectedVariants = expectedVariants();
        int sequence = 0;
        for (var backup : ParallelLogDecoder.discoverCorpus(
                Path.of(args[0]), Long.parseLong(args[1]), Long.parseLong(args[2]))) {
            try (var cursor = NativeRecordReader.open(backup)) {
                while (cursor.hasNext()) {
                    var record = cursor.next();
                    if (record.tid() != Long.parseLong(args[3])
                            || record.operationCode() != NativeRecordReader.UT_UPSERT) {
                        continue;
                    }
                    var row = decoder.decode(record, plan, cursor.readPayload(record));
                    sequence++;
                    if (!row.fullImage() || !"940804001".equals(normalize(row.values().get(0)))) {
                        throw new IllegalStateException("generic row identity mismatch");
                    }
                    if ((sequence & 1) == 1) {
                        int variant = (sequence + 1) / 2;
                        String actual = normalize(row.values().get(variant));
                        String expected = expectedVariants.get(variant - 1);
                        if (!expected.equals(actual)) {
                            throw new IllegalStateException("variant " + variant
                                    + " mismatch: expected=" + expected + " actual=" + actual);
                        }
                    }
                }
            }
        }
        if (sequence != 64) {
            throw new IllegalStateException("expected 64 generic rows, found " + sequence);
        }
        System.out.printf("generic_all_types=PASS images=%d columns=%d fingerprint=%s%n",
                sequence, plan.metadata().columns().size(), plan.schemaFingerprint());
    }

    private static String normalize(NativeRowDecoder.ColumnValue value) {
        Object raw = value.value();
        if (raw instanceof byte[] bytes) {
            return HexFormat.of().formatHex(bytes);
        }
        if (raw instanceof java.math.BigDecimal decimal) {
            return decimal.toPlainString();
        }
        return String.valueOf(raw);
    }

    static TableDecodePlan plan() {
        List<TableMetadata.Column> columns = new ArrayList<>();
        add(columns, "ID", "BIGINT", Types.BIGINT, 19, 0);
        add(columns, "ROW_VERSION", "BIGINT", Types.BIGINT, 19, 0);
        add(columns, "IS_DELETED", "BOOLEAN", Types.BOOLEAN, 1, 0);
        add(columns, "EVENT_TS", "TIMESTAMP", Types.TIMESTAMP, 27, 7);
        add(columns, "EVENT_ENVELOPE", "NVARCHAR", Types.NVARCHAR, 2048, 0);
        add(columns, "C_TINYINT", "TINYINT", Types.TINYINT, 3, 0);
        add(columns, "C_SMALLINT", "SMALLINT", Types.SMALLINT, 5, 0);
        add(columns, "C_INTEGER", "INTEGER", Types.INTEGER, 10, 0);
        add(columns, "C_BIGINT", "BIGINT", Types.BIGINT, 19, 0);
        add(columns, "C_DECIMAL", "DECIMAL", Types.DECIMAL, 34, 10);
        add(columns, "C_SMALLDECIMAL", "SMALLDECIMAL", Types.DECIMAL, 16, 0);
        add(columns, "C_REAL", "REAL", Types.REAL, 7, 0);
        add(columns, "C_DOUBLE", "DOUBLE", Types.DOUBLE, 15, 0);
        add(columns, "C_BOOLEAN", "BOOLEAN", Types.BOOLEAN, 1, 0);
        add(columns, "C_CHAR", "CHAR", Types.CHAR, 16, 0);
        add(columns, "C_VARCHAR", "VARCHAR", Types.VARCHAR, 64, 0);
        add(columns, "C_NCHAR", "NCHAR", Types.NCHAR, 16, 0);
        add(columns, "C_NVARCHAR", "NVARCHAR", Types.NVARCHAR, 64, 0);
        add(columns, "C_ALPHANUM", "ALPHANUM", Types.VARCHAR, 32, 0);
        add(columns, "C_SHORTTEXT", "SHORTTEXT", Types.NVARCHAR, 64, 0);
        add(columns, "C_BINARY", "BINARY", Types.BINARY, 16, 0);
        add(columns, "C_VARBINARY", "VARBINARY", Types.VARBINARY, 64, 0);
        add(columns, "C_DATE", "DATE", Types.DATE, 10, 0);
        add(columns, "C_TIME", "TIME", Types.TIME, 8, 0);
        add(columns, "C_SECONDDATE", "SECONDDATE", Types.TIMESTAMP, 19, 0);
        add(columns, "C_TIMESTAMP", "TIMESTAMP", Types.TIMESTAMP, 27, 7);
        add(columns, "C_CLOB", "CLOB", Types.CLOB, 0, 0);
        add(columns, "C_NCLOB", "NCLOB", Types.NCLOB, 0, 0);
        add(columns, "C_BLOB", "BLOB", Types.BLOB, 0, 0);
        add(columns, "C_TEXT", "TEXT", Types.CLOB, 0, 0);
        add(columns, "C_BINTEXT", "BINTEXT", Types.CLOB, 0, 0);
        add(columns, "C_ST_POINT", "ST_POINT", Types.OTHER, 0, 0);
        add(columns, "C_ST_GEOMETRY", "ST_GEOMETRY", Types.OTHER, 0, 0);
        TableMetadata metadata = new TableMetadata(
                "CDC_CONTRACT_C0803B", "ALL_TYPES", "COLUMN", 968228924L, 0,
                columns,
                List.of(new TableMetadata.AuxiliaryColumn(
                        234, 65536, "$_SYS_SHADOW_C_SHORTTEXT$", "TEXT")),
                List.of("ID"));
        return TableDecodePlan.compile(metadata);
    }

    private static void add(
            List<TableMetadata.Column> columns,
            String name,
            String type,
            int dataTypeId,
            int precision,
            int scale) {
        int ordinal = columns.size() + 1;
        columns.add(new TableMetadata.Column(
                ordinal, 200L + ordinal, name, type, dataTypeId,
                Math.max(0, precision), Math.max(0, precision), Math.max(0, scale),
                false, false, false, ""));
    }

    private static List<String> expectedVariants() {
        return List.of(
                "19002", "true", "2026-08-05T01:02:03.456789200",
                "ATYPE|A0804B|VARIANT", "202", "20202", "202020202",
                "8202020202", "987654321098765432109876.0987654321",
                "98765.4321", "321.75", "987654.875", "false",
                "CHAR-VARIANT", "varchar-variant", "NCHAR-ВАР",
                "nvarchar-вариант", "ALPHA0804VAR", "shorttext-variant",
                "0f0e0d0c0b0a09080706050403020100",
                "76617262696e6172792d76617269616e74",
                "2031-01-02", "23:45:01", "2031-01-02T23:45:01",
                "2031-01-02T23:45:01.111222400", "clob-variant-A0804B",
                "nclob-вариант-A0804B", "626c6f622d76617269616e742d413038303442",
                "text variant A0804B", "bintext variant A0804B",
                "POINT (3 4)", "LINESTRING (2 3, 4 5)");
    }
}

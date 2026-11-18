package io.hanacdc.cli;

import io.hanacdc.decoder.NativeFixedScalarDecoder;
import io.hanacdc.decoder.NativeDecimalDecoder;
import io.hanacdc.decoder.NativeRecordReader;
import io.hanacdc.decoder.NativeRowDescriptorLayout;
import io.hanacdc.decoder.NativeSpatialDecoder;
import io.hanacdc.decoder.NativeTemporalDecoder;
import io.hanacdc.decoder.NativeLengthPrefixedDecoder;
import io.hanacdc.decoder.NativeVariableSlotLayout;
import io.hanacdc.decoder.NativeVariableValueDecoder;
import io.hanacdc.decoder.ParallelLogDecoder;
import io.hanacdc.decoder.UnifiedTableRowLayout;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Base64;

/** Integration probe for descriptor-driven fixed scalar decoding on A0804B. */
public final class NativeAllTypesFixedScalarMain {
    private NativeAllTypesFixedScalarMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("usage: NativeAllTypesFixedScalarMain BACKUP_DIR FIRST LAST TID");
            System.exit(2);
        }
        Path directory = Path.of(args[0]);
        long first = Long.parseLong(args[1]);
        long last = Long.parseLong(args[2]);
        long tid = Long.parseLong(args[3]);
        List<ParallelLogDecoder.BackupFile> backups =
                ParallelLogDecoder.discoverCorpus(directory, first, last);
        int sequence = 0;
        for (var backup : backups) {
            for (var record : NativeRecordReader.readHeaders(backup)) {
                if (record.tid() != tid
                        || record.outerCode() != NativeRecordReader.OUTER_UNIFIED_TABLE
                        || record.operationCode() != NativeRecordReader.UT_UPSERT) {
                    continue;
                }
                byte[] payload = NativeRecordReader.readPayload(record);
                var vector = UnifiedTableRowLayout.parse(record.operationCode(), payload)
                        .orElseThrow(() -> new IllegalStateException("missing row vector"));
                var layout = NativeRowDescriptorLayout.parse(vector, payload)
                        .orElseThrow(() -> new IllegalStateException("missing row descriptor"));
                long id = (Long) NativeFixedScalarDecoder.decode(
                        layout, 0, NativeFixedScalarDecoder.Type.BIGINT, payload);
                long rowVersion = (Long) NativeFixedScalarDecoder.decode(
                        layout, 1, NativeFixedScalarDecoder.Type.BIGINT, payload);
                boolean deleted = (Boolean) NativeFixedScalarDecoder.decode(
                        layout, 2, NativeFixedScalarDecoder.Type.BOOLEAN, payload);
                int tiny = (Integer) NativeFixedScalarDecoder.decode(
                        layout, 5, NativeFixedScalarDecoder.Type.TINYINT, payload);
                int small = (Integer) NativeFixedScalarDecoder.decode(
                        layout, 6, NativeFixedScalarDecoder.Type.SMALLINT, payload);
                int integer = (Integer) NativeFixedScalarDecoder.decode(
                        layout, 7, NativeFixedScalarDecoder.Type.INTEGER, payload);
                long bigint = (Long) NativeFixedScalarDecoder.decode(
                        layout, 8, NativeFixedScalarDecoder.Type.BIGINT, payload);
                var decimal = NativeDecimalDecoder.decodeDecimal(layout, 9, 10, 16, payload);
                var smallDecimal = NativeDecimalDecoder.decodeSmallDecimal(layout, 10, payload);
                float real = (Float) NativeFixedScalarDecoder.decode(
                        layout, 11, NativeFixedScalarDecoder.Type.REAL, payload);
                double doubleValue = (Double) NativeFixedScalarDecoder.decode(
                        layout, 12, NativeFixedScalarDecoder.Type.DOUBLE, payload);
                boolean bool = (Boolean) NativeFixedScalarDecoder.decode(
                        layout, 13, NativeFixedScalarDecoder.Type.BOOLEAN, payload);
                var date = NativeTemporalDecoder.decodeDate(layout, 22, payload);
                var time = NativeTemporalDecoder.decodeTime(layout, 23, payload);
                var secondDate = NativeTemporalDecoder.decodeSecondDate(layout, 24, payload);
                var timestamp = NativeTemporalDecoder.decodeTimestamp(layout, 25, payload);
                String charValue = NativeLengthPrefixedDecoder.decodeUtf8(layout, 14, payload);
                String varchar = NativeLengthPrefixedDecoder.decodeUtf8(layout, 15, payload);
                String nchar = NativeLengthPrefixedDecoder.decodeUtf8(layout, 16, payload);
                String nvarchar = NativeLengthPrefixedDecoder.decodeUtf8(layout, 17, payload);
                String shorttext = NativeLengthPrefixedDecoder.decodeUtf8(layout, 19, payload);
                String binary = Base64.getEncoder().encodeToString(
                        NativeLengthPrefixedDecoder.decodeBytes(layout, 20, payload));
                String varbinary = Base64.getEncoder().encodeToString(
                        NativeLengthPrefixedDecoder.decodeBytes(layout, 21, payload));
                var variable = NativeVariableSlotLayout.parse(layout, payload)
                        .orElseThrow(() -> new IllegalStateException("missing variable-slot layout"));
                if (variable.slots().size() != 10) {
                    throw new IllegalStateException("expected 10 variable slots");
                }
                String envelope = NativeVariableValueDecoder.decodeUtf8(variable.slots().get(0));
                String alphanum = NativeVariableValueDecoder.decodeTaggedUtf8(
                        variable.slots().get(1), 0x20);
                String clob = NativeVariableValueDecoder.decodeTaggedUtf8(
                        variable.slots().get(2), 0xb0);
                String nclob = NativeVariableValueDecoder.decodeTaggedUtf8(
                        variable.slots().get(3), 0xd0);
                String blob = Base64.getEncoder().encodeToString(
                        NativeVariableValueDecoder.decodeTaggedBytes(variable.slots().get(4), 0x90));
                String point = NativeSpatialDecoder.decodePointWkt(variable.slots().get(7));
                String geometry = NativeSpatialDecoder.decodeLineStringWkt(variable.slots().get(8));
                if (id != 940_804_001L) {
                    throw new IllegalStateException("decoded wrong PK at " + record.position());
                }
                System.out.printf(Locale.ROOT,
                        "{\"sequence\":%d,\"position\":%d,\"data_offset\":%d,"
                                + "\"id\":%d,\"row_version\":%d,\"is_deleted\":%s,"
                                + "\"tinyint\":%d,\"smallint\":%d,\"integer\":%d,"
                                + "\"bigint\":%d,\"real\":%.9g,\"double\":%.17g,"
                                + "\"decimal\":\"%s\",\"smalldecimal\":\"%s\","
                                + "\"boolean\":%s,\"date\":\"%s\",\"time\":\"%s\","
                                + "\"seconddate\":\"%s\",\"timestamp\":\"%s\","
                                + "\"char\":\"%s\",\"varchar\":\"%s\","
                                + "\"nchar\":\"%s\",\"nvarchar\":\"%s\","
                                + "\"shorttext\":\"%s\",\"binary_base64\":\"%s\","
                                + "\"varbinary_base64\":\"%s\",\"envelope\":\"%s\","
                                + "\"alphanum\":\"%s\",\"clob\":\"%s\","
                                + "\"nclob\":\"%s\",\"blob_base64\":\"%s\","
                                + "\"point_wkt\":\"%s\",\"geometry_wkt\":\"%s\"}%n",
                        ++sequence, record.position(), layout.dataAreaOffset(), id, rowVersion,
                        deleted, tiny, small, integer, bigint, real, doubleValue,
                        decimal.toPlainString(), smallDecimal.toPlainString(), bool,
                        date, time, secondDate, timestamp,
                        ParallelLogDecoder.escapeJson(charValue),
                        ParallelLogDecoder.escapeJson(varchar),
                        ParallelLogDecoder.escapeJson(nchar),
                        ParallelLogDecoder.escapeJson(nvarchar),
                        ParallelLogDecoder.escapeJson(shorttext), binary, varbinary,
                        ParallelLogDecoder.escapeJson(envelope),
                        ParallelLogDecoder.escapeJson(alphanum),
                        ParallelLogDecoder.escapeJson(clob),
                        ParallelLogDecoder.escapeJson(nclob), blob,
                        ParallelLogDecoder.escapeJson(point),
                        ParallelLogDecoder.escapeJson(geometry));
            }
        }
        System.out.printf(Locale.ROOT,
                "{\"kind\":\"native_all_types_fixed_scalar_summary\","
                        + "\"tid\":%d,\"images\":%d}%n", tid, sequence);
        if (sequence != 64) {
            throw new IllegalStateException("expected 64 UPSERT images, found " + sequence);
        }
    }
}

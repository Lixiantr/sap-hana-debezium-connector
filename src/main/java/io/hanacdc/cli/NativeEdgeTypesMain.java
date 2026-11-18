package io.hanacdc.cli;

import io.hanacdc.decoder.NativeDecimalDecoder;
import io.hanacdc.decoder.NativeFixedScalarDecoder;
import io.hanacdc.decoder.NativeRecordReader;
import io.hanacdc.decoder.NativeRowDescriptorLayout;
import io.hanacdc.decoder.NativeTextDecoder;
import io.hanacdc.decoder.NativeVariableSlotLayout;
import io.hanacdc.decoder.NativeVariableValueDecoder;
import io.hanacdc.decoder.ParallelLogDecoder;
import io.hanacdc.decoder.UnifiedTableRowLayout;

import java.nio.file.Path;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;

/** Integration probe for the nullable/LOB E0804C native row grammar. */
public final class NativeEdgeTypesMain {
    private NativeEdgeTypesMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 4 || args.length > 6) {
            System.err.println("usage: NativeEdgeTypesMain BACKUP_DIR FIRST LAST TID [EXPECTED [ID]]");
            System.exit(2);
        }
        Path directory = Path.of(args[0]);
        long first = Long.parseLong(args[1]);
        long last = Long.parseLong(args[2]);
        long tid = Long.parseLong(args[3]);
        int expected = args.length == 5 ? Integer.parseInt(args[4]) : 60;
        if (args.length == 6) expected = Integer.parseInt(args[4]);
        long expectedId = args.length == 6 ? Long.parseLong(args[5]) : 940_804_002L;
        List<ParallelLogDecoder.BackupFile> backups =
                ParallelLogDecoder.discoverCorpus(directory, first, last);
        int sequence = 0;
        for (var backup : backups) {
            for (var record : NativeRecordReader.readHeaders(backup)) {
                if (record.tid() != tid
                        || record.operationCode() != NativeRecordReader.UT_UPSERT) continue;
                byte[] payload = NativeRecordReader.readPayload(record);
                var vector = UnifiedTableRowLayout.parse(record.operationCode(), payload)
                        .orElseThrow(() -> new IllegalStateException("missing row vector"));
                var row = NativeRowDescriptorLayout.parse(vector, payload)
                        .orElseThrow(() -> new IllegalStateException("missing row descriptor"));
                if (row.encodingMode() != 0x20 || row.descriptors().size() != 10
                        || row.variableSlotCount() != 6) {
                    throw new IllegalStateException("unexpected E0804C row grammar");
                }
                var variable = NativeVariableSlotLayout.parse(row, payload)
                        .orElseThrow(() -> new IllegalStateException("missing variable layout"));
                long id = (Long) NativeFixedScalarDecoder.decode(
                        row, 0, NativeFixedScalarDecoder.Type.BIGINT, payload);
                long rowVersion = (Long) NativeFixedScalarDecoder.decode(
                        row, 1, NativeFixedScalarDecoder.Type.BIGINT, payload);
                String decimal = row.descriptors().get(2).isNull() ? null
                        : NativeDecimalDecoder.decodeDecimal(row, 2, 10, 16, payload).toPlainString();
                String smallDecimal = row.descriptors().get(3).isNull() ? null
                        : NativeDecimalDecoder.decodeSmallDecimal(row, 3, payload).toPlainString();
                String nvarchar = nullableUtf8(variable.slots().get(0));
                TaggedValue clob = tagged(variable.slots().get(1), 0xb0, true);
                TaggedValue nclob = tagged(variable.slots().get(2), 0xd0, true);
                TaggedValue blob = tagged(variable.slots().get(3), 0x90, false);
                String textRaw = nullableBase64(variable.slots().get(4));
                String bintextRaw = nullableBase64(variable.slots().get(5));
                var text = variable.slots().get(4).isNull() ? null
                        : NativeTextDecoder.decodeText(variable.slots().get(4));
                var bintext = variable.slots().get(5).isNull() ? null
                        : NativeTextDecoder.decodeBinText(variable.slots().get(5));
                if (id != expectedId) {
                    throw new IllegalStateException("decoded wrong E0804C PK");
                }
                System.out.printf(Locale.ROOT,
                        "{\"sequence\":%d,\"position\":%d,\"id\":%d,"
                                + "\"row_version\":%d,\"decimal\":%s,"
                                + "\"smalldecimal\":%s,\"nvarchar\":%s,"
                                + "\"clob_inline\":%s,\"clob_locator_hex\":%s,"
                                + "\"nclob_inline\":%s,\"nclob_locator_hex\":%s,"
                                + "\"blob_inline_base64\":%s,\"blob_locator_hex\":%s,"
                                + "\"text_raw_base64\":%s,\"bintext_raw_base64\":%s,"
                                + "\"text\":%s,\"bintext\":%s,\"bintext_language\":%s}%n",
                        ++sequence, record.position(), id, rowVersion,
                        json(decimal), json(smallDecimal), json(nvarchar),
                        json(clob.inline()), json(clob.locatorHex()),
                        json(nclob.inline()), json(nclob.locatorHex()),
                        json(blob.inline()), json(blob.locatorHex()),
                        json(textRaw), json(bintextRaw),
                        json(text == null ? null : text.text()),
                        json(bintext == null ? null : bintext.text()),
                        json(bintext == null ? null : bintext.language()));
            }
        }
        System.out.printf(Locale.ROOT,
                "{\"kind\":\"native_edge_summary\",\"tid\":%d,\"images\":%d}%n",
                tid, sequence);
        if (sequence != expected) {
            throw new IllegalStateException("expected " + expected + " edge images, found " + sequence);
        }
    }

    private static String nullableUtf8(NativeVariableSlotLayout.Slot slot) {
        return slot.isNull() ? null : NativeVariableValueDecoder.decodeUtf8(slot);
    }

    private static String nullableBase64(NativeVariableSlotLayout.Slot slot) {
        return slot.isNull() ? null : Base64.getEncoder().encodeToString(slot.data());
    }

    private static TaggedValue tagged(
            NativeVariableSlotLayout.Slot slot, int expectedTag, boolean text) {
        if (slot.isNull()) return new TaggedValue(null, null);
        byte[] value = slot.data();
        if (value.length > 0 && Byte.toUnsignedInt(value[0]) == expectedTag) {
            return new TaggedValue(text
                    ? NativeVariableValueDecoder.decodeTaggedUtf8(slot, expectedTag)
                    : Base64.getEncoder().encodeToString(
                            NativeVariableValueDecoder.decodeTaggedBytes(slot, expectedTag)), null);
        }
        return new TaggedValue(null, HexFormat.of().formatHex(value));
    }

    private static String json(String value) {
        return value == null ? "null" : "\"" + ParallelLogDecoder.escapeJson(value) + "\"";
    }

    private record TaggedValue(String inline, String locatorHex) {}
}

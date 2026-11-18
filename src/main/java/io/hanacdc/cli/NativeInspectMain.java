package io.hanacdc.cli;

import io.hanacdc.decoder.NativeRecordReader;
import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;
import io.hanacdc.decoder.ParallelLogDecoder;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;
import io.hanacdc.decoder.UnifiedTableRowLayout;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

/** Emits native HANA outer-record metadata as deterministic NDJSON. */
public final class NativeInspectMain {
    private NativeInspectMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 3 || args.length > 4) {
            System.err.println("usage: NativeInspectMain BACKUP_DIR FIRST_POSITION LAST_POSITION [TID]");
            System.exit(2);
        }
        Path directory = Path.of(args[0]);
        long first = Long.parseLong(args[1]);
        long last = Long.parseLong(args[2]);
        long wantedTid = args.length == 4 ? Long.parseLong(args[3]) : -1;
        List<BackupFile> backups = ParallelLogDecoder.discoverCorpus(directory, first, last);
        long records = 0;
        for (BackupFile backup : backups) {
            for (NativeRecordHeader record : NativeRecordReader.readHeaders(backup)) {
                if (wantedTid >= 0 && record.tid() != wantedTid) {
                    continue;
                }
                records++;
                var rowLayout = UnifiedTableRowLayout.parse(
                        record.operationCode(), NativeRecordReader.readPayload(record));
                String columns = rowLayout
                        .map(layout -> layout.internalColumnIds().toString())
                        .orElse("[]");
                int rowBodyOffset = rowLayout
                        .map(UnifiedTableRowLayout::encodedBodyOffset)
                        .orElse(-1);
                System.out.printf(Locale.ROOT,
                        "{\"kind\":\"native_record\",\"file\":\"%s\"," +
                                "\"file_offset\":%d,\"position\":%d,\"tid\":%d," +
                                "\"outer_code\":\"0x%08x\",\"outer_type\":\"%s\"," +
                                "\"payload_size\":%d,\"allocation_bytes\":%d," +
                                "\"poid\":%d,\"container\":\"%s\"," +
                                "\"operation_code\":%d,\"operation\":\"%s\"," +
                                "\"first_row_id\":%d,\"internal_column_ids\":\"%s\"," +
                                "\"row_body_payload_offset\":%d}%n",
                        ParallelLogDecoder.escapeJson(backup.fileName()),
                        record.fileOffset(), record.position(), record.tid(), record.outerCode(),
                        record.outerType(), record.payloadSize(), record.allocationBytes(),
                        record.poid(), record.containerHex(), record.operationCode(),
                        record.operationType(), record.firstRowId(),
                        ParallelLogDecoder.escapeJson(columns), rowBodyOffset);
            }
        }
        System.out.printf(Locale.ROOT,
                "{\"kind\":\"native_summary\",\"files\":%d,\"records\":%d," +
                        "\"first_position\":%d,\"last_position\":%d}%n",
                backups.size(), records, first, last);
    }
}

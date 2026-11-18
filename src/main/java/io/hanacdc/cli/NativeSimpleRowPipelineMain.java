package io.hanacdc.cli;

import io.hanacdc.decoder.NativeRecordReader;
import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;
import io.hanacdc.decoder.ParallelLogDecoder;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;
import io.hanacdc.decoder.SimpleRowProbeDecoder;
import io.hanacdc.decoder.SimpleRowProbeDecoder.DecodedImage;
import io.hanacdc.decoder.CapturedTransactionBuffer;
import io.hanacdc.decoder.NativeTableFilter;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

/** Transaction-aware proof pipeline for the controlled F0804A SIMPLE_ROW corpus. */
public final class NativeSimpleRowPipelineMain {
    private NativeSimpleRowPipelineMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("usage: NativeSimpleRowPipelineMain BACKUP_DIR FIRST LAST CONTAINER_HEX");
            System.exit(2);
        }
        Path directory = Path.of(args[0]);
        long first = Long.parseLong(args[1]);
        long last = Long.parseLong(args[2]);
        long targetContainer = Long.parseUnsignedLong(args[3].replaceFirst("^0x", ""), 16);
        List<BackupFile> backups = ParallelLogDecoder.discoverCorpus(directory, first, last);
        NativeTableFilter tableFilter = new NativeTableFilter(List.of(
                new NativeTableFilter.TableBinding(
                        "CDC_FORMAT_F0804A", "SIMPLE_ROW", 166001,
                        targetContainer, "F0804A:SIMPLE_ROW:v1", true)));
        CapturedTransactionBuffer<Change> transactions = new CapturedTransactionBuffer<>();
        int committed = 0;
        int rolledBack = 0;
        long excludedTableRecords = 0;
        long auxiliaryRecords = 0;
        for (BackupFile backup : backups) {
            for (NativeRecordHeader record : NativeRecordReader.readHeaders(backup)) {
                NativeTableFilter.Disposition disposition = tableFilter.classify(record);
                if (disposition == NativeTableFilter.Disposition.CAPTURED_TABLE) {
                    DecodedImage image = SimpleRowProbeDecoder.decode(record).orElse(null);
                    transactions.capture(record.tid(), new Change(record, image));
                }
                else if (disposition == NativeTableFilter.Disposition.EXCLUDED_TABLE) {
                    excludedTableRecords++;
                }
                else if (disposition == NativeTableFilter.Disposition.AUXILIARY) {
                    auxiliaryRecords++;
                }
                if (record.outerCode() == NativeRecordReader.OUTER_COMMIT) {
                    var completed = transactions.complete(
                            record.tid(), CapturedTransactionBuffer.Outcome.COMMIT);
                    if (completed.isPresent()) {
                        committed++;
                        emit("committed_transaction", record, completed.get().changes());
                    }
                }
                else if (record.outerCode() == NativeRecordReader.OUTER_ROLLBACK) {
                    var completed = transactions.complete(
                            record.tid(), CapturedTransactionBuffer.Outcome.ROLLBACK);
                    if (completed.isPresent()) {
                        rolledBack++;
                        emit("rolled_back_transaction", record, completed.get().changes());
                    }
                }
            }
        }
        System.out.printf(Locale.ROOT,
                "{\"kind\":\"native_simple_row_summary\",\"files\":%d," +
                        "\"committed_transactions\":%d,\"rolled_back_transactions\":%d," +
                        "\"open_transactions\":%d,\"excluded_table_records\":%d," +
                        "\"auxiliary_records\":%d}%n",
                backups.size(), committed, rolledBack,
                transactions.openCapturedTransactions(), excludedTableRecords, auxiliaryRecords);
        if (transactions.openCapturedTransactions() != 0) {
            throw new IllegalStateException("corpus ended with open target transactions");
        }
    }

    private static void emit(
            String kind, NativeRecordHeader terminal, List<Change> changes) {
        StringBuilder json = new StringBuilder(1024)
                .append("{\"kind\":\"").append(kind).append("\",\"tid\":")
                .append(terminal.tid()).append(",\"terminal_position\":")
                .append(terminal.position()).append(",\"changes\":[");
        for (int index = 0; index < changes.size(); index++) {
            if (index > 0) json.append(',');
            Change change = changes.get(index);
            json.append("{\"position\":").append(change.record.position())
                    .append(",\"operation\":\"").append(change.record.operationType()).append('"')
                    .append(",\"row_id\":").append(change.record.firstRowId());
            if (change.image != null && change.image.fullRow().isPresent()) {
                var row = change.image.fullRow().get();
                json.append(",\"image_kind\":\"full\",\"id\":").append(row.id())
                        .append(",\"row_version\":").append(row.rowVersion())
                        .append(",\"is_deleted\":").append(row.deleted())
                        .append(",\"value_text\":\"")
                        .append(ParallelLogDecoder.escapeJson(row.valueText())).append('"');
            }
            else if (change.image != null && change.image.sparseUpdate().isPresent()) {
                var row = change.image.sparseUpdate().get();
                json.append(",\"image_kind\":\"sparse\",\"row_version\":")
                        .append(row.rowVersion()).append(",\"value_text\":\"")
                        .append(ParallelLogDecoder.escapeJson(row.valueText())).append('"');
            }
            json.append('}');
        }
        System.out.println(json.append("]}").toString());
    }

    private record Change(NativeRecordHeader record, DecodedImage image) {}
}

package io.hanacdc.cli;

import io.hanacdc.decoder.NativeBackupDecoder;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;

import java.nio.file.Path;
import java.util.Locale;

/** Validates native backup objects and counts outer records as a stream. */
public final class NativeBackupValidateMain {
    private NativeBackupValidateMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("usage: NativeBackupValidateMain <log_backup>...");
        }
        NativeBackupDecoder decoder = new NativeBackupDecoder();
        for (String argument : args) {
            BackupFile backup = BackupFile.parse(Path.of(argument))
                    .orElseThrow(() -> new IllegalArgumentException(
                            "invalid log-backup file name: " + argument));
            long records = 0;
            long unknownOuter = 0;
            long firstRecord = -1;
            long lastRecord = -1;
            try (NativeBackupDecoder.Cursor cursor = decoder.open(backup)) {
                while (cursor.hasNext()) {
                    var record = cursor.next();
                    if (firstRecord < 0) {
                        firstRecord = record.header().position();
                    }
                    lastRecord = record.header().position();
                    records++;
                    if (record.header().outerType().startsWith("UNKNOWN_")) {
                        unknownOuter++;
                    }
                }
                var metadata = cursor.metadata();
                System.out.printf(Locale.ROOT,
                        "file=%s dbid=%s timestamp=%d first=%d last=%d records=%d "
                                + "unknown_outer=%d first_record=%d last_record=%d "
                                + "encryption=%s compression=%s bytes=%d%n",
                        backup.path(), metadata.databaseId(), metadata.backupTimestamp(),
                        backup.first(), backup.last(), records, unknownOuter, firstRecord,
                        lastRecord, metadata.encryption(), metadata.compression(),
                        metadata.physicalBytes());
            }
        }
    }
}

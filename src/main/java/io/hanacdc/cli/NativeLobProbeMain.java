package io.hanacdc.cli;

import io.hanacdc.decoder.NativeLobReconstructor;
import io.hanacdc.decoder.NativeRecordReader;
import io.hanacdc.decoder.ParallelLogDecoder;

import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;

/** Integration probe for E0804C LOB reconstruction. */
public final class NativeLobProbeMain {
    private NativeLobProbeMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 4 || args.length > 5) {
            System.err.println("usage: NativeLobProbeMain BACKUP_DIR FIRST LAST TID [EXPECTED]");
            System.exit(2);
        }
        Path directory = Path.of(args[0]);
        long first = Long.parseLong(args[1]);
        long last = Long.parseLong(args[2]);
        long tid = Long.parseLong(args[3]);
        int expected = args.length == 5 ? Integer.parseInt(args[4]) : 5;
        List<NativeRecordReader.NativeRecordHeader> transaction = new ArrayList<>();
        for (var backup : ParallelLogDecoder.discoverCorpus(directory, first, last)) {
            for (var record : NativeRecordReader.readHeaders(backup)) {
                if (record.tid() == tid) transaction.add(record);
            }
        }
        var values = NativeLobReconstructor.reconstruct(transaction);
        var ordered = values.values().stream()
                .sorted(Comparator.comparingLong(NativeLobReconstructor.LobValue::locatorId))
                .toList();
        for (var value : ordered) {
            String hash = HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(value.data()));
            System.out.printf(Locale.ROOT,
                    "{\"locator\":\"0x%016x\",\"storage\":\"%s\","
                            + "\"bytes\":%d,\"characters\":%d,\"sha256\":\"%s\"}%n",
                    value.locatorId(), value.storage(), value.data().length,
                    value.characterSize(), hash);
        }
        System.out.printf(Locale.ROOT,
                "{\"kind\":\"native_lob_summary\",\"tid\":%d,\"values\":%d}%n",
                tid, ordered.size());
        if (ordered.size() != expected) {
            throw new IllegalStateException("expected " + expected + " external LOB values");
        }
    }
}

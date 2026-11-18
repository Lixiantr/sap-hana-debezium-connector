package io.hanacdc.cli;

import io.hanacdc.decoder.NativeLobReconstructor;
import io.hanacdc.decoder.NativeRecordReader;
import io.hanacdc.decoder.ParallelLogDecoder;

import java.io.IOException;
import java.nio.file.Path;

/** Negative real-corpus gate: removes one LOB record and requires fail-closed decoding. */
public final class NativeLobFaultInjectionMain {
    private NativeLobFaultInjectionMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            throw new IllegalArgumentException(
                    "usage: NativeLobFaultInjectionMain DIR FIRST LAST TID SKIP_POSITION FATAL_CODE");
        }
        Path directory = Path.of(args[0]);
        long first = Long.parseLong(args[1]);
        long last = Long.parseLong(args[2]);
        long tid = Long.parseLong(args[3]);
        long skipPosition = Long.parseLong(args[4]);
        String expected = args[5];
        boolean skipped = false;
        try (NativeLobReconstructor.Session session = new NativeLobReconstructor.Session()) {
            try {
                for (var backup : ParallelLogDecoder.discoverCorpus(directory, first, last)) {
                    try (var cursor = NativeRecordReader.open(backup)) {
                        while (cursor.hasNext()) {
                            var record = cursor.next();
                            if (record.tid() != tid) continue;
                            if (record.position() == skipPosition) {
                                skipped = true;
                                continue;
                            }
                            session.accept(record, (record.outerCode()
                                    == NativeRecordReader.OUTER_LOB_WRITE
                                    || record.outerCode()
                                    == NativeRecordReader.OUTER_VIRTUAL_FILE)
                                    ? cursor.readPayload(record) : new byte[0]);
                        }
                    }
                }
                if (!skipped) {
                    throw new IllegalStateException(
                            "fault injection position was not found: " + skipPosition);
                }
                session.finish();
            }
            catch (IOException expectedFailure) {
                if (!skipped || expectedFailure.getMessage() == null
                        || !expectedFailure.getMessage().contains(expected)) {
                    throw expectedFailure;
                }
                System.out.printf("native_lob_fault=PASS skip_position=%d code=%s message=%s%n",
                        skipPosition, expected, expectedFailure.getMessage());
                return;
            }
        }
        throw new IllegalStateException("fault injection did not fail closed");
    }
}

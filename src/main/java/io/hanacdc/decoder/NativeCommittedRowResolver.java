package io.hanacdc.decoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Atomic publication barrier between transaction-scoped LOB state and committed rows. */
final class NativeCommittedRowResolver {
    private NativeCommittedRowResolver() {}

    static List<NativeRowDecoder.DecodedRow> resolve(
            List<NativeRowDecoder.DecodedRow> rows,
            Map<Long, NativeLobReconstructor.LobValue> lobs) throws IOException {
        NativeRowDecoder decoder = new NativeRowDecoder();
        List<NativeRowDecoder.DecodedRow> resolved = new ArrayList<>(rows.size());
        for (NativeRowDecoder.DecodedRow row : rows) {
            resolved.add(decoder.resolveLobs(row, lobs));
        }
        return List.copyOf(resolved);
    }
}

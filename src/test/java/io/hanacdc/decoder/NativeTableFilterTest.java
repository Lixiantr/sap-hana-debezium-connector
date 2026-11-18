package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class NativeTableFilterTest {
    private static final long TARGET = 0xfe0000018fL;
    private static final long OTHER = 0xfe00000190L;

    @Test
    void distinguishesCapturedAndExcludedTablesButPreservesTransactionControl() {
        NativeTableFilter filter = new NativeTableFilter(List.of(
                new NativeTableFilter.TableBinding(
                        "CDC", "TARGET", 166001, TARGET, "sha256:target", true),
                new NativeTableFilter.TableBinding(
                        "CDC", "OTHER", 166002, OTHER, "sha256:other", false)));

        assertEquals(NativeTableFilter.Disposition.CAPTURED_TABLE,
                filter.classify(record(NativeRecordReader.OUTER_UNIFIED_TABLE, TARGET, 7)));
        assertEquals(NativeTableFilter.Disposition.EXCLUDED_TABLE,
                filter.classify(record(NativeRecordReader.OUTER_UNIFIED_TABLE, OTHER, 7)));
        assertEquals(NativeTableFilter.Disposition.EXCLUDED_TABLE,
                filter.classify(record(NativeRecordReader.OUTER_UNIFIED_TABLE, 0x1234, 7)));
        assertEquals(NativeTableFilter.Disposition.TRANSACTION_CONTROL,
                filter.classify(record(NativeRecordReader.OUTER_COMMIT, -1, 7)));
        assertEquals(NativeTableFilter.Disposition.TRANSACTION_CONTROL,
                filter.classify(record(NativeRecordReader.OUTER_ROLLBACK, -1, 7)));
        assertEquals(NativeTableFilter.Disposition.AUXILIARY,
                filter.classify(record(NativeRecordReader.OUTER_VIRTUAL_FILE, 0x99, 7)));
    }

    private static NativeRecordHeader record(int outer, long container, long tid) {
        return new NativeRecordHeader(
                null, 0, 1, tid, outer, 64, 128, 1, container,
                outer == NativeRecordReader.OUTER_UNIFIED_TABLE
                        ? NativeRecordReader.UT_INSERT_DATA : -1,
                -1);
    }
}

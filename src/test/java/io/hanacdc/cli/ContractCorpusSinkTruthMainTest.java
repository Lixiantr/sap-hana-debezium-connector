package io.hanacdc.cli;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class ContractCorpusSinkTruthMainTest {
    @Test
    void materializesContractCorpusAndToleratesAtLeastOnceReplay() throws Exception {
        var sink = new ContractCorpusSinkTruthMain.Materializer();
        var id1 = row(941804001, 1, "ID1_CREATE");
        var id1Final = row(941804001, 3, "ID1_FINAL");
        var id1Deleted = row(941804001, 4, "ID1_TOMBSTONE");
        var id3 = row(941804003, 1, "ID3_CREATE");
        var id3Final = ContractCorpusSinkTruthMain.expectedLiveRow();
        var id4 = row(941804004, 1, "ID4_CREATE");
        var id4Deleted = row(941804004, 2, "ID4_TOMBSTONE");

        sink.accept(941804001, "c", id1, 1, 11);
        sink.accept(941804001, "u", id1Final, 2, 12);
        sink.accept(941804001, "d", id1Deleted, 3, 13);
        sink.accept(941804003, "c", id3, 4, 14);
        sink.accept(941804003, "u", id3Final, 5, 15);
        sink.accept(941804004, "c", id4, 6, 16);
        sink.accept(941804004, "d", id4Deleted, 7, 17);
        sink.accept(941804003, "u", id3Final, 5, 15);
        sink.accept(941804001, "c", id1, 1, 11);

        var result = sink.requireContractTruth();
        assertEquals(9, result.records());
        assertEquals(7, result.uniqueEvents());
        assertEquals(1, result.liveRows());
    }

    @Test
    void validatesCommittedThreeEventPrefixWithoutPartialRow() throws Exception {
        var sink = new ContractCorpusSinkTruthMain.Materializer();
        sink.accept(941804001, "c", row(941804001, 1, "ID1_CREATE"), 1, 11);
        sink.accept(941804001, "u", row(941804001, 3, "ID1_FINAL"), 2, 12);
        sink.accept(941804001, "d", row(941804001, 4, "ID1_TOMBSTONE"), 3, 13);

        var result = sink.requireContractPrefix3();
        assertEquals(3, result.uniqueEvents());
        assertEquals(0, result.liveRows());
    }

    private static ContractCorpusSinkTruthMain.Row row(
            long id, long version, String text) {
        return new ContractCorpusSinkTruthMain.Row(id, text.contains("TOMBSTONE"),
                version, text, new BigDecimal("1.0000"), "timestamp", "clob", "nclob",
                "blob");
    }
}

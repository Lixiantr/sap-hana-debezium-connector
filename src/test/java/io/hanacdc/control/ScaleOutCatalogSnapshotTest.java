package io.hanacdc.control;

import io.hanacdc.connect.SourceStreamId;
import io.hanacdc.schema.TableMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ScaleOutCatalogSnapshotTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void capturesAllStreamsConcurrentlyAndBuildsIndependentManifests() throws Exception {
        FakeControlPlane control = new FakeControlPlane(false);
        var snapshot = ScaleOutCatalogSnapshot.capture(control, services(),
                Map.of(stream(2), 100L, stream(3), 200L), 0, 2,
                contract(), mapper());

        assertEquals(2, snapshot.statuses().size());
        assertEquals(1, snapshot.manifests().get(stream(2)).size());
        assertEquals(1, snapshot.manifests().get(stream(3)).size());
        assertTrue(control.maximumConcurrentCalls.get() >= 2);
    }

    @Test
    void gapInOneStreamFailsWholeCapture() {
        SQLException error = assertThrows(SQLException.class, () ->
                ScaleOutCatalogSnapshot.capture(new FakeControlPlane(true), services(),
                        Map.of(stream(2), 100L, stream(3), 200L), 0, 2,
                        contract(), mapper()));
        assertTrue(error.getMessage().contains("FATAL_GAP"));
    }

    private PathMapper mapper() {
        return PathMapper.parse("/hana/log=" + temporaryDirectory.toAbsolutePath());
    }

    private static List<PersistenceService> services() {
        return List.of(new PersistenceService("hana01", 39040, "indexserver", 2),
                new PersistenceService("hana02", 39040, "indexserver", 3));
    }

    private static SourceStreamId stream(long volume) {
        return new SourceStreamId(volume, "indexserver");
    }

    private static HanaContract contract() {
        return new HanaContract(Set.of("immediate"), Set.of("file"), Set.of("none"),
                false, false, Long.MAX_VALUE, 900_000, List.of("2.00.088"));
    }

    private static final class FakeControlPlane implements HanaControlPlane {
        private final boolean gap;
        private final AtomicInteger activeCalls = new AtomicInteger();
        private final AtomicInteger maximumConcurrentCalls = new AtomicInteger();

        private FakeControlPlane(boolean gap) {
            this.gap = gap;
        }

        @Override
        public PersistenceStreamStatus persistenceStreamStatus(long sourceId)
                throws SQLException {
            int active = activeCalls.incrementAndGet();
            maximumConcurrentCalls.accumulateAndGet(active, Math::max);
            try {
                try {
                    Thread.sleep(30);
                }
                catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new SQLException(error);
                }
                long first = sourceId == 2 ? 100 : gap ? 201 : 200;
                long last = sourceId == 2 ? 108 : 208;
                return new PersistenceStreamStatus(sourceId, first, 8192,
                        last, last + 4, Instant.parse("2026-08-07T12:00:00Z"));
            }
            finally {
                activeCalls.decrementAndGet();
            }
        }

        @Override
        public List<LogBackupEntry> successfulLogBackups(
                long sourceId, long first, long last) {
            String host = sourceId == 2 ? "hana01" : "hana02";
            return List.of(new LogBackupEntry(sourceId * 10, sourceId * 100,
                    sourceId, host, "indexserver", first, last, 8192, "file",
                    "/hana/log/log_backup_" + sourceId + "_0_" + first + "_"
                            + last + "." + sourceId * 100,
                    "", 1, false, 0, "", Instant.parse("2026-08-07T12:00:00Z")));
        }

        @Override
        public List<PersistenceService> persistenceServices() {
            return services();
        }

        @Override
        public HanaCapabilities inspect() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<TableMetadata> columnTableMetadata() {
            return List.of();
        }
    }
}

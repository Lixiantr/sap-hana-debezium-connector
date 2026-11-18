package io.hanacdc.connect;

import org.junit.jupiter.api.Test;
import org.apache.kafka.connect.errors.RetriableException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class FailureClassifierTest {
    @Test
    void neverRetriesContinuityOrCorruptionFailures() {
        var gap = FailureClassifier.classify(
                new IOException("FATAL_GAP: expected 100 but got 120"));
        assertEquals(FailureClassifier.Category.FATAL_GAP, gap.category());
        assertFalse(gap.retryable());

        var retention = FailureClassifier.classify(
                new IllegalStateException("FATAL_RETENTION_OBJECT_MISSING: backup_id=1"));
        assertEquals(FailureClassifier.Category.FATAL_GAP, retention.category());
        assertEquals("FATAL_RETENTION_OBJECT_MISSING", retention.code());
        assertFalse(retention.retryable());

        var replay = FailureClassifier.classify(
                new IOException("FATAL_STATE_REPLAY_BACKUP_MISSING: backup_id=1"));
        assertEquals(FailureClassifier.Category.FATAL_GAP, replay.category());
        assertFalse(replay.retryable());

        var missingState = FailureClassifier.classify(
                new IOException("FATAL_STATE_OBJECT_MISSING: generation=34"));
        assertEquals(FailureClassifier.Category.FATAL_CORRUPTION,
                missingState.category());
        assertEquals("FATAL_STATE_OBJECT_MISSING", missingState.code());
        assertFalse(missingState.retryable());

        var foreignSource = FailureClassifier.classify(
                new IOException("CATALOG_SOURCE_IDENTITY_MISMATCH: expected=2 actual=3"));
        assertEquals(FailureClassifier.Category.FATAL_CORRUPTION,
                foreignSource.category());
        assertEquals("CATALOG_SOURCE_IDENTITY_MISMATCH", foreignSource.code());
        assertFalse(foreignSource.retryable());

        var corrupt = FailureClassifier.classify(
                new IOException("FATAL_DECODE_WARNING: backup_id=1"));
        assertEquals(FailureClassifier.Category.FATAL_CORRUPTION, corrupt.category());
        assertFalse(corrupt.retryable());

        for (String message : java.util.List.of(
                "FATAL_STATE_CHECKSUM_MISMATCH: expected=a actual=b",
                "FATAL_BACKUP_HEADER: DBID mismatch",
                "FATAL_BACKUP_SIZE: native data ends before redo range",
                "FATAL_LOB_RECONSTRUCTION: overwrite gap",
                "FATAL_NATIVE_ROW: invalid descriptor",
                "FATAL_ORDERING: position did not advance",
                "FATAL_TRANSACTION_INCOMPLETE: open TIDs=[7]",
                "FATAL_SCALE_OUT_TRANSACTION: conflicting outcome",
                "FATAL_SCALE_OUT_MANIFEST: foreign source",
                "FATAL_SCALE_OUT_STATUS_IDENTITY: wrong volume",
                "FATAL_SCALE_OUT_WATERMARK_REGRESSION: hwm moved back",
                "CATALOG_HEADER_IDENTITY_MISMATCH: backup",
                "UNSUPPORTED_OFFSET_VERSION: 2",
                "INVALID_SOURCE_OFFSET: invalid coordinates",
                "stored offset state_generation is missing or not numeric",
                "FATAL_FUTURE_FORMAT: unmapped code must still fail closed")) {
            var decision = FailureClassifier.classify(new IOException(message));
            assertEquals(FailureClassifier.Category.FATAL_CORRUPTION,
                    decision.category(), message);
            assertFalse(decision.retryable(), message);
        }
    }

    @Test
    void mapsEpochAndDataContractFailuresToNonRetriableCategories() {
        for (String message : java.util.List.of(
                "SOURCE_EPOCH_MISMATCH: stored=old current=new",
                "FATAL_STATE_EPOCH_MISMATCH: stored=old current=new",
                "FATAL_TOPOLOGY_EPOCH_MISMATCH: stored=old current=new",
                "FATAL_TOPOLOGY_CHANGE: partition moved")) {
            var decision = FailureClassifier.classify(new IOException(message));
            assertEquals(FailureClassifier.Category.SOURCE_EPOCH_CHANGED,
                    decision.category(), message);
            assertFalse(decision.retryable(), message);
        }

        for (String message : java.util.List.of(
                "FATAL_DML_CONTRACT_VIOLATION: ordinary UPDATE",
                "FATAL_SCHEMA_CONTRACT: no primary key",
                "FATAL_SCHEMA_CHANGE: selected table record changed",
                "FATAL_TABLE_BINDING: container identity changed",
                "FATAL_SOURCE_TOPOLOGY: source.id mismatch",
                "FATAL_SCALE_OUT_COMMIT_GRAMMAR_UNCERTIFIED: qualification required",
                "FATAL_UNSUPPORTED_SOURCE_TOPOLOGY: scale_out requires P2")) {
            var decision = FailureClassifier.classify(new IOException(message));
            assertEquals(FailureClassifier.Category.DATA_CONTRACT_VIOLATION,
                    decision.category(), message);
            assertFalse(decision.retryable(), message);
        }

        assertLargeValueFailure("FATAL_INCOMPLETE_LARGE_VALUE: locator=42",
                "FATAL_INCOMPLETE_LARGE_VALUE");
        assertLargeValueFailure("FATAL_EVENT_TOO_LARGE: serialized_bytes=101 maximum_bytes=100",
                "FATAL_EVENT_TOO_LARGE");
    }

    @Test
    void mapsBoundedLimitFailuresToOperatorResourceCategory() {
        for (String message : java.util.List.of(
                "FATAL_RESOURCE_LIMIT: MASS image buffer limit exceeded",
                "FATAL_TRANSACTION_LIMIT: legacy transaction limit")) {
            var decision = FailureClassifier.classify(new IOException(message));
            assertEquals(FailureClassifier.Category.RESOURCE_EXHAUSTED,
                    decision.category(), message);
            assertEquals("RESOURCE_EXHAUSTED", decision.code(), message);
            assertFalse(decision.retryable(), message);
        }
    }

    private static void assertLargeValueFailure(String message, String code) {
        var decision = FailureClassifier.classify(new IOException(message));
        assertEquals(FailureClassifier.Category.DATA_CONTRACT_VIOLATION,
                decision.category());
        assertEquals(code, decision.code());
        assertFalse(decision.retryable());
    }

    @Test
    void mapsPersistentLocalStorageFailuresToNonRetriableCategory() {
        for (Throwable error : java.util.List.<Throwable>of(
                new java.nio.file.AccessDeniedException("state.tmp"),
                new java.nio.file.FileSystemException(
                        "state.tmp", null, "No space left on device"),
                new java.io.IOException("Too many open files"),
                new SQLException("JDBC connect failed",
                        new java.net.SocketException("No file descriptors available")),
                new java.nio.file.ReadOnlyFileSystemException())) {
            var decision = FailureClassifier.classify(error);
            assertEquals(FailureClassifier.Category.RESOURCE_EXHAUSTED,
                    decision.category());
            assertEquals("STORAGE_UNAVAILABLE", decision.code());
            assertFalse(decision.retryable());
        }
    }

    @Test
    void retriesTransientControlPlaneFailures() {
        var transientFailure = FailureClassifier.classify(
                new IOException("control query failed", new SQLException("connection reset")));
        assertEquals(FailureClassifier.Category.RETRIABLE, transientFailure.category());
        assertTrue(transientFailure.retryable());

        var wrapped = FailureClassifier.wrap("refresh HANA control plane",
                new IOException("control query failed",
                        new SQLException("connection reset")));
        assertTrue(wrapped instanceof RetriableException);
        var classified = (FailureClassifier.ClassifiedRetriableException) wrapped;
        assertEquals(FailureClassifier.Category.RETRIABLE,
                classified.decision().category());
        assertTrue(classified.decision().retryable());

        var stale = FailureClassifier.classify(
                new AsyncPolicyRefresher.StaleResultException(60_001, 60_000));
        assertEquals(FailureClassifier.Category.RETRIABLE, stale.category());
        assertEquals("CONTROL_PLANE_STALE", stale.code());
        assertTrue(stale.retryable());
    }

    @Test
    void wrappedFailurePreservesMachineReadableDecision() {
        var wrapped = FailureClassifier.wrap("restore reducer state",
                new IOException("FATAL_STATE_TRUNCATED"));
        var classified = (FailureClassifier.ClassifiedConnectException) wrapped;

        assertEquals(FailureClassifier.Category.FATAL_CORRUPTION,
                classified.decision().category());
        assertEquals("SOURCE_CORRUPTION", classified.decision().code());
        assertFalse(classified.decision().retryable());
    }

    @Test
    void wrappedFailurePreservesDecisionAcrossJavaSerialization() throws Exception {
        var original = FailureClassifier.wrap("restore reducer state",
                new IOException("FATAL_STATE_TRUNCATED"));

        byte[] serialized;
        try (var bytes = new ByteArrayOutputStream();
                var output = new ObjectOutputStream(bytes)) {
            output.writeObject(original);
            output.flush();
            serialized = bytes.toByteArray();
        }

        Object restored;
        try (var input = new ObjectInputStream(new ByteArrayInputStream(serialized))) {
            restored = input.readObject();
        }

        assertTrue(restored instanceof FailureClassifier.ClassifiedConnectException);
        var classified = (FailureClassifier.ClassifiedConnectException) restored;
        assertEquals(FailureClassifier.Category.FATAL_CORRUPTION,
                classified.decision().category());
        assertEquals("SOURCE_CORRUPTION", classified.decision().code());
        assertFalse(classified.decision().retryable());
    }
}

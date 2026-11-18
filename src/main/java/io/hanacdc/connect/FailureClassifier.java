package io.hanacdc.connect;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;

import java.io.Serial;
import java.io.Serializable;
import java.sql.SQLException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.ReadOnlyFileSystemException;
import java.util.Locale;

/** Fail-closed operational classification shared by task and test harnesses. */
public final class FailureClassifier {
    public enum Category {
        RETRIABLE,
        POLICY_PAUSED,
        FATAL_GAP,
        FATAL_CORRUPTION,
        RESOURCE_EXHAUSTED,
        SOURCE_EPOCH_CHANGED,
        DATA_CONTRACT_VIOLATION
    }

    public record Decision(Category category, String code, boolean retryable)
            implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    private FailureClassifier() {}

    public static Decision classify(Throwable error) {
        String message = messages(error).toUpperCase(Locale.ROOT);
        if (message.contains("SOURCE_EPOCH_CHANGED")
                || message.contains("SOURCE_EPOCH_MISMATCH")
                || message.contains("FATAL_STATE_EPOCH_MISMATCH")
                || message.contains("FATAL_TOPOLOGY_EPOCH_MISMATCH")
                || message.contains("FATAL_TOPOLOGY_CHANGE")) {
            return decision(Category.SOURCE_EPOCH_CHANGED, "SOURCE_EPOCH_CHANGED");
        }
        if (message.contains("DATA_CONTRACT_VIOLATION")
                || message.contains("FATAL_DML_CONTRACT_VIOLATION")
                || message.contains("FATAL_SCHEMA_CONTRACT")
                || message.contains("FATAL_SCHEMA_CHANGE")
                || message.contains("FATAL_TABLE_BINDING")
                || message.contains("FATAL_SOURCE_TOPOLOGY")
                || message.contains("FATAL_SCALE_OUT_COMMIT_GRAMMAR_UNCERTIFIED")
                || message.contains("FATAL_UNSUPPORTED_SOURCE_TOPOLOGY")) {
            return decision(Category.DATA_CONTRACT_VIOLATION, "DATA_CONTRACT_VIOLATION");
        }
        if (message.contains("FATAL_INCOMPLETE_LARGE_VALUE")) {
            return decision(Category.DATA_CONTRACT_VIOLATION,
                    "FATAL_INCOMPLETE_LARGE_VALUE");
        }
        if (message.contains("FATAL_EVENT_TOO_LARGE")) {
            return decision(Category.DATA_CONTRACT_VIOLATION,
                    "FATAL_EVENT_TOO_LARGE");
        }
        if (message.contains("FATAL_CONTROL_PLANE_STALE")) {
            return new Decision(Category.RETRIABLE, "CONTROL_PLANE_STALE", true);
        }
        if (message.contains("FATAL_RETENTION_OBJECT_MISSING")) {
            return decision(Category.FATAL_GAP, "FATAL_RETENTION_OBJECT_MISSING");
        }
        if (message.contains("FATAL_RETENTION")) {
            return decision(Category.FATAL_GAP, "FATAL_RETENTION");
        }
        if (message.contains("FATAL_STATE_REPLAY_BACKUP_MISSING")) {
            return decision(Category.FATAL_GAP, "FATAL_STATE_REPLAY_BACKUP_MISSING");
        }
        if (message.contains("FATAL_STATE_OBJECT_MISSING")) {
            return decision(Category.FATAL_CORRUPTION, "FATAL_STATE_OBJECT_MISSING");
        }
        if (message.contains("CATALOG_SOURCE_IDENTITY_MISMATCH")) {
            return decision(Category.FATAL_CORRUPTION,
                    "CATALOG_SOURCE_IDENTITY_MISMATCH");
        }
        if (message.contains("FATAL_GAP")
                || message.contains("FATAL_PREDECESSOR")
                || message.contains("FATAL_OBJECT_MISSING")) {
            return decision(Category.FATAL_GAP, "FATAL_GAP");
        }
        if (message.contains("HANA POLICY GATE")
                || message.contains("HANA CAPABILITY GATE")) {
            return decision(Category.POLICY_PAUSED, "HANA_POLICY");
        }
        if (message.contains("FATAL_RESOURCE_LIMIT")
                || message.contains("FATAL_TRANSACTION_LIMIT")) {
            return decision(Category.RESOURCE_EXHAUSTED, "RESOURCE_EXHAUSTED");
        }
        if (isLocalStorageUnavailable(error)) {
            return decision(Category.RESOURCE_EXHAUSTED, "STORAGE_UNAVAILABLE");
        }
        if (message.contains("FATAL_STATE_")
                || message.contains("FATAL_BACKUP_")
                || message.contains("FATAL_CORRUPTION")
                || message.contains("FATAL_DECODE")
                || message.contains("FATAL_LOB_")
                || message.contains("FATAL_NATIVE_ROW")
                || message.contains("FATAL_ORDERING")
                || message.contains("FATAL_TRANSACTION_")
                || message.contains("FATAL_SCALE_OUT_TRANSACTION")
                || message.contains("FATAL_SCALE_OUT_MANIFEST")
                || message.contains("FATAL_SCALE_OUT_STATUS_IDENTITY")
                || message.contains("FATAL_SCALE_OUT_WATERMARK_REGRESSION")
                || message.contains("UNSUPPORTED_OFFSET_VERSION")
                || message.contains("INVALID_SOURCE_OFFSET")
                || message.contains("STORED OFFSET")
                || message.contains("OFFSET_NOT_FOUND")
                || message.contains("OFFSET_IDENTITY_MISMATCH")
                || message.contains("AMBIGUOUS_")
                || message.contains("CATALOG_HEADER_IDENTITY_MISMATCH")) {
            return decision(Category.FATAL_CORRUPTION, "SOURCE_CORRUPTION");
        }
        // A newly introduced fatal code must fail closed even before it receives
        // a more specific category in this registry.
        if (message.contains("FATAL_")) {
            return decision(Category.FATAL_CORRUPTION, "UNMAPPED_FATAL");
        }
        if (hasCause(error, SQLException.class)
                || hasCause(error, java.net.SocketException.class)) {
            return new Decision(Category.RETRIABLE, "SOURCE_UNAVAILABLE", true);
        }
        return new Decision(Category.RETRIABLE, "UNCLASSIFIED_IO", true);
    }

    public static ConnectException wrap(String context, Throwable error) {
        Decision decision = classify(error);
        if (decision.retryable()) {
            return new ClassifiedRetriableException(
                    decision,
                    decision.code() + ": " + context,
                    error);
        }
        return new ClassifiedConnectException(
                decision,
                decision.code() + ": " + context,
                error);
    }

    public static final class ClassifiedRetriableException extends RetriableException {
        @Serial
        private static final long serialVersionUID = 1L;

        private final Decision decision;

        private ClassifiedRetriableException(
                Decision decision,
                String message,
                Throwable cause) {
            super(message, cause);
            this.decision = decision;
        }

        public Decision decision() {
            return decision;
        }
    }

    public static final class ClassifiedConnectException extends ConnectException {
        @Serial
        private static final long serialVersionUID = 1L;

        private final Decision decision;

        private ClassifiedConnectException(
                Decision decision,
                String message,
                Throwable cause) {
            super(message, cause);
            this.decision = decision;
        }

        public Decision decision() {
            return decision;
        }
    }

    private static Decision decision(Category category, String code) {
        return new Decision(category, code, false);
    }

    private static String messages(Throwable error) {
        StringBuilder result = new StringBuilder();
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (current.getMessage() != null) {
                result.append(current.getMessage()).append('\n');
            }
        }
        return result.toString();
    }

    private static boolean hasCause(Throwable error, Class<? extends Throwable> type) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (type.isInstance(current)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isLocalStorageUnavailable(Throwable error) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (current instanceof AccessDeniedException
                    || current instanceof ReadOnlyFileSystemException) {
                return true;
            }
            if (current instanceof FileSystemException fileSystem) {
                String reason = String.valueOf(fileSystem.getReason())
                        .toUpperCase(Locale.ROOT);
                if (storageReason(reason)) {
                    return true;
                }
            }
            if (current instanceof java.io.IOException
                    && storageReason(String.valueOf(current.getMessage())
                    .toUpperCase(Locale.ROOT))) {
                return true;
            }
        }
        return false;
    }

    private static boolean storageReason(String message) {
        return message.contains("NO SPACE LEFT ON DEVICE")
                || message.contains("DISK QUOTA EXCEEDED")
                || message.contains("READ-ONLY FILE SYSTEM")
                || message.contains("TOO MANY OPEN FILES")
                || message.contains("NO FILE DESCRIPTORS AVAILABLE")
                || message.contains("PERMISSION DENIED")
                || message.contains("ACCESS IS DENIED");
    }
}

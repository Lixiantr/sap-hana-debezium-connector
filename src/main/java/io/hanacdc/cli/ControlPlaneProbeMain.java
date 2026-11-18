package io.hanacdc.cli;

import io.hanacdc.control.CapabilityAssessment;
import io.hanacdc.control.HanaCapabilities;
import io.hanacdc.control.HanaContract;
import io.hanacdc.control.JdbcHanaControlPlane;
import io.hanacdc.control.LogBackupEntry;
import io.hanacdc.control.ManifestPlanner;
import io.hanacdc.control.PathMapper;
import io.hanacdc.decoder.ParallelLogDecoder;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Operational smoke probe for the same read-only control plane used by SourceTask. */
public final class ControlPlaneProbeMain {
    private ControlPlaneProbeMain() {}

    public static void main(String[] args) {
        try {
            run(arguments(args));
        }
        catch (Exception error) {
            System.err.println("error: " + error.getMessage());
            System.exit(1);
        }
    }

    private static void run(Map<String, String> values) throws Exception {
        String password = Files.readString(Path.of(required(values, "--password-file"))).trim();
        long sourceId = number(values, "--source-id", 2);
        long volume = number(values, "--volume", sourceId);
        long partition = number(values, "--partition", 0);
        long maximumBytes = number(values, "--max-backup-bytes", Integer.MAX_VALUE);
        long maximumDelayMs = number(values, "--max-delay-ms", 900_000);
        String versionPrefix = values.getOrDefault("--supported-version", "2.00.088");
        HanaContract contract = new HanaContract(
                Set.of("immediate"), Set.of("file"), Set.of("none"),
                false, false, maximumBytes, maximumDelayMs, List.of(versionPrefix));

        try (JdbcHanaControlPlane control = JdbcHanaControlPlane.connect(
                values.getOrDefault("--driver", "com.sap.db.jdbc.Driver"),
                required(values, "--jdbc-url"),
                required(values, "--user"),
                password,
                sourceId,
                (int) number(values, "--query-timeout-seconds", 30))) {
            HanaCapabilities capabilities = control.inspect();
            CapabilityAssessment assessment = contract.assess(capabilities);
            long hwm = capabilities.successfulBackupHighWatermark();
            List<LogBackupEntry> entries = hwm == 0
                    ? List.of()
                    : control.successfulLogBackups(sourceId, 0, hwm);
            List<BackupFile> files = entries.isEmpty()
                    ? List.of()
                    : ManifestPlanner.contiguousFiles(
                            entries, 0, hwm, sourceId, volume, partition, contract,
                            PathMapper.parse(required(values, "--path-mapping")));
            System.out.println(json(capabilities, assessment, entries, files));
            if (!assessment.canRead()) {
                System.exit(2);
            }
        }
    }

    private static String json(
            HanaCapabilities value,
            CapabilityAssessment assessment,
            List<LogBackupEntry> entries,
            List<BackupFile> files) {
        return String.format(Locale.ROOT,
                "{\"kind\":\"hana_control_plane\","
                        + "\"system_id\":\"%s\",\"database\":\"%s\","
                        + "\"version\":\"%s\",\"log_mode\":\"%s\","
                        + "\"automatic_log_backup\":%s,\"timeout_seconds\":%d,"
                        + "\"interval_mode\":\"%s\",\"segment_mib\":%d,"
                        + "\"max_backup_gib\":%d,\"backint\":%s,"
                        + "\"compression\":%s,\"encryption\":%s,"
                        + "\"broken_history_entries\":%d,"
                        + "\"earliest_backup_position\":%d,"
                        + "\"retained_backup_bytes\":%d,"
                        + "\"backup_hwm\":%d,\"redo_head\":%d,"
                        + "\"contract_ready\":%s,\"contract_can_read\":%s,"
                        + "\"violations\":\"%s\",\"catalog_entries\":%d,"
                        + "\"contiguous_files\":%d}",
                escape(value.systemId()), escape(value.databaseName()), escape(value.version()),
                escape(value.logMode()), value.automaticLogBackupEnabled(),
                value.logBackupTimeoutSeconds(), escape(value.logBackupIntervalMode()),
                value.logSegmentSizeMiB(), value.maxLogBackupSizeGiB(),
                value.logBackupUsingBackint(), value.logBackupCompressionEnabled(),
                value.backupEncryptionEnabled(), value.brokenHistoryEntries(),
                value.earliestSuccessfulBackupPosition(),
                value.retainedSuccessfulBackupBytes(),
                value.successfulBackupHighWatermark(), value.redoHead(),
                assessment.ready(), assessment.canRead(), escape(assessment.summary()),
                entries.size(), files.size());
    }

    private static String escape(String value) {
        return ParallelLogDecoder.escapeJson(value == null ? "" : value);
    }

    private static Map<String, String> arguments(String[] args) {
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("arguments must be --name value pairs");
        }
        Map<String, String> result = new LinkedHashMap<>();
        for (int index = 0; index < args.length; index += 2) {
            if (!args[index].startsWith("--")) {
                throw new IllegalArgumentException("expected option, got " + args[index]);
            }
            result.put(args[index], args[index + 1]);
        }
        return result;
    }

    private static String required(Map<String, String> values, String name) {
        String value = values.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing " + name);
        }
        return value;
    }

    private static long number(Map<String, String> values, String name, long defaultValue) {
        String value = values.get(name);
        return value == null ? defaultValue : Long.parseLong(value);
    }
}

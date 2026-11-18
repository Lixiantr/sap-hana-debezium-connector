package io.hanacdc.connect;

import org.apache.kafka.connect.errors.ConnectException;

/** Conservative, reproducible worker selection for the native ordered path. */
final class NativeWorkerSelector {
    private static final int LEGACY_QUALIFIED_AUTO_CAP = 3;

    private NativeWorkerSelector() {}

    static int select(
            String configured,
            int maximum,
            int availableProcessors,
            long largestBackupBytes,
            long targetChunkBytes,
            int chunksPerThread) {
        return select(configured, maximum, availableProcessors, largestBackupBytes,
                targetChunkBytes, chunksPerThread, "scan.threads", "scan.max.threads",
                LEGACY_QUALIFIED_AUTO_CAP);
    }

    static int select(
            String configured,
            int maximum,
            int availableProcessors,
            long largestBackupBytes,
            long targetChunkBytes,
            int chunksPerThread,
            String threadsName,
            String maximumName) {
        return select(configured, maximum, availableProcessors, largestBackupBytes,
                targetChunkBytes, chunksPerThread, threadsName, maximumName,
                LEGACY_QUALIFIED_AUTO_CAP);
    }

    static int select(
            String configured,
            int maximum,
            int availableProcessors,
            long largestBackupBytes,
            long targetChunkBytes,
            int chunksPerThread,
            String threadsName,
            String maximumName,
            int qualifiedAutoCap) {
        if (maximum <= 0 || availableProcessors <= 0
                || largestBackupBytes < 0 || targetChunkBytes <= 0
                || chunksPerThread <= 0 || qualifiedAutoCap <= 0) {
            throw new IllegalArgumentException("invalid native worker selection limits");
        }
        if (!"auto".equalsIgnoreCase(configured)) {
            try {
                int value = Integer.parseInt(configured);
                if (value <= 0 || value > maximum) {
                    throw new ConnectException(
                            threadsName + " must be in 1.." + maximumName);
                }
                return value;
            }
            catch (NumberFormatException error) {
                throw new ConnectException(
                        threadsName + " must be auto or a positive integer", error);
            }
        }
        long workerChunkBytes = Math.max(1, targetChunkBytes / chunksPerThread);
        long usefulBySize = largestBackupBytes == 0 ? 1
                : 1 + (largestBackupBytes - 1) / workerChunkBytes;
        return (int) Math.max(1, Math.min(
                Math.min(Math.min((long) maximum, availableProcessors),
                        qualifiedAutoCap), usefulBySize));
    }
}

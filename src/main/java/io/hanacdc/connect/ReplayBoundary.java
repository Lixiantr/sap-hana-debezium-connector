package io.hanacdc.connect;

/** Safe native replay anchor represented by one catalogued backup and committed HWM. */
public record ReplayBoundary(
        long backupId,
        long fileFirst,
        long fileLast,
        long replayPosition,
        long committedPosition) {

    public ReplayBoundary {
        if (backupId < 0 || fileFirst < 0 || fileLast <= fileFirst
                || replayPosition < fileFirst || replayPosition >= fileLast
                || committedPosition < fileFirst) {
            throw new IllegalArgumentException("invalid native replay boundary");
        }
    }
}

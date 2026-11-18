package io.hanacdc.connect;

import io.hanacdc.decoder.NativeTransactionReducer;
import io.hanacdc.decoder.NativeTransactionReducer.KeyPart;
import io.hanacdc.decoder.NativeTransactionReducer.RowKey;
import io.hanacdc.decoder.NativeTransactionReducer.Snapshot;
import io.hanacdc.decoder.NativeTransactionReducer.StateDelta;
import io.hanacdc.decoder.NativeTransactionReducer.TombstoneState;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Immutable, checksummed and atomically published reducer checkpoints. */
public final class DurableReducerStateStore {
    private static final int MAGIC = 0x48434453; // HCDS
    private static final int LEGACY_FULL_FORMAT_VERSION = 3;
    private static final int FORMAT_VERSION = 4;
    private static final int KIND_FULL = 0;
    private static final int KIND_DELTA = 1;
    private static final int MAX_DELTA_CHAIN = 1024;
    private static final int MAX_STRING_BYTES = 4 * 1024 * 1024;
    private static final int MAX_ENTRIES = 1_000_000;
    private static final long MAX_STATE_BYTES = 2L * 1024 * 1024 * 1024;
    private static final Pattern CHECKPOINT_FILE = Pattern.compile(
            "state-([1-9][0-9]*)-([0-9a-f]{64})\\.bin");

    public record Checkpoint(long generation, String sha256, Path path) {
        public Checkpoint {
            if (generation <= 0) {
                throw new IllegalArgumentException("state generation must be positive");
            }
            sha256 = checkedSha256(sha256);
            path = Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
        }
    }

    public record RestoredCheckpoint(Snapshot snapshot, ReplayBoundary boundary) {
        public RestoredCheckpoint {
            Objects.requireNonNull(snapshot, "snapshot");
            Objects.requireNonNull(boundary, "boundary");
        }
    }

    public record Description(long generation, String sha256, Path path, long sizeBytes) {
        public Description {
            if (generation <= 0 || sizeBytes <= 0 || sizeBytes > MAX_STATE_BYTES) {
                throw new IllegalArgumentException("invalid state description coordinates");
            }
            sha256 = checkedSha256(sha256);
            path = Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
        }
    }

    public record StateFile(long generation, String sha256, Path path, long sizeBytes) {
        public StateFile {
            if (generation <= 0 || sizeBytes <= 0 || sizeBytes > MAX_STATE_BYTES) {
                throw new IllegalArgumentException("invalid state file coordinates");
            }
            sha256 = checkedSha256(sha256);
            path = Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
        }
    }

    private record StoredState(
            ReplayBoundary boundary,
            Snapshot snapshot,
            StateDelta delta,
            Checkpoint parent) {
        private boolean full() {
            return snapshot != null;
        }
    }

    private final Path directory;

    public DurableReducerStateStore(Path directory) {
        this.directory = Objects.requireNonNull(directory, "directory")
                .toAbsolutePath().normalize();
    }

    public Checkpoint write(
            SourceEpoch epoch,
            long generation,
            ReplayBoundary boundary,
            Snapshot snapshot)
            throws IOException {
        Objects.requireNonNull(snapshot, "snapshot");
        return writeState(epoch, generation, boundary, snapshot, null, null);
    }

    public Checkpoint writeDelta(
            SourceEpoch epoch,
            long generation,
            ReplayBoundary boundary,
            Checkpoint parent,
            StateDelta delta) throws IOException {
        Objects.requireNonNull(parent, "parent");
        Objects.requireNonNull(delta, "delta");
        if (parent.generation() >= generation) {
            throw new IllegalArgumentException("delta parent must precede generation");
        }
        return writeState(epoch, generation, boundary, null, parent, delta);
    }

    private Checkpoint writeState(
            SourceEpoch epoch,
            long generation,
            ReplayBoundary boundary,
            Snapshot snapshot,
            Checkpoint parent,
            StateDelta delta) throws IOException {
        Objects.requireNonNull(epoch, "epoch");
        Objects.requireNonNull(boundary, "boundary");
        if (generation <= 0) {
            throw new IllegalArgumentException("state generation must be positive");
        }
        Files.createDirectories(directory);
        Path temporary = directory.resolve(".state-" + generation + "-"
                + UUID.randomUUID() + ".tmp");
        MessageDigest digest = digest();
        try {
            try (FileChannel channel = FileChannel.open(temporary,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                 DataOutputStream output = new DataOutputStream(new BufferedOutputStream(
                         new DigestOutputStream(Channels.newOutputStream(channel), digest)))) {
                writeHeader(output, epoch, generation, boundary,
                        snapshot == null ? KIND_DELTA : KIND_FULL, parent);
                if (snapshot != null) writeSnapshot(output, snapshot);
                else writeDelta(output, delta);
                output.flush();
                channel.force(true);
            }
            String sha256 = HexFormat.of().formatHex(digest.digest());
            Path target = path(generation, sha256);
            if (Files.isRegularFile(target)) {
                String existing = fileSha256(target);
                if (!sha256.equals(existing)) {
                    throw new IOException("FATAL_STATE_EXISTING_CHECKSUM_MISMATCH: "
                            + target);
                }
                return new Checkpoint(generation, sha256, target);
            }
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
            }
            catch (AtomicMoveNotSupportedException error) {
                throw new IOException("FATAL_STATE_ATOMIC_MOVE_UNSUPPORTED: " + directory,
                        error);
            }
            forceDirectory();
            return new Checkpoint(generation, sha256, target);
        }
        finally {
            Files.deleteIfExists(temporary);
        }
    }

    /** Computes the exact immutable filename and byte size without creating a file. */
    public Description describe(
            SourceEpoch epoch,
            long generation,
            ReplayBoundary boundary,
            Snapshot snapshot) throws IOException {
        Objects.requireNonNull(snapshot, "snapshot");
        return describeState(epoch, generation, boundary, snapshot, null, null);
    }

    public Description describeDelta(
            SourceEpoch epoch,
            long generation,
            ReplayBoundary boundary,
            Checkpoint parent,
            StateDelta delta) throws IOException {
        Objects.requireNonNull(parent, "parent");
        Objects.requireNonNull(delta, "delta");
        if (parent.generation() >= generation) {
            throw new IllegalArgumentException("delta parent must precede generation");
        }
        return describeState(epoch, generation, boundary, null, parent, delta);
    }

    private Description describeState(
            SourceEpoch epoch,
            long generation,
            ReplayBoundary boundary,
            Snapshot snapshot,
            Checkpoint parent,
            StateDelta delta) throws IOException {
        Objects.requireNonNull(epoch, "epoch");
        Objects.requireNonNull(boundary, "boundary");
        if (generation <= 0) {
            throw new IllegalArgumentException("state generation must be positive");
        }
        MessageDigest digest = digest();
        CountingOutputStream counter = new CountingOutputStream();
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(
                new DigestOutputStream(counter, digest)))) {
            writeHeader(output, epoch, generation, boundary,
                    snapshot == null ? KIND_DELTA : KIND_FULL, parent);
            if (snapshot != null) writeSnapshot(output, snapshot);
            else writeDelta(output, delta);
        }
        long size = counter.count();
        if (size <= 0 || size > MAX_STATE_BYTES) {
            throw new IOException("FATAL_STATE_SIZE: " + size);
        }
        String sha256 = HexFormat.of().formatHex(digest.digest());
        return new Description(generation, sha256, path(generation, sha256), size);
    }

    public RestoredCheckpoint read(SourceEpoch expectedEpoch, Checkpoint checkpoint)
            throws IOException {
        Objects.requireNonNull(expectedEpoch, "expectedEpoch");
        Objects.requireNonNull(checkpoint, "checkpoint");
        StoredState target = readStored(expectedEpoch, checkpoint);
        ReplayBoundary boundary = target.boundary();
        if (target.full()) {
            return new RestoredCheckpoint(target.snapshot(), boundary);
        }
        List<StateDelta> deltas = new ArrayList<>();
        StoredState current = target;
        Checkpoint currentReference = checkpoint;
        while (!current.full()) {
            if (deltas.size() >= MAX_DELTA_CHAIN) {
                throw new IOException("FATAL_STATE_DELTA_CHAIN_LIMIT: generation="
                        + checkpoint.generation());
            }
            deltas.add(current.delta());
            Checkpoint parent = current.parent();
            if (parent == null || parent.generation() >= currentReference.generation()) {
                throw new IOException("FATAL_STATE_DELTA_PARENT: generation="
                        + currentReference.generation());
            }
            currentReference = parent;
            current = readStored(expectedEpoch, parent);
        }
        NativeTransactionReducer reducer = new NativeTransactionReducer();
        reducer.restore(current.snapshot());
        for (int index = deltas.size() - 1; index >= 0; index--) {
            reducer.apply(deltas.get(index));
        }
        return new RestoredCheckpoint(reducer.snapshot(), boundary);
    }

    private StoredState readStored(SourceEpoch expectedEpoch, Checkpoint checkpoint)
            throws IOException {
        Path expectedPath = path(checkpoint.generation(), checkpoint.sha256());
        if (!expectedPath.equals(checkpoint.path())) {
            throw new IOException("FATAL_STATE_PATH_MISMATCH: " + checkpoint.path());
        }
        try {
            long size = Files.size(expectedPath);
            if (size <= 0 || size > MAX_STATE_BYTES) {
                throw new IOException("FATAL_STATE_SIZE: " + size);
            }
            MessageDigest digest = digest();
            ReplayBoundary boundary;
            Snapshot snapshot = null;
            StateDelta delta = null;
            Checkpoint parent = null;
            try (DataInputStream input = new DataInputStream(new BufferedInputStream(
                    new DigestInputStream(Files.newInputStream(expectedPath), digest)))) {
                int magic = input.readInt();
                int version = input.readInt();
                if (magic != MAGIC || version != FORMAT_VERSION
                        && version != LEGACY_FULL_FORMAT_VERSION) {
                    throw new IOException("FATAL_STATE_FORMAT: magic/version mismatch");
                }
                String epoch = readString(input);
                long generation = input.readLong();
                if (expectedEpoch != null && !expectedEpoch.id().equals(epoch)) {
                    throw new IOException("FATAL_STATE_EPOCH_MISMATCH: stored=" + epoch
                            + " current=" + expectedEpoch.id());
                }
                if (generation != checkpoint.generation()) {
                    throw new IOException("FATAL_STATE_GENERATION_MISMATCH: stored="
                            + generation + " expected=" + checkpoint.generation());
                }
                boundary = new ReplayBoundary(input.readLong(), input.readLong(),
                        input.readLong(), input.readLong(), input.readLong());
                if (version == LEGACY_FULL_FORMAT_VERSION) {
                    snapshot = readSnapshot(input);
                }
                else {
                    int kind = input.readUnsignedByte();
                    if (kind == KIND_FULL) {
                        snapshot = readSnapshot(input);
                    }
                    else if (kind == KIND_DELTA) {
                        long parentGeneration = input.readLong();
                        String parentSha256 = readString(input);
                        parent = reference(parentGeneration, parentSha256);
                        delta = readDelta(input);
                    }
                    else {
                        throw new IOException("FATAL_STATE_KIND: " + kind);
                    }
                }
                if (input.read() != -1) {
                    throw new IOException("FATAL_STATE_TRAILING_BYTES");
                }
            }
            String actual = HexFormat.of().formatHex(digest.digest());
            if (!checkpoint.sha256().equals(actual)) {
                throw new IOException("FATAL_STATE_CHECKSUM_MISMATCH: expected="
                        + checkpoint.sha256() + " actual=" + actual);
            }
            return new StoredState(boundary, snapshot, delta, parent);
        }
        catch (NoSuchFileException error) {
            throw new IOException("FATAL_STATE_OBJECT_MISSING: generation="
                    + checkpoint.generation() + " path=" + expectedPath, error);
        }
    }

    public Checkpoint reference(long generation, String sha256) {
        return new Checkpoint(generation, sha256, path(generation, sha256));
    }

    public StateFile stateFile(Checkpoint checkpoint) throws IOException {
        Objects.requireNonNull(checkpoint, "checkpoint");
        Path expected = path(checkpoint.generation(), checkpoint.sha256());
        if (!expected.equals(checkpoint.path()) || !Files.isRegularFile(expected)) {
            throw new IOException("FATAL_STATE_INVENTORY_CHECKPOINT_MISSING: " + expected);
        }
        return checkedStateFile(checkpoint.generation(), checkpoint.sha256(), expected);
    }

    /** Inventories unconfirmed immutable files after the authoritative generation. */
    public List<StateFile> inventoryAfter(long generation) throws IOException {
        if (generation < 0) {
            throw new IllegalArgumentException("generation must not be negative");
        }
        if (!Files.isDirectory(directory)) {
            return List.of();
        }
        List<StateFile> result = new ArrayList<>();
        try (var entries = Files.newDirectoryStream(directory)) {
            for (Path entry : entries) {
                Path name = entry.getFileName();
                if (name == null) {
                    continue;
                }
                Matcher matcher = CHECKPOINT_FILE.matcher(name.toString());
                if (!matcher.matches()) {
                    continue;
                }
                long candidate = generation(matcher, name);
                if (candidate <= generation) {
                    continue;
                }
                if (!Files.isRegularFile(entry, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("FATAL_STATE_INVENTORY_NON_REGULAR: " + entry);
                }
                result.add(checkedStateFile(candidate, matcher.group(2), entry));
            }
        }
        return List.copyOf(result);
    }

    /**
     * Deletes immutable checkpoints older than an offset-confirmed checkpoint.
     * The retained file must already exist; newer checkpoints may still be in flight
     * and are deliberately left untouched.
     */
    public int pruneBefore(Checkpoint retained) throws IOException {
        Objects.requireNonNull(retained, "retained");
        Path expectedPath = path(retained.generation(), retained.sha256());
        if (!expectedPath.equals(retained.path())) {
            throw new IOException("FATAL_STATE_PRUNE_PATH_MISMATCH: " + retained.path());
        }
        if (!Files.isRegularFile(expectedPath)) {
            throw new IOException("FATAL_STATE_PRUNE_RETAINED_MISSING: " + expectedPath);
        }
        if (!Files.isDirectory(directory)) {
            throw new IOException("FATAL_STATE_PRUNE_DIRECTORY_MISSING: " + directory);
        }

        Set<Path> required = new LinkedHashSet<>();
        Checkpoint ancestor = retained;
        for (int depth = 0; depth <= MAX_DELTA_CHAIN; depth++) {
            required.add(ancestor.path());
            StoredState state = readStored(null, ancestor);
            if (state.full()) break;
            if (depth == MAX_DELTA_CHAIN || state.parent() == null
                    || state.parent().generation() >= ancestor.generation()) {
                throw new IOException("FATAL_STATE_DELTA_PARENT: generation="
                        + ancestor.generation());
            }
            ancestor = state.parent();
        }

        int deleted = 0;
        try (var entries = Files.newDirectoryStream(directory)) {
            for (Path entry : entries) {
                Path name = entry.getFileName();
                if (name == null) {
                    continue;
                }
                Matcher matcher = CHECKPOINT_FILE.matcher(name.toString());
                if (!matcher.matches()) {
                    continue;
                }
                long candidate = generation(matcher, name);
                if (candidate < retained.generation()
                        && !required.contains(entry.toAbsolutePath().normalize())
                        && Files.deleteIfExists(entry)) {
                    deleted++;
                }
            }
        }
        if (deleted > 0) {
            forceDirectory();
        }
        return deleted;
    }

    private void writeHeader(
            DataOutputStream output,
            SourceEpoch epoch,
            long generation,
            ReplayBoundary boundary,
            int kind,
            Checkpoint parent)
            throws IOException {
        output.writeInt(MAGIC);
        output.writeInt(FORMAT_VERSION);
        writeString(output, epoch.id());
        output.writeLong(generation);
        output.writeLong(boundary.backupId());
        output.writeLong(boundary.fileFirst());
        output.writeLong(boundary.fileLast());
        output.writeLong(boundary.replayPosition());
        output.writeLong(boundary.committedPosition());
        output.writeByte(kind);
        if (kind == KIND_DELTA) {
            if (parent == null || parent.generation() >= generation) {
                throw new IOException("FATAL_STATE_DELTA_PARENT: generation=" + generation);
            }
            output.writeLong(parent.generation());
            writeString(output, parent.sha256());
        }
    }

    private static void writeSnapshot(DataOutputStream output, Snapshot snapshot)
            throws IOException {
        output.writeInt(snapshot.version());
        output.writeInt(snapshot.tombstones().size());
        for (TombstoneState state : snapshot.tombstones()) {
            writeString(output, state.tableIdentity());
            output.writeLong(state.containerId());
            output.writeLong(state.rowLocator());
            writeRowKey(output, state.key());
        }
        output.writeInt(snapshot.deletedKeys().size());
        for (RowKey key : snapshot.deletedKeys()) {
            writeRowKey(output, key);
        }
    }

    private static void writeDelta(DataOutputStream output, StateDelta delta)
            throws IOException {
        writeTombstones(output, delta.beforeTombstones());
        writeTombstones(output, delta.afterTombstones());
        output.writeInt(delta.addedDeletedKeys().size());
        for (RowKey key : delta.addedDeletedKeys()) writeRowKey(output, key);
    }

    private static void writeTombstones(
            DataOutputStream output, List<TombstoneState> tombstones) throws IOException {
        output.writeInt(tombstones.size());
        for (TombstoneState state : tombstones) {
            writeString(output, state.tableIdentity());
            output.writeLong(state.containerId());
            output.writeLong(state.rowLocator());
            writeRowKey(output, state.key());
        }
    }

    private static Snapshot readSnapshot(DataInputStream input) throws IOException {
        int version = input.readInt();
        int tombstoneCount = count(input.readInt(), "tombstones");
        List<TombstoneState> tombstones = new ArrayList<>(tombstoneCount);
        for (int index = 0; index < tombstoneCount; index++) {
            String tableIdentity = readString(input);
            long containerId = input.readLong();
            long locator = input.readLong();
            tombstones.add(new TombstoneState(
                    tableIdentity, containerId, locator, readRowKey(input)));
        }
        int deletedCount = count(input.readInt(), "deletedKeys");
        List<RowKey> deleted = new ArrayList<>(deletedCount);
        for (int index = 0; index < deletedCount; index++) {
            deleted.add(readRowKey(input));
        }
        try {
            return new Snapshot(version, tombstones, deleted);
        }
        catch (IllegalArgumentException error) {
            throw new IOException("FATAL_STATE_CONTENT: " + error.getMessage(), error);
        }
    }

    private static StateDelta readDelta(DataInputStream input) throws IOException {
        List<TombstoneState> before = readTombstones(input, "beforeTombstones");
        List<TombstoneState> after = readTombstones(input, "afterTombstones");
        int deletedCount = count(input.readInt(), "addedDeletedKeys");
        List<RowKey> deleted = new ArrayList<>(deletedCount);
        for (int index = 0; index < deletedCount; index++) {
            deleted.add(readRowKey(input));
        }
        return new StateDelta(before, after, deleted);
    }

    private static List<TombstoneState> readTombstones(
            DataInputStream input, String name) throws IOException {
        int tombstoneCount = count(input.readInt(), name);
        List<TombstoneState> tombstones = new ArrayList<>(tombstoneCount);
        for (int index = 0; index < tombstoneCount; index++) {
            String tableIdentity = readString(input);
            long containerId = input.readLong();
            long locator = input.readLong();
            tombstones.add(new TombstoneState(
                    tableIdentity, containerId, locator, readRowKey(input)));
        }
        return tombstones;
    }

    private static void writeRowKey(DataOutputStream output, RowKey key) throws IOException {
        writeString(output, key.tableIdentity());
        output.writeInt(key.parts().size());
        for (KeyPart part : key.parts()) {
            writeString(output, part.columnName());
            writeString(output, part.typeName());
            writeString(output, part.canonicalValue());
        }
    }

    private static RowKey readRowKey(DataInputStream input) throws IOException {
        String tableIdentity = readString(input);
        int parts = count(input.readInt(), "keyParts");
        if (parts == 0) {
            throw new IOException("FATAL_STATE_CONTENT: row key has no parts");
        }
        List<KeyPart> result = new ArrayList<>(parts);
        for (int index = 0; index < parts; index++) {
            result.add(new KeyPart(readString(input), readString(input), readString(input)));
        }
        return new RowKey(tableIdentity, result);
    }

    private Path path(long generation, String sha256) {
        return directory.resolve("state-" + generation + "-" + checkedSha256(sha256) + ".bin")
                .toAbsolutePath().normalize();
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        byte[] encoded = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (encoded.length > MAX_STRING_BYTES) {
            throw new IOException("FATAL_STATE_STRING_TOO_LARGE: " + encoded.length);
        }
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static String readString(DataInputStream input) throws IOException {
        int length;
        try {
            length = input.readInt();
        }
        catch (EOFException error) {
            throw new IOException("FATAL_STATE_TRUNCATED", error);
        }
        if (length < 0 || length > MAX_STRING_BYTES) {
            throw new IOException("FATAL_STATE_STRING_SIZE: " + length);
        }
        byte[] encoded = input.readNBytes(length);
        if (encoded.length != length) {
            throw new IOException("FATAL_STATE_TRUNCATED");
        }
        String value = new String(encoded, java.nio.charset.StandardCharsets.UTF_8);
        if (!java.util.Arrays.equals(encoded,
                value.getBytes(java.nio.charset.StandardCharsets.UTF_8))) {
            throw new IOException("FATAL_STATE_NON_CANONICAL_UTF8");
        }
        return value;
    }

    private static int count(int value, String name) throws IOException {
        if (value < 0 || value > MAX_ENTRIES) {
            throw new IOException("FATAL_STATE_COUNT " + name + "=" + value);
        }
        return value;
    }

    private void forceDirectory() throws IOException {
        try (FileChannel directoryChannel = FileChannel.open(directory, StandardOpenOption.READ)) {
            directoryChannel.force(true);
        }
    }

    private static String fileSha256(Path path) throws IOException {
        MessageDigest digest = digest();
        try (java.io.InputStream input = Files.newInputStream(path)) {
            byte[] buffer = new byte[64 * 1024];
            int read;
            while ((read = input.read(buffer)) >= 0) {
                if (read > 0) {
                    digest.update(buffer, 0, read);
                }
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static String checkedSha256(String value) {
        Objects.requireNonNull(value, "sha256");
        if (value.length() != 64 || !value.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("sha256 must be lowercase hexadecimal");
        }
        return value;
    }

    private static long generation(Matcher matcher, Path name) throws IOException {
        try {
            return Long.parseLong(matcher.group(1));
        }
        catch (NumberFormatException error) {
            throw new IOException("FATAL_STATE_GENERATION: " + name, error);
        }
    }

    private static StateFile checkedStateFile(
            long generation, String sha256, Path path) throws IOException {
        long size = Files.size(path);
        if (size <= 0 || size > MAX_STATE_BYTES) {
            throw new IOException("FATAL_STATE_SIZE: " + size + " path=" + path);
        }
        return new StateFile(generation, sha256, path, size);
    }

    private static final class CountingOutputStream extends OutputStream {
        private long count;

        @Override
        public void write(int value) {
            count = Math.addExact(count, 1);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) {
            Objects.checkFromIndexSize(offset, length, bytes.length);
            count = Math.addExact(count, length);
        }

        long count() {
            return count;
        }
    }
}

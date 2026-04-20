package com.loomq.snapshot;

import com.loomq.domain.intent.Intent;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.store.IntentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Snapshot 管理器。
 *
 * 负责 Intent 存储的快照创建和恢复，快照采用轻量二进制格式：
 * magic + version + createdAt + walOffset + recordCount + [intentLength + intentBytes]*
 */
public class SnapshotManager {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);
    private static final String SNAPSHOT_PREFIX = "snapshot-";
    private static final String SNAPSHOT_SUFFIX = ".snap.gz";
    private static final int MAX_SNAPSHOTS_TO_KEEP = 3;
    private static final int SNAPSHOT_MAGIC = 0x534E4150; // SNAP
    private static final int SNAPSHOT_VERSION = 1;
    private static final DateTimeFormatter SNAPSHOT_TIMESTAMP_FORMAT =
        DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS").withZone(ZoneOffset.UTC);

    private final Path snapshotDir;

    public SnapshotManager(String dataDir) {
        this.snapshotDir = Paths.get(dataDir, "snapshots");
    }

    /**
     * 创建快照（同步）。
     */
    public SnapshotInfo createSnapshot(IntentStore store, long walOffset) {
        ensureSnapshotDir();

        String timestamp = SNAPSHOT_TIMESTAMP_FORMAT.format(Instant.now());
        String filename = SNAPSHOT_PREFIX + timestamp + SNAPSHOT_SUFFIX;
        Path snapshotPath = snapshotDir.resolve(filename);

        logger.info("Creating snapshot: {}", filename);

        try {
            SnapshotData data = captureSnapshotData(store, walOffset);
            writeSnapshotData(snapshotPath, data);

            long size = Files.size(snapshotPath);
            SnapshotInfo info = new SnapshotInfo(filename, data.walOffset, size, timestamp);

            logger.info("Snapshot created: {}, size={}, intents={}, offset={}",
                filename, size, data.intentCount, data.walOffset);

            cleanupOldSnapshots();
            return info;

        } catch (IOException e) {
            logger.error("Failed to create snapshot: {}", filename, e);
            throw new RuntimeException("Snapshot creation failed", e);
        }
    }

    /**
     * 创建快照（异步）。
     */
    public CompletableFuture<SnapshotInfo> createSnapshotAsync(IntentStore store, long walOffset) {
        return CompletableFuture.supplyAsync(() -> createSnapshot(store, walOffset));
    }

    /**
     * 从快照恢复。
     */
    public SnapshotResult restoreFromSnapshot() {
        Optional<Path> latestSnapshot = findLatestSnapshot();

        if (latestSnapshot.isEmpty()) {
            logger.info("No snapshot found, starting fresh");
            return SnapshotResult.empty();
        }

        Path snapshotPath = latestSnapshot.get();
        logger.info("Restoring from snapshot: {}", snapshotPath.getFileName());

        try {
            SnapshotData data = readSnapshotData(snapshotPath);
            List<Intent> intents = new ArrayList<>(data.encodedIntents.size());
            for (byte[] encodedIntent : data.encodedIntents) {
                intents.add(IntentBinaryCodec.decode(encodedIntent));
            }

            logger.info("Snapshot restored: intents={}, offset={}",
                data.intentCount, data.walOffset);

            return new SnapshotResult(intents, data.walOffset);

        } catch (IOException e) {
            logger.error("Failed to restore from snapshot: {}", snapshotPath, e);
            throw new RuntimeException("Snapshot restore failed", e);
        }
    }

    /**
     * 流式从快照恢复。
     *
     * @param consumer 每条 intent 的应用回调
     * @return 恢复摘要
     */
    public SnapshotRestoreResult restoreFromSnapshot(Consumer<Intent> consumer) {
        Optional<Path> latestSnapshot = findLatestSnapshot();

        if (latestSnapshot.isEmpty()) {
            logger.info("No snapshot found, starting fresh");
            return SnapshotRestoreResult.empty();
        }

        Path snapshotPath = latestSnapshot.get();
        logger.info("Restoring from snapshot: {}", snapshotPath.getFileName());

        try {
            SnapshotHeader header = readSnapshotHeaderAndApply(snapshotPath, consumer);

            logger.info("Snapshot restored: intents={}, offset={}",
                header.intentCount(), header.walOffset());

            return new SnapshotRestoreResult(header.intentCount(), header.walOffset());
        } catch (IOException e) {
            logger.error("Failed to restore from snapshot: {}", snapshotPath, e);
            throw new RuntimeException("Snapshot restore failed", e);
        }
    }

    /**
     * 获取最新的快照信息。
     */
    public Optional<SnapshotInfo> getLatestSnapshotInfo() {
        return findLatestSnapshot().map(this::readSnapshotInfo);
    }

    private SnapshotData captureSnapshotData(IntentStore store, long walOffset) {
        List<byte[]> encodedIntents = new ArrayList<>();
        for (Intent intent : store.getAllIntents().values()) {
            encodedIntents.add(IntentBinaryCodec.encode(intent));
        }

        return new SnapshotData(encodedIntents, encodedIntents.size(), walOffset, Instant.now());
    }

    private void writeSnapshotData(Path snapshotPath, SnapshotData data) throws IOException {
        try (OutputStream fos = Files.newOutputStream(snapshotPath);
             GZIPOutputStream gzos = new GZIPOutputStream(fos);
             DataOutputStream dos = new DataOutputStream(gzos)) {

            dos.writeInt(SNAPSHOT_MAGIC);
            dos.writeInt(SNAPSHOT_VERSION);
            dos.writeLong(data.createdAt.toEpochMilli());
            dos.writeLong(data.walOffset);
            dos.writeInt(data.intentCount);

            for (byte[] encodedIntent : data.encodedIntents) {
                dos.writeInt(encodedIntent.length);
                dos.write(encodedIntent);
            }
        }
    }

    private SnapshotData readSnapshotData(Path path) throws IOException {
        try (InputStream fis = Files.newInputStream(path);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             DataInputStream dis = new DataInputStream(gzis)) {

            SnapshotHeader header = readSnapshotHeader(dis);
            List<byte[]> encodedIntents = new ArrayList<>(header.intentCount());

            for (int i = 0; i < header.intentCount(); i++) {
                int length = dis.readInt();
                if (length <= 0) {
                    throw new IOException("Invalid snapshot record length: " + length);
                }

                byte[] encodedIntent = new byte[length];
                dis.readFully(encodedIntent);
                encodedIntents.add(encodedIntent);
            }

            return new SnapshotData(
                encodedIntents,
                header.intentCount(),
                header.walOffset(),
                Instant.ofEpochMilli(header.createdAtMillis())
            );
        }
    }

    private SnapshotHeader readSnapshotHeaderAndApply(Path path, Consumer<Intent> consumer) throws IOException {
        try (InputStream fis = Files.newInputStream(path);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             DataInputStream dis = new DataInputStream(gzis)) {

            SnapshotHeader header = readSnapshotHeader(dis);

            for (int i = 0; i < header.intentCount(); i++) {
                int length = dis.readInt();
                if (length <= 0) {
                    throw new IOException("Invalid snapshot record length: " + length);
                }

                byte[] encodedIntent = new byte[length];
                dis.readFully(encodedIntent);
                consumer.accept(IntentBinaryCodec.decode(encodedIntent));
            }

            return header;
        }
    }

    private SnapshotInfo readSnapshotInfo(Path path) {
        try (InputStream fis = Files.newInputStream(path);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             DataInputStream dis = new DataInputStream(gzis)) {

            SnapshotHeader header = readSnapshotHeader(dis);
            long size = Files.size(path);
            String filename = path.getFileName().toString();
            String timestamp = filename
                .replace(SNAPSHOT_PREFIX, "")
                .replace(SNAPSHOT_SUFFIX, "");
            return new SnapshotInfo(filename, header.walOffset(), size, timestamp);
        } catch (IOException e) {
            logger.error("Failed to read snapshot info: {}", path, e);
            return null;
        }
    }

    private SnapshotHeader readSnapshotHeader(DataInputStream dis) throws IOException {
        int magic = dis.readInt();
        if (magic != SNAPSHOT_MAGIC) {
            throw new IOException("Invalid snapshot magic: " + Integer.toHexString(magic));
        }

        int version = dis.readInt();
        if (version != SNAPSHOT_VERSION) {
            throw new IOException("Unsupported snapshot version: " + version);
        }

        long createdAtMillis = dis.readLong();
        long walOffset = dis.readLong();
        int intentCount = dis.readInt();

        return new SnapshotHeader(createdAtMillis, walOffset, intentCount);
    }

    private Optional<Path> findLatestSnapshot() {
        ensureSnapshotDir();

        try (var stream = Files.list(snapshotDir)) {
            return stream
                .filter(p -> p.getFileName().toString().startsWith(SNAPSHOT_PREFIX))
                .filter(p -> p.getFileName().toString().endsWith(SNAPSHOT_SUFFIX))
                .max(Comparator.comparing(this::getLastModifiedTime));
        } catch (IOException e) {
            logger.error("Failed to list snapshots", e);
            return Optional.empty();
        }
    }

    private long getLastModifiedTime(Path path) {
        try {
            return Files.getLastModifiedTime(path).toMillis();
        } catch (IOException e) {
            return 0;
        }
    }

    private void cleanupOldSnapshots() {
        ensureSnapshotDir();

        try (var stream = Files.list(snapshotDir)) {
            List<Path> snapshots = stream
                .filter(p -> p.getFileName().toString().startsWith(SNAPSHOT_PREFIX))
                .filter(p -> p.getFileName().toString().endsWith(SNAPSHOT_SUFFIX))
                .sorted(Comparator.comparing(this::getLastModifiedTime).reversed())
                .toList();

            for (int i = MAX_SNAPSHOTS_TO_KEEP; i < snapshots.size(); i++) {
                Files.deleteIfExists(snapshots.get(i));
                logger.debug("Deleted old snapshot: {}", snapshots.get(i).getFileName());
            }
        } catch (IOException e) {
            logger.warn("Failed to cleanup old snapshots", e);
        }
    }

    private void ensureSnapshotDir() {
        try {
            Files.createDirectories(snapshotDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create snapshot directory: " + snapshotDir, e);
        }
    }

    /**
     * 快照数据。
     */
    public static class SnapshotData {
        public final List<byte[]> encodedIntents;
        public final int intentCount;
        public final long walOffset;
        public final Instant createdAt;

        public SnapshotData(List<byte[]> encodedIntents, int intentCount, long walOffset, Instant createdAt) {
            this.encodedIntents = encodedIntents;
            this.intentCount = intentCount;
            this.walOffset = walOffset;
            this.createdAt = createdAt;
        }
    }

    /**
     * 快照信息。
     */
    public static class SnapshotInfo {
        public final String filename;
        public final long walOffset;
        public final long sizeBytes;
        public final String timestamp;

        public SnapshotInfo(String filename, long walOffset, long sizeBytes, String timestamp) {
            this.filename = filename;
            this.walOffset = walOffset;
            this.sizeBytes = sizeBytes;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("SnapshotInfo{file=%s, offset=%d, size=%d bytes}",
                filename, walOffset, sizeBytes);
        }
    }

    /**
     * 快照恢复结果。
     */
    public static class SnapshotResult {
        public final List<Intent> intents;
        public final long walOffset;

        public SnapshotResult(List<Intent> intents, long walOffset) {
            this.intents = intents != null ? intents : Collections.emptyList();
            this.walOffset = walOffset;
        }

        public static SnapshotResult empty() {
            return new SnapshotResult(Collections.emptyList(), 0);
        }

        public boolean isEmpty() {
            return intents.isEmpty();
        }
    }

    /**
     * 流式恢复摘要。
     */
    public record SnapshotRestoreResult(int restoredCount, long walOffset) {
        public static SnapshotRestoreResult empty() {
            return new SnapshotRestoreResult(0, 0);
        }
    }

    private record SnapshotHeader(long createdAtMillis, long walOffset, int intentCount) {}
}

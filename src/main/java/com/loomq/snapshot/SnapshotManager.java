package com.loomq.snapshot;

import com.loomq.entity.v5.Intent;
import com.loomq.store.IntentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Snapshot 管理器 (v0.5)
 *
 * 负责 Intent 存储的模糊快照（Fuzzy Snapshot）创建和恢复
 * 配合 WAL 实现快速启动和故障恢复
 *
 * @author loomq
 * @since v0.5.0
 */
public class SnapshotManager {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);
    private static final String SNAPSHOT_PREFIX = "snapshot-";
    private static final String SNAPSHOT_SUFFIX = ".snap.gz";
    private static final int MAX_SNAPSHOTS_TO_KEEP = 3;

    private final Path snapshotDir;
    private final ScheduledExecutorService snapshotExecutor;
    private volatile boolean running = false;

    public SnapshotManager(String dataDir) {
        this.snapshotDir = Paths.get(dataDir, "snapshots");
        this.snapshotExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "snapshot-manager");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 启动快照管理器
     */
    public void start() {
        if (running) return;
        running = true;

        try {
            Files.createDirectories(snapshotDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create snapshot directory: " + snapshotDir, e);
        }

        // 每 5 分钟执行一次快照
        snapshotExecutor.scheduleAtFixedRate(
            this::createSnapshotAsync, 5, 5, TimeUnit.MINUTES);

        logger.info("SnapshotManager started, directory: {}", snapshotDir);
    }

    /**
     * 停止快照管理器
     */
    public void stop() {
        running = false;
        snapshotExecutor.shutdown();
        try {
            if (!snapshotExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                snapshotExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            snapshotExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("SnapshotManager stopped");
    }

    /**
     * 创建快照（异步）
     */
    public CompletableFuture<SnapshotInfo> createSnapshotAsync() {
        return CompletableFuture.supplyAsync(this::createSnapshot,
            snapshotExecutor);
    }

    /**
     * 创建快照（同步）
     *
     * @return 快照信息
     */
    public SnapshotInfo createSnapshot() {
        String timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
        String filename = SNAPSHOT_PREFIX + timestamp + SNAPSHOT_SUFFIX;
        Path snapshotPath = snapshotDir.resolve(filename);

        logger.info("Creating snapshot: {}", filename);

        try {
            // 注意：这里创建的是"模糊快照"，不锁定 IntentStore
            // 实际数据在恢复时通过 WAL replay 补齐
            SnapshotData data = captureSnapshotData();

            try (OutputStream fos = Files.newOutputStream(snapshotPath);
                 GZIPOutputStream gzos = new GZIPOutputStream(fos);
                 ObjectOutputStream oos = new ObjectOutputStream(gzos)) {

                oos.writeObject(data);
            }

            long size = Files.size(snapshotPath);
            SnapshotInfo info = new SnapshotInfo(filename, data.walOffset, size, timestamp);

            logger.info("Snapshot created: {}, size={}, intents={}, offset={}",
                filename, size, data.intentCount, data.walOffset);

            // 清理旧快照
            cleanupOldSnapshots();

            return info;

        } catch (IOException e) {
            logger.error("Failed to create snapshot: {}", filename, e);
            throw new RuntimeException("Snapshot creation failed", e);
        }
    }

    /**
     * 从快照恢复
     *
     * @return 恢复的快照数据和 WAL offset
     */
    public SnapshotResult restoreFromSnapshot() {
        Optional<Path> latestSnapshot = findLatestSnapshot();

        if (latestSnapshot.isEmpty()) {
            logger.info("No snapshot found, starting fresh");
            return SnapshotResult.empty();
        }

        Path snapshotPath = latestSnapshot.get();
        logger.info("Restoring from snapshot: {}", snapshotPath.getFileName());

        try (InputStream fis = Files.newInputStream(snapshotPath);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             ObjectInputStream ois = new ObjectInputStream(gzis)) {

            SnapshotData data = (SnapshotData) ois.readObject();

            logger.info("Snapshot restored: intents={}, offset={}",
                data.intentCount, data.walOffset);

            return new SnapshotResult(data.intents, data.walOffset);

        } catch (IOException | ClassNotFoundException e) {
            logger.error("Failed to restore from snapshot: {}", snapshotPath, e);
            throw new RuntimeException("Snapshot restore failed", e);
        }
    }

    /**
     * 获取最新的快照信息
     */
    public Optional<SnapshotInfo> getLatestSnapshotInfo() {
        return findLatestSnapshot()
            .map(this::readSnapshotInfo)
            .flatMap(Optional::ofNullable);
    }

    /**
     * 捕获快照数据
     */
    private SnapshotData captureSnapshotData() {
        // 获取当前 WAL offset（这里简化处理，实际需要与 WAL 集成）
        long currentWalOffset = System.currentTimeMillis();

        return new SnapshotData(
            Collections.emptyList(),  // 模糊快照不保存实际数据
            0,
            currentWalOffset,
            Instant.now()
        );
    }

    /**
     * 从 IntentStore 捕获快照（如果需要完整快照）
     */
    public SnapshotData captureFromStore(IntentStore store) {
        List<Intent> intents = new ArrayList<>(store.getAllIntents().values());
        long currentWalOffset = System.currentTimeMillis();

        return new SnapshotData(intents, intents.size(), currentWalOffset, Instant.now());
    }

    /**
     * 查找最新的快照文件
     */
    private Optional<Path> findLatestSnapshot() {
        try {
            return Files.list(snapshotDir)
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

    /**
     * 读取快照信息（不读取完整数据）
     */
    private SnapshotInfo readSnapshotInfo(Path path) {
        try {
            long size = Files.size(path);
            String filename = path.getFileName().toString();
            String timestamp = filename
                .replace(SNAPSHOT_PREFIX, "")
                .replace(SNAPSHOT_SUFFIX, "");
            return new SnapshotInfo(filename, 0, size, timestamp);
        } catch (IOException e) {
            logger.error("Failed to read snapshot info: {}", path, e);
            return null;
        }
    }

    /**
     * 清理旧快照，只保留最新的几个
     */
    private void cleanupOldSnapshots() {
        try {
            List<Path> snapshots = Files.list(snapshotDir)
                .filter(p -> p.getFileName().toString().startsWith(SNAPSHOT_PREFIX))
                .sorted(Comparator.comparing(this::getLastModifiedTime).reversed())
                .toList();

            if (snapshots.size() > MAX_SNAPSHOTS_TO_KEEP) {
                for (int i = MAX_SNAPSHOTS_TO_KEEP; i < snapshots.size(); i++) {
                    Files.deleteIfExists(snapshots.get(i));
                    logger.debug("Deleted old snapshot: {}", snapshots.get(i).getFileName());
                }
            }
        } catch (IOException e) {
            logger.warn("Failed to cleanup old snapshots", e);
        }
    }

    // ==================== 数据类 ====================

    /**
     * 快照数据（可序列化）
     */
    public static class SnapshotData implements Serializable {
        private static final long serialVersionUID = 1L;

        public final List<Intent> intents;
        public final int intentCount;
        public final long walOffset;
        public final Instant createdAt;

        public SnapshotData(List<Intent> intents, int intentCount,
                           long walOffset, Instant createdAt) {
            this.intents = intents;
            this.intentCount = intentCount;
            this.walOffset = walOffset;
            this.createdAt = createdAt;
        }
    }

    /**
     * 快照信息
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
     * 快照恢复结果
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
}

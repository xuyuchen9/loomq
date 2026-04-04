package com.loomq.wal.v2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Checkpoint 管理器 (V0.3+ 持久化恢复)
 *
 * 设计目标 (设计文档 §4.3)：
 * 1. 每 10 万条记录生成一次 checkpoint
 * 2. 记录 lastApplied 位置
 * 3. 支持快速恢复（从 checkpoint 位置开始）
 *
 * Checkpoint 文件格式：
 * - 文件名：checkpoint.meta
 * - 二进制格式，便于快速读写
 *
 * @author loomq
 * @since v0.3+
 */
public class CheckpointManager {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointManager.class);

    // Checkpoint 文件名
    public static final String CHECKPOINT_FILE = "checkpoint.meta";

    // 文件魔数
    private static final int MAGIC = 0x4C4D5143;  // "LMQC" (LoomQ Checkpoint)

    // 文件版本
    private static final short VERSION = 1;

    // 触发 checkpoint 的记录数阈值（设计文档要求：10 万条）
    public static final long DEFAULT_CHECKPOINT_INTERVAL = 100_000;

    // 文件偏移量
    private static final int OFFSET_MAGIC = 0;
    private static final int OFFSET_VERSION = 4;
    private static final int OFFSET_TIMESTAMP = 6;
    private static final int OFFSET_LAST_SEQ = 14;
    private static final int OFFSET_SEGMENT_SEQ = 22;
    private static final int OFFSET_SEGMENT_POS = 26;
    private static final int OFFSET_TASK_COUNT = 34;
    private static final int HEADER_SIZE = 42;

    // 数据目录
    private final Path dataDir;

    // Checkpoint 文件路径
    private final Path checkpointFile;

    // Checkpoint 间隔
    private final long checkpointInterval;

    // 上次 checkpoint 后的记录数
    private final AtomicLong recordsSinceLastCheckpoint = new AtomicLong(0);

    // 读写锁
    private final ReentrantLock lock = new ReentrantLock();

    // 当前 checkpoint 状态
    private volatile Checkpoint currentCheckpoint;

    /**
     * Checkpoint 数据结构
     */
    public record Checkpoint(
            long timestamp,          // Checkpoint 时间戳
            long lastRecordSeq,      // 最后应用的记录序号
            int segmentSeq,          // 当前段序号
            long segmentPosition,    // 当前段文件位置
            long taskCount           // 活跃任务数
    ) {
        public boolean isValid() {
            return timestamp > 0;
        }
    }

    /**
     * 创建 Checkpoint 管理器
     *
     * @param dataDir 数据目录
     */
    public CheckpointManager(Path dataDir) {
        this(dataDir, DEFAULT_CHECKPOINT_INTERVAL);
    }

    /**
     * 创建 Checkpoint 管理器
     *
     * @param dataDir 数据目录
     * @param checkpointInterval Checkpoint 间隔（记录数）
     */
    public CheckpointManager(Path dataDir, long checkpointInterval) {
        this.dataDir = dataDir;
        this.checkpointFile = dataDir.resolve(CHECKPOINT_FILE);
        this.checkpointInterval = checkpointInterval;

        // 加载现有 checkpoint
        this.currentCheckpoint = load();

        logger.info("CheckpointManager created, file={}, interval={}, lastCheckpoint={}",
                checkpointFile, checkpointInterval,
                currentCheckpoint != null ? "seq=" + currentCheckpoint.lastRecordSeq() : "none");
    }

    /**
     * 记录写入事件（用于触发 checkpoint）
     *
     * @return true 如果应该执行 checkpoint
     */
    public boolean recordWrite() {
        long count = recordsSinceLastCheckpoint.incrementAndGet();
        return count >= checkpointInterval;
    }

    /**
     * 执行 checkpoint
     *
     * @param lastRecordSeq 最后记录序号
     * @param segmentSeq 当前段序号
     * @param segmentPosition 当前段位置
     * @param taskCount 活跃任务数
     */
    public void checkpoint(long lastRecordSeq, int segmentSeq, long segmentPosition, long taskCount) {
        Checkpoint cp = new Checkpoint(
                System.currentTimeMillis(),
                lastRecordSeq,
                segmentSeq,
                segmentPosition,
                taskCount
        );

        save(cp);
        this.currentCheckpoint = cp;
        recordsSinceLastCheckpoint.set(0);

        logger.info("Checkpoint saved: seq={}, segment={}, pos={}, tasks={}",
                lastRecordSeq, segmentSeq, segmentPosition, taskCount);
    }

    /**
     * 保存 checkpoint 到文件
     */
    private void save(Checkpoint cp) {
        lock.lock();
        try {
            // 确保目录存在
            if (!Files.exists(dataDir)) {
                Files.createDirectories(dataDir);
            }

            // 写入临时文件（原子性保证）
            Path tempFile = dataDir.resolve(CHECKPOINT_FILE + ".tmp");

            try (FileChannel channel = FileChannel.open(tempFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {

                ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);

                // Header
                buffer.putInt(MAGIC);
                buffer.putShort(VERSION);
                buffer.putLong(cp.timestamp());
                buffer.putLong(cp.lastRecordSeq());
                buffer.putInt(cp.segmentSeq());
                buffer.putLong(cp.segmentPosition());
                buffer.putLong(cp.taskCount());

                buffer.flip();
                channel.write(buffer);
                channel.force(true);
            }

            // 原子性重命名
            Files.move(tempFile, checkpointFile,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);

        } catch (IOException e) {
            logger.error("Failed to save checkpoint", e);
            throw new RuntimeException("Checkpoint save failed", e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 加载 checkpoint
     */
    private Checkpoint load() {
        if (!Files.exists(checkpointFile)) {
            logger.debug("No checkpoint file found: {}", checkpointFile);
            return null;
        }

        lock.lock();
        try (FileChannel channel = FileChannel.open(checkpointFile, StandardOpenOption.READ)) {

            if (channel.size() < HEADER_SIZE) {
                logger.warn("Checkpoint file too small: {}", channel.size());
                return null;
            }

            ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
            channel.read(buffer);
            buffer.flip();

            // 验证魔数
            int magic = buffer.getInt();
            if (magic != MAGIC) {
                logger.warn("Invalid checkpoint magic: {}", magic);
                return null;
            }

            // 验证版本
            short version = buffer.getShort();
            if (version != VERSION) {
                logger.warn("Unsupported checkpoint version: {}", version);
                return null;
            }

            long timestamp = buffer.getLong();
            long lastRecordSeq = buffer.getLong();
            int segmentSeq = buffer.getInt();
            long segmentPosition = buffer.getLong();
            long taskCount = buffer.getLong();

            Checkpoint cp = new Checkpoint(timestamp, lastRecordSeq, segmentSeq, segmentPosition, taskCount);

            logger.info("Checkpoint loaded: {}", cp);
            return cp;

        } catch (IOException e) {
            logger.error("Failed to load checkpoint", e);
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取当前 checkpoint
     */
    public Checkpoint getCurrentCheckpoint() {
        return currentCheckpoint;
    }

    /**
     * 检查是否存在有效的 checkpoint
     */
    public boolean hasValidCheckpoint() {
        return currentCheckpoint != null && currentCheckpoint.isValid();
    }

    /**
     * 获取恢复起始位置
     *
     * @return 恢复起始点（segmentSeq, position），如果没有 checkpoint 返回 (0, 0)
     */
    public RecoveryPosition getRecoveryStartPosition() {
        if (currentCheckpoint == null) {
            return new RecoveryPosition(0, 0, 0);
        }
        return new RecoveryPosition(
                currentCheckpoint.segmentSeq(),
                currentCheckpoint.segmentPosition(),
                currentCheckpoint.lastRecordSeq()
        );
    }

    /**
     * 清除 checkpoint（用于完整恢复或测试）
     */
    public void clear() {
        lock.lock();
        try {
            if (Files.exists(checkpointFile)) {
                Files.delete(checkpointFile);
            }
            currentCheckpoint = null;
            recordsSinceLastCheckpoint.set(0);
            logger.info("Checkpoint cleared");
        } catch (IOException e) {
            logger.error("Failed to clear checkpoint", e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 恢复起始位置
     */
    public record RecoveryPosition(
            int segmentSeq,       // 起始段序号
            long segmentPosition, // 段内偏移
            long lastRecordSeq    // 已应用的最后记录序号
    ) {}
}

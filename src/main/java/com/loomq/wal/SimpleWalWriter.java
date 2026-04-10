package com.loomq.wal;

import com.loomq.config.WalConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.CRC32;

/**
 * 极简 WAL 写入器 - 第一性原理实现
 *
 * 格式：| Length (4B) | Payload (N) | CRC32 (4B) |
 * 总计：8 字节固定头部开销
 *
 * 核心设计：
 * 1. 不做字段解析，只做字节搬运
 * 2. 批量刷盘摊平 fsync 开销
 * 3. MemorySegment 零拷贝写入
 * 4. 分段条件变量精确唤醒
 *
 * @author loomq
 * @since v0.6.1
 */
public class SimpleWalWriter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(SimpleWalWriter.class);

    // ========== 格式常量 ==========
    private static final int HEADER_SIZE = 4;  // Length field
    private static final int CHECKSUM_SIZE = 4; // CRC32
    private static final int RECORD_OVERHEAD = HEADER_SIZE + CHECKSUM_SIZE;

    // ========== 默认配置 ==========
    private static final long DEFAULT_INITIAL_SIZE = 64 * 1024 * 1024;  // 64MB
    private static final long DEFAULT_MAX_SIZE = 1024 * 1024 * 1024;    // 1GB
    private static final long DEFAULT_FLUSH_THRESHOLD = 64 * 1024;      // 64KB
    private static final long DEFAULT_FLUSH_INTERVAL_MS = 10;

    // ========== 文件管理 ==========
    private final Path dataDir;
    private final Path walPath;
    private final long initialSize;
    private final long maxSize;
    private final Arena arena;
    private FileChannel fileChannel;
    private MemorySegment mappedRegion;
    private volatile long mappedSize;
    private int segmentSeq = 0;

    // ========== 写入状态 ==========
    private final AtomicLong writePosition = new AtomicLong(0);
    private volatile long flushedPosition = 0;

    // ========== 刷盘协调 ==========
    private final StripedCondition flushConditions;
    private final Thread flushThread;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final long flushThreshold;
    private final long flushIntervalNs;

    // ========== 统计 ==========
    private final Stats stats = new Stats();

    public static class Stats {
        private final AtomicLong writeCount = new AtomicLong(0);
        private final AtomicLong flushCount = new AtomicLong(0);
        private final AtomicLong totalFlushTimeNs = new AtomicLong(0);
        private final AtomicLong totalBytesWritten = new AtomicLong(0);

        public void recordWrite(int bytes) {
            writeCount.incrementAndGet();
            totalBytesWritten.addAndGet(bytes);
        }

        public void recordFlush(long elapsedNs) {
            flushCount.incrementAndGet();
            totalFlushTimeNs.addAndGet(elapsedNs);
        }

        public long getWriteCount() { return writeCount.get(); }
        public long getFlushCount() { return flushCount.get(); }
        public long getTotalBytesWritten() { return totalBytesWritten.get(); }
        public double getAvgFlushTimeMs() {
            long flushes = flushCount.get();
            return flushes > 0 ? (totalFlushTimeNs.get() / (double) flushes) / 1_000_000.0 : 0;
        }
    }

    public SimpleWalWriter(WalConfig config, String shardId) throws IOException {
        this.dataDir = Path.of(config.dataDir(), shardId);
        this.initialSize = config.memorySegmentInitialSizeMb() * 1024L * 1024L;
        this.maxSize = config.memorySegmentMaxSizeMb() * 1024L * 1024L;
        this.flushThreshold = config.memorySegmentFlushThresholdKb() * 1024L;
        this.flushIntervalNs = config.memorySegmentFlushIntervalMs() * 1_000_000L;

        // 初始化分段条件变量
        this.flushConditions = new StripedCondition(config.memorySegmentStripeCount());

        // 创建目录
        Files.createDirectories(dataDir);

        // 初始化 Arena
        this.arena = Arena.ofShared();

        // 创建 WAL 文件路径（简化：单段文件）
        this.walPath = dataDir.resolve("wal.bin");

        // 打开 WAL 文件
        openWalFile();

        // 创建刷盘线程（延迟启动）
        this.flushThread = Thread.ofPlatform()
            .name("wal-flusher-" + shardId)
            .daemon(true)
            .unstarted(this::flushLoop);

        logger.info("SimpleWalWriter created: path={}, initialSize={}MB, flushThreshold={}KB",
            walPath, initialSize / 1024 / 1024, flushThreshold / 1024);
    }

    private void openWalFile() throws IOException {
        // 创建文件
        this.fileChannel = FileChannel.open(walPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE);

        // 预分配空间
        fileChannel.truncate(initialSize);

        // 内存映射
        this.mappedRegion = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, initialSize, arena);
        this.mappedSize = initialSize;
        this.writePosition.set(0);
        this.flushedPosition = 0;

        logger.info("Opened WAL file: {}", walPath);
    }

    /**
     * 启动写入器
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            flushThread.start();
            logger.info("SimpleWalWriter started");
        }
    }

    /**
     * 异步写入 - 不等待刷盘
     */
    public CompletableFuture<Long> writeAsync(byte[] payload) {
        long position = writeInternal(payload);
        return CompletableFuture.completedFuture(position);
    }

    /**
     * DURABLE 写入 - 等待刷盘完成
     */
    public CompletableFuture<Long> writeDurable(byte[] payload) {
        long position = writeInternal(payload);
        awaitFlush(position);
        return CompletableFuture.completedFuture(position);
    }

    /**
     * 内部写入方法
     */
    private long writeInternal(byte[] payload) {
        if (closed.get()) {
            throw new IllegalStateException("Writer is closed");
        }

        int payloadLen = payload.length;
        int recordSize = RECORD_OVERHEAD + payloadLen;

        // 分配空间
        long startPos = writePosition.getAndAdd(recordSize);
        long endPos = startPos + recordSize;

        // 确保容量
        ensureCapacity(endPos);

        // 写入记录
        MemorySegment region = mappedRegion;

        // 使用 1 字节对齐避免内存对齐问题（JDK 22+）
        var INT_UNALIGNED = java.lang.foreign.ValueLayout.JAVA_INT.withByteAlignment(1);

        // Length (大端序)
        region.set(INT_UNALIGNED, startPos, payloadLen);

        // Payload
        MemorySegment payloadSegment = MemorySegment.ofArray(payload);
        MemorySegment.copy(payloadSegment, 0, region, startPos + HEADER_SIZE, payloadLen);

        // CRC32
        int crc = calculateCrc(payload);
        region.set(INT_UNALIGNED, startPos + HEADER_SIZE + payloadLen, crc);

        stats.recordWrite(recordSize);

        return startPos;
    }

    /**
     * 计算 CRC32
     */
    private int calculateCrc(byte[] data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return (int) crc32.getValue();
    }

    /**
     * 等待指定位置刷盘完成
     *
     * 使用 StripedCondition 分段等待 + 超时兜底，虚拟线程友好
     */
    private void awaitFlush(long position) {
        if (flushedPosition >= position) {
            return;
        }

        // 唤醒刷盘线程
        LockSupport.unpark(flushThread);

        // 慢速路径：使用 StripedCondition 分段等待，带超时保护
        // 如果 StripedCondition 信号丢失，超时机制确保不会无限等待
        long deadline = System.nanoTime() + 5_000_000_000L; // 5秒超时
        while (flushedPosition < position) {
            if (System.nanoTime() > deadline) {
                throw new RuntimeException("Timeout waiting for flush, position=" + position + ", flushed=" + flushedPosition);
            }

            try {
                // 尝试使用 StripedCondition 等待（更高效）
                // 但带超时，防止信号丢失
                long remainingNs = deadline - System.nanoTime();
                if (remainingNs <= 0) break;

                // 使用带超时的等待
                boolean signaled = flushConditions.awaitNanos(position, () -> flushedPosition, Math.min(remainingNs, 100_000_000L));
                if (!signaled) {
                    // 超时未收到信号，再次唤醒刷盘线程
                    LockSupport.unpark(flushThread);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for flush", e);
            }
        }
    }

    /**
     * 确保容量足够
     */
    private void ensureCapacity(long required) {
        if (required <= mappedSize) {
            return;
        }

        if (required > maxSize) {
            throw new IllegalStateException("WAL segment exceeds max size: " + required);
        }

        synchronized (this) {
            if (required <= mappedSize) {
                return;
            }

            try {
                expandMapping(required);
            } catch (IOException e) {
                throw new RuntimeException("Failed to expand WAL", e);
            }
        }
    }

    /**
     * 扩展映射
     */
    private void expandMapping(long required) throws IOException {
        long newSize = Math.min(Math.max(mappedSize * 2, required), maxSize);

        logger.info("Expanding WAL from {}MB to {}MB", mappedSize / 1024 / 1024, newSize / 1024 / 1024);

        // 先刷盘
        mappedRegion.force();

        // 扩展文件
        fileChannel.truncate(newSize);

        // 重新映射
        MemorySegment newRegion = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, newSize, arena);
        mappedRegion = newRegion;
        mappedSize = newSize;
    }

    /**
     * 刷盘循环
     *
     * 批量刷盘策略：
     * 1. 待刷数据 >= threshold：立即刷盘
     * 2. 待刷数据 > 0 且有等待者：立即刷盘（DURABLE 请求）
     * 3. 待刷数据 > 0 但无等待者：短暂休眠后重试
     * 4. 无数据：休眠等待
     *
     * 注意：park() 和 unpark() 存在竞态条件，unpark() 的信号可能在 park() 之前被消费。
     * 解决方案：在 park() 前再次检查等待者，避免信号丢失导致死锁。
     */
    private void flushLoop() {
        logger.info("Flush loop started, threshold={}KB, interval={}ms",
            flushThreshold / 1024, flushIntervalNs / 1_000_000);

        while (running.get()) {
            try {
                long currentWritePos = writePosition.get();
                long currentFlushPos = flushedPosition;
                long pendingBytes = currentWritePos - currentFlushPos;

                // 刷盘条件：
                // 1. 待刷数据达到阈值
                // 2. 或有待刷数据且有等待者（DURABLE 请求）
                boolean hasWaiters = flushConditions.getWaiterCount() > 0;
                boolean shouldFlush = pendingBytes >= flushThreshold ||
                                      (pendingBytes > 0 && hasWaiters);

                if (shouldFlush) {
                    // 执行刷盘
                    long startNs = System.nanoTime();
                    mappedRegion.force();
                    long elapsedNs = System.nanoTime() - startNs;

                    // 更新刷盘位置
                    flushedPosition = currentWritePos;

                    stats.recordFlush(elapsedNs);

                    // 唤醒等待者
                    flushConditions.signalRange(currentFlushPos, currentWritePos);

                    logger.debug("Flushed {} bytes in {} µs", pendingBytes, elapsedNs / 1000);
                } else if (pendingBytes > 0) {
                    // 有数据但未达阈值，检查是否有等待者出现
                    // 注意：这里再次检查避免 park/unpark 竞态条件
                    if (flushConditions.getWaiterCount() > 0) {
                        continue;  // 有等待者，立即重新检查
                    }
                    LockSupport.parkNanos(flushIntervalNs);
                } else {
                    // 无数据，短暂休眠
                    LockSupport.parkNanos(flushIntervalNs);
                }

            } catch (Exception e) {
                logger.error("Flush loop error", e);
                LockSupport.parkNanos(10_000_000L); // 10ms
            }
        }

        logger.info("Flush loop ended");
    }

    // ========== 状态查询 ==========

    public long getWritePosition() {
        return writePosition.get();
    }

    public long getFlushedPosition() {
        return flushedPosition;
    }

    public Stats getStats() {
        return stats;
    }

    // ========== 关闭 ==========

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        logger.info("Closing SimpleWalWriter...");

        running.set(false);
        flushConditions.signalAllStripes();
        flushThread.interrupt();

        try {
            flushThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 最终刷盘
        try {
            if (mappedRegion != null) {
                mappedRegion.force();
            }
        } catch (Exception e) {
            logger.error("Final flush failed", e);
        }

        // 关闭资源
        try {
            if (fileChannel != null && fileChannel.isOpen()) {
                fileChannel.close();
            }
        } catch (IOException e) {
            logger.error("Close file channel failed", e);
        }

        arena.close();

        logger.info("SimpleWalWriter closed, writes={}, flushes={}",
            stats.getWriteCount(), stats.getFlushCount());
    }
}

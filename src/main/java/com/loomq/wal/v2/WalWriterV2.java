package com.loomq.wal.v2;

import com.loomq.config.WalConfig;
import com.loomq.entity.EventType;
import com.loomq.wal.WalRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * WAL 写入器 V2 - 高吞吐实现
 *
 * 核心特性：
 * 1. RingBuffer 缓冲写入（无锁）
 * 2. Group Commit 批量刷盘
 * 3. 独立刷盘线程
 * 4. 写入确认机制
 *
 * 性能目标：
 * - 单线程写入：> 5万 QPS
 * - 批量刷盘延迟：P99 < 20ms
 *
 * @author loomq
 * @since v0.3
 */
public class WalWriterV2 implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(WalWriterV2.class);

    // RingBuffer 容量
    private static final int RING_BUFFER_SIZE = 65536;  // 64K

    // 默认批量大小
    private static final int DEFAULT_BATCH_SIZE = 1000;

    // 最大等待时间（ms）
    private static final int MAX_FLUSH_INTERVAL_MS = 100;

    // 配置
    private final WalConfig config;

    // 数据目录
    private final Path dataDir;

    // 当前段文件通道
    private FileChannel currentChannel;

    // 当前段文件路径
    private Path currentSegmentPath;

    // 段序号
    private int segmentSeq = 0;

    // 记录序号
    private final AtomicLong recordSeq = new AtomicLong(0);

    // RingBuffer（生产者写入）
    private final RingBuffer<WriteRequest> ringBuffer;

    // 刷盘线程
    private final ExecutorService flushExecutor;

    // 运行标志
    private final AtomicBoolean running = new AtomicBoolean(false);

    // 关闭标志
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // 统计
    private final Stats stats = new Stats();

    // 刷盘批次大小
    private final int batchSize;

    // 写入超时时间
    private final long writeTimeoutMs;

    // Checkpoint 管理器
    private CheckpointManager checkpointManager;

    // 活跃任务数（用于 checkpoint）
    private final AtomicLong activeTaskCount = new AtomicLong(0);

    /**
     * 写入请求
     */
    private static class WriteRequest {
        final long sequence;
        final String taskId;
        final String bizKey;
        final EventType eventType;
        final long eventTime;
        final byte[] payload;
        final CompletableFuture<Long> future;

        WriteRequest(long sequence, String taskId, String bizKey,
                     EventType eventType, long eventTime, byte[] payload) {
            this.sequence = sequence;
            this.taskId = taskId;
            this.bizKey = bizKey;
            this.eventType = eventType;
            this.eventTime = eventTime;
            this.payload = payload;
            this.future = new CompletableFuture<>();
        }
    }

    /**
     * 创建 WalWriterV2
     *
     * @param config WAL 配置
     * @param shardId 分片 ID（用于目录隔离）
     * @throws IOException 如果初始化失败
     */
    public WalWriterV2(WalConfig config, String shardId) throws IOException {
        this.config = config;
        this.dataDir = Path.of(config.dataDir(), shardId);
        this.ringBuffer = new RingBuffer<>(RING_BUFFER_SIZE);
        this.batchSize = DEFAULT_BATCH_SIZE;
        this.writeTimeoutMs = 5000;

        // 创建数据目录
        Files.createDirectories(dataDir);

        // 初始化段文件
        initializeSegment();

        // 创建刷盘线程
        this.flushExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "wal-flush-" + shardId);
            t.setDaemon(true);
            return t;
        });

        logger.info("WalWriterV2 created for shard: {}, dataDir: {}", shardId, dataDir);
    }

    /**
     * 初始化段文件
     */
    private void initializeSegment() throws IOException {
        // 查找现有段
        List<Path> existingSegments = new ArrayList<>();
        if (Files.exists(dataDir)) {
            Files.list(dataDir)
                    .filter(p -> p.toString().endsWith(".wal"))
                    .sorted()
                    .forEach(existingSegments::add);
        }

        if (existingSegments.isEmpty()) {
            // 创建新段
            createNewSegment();
        } else {
            // 使用最后一个段
            currentSegmentPath = existingSegments.get(existingSegments.size() - 1);
            segmentSeq = extractSegmentSeq(currentSegmentPath.getFileName().toString());
            currentChannel = FileChannel.open(
                    currentSegmentPath,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND
            );
            logger.info("Opened existing segment: {}", currentSegmentPath);
        }
    }

    /**
     * 创建新段
     */
    private void createNewSegment() throws IOException {
        String fileName = String.format("%08d.wal", segmentSeq);
        currentSegmentPath = dataDir.resolve(fileName);
        currentChannel = FileChannel.open(
                currentSegmentPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE
        );
        logger.info("Created new segment: {}", currentSegmentPath);
    }

    /**
     * 提取段序号
     */
    private int extractSegmentSeq(String fileName) {
        try {
            return Integer.parseInt(fileName.substring(0, 8));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /**
     * 启动刷盘线程
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        flushExecutor.submit(this::flushLoop);
        logger.info("WalWriterV2 started");
    }

    /**
     * 停止刷盘线程
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        logger.info("Stopping WalWriterV2...");

        // 最后一次刷盘
        flushAll();

        // 关闭刷盘线程
        flushExecutor.shutdown();
        try {
            if (!flushExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                flushExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            flushExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("WalWriterV2 stopped");
    }

    /**
     * 刷盘循环
     */
    private void flushLoop() {
        while (running.get() || !ringBuffer.isEmpty()) {
            try {
                int flushed = flushBatch();

                if (flushed == 0) {
                    // 没有数据，短暂休眠
                    Thread.sleep(1);
                }
            } catch (Exception e) {
                logger.error("Flush loop error", e);
            }
        }
    }

    /**
     * 批量刷盘
     *
     * @return 刷盘的记录数
     */
    private int flushBatch() {
        List<WriteRequest> batch = ringBuffer.drainToList(batchSize);

        if (batch.isEmpty()) {
            return 0;
        }

        long startTime = System.nanoTime();

        try {
            // 批量编码
            ByteBuffer buffer = encodeBatch(batch);

            // 写入文件
            while (buffer.hasRemaining()) {
                currentChannel.write(buffer);
            }

            // 刷盘
            boolean force = config.syncOnWrite() || "per_record".equals(config.flushStrategy());
            if (force) {
                currentChannel.force(false);
            }

            // 完成 Future
            long position = currentChannel.position();
            for (WriteRequest request : batch) {
                request.future.complete(position);
            }

            // 统计
            long elapsedNs = System.nanoTime() - startTime;
            stats.recordFlush(batch.size(), elapsedNs);

            // 检查是否需要 checkpoint
            if (checkpointManager != null && checkpointManager.recordWrite()) {
                triggerCheckpoint(position);
            }

            return batch.size();

        } catch (IOException e) {
            // 失败，通知所有请求
            for (WriteRequest request : batch) {
                request.future.completeExceptionally(e);
            }
            logger.error("Flush batch failed", e);
            return batch.size();
        }
    }

    /**
     * 触发 checkpoint
     */
    private void triggerCheckpoint(long position) {
        try {
            checkpointManager.checkpoint(
                    recordSeq.get(),
                    segmentSeq,
                    position,
                    activeTaskCount.get()
            );
        } catch (Exception e) {
            logger.error("Checkpoint failed", e);
        }
    }

    /**
     * 设置 Checkpoint 管理器
     */
    public void setCheckpointManager(CheckpointManager checkpointManager) {
        this.checkpointManager = checkpointManager;
    }

    /**
     * 更新活跃任务数（用于 checkpoint）
     */
    public void setActiveTaskCount(long count) {
        this.activeTaskCount.set(count);
    }

    /**
     * 编码批量数据
     */
    private ByteBuffer encodeBatch(List<WriteRequest> batch) {
        // 计算总大小
        int totalSize = 0;
        List<byte[]> encodedRecords = new ArrayList<>(batch.size());

        for (WriteRequest request : batch) {
            WalRecord record = WalRecord.create(
                    segmentSeq,
                    request.sequence,
                    request.taskId,
                    request.bizKey,
                    request.eventType,
                    request.eventTime,
                    request.payload
            );
            byte[] data = record.encode();
            encodedRecords.add(data);
            totalSize += data.length;
        }

        // 合并到一个 ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        for (byte[] data : encodedRecords) {
            buffer.put(data);
        }
        buffer.flip();

        return buffer;
    }

    /**
     * 刷空缓冲区
     */
    private void flushAll() {
        while (!ringBuffer.isEmpty()) {
            flushBatch();
        }

        // 强制刷盘
        try {
            if (currentChannel != null && currentChannel.isOpen()) {
                currentChannel.force(true);
            }
        } catch (IOException e) {
            logger.error("Final flush failed", e);
        }
    }

    /**
     * 写入记录（异步）
     *
     * @return CompletableFuture，完成后返回文件位置
     */
    public CompletableFuture<Long> appendAsync(String taskId, String bizKey,
                                                EventType eventType, long eventTime,
                                                byte[] payload) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Writer is closed"));
        }

        long seq = recordSeq.incrementAndGet();
        WriteRequest request = new WriteRequest(
                seq, taskId, bizKey, eventType, eventTime, payload
        );

        // 写入 RingBuffer
        if (!ringBuffer.offer(request)) {
            stats.recordOverflow();
            return CompletableFuture.failedFuture(
                    new IllegalStateException("RingBuffer is full"));
        }

        stats.recordWrite();
        return request.future;
    }

    /**
     * 写入记录（同步）
     *
     * @return 文件位置
     * @throws IOException 如果写入失败
     */
    public long append(String taskId, String bizKey,
                       EventType eventType, long eventTime,
                       byte[] payload) throws IOException {
        try {
            return appendAsync(taskId, bizKey, eventType, eventTime, payload)
                    .get(writeTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Write interrupted", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new IOException("Write failed", e.getCause());
        } catch (TimeoutException e) {
            throw new IOException("Write timeout", e);
        }
    }

    /**
     * 切换段文件
     */
    public void rollover() throws IOException {
        flushAll();

        // 关闭当前段
        currentChannel.close();

        // 创建新段
        segmentSeq++;
        createNewSegment();

        logger.info("Rolled over to new segment: {}", currentSegmentPath);
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        stop();

        // 关闭通道
        if (currentChannel != null && currentChannel.isOpen()) {
            currentChannel.close();
        }

        logger.info("WalWriterV2 closed");
    }

    // Getters
    public Path getDataDir() {
        return dataDir;
    }

    public long getRecordCount() {
        return recordSeq.get();
    }

    public int getPendingCount() {
        return ringBuffer.size();
    }

    public Stats getStats() {
        return stats;
    }

    /**
     * 统计信息
     */
    public static class Stats {
        private final LongAdder writeCount = new LongAdder();
        private final LongAdder flushCount = new LongAdder();
        private final LongAdder overflowCount = new LongAdder();
        private final LongAdder totalFlushTimeNs = new LongAdder();

        void recordWrite() {
            writeCount.increment();
        }

        void recordFlush(int count, long elapsedNs) {
            flushCount.add(count);
            totalFlushTimeNs.add(elapsedNs);
        }

        void recordOverflow() {
            overflowCount.increment();
        }

        public long getWriteCount() {
            return writeCount.sum();
        }

        public long getFlushCount() {
            return flushCount.sum();
        }

        public long getOverflowCount() {
            return overflowCount.sum();
        }

        public double getAverageFlushTimeMs() {
            long flushes = flushCount.sum();
            return flushes > 0 ? (totalFlushTimeNs.sum() / flushes) / 1_000_000.0 : 0;
        }

        @Override
        public String toString() {
            return String.format("Stats{writes=%d, flushes=%d, overflows=%d, avgFlushTime=%.2fms}",
                    getWriteCount(), getFlushCount(), getOverflowCount(), getAverageFlushTimeMs());
        }
    }
}

package com.loomq.wal;

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
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
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
public class WalWriter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(WalWriter.class);

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

    // ========== 健康监控 ==========

    // 最后一次刷盘时间
    private volatile long lastFlushTime = System.currentTimeMillis();

    // 刷盘异常计数
    private final LongAdder flushErrorCount = new LongAdder();

    // 等待刷盘确认的 Future 映射
    private final ConcurrentNavigableMap<Long, List<CompletableFuture<Long>>> pendingFlushMap =
        new ConcurrentSkipListMap<>();

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
    public WalWriter(WalConfig config, String shardId) throws IOException {
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
        logger.info("Flush loop started");

        while (running.get() || !ringBuffer.isEmpty()) {
            try {
                int flushed = flushBatch();

                if (flushed == 0) {
                    // 没有数据，短暂休眠
                    Thread.sleep(1);
                }

                // 更新健康状态
                lastFlushTime = System.currentTimeMillis();

            } catch (Throwable t) {
                flushErrorCount.increment();
                logger.error("Flush loop error, continuing...", t);
                // 不退出循环，继续尝试刷盘

                // 短暂休眠避免 CPU 空转
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        logger.info("Flush loop ended");
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
        long firstSequence = batch.get(0).sequence;
        long lastSequence = batch.get(batch.size() - 1).sequence;

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

            // 获取当前文件位置
            long position = currentChannel.position();

            // 完成已提交的 Future（ACK 语义：已入队即视为成功）
            for (WriteRequest request : batch) {
                request.future.complete(position);
            }

            // 通知等待刷盘确认的 Future
            notifyPendingFlushFutures(firstSequence, lastSequence, position);

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
            // 通知等待的 Future 也失败
            notifyPendingFlushFuturesExceptionally(firstSequence, lastSequence, e);
            logger.error("Flush batch failed", e);
            return batch.size();
        }
    }

    /**
     * 通知等待刷盘确认的 Future
     */
    private void notifyPendingFlushFutures(long firstSeq, long lastSeq, long position) {
        // 获取所有小于等于 lastSeq 的等待 Future
        var headMap = pendingFlushMap.headMap(lastSeq, true);

        for (var entry : headMap.entrySet()) {
            long seq = entry.getKey();
            if (seq >= firstSeq) {
                // 完成该序列号对应的所有 Future
                for (CompletableFuture<Long> future : entry.getValue()) {
                    future.complete(position);
                }
            }
        }

        // 清理已完成的 Future
        headMap.clear();
    }

    /**
     * 通知等待刷盘确认的 Future（异常情况）
     */
    private void notifyPendingFlushFuturesExceptionally(long firstSeq, long lastSeq, IOException e) {
        var headMap = pendingFlushMap.headMap(lastSeq, true);

        for (var entry : headMap.entrySet()) {
            long seq = entry.getKey();
            if (seq >= firstSeq) {
                for (CompletableFuture<Long> future : entry.getValue()) {
                    future.completeExceptionally(e);
                }
            }
        }

        headMap.clear();
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
     * 写入记录（异步）- ASYNC 语义：已入队即返回
     *
     * @return CompletableFuture，记录已提交到 RingBuffer 即完成
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
     * 写入记录（带超时尝试）- 支持背压控制
     *
     * @return CompletableFuture，如果无法立即入队则返回失败的 Future
     */
    public CompletableFuture<Long> appendAsyncWithTimeout(String taskId, String bizKey,
                                                           EventType eventType, long eventTime,
                                                           byte[] payload,
                                                           long timeout, TimeUnit unit) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Writer is closed"));
        }

        long seq = recordSeq.incrementAndGet();
        WriteRequest request = new WriteRequest(
                seq, taskId, bizKey, eventType, eventTime, payload
        );

        // 尝试写入 RingBuffer（带超时）
        if (!ringBuffer.offerWithTimeout(request, timeout, unit)) {
            stats.recordOverflow();
            return CompletableFuture.failedFuture(
                    new WALOverloadException("WAL write pressure too high, RingBuffer is full"));
        }

        stats.recordWrite();
        return request.future;
    }

    /**
     * 等待指定序列号的记录刷盘完成（DURABLE 语义）
     *
     * @param sequence 记录序列号（从 appendAsync 返回的 Future 获取）
     * @return CompletableFuture，刷盘完成后返回文件位置
     */
    public CompletableFuture<Long> waitForFlush(long sequence) {
        CompletableFuture<Long> flushFuture = new CompletableFuture<>();

        // 检查是否已经刷盘
        if (isFlushed(sequence)) {
            flushFuture.complete(getCurrentPosition());
            return flushFuture;
        }

        // 添加到等待映射
        pendingFlushMap.computeIfAbsent(sequence, k -> new ArrayList<>()).add(flushFuture);

        // 再次检查（避免竞态条件）
        if (isFlushed(sequence)) {
            pendingFlushMap.remove(sequence);
            flushFuture.complete(getCurrentPosition());
        }

        return flushFuture;
    }

    /**
     * 检查指定序列号是否已刷盘
     */
    private boolean isFlushed(long sequence) {
        // 如果刷盘位置已超过该序列号，则认为已刷盘
        return pendingFlushMap.isEmpty() || pendingFlushMap.firstKey() > sequence;
    }

    /**
     * 获取当前文件位置
     */
    private long getCurrentPosition() {
        try {
            return currentChannel != null && currentChannel.isOpen()
                ? currentChannel.position()
                : -1;
        } catch (IOException e) {
            return -1;
        }
    }

    /**
     * 写入记录（同步）- DURABLE 语义：等待刷盘完成
     *
     * @return 文件位置
     * @throws IOException 如果写入失败
     */
    public long append(String taskId, String bizKey,
                       EventType eventType, long eventTime,
                       byte[] payload) throws IOException {
        return append(taskId, bizKey, eventType, eventTime, payload, true);
    }

    /**
     * 写入记录（同步）- 可配置是否等待刷盘
     *
     * @param waitForFlush 是否等待刷盘完成
     * @return 文件位置
     * @throws IOException 如果写入失败
     */
    public long append(String taskId, String bizKey,
                       EventType eventType, long eventTime,
                       byte[] payload, boolean waitForFlush) throws IOException {
        try {
            // 1. 提交到 RingBuffer
            CompletableFuture<Long> committedFuture = appendAsync(
                taskId, bizKey, eventType, eventTime, payload);

            long sequence = recordSeq.get();

            // 2. 等待入队完成
            Long position = committedFuture.get(writeTimeoutMs, TimeUnit.MILLISECONDS);

            // 3. ASYNC 语义：不等待刷盘
            if (!waitForFlush) {
                return position;
            }

            // 4. DURABLE 语义：等待刷盘完成
            CompletableFuture<Long> flushFuture = waitForFlush(sequence);

            // 动态计算超时：max(5s, batchInterval * 10)
            long flushTimeoutMs = Math.max(writeTimeoutMs,
                config.batchFlushIntervalMs() * 10);

            return flushFuture.get(flushTimeoutMs, TimeUnit.MILLISECONDS);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Write interrupted", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new IOException("Write failed", e.getCause());
        } catch (TimeoutException e) {
            throw new IOException("Write timeout (may be in unknown state, check idempotency)", e);
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

    // ========== 健康监控 API ==========

    /**
     * 获取最后一次刷盘时间
     */
    public long getLastFlushTime() {
        return lastFlushTime;
    }

    /**
     * 获取刷盘错误计数
     */
    public long getFlushErrorCount() {
        return flushErrorCount.sum();
    }

    /**
     * 健康检查
     *
     * @return true 如果健康
     */
    public boolean isHealthy() {
        long idleTime = System.currentTimeMillis() - lastFlushTime;
        long maxIdleTime = Math.max(5000, config.batchFlushIntervalMs() * 2);
        return idleTime < maxIdleTime && flushErrorCount.sum() < 10;
    }

    /**
     * 获取健康状态详情
     */
    public HealthStatus getHealthStatus() {
        long idleTime = System.currentTimeMillis() - lastFlushTime;
        long maxIdleTime = Math.max(5000, config.batchFlushIntervalMs() * 2);
        boolean healthy = idleTime < maxIdleTime && flushErrorCount.sum() < 10;

        return new HealthStatus(healthy, idleTime, flushErrorCount.sum(),
            ringBuffer.size(), running.get());
    }

    /**
     * 强制立即刷盘（紧急使用）
     */
    public void flushNow() {
        flushAll();
        lastFlushTime = System.currentTimeMillis();
    }

    /**
     * 健康状态
     */
    public record HealthStatus(
        boolean healthy,
        long idleTimeMs,
        long errorCount,
        int pendingWrites,
        boolean running
    ) {}

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

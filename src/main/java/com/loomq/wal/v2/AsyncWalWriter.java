package com.loomq.wal.v2;

import com.loomq.config.WalConfig;
import com.loomq.entity.EventType;
import com.loomq.wal.WalRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * 异步 WAL 写入器 (V0.2 核心优化)
 *
 * 第一性原理推导：
 * 1. WAL 写入的本质：持久化任务事件
 * 2. V0.1 问题：全局锁 + 同步 fsync，阻塞所有写入
 * 3. V0.2 方案：异步写入 + Group Commit
 *
 * 设计原理：
 * - 业务线程写入 RingBuffer（无锁/低锁竞争）
 * - 单线程消费，批量写入 + 批量 fsync
 * - Group Commit：多条记录合并一次 fsync
 *
 * P0 修复：
 * - 严格 RingBuffer：publish 必须等待 slot 可用，不允许覆盖未消费数据
 * - DURABLE 语义：flushedSequence 追踪物理刷盘位置
 *
 * 投递语义：At-Least-Once
 * - 任务可能被多次执行
 * - 下游服务必须基于 taskId 幂等
 */
public class AsyncWalWriter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(AsyncWalWriter.class);

    // RingBuffer 配置
    private static final int BUFFER_SIZE = 65536;  // 2^16，必须是 2 的幂

    // RingBuffer（简化版，使用数组 + 序列号）
    private final WalEvent[] buffer;
    private final AtomicLong publishSequence = new AtomicLong(-1);
    private final AtomicLong consumeSequence = new AtomicLong(-1);

    // 【P0-2 修复】已刷盘序列号（物理刷盘位置）
    private volatile long flushedSequence = -1;

    // 批量写入配置
    private final int batchSize;
    private final long batchTimeoutMs;

    // 底层写入器
    private final SyncWalWriter syncWriter;

    // 消费线程
    private final ExecutorService consumer;
    private final AtomicBoolean running = new AtomicBoolean(false);

    // 统计
    private final AtomicLong totalEvents = new AtomicLong(0);
    private final AtomicLong totalBatches = new AtomicLong(0);
    private final AtomicLong totalFsyncs = new AtomicLong(0);

    // 【P0-1 修复】移除背压阈值，改为严格阻塞模式
    // 不再允许覆盖未消费数据

    // Checkpoint 管理器（可选）
    private CheckpointManager checkpointManager;

    public AsyncWalWriter(WalConfig config) throws IOException {
        this.buffer = new WalEvent[BUFFER_SIZE];
        this.batchSize = 1000;
        this.batchTimeoutMs = 100;

        this.syncWriter = new SyncWalWriter(config);

        this.consumer = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "wal-consumer");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 启动写入器
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        consumer.submit(this::consumeLoop);
        logger.info("AsyncWalWriter started, bufferSize={}, batchSize={}, batchTimeout={}ms",
                BUFFER_SIZE, batchSize, batchTimeoutMs);
    }

    /**
     * 追加事件（严格阻塞模式，P0-1 修复）
     *
     * 【P0-1 修复】严格 RingBuffer：
     * - publish 必须等待 slot 可用
     * - 不允许覆盖未消费数据
     * - 保证数据不丢失
     *
     * @return 事件序列号，用于追踪
     */
    public long append(String taskId, String bizKey, EventType eventType,
                       long eventTime, byte[] payload) throws IOException {

        // 【P0-1 修复】严格等待：直到有可用槽位
        // 生产者-消费者距离约束：publishSequence - consumeSequence < BUFFER_SIZE
        while (publishSequence.get() - consumeSequence.get() >= BUFFER_SIZE) {
            if (!running.get()) {
                throw new IOException("WAL writer is closed");
            }
            // 阻塞等待 1μs
            LockSupport.parkNanos(1_000);
        }

        // 发布事件
        long sequence = publishSequence.incrementAndGet();
        int index = (int) (sequence & (BUFFER_SIZE - 1));  // 位运算取模

        buffer[index] = new WalEvent(sequence, taskId, bizKey, eventType, eventTime, payload);
        totalEvents.incrementAndGet();

        return sequence;
    }

    /**
     * 消费循环（单线程）
     */
    private void consumeLoop() {
        List<WalEvent> batch = new ArrayList<>(batchSize);
        long lastFlushTime = System.currentTimeMillis();

        while (running.get()) {
            try {
                // 检查是否有新事件
                long currentConsume = consumeSequence.get();
                long currentPublish = publishSequence.get();

                if (currentPublish > currentConsume) {
                    // 有新事件，取出
                    long nextConsume = currentConsume + 1;
                    int index = (int) (nextConsume & (BUFFER_SIZE - 1));

                    WalEvent event = buffer[index];
                    if (event != null) {
                        batch.add(event);
                        buffer[index] = null;  // 清空，帮助 GC
                    }

                    consumeSequence.set(nextConsume);

                    // 批量写入条件
                    boolean shouldFlush = batch.size() >= batchSize ||
                            System.currentTimeMillis() - lastFlushTime >= batchTimeoutMs;

                    if (shouldFlush && !batch.isEmpty()) {
                        flushBatch(batch);
                        batch.clear();
                        lastFlushTime = System.currentTimeMillis();
                    }

                } else {
                    // 无新事件，等待或刷新
                    if (!batch.isEmpty() &&
                            System.currentTimeMillis() - lastFlushTime >= batchTimeoutMs) {
                        flushBatch(batch);
                        batch.clear();
                        lastFlushTime = System.currentTimeMillis();
                    } else {
                        // 短暂等待
                        Thread.sleep(10);
                    }
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Consume loop error", e);
            }
        }

        // 退出前刷新剩余
        if (!batch.isEmpty()) {
            try {
                flushBatch(batch);
            } catch (IOException e) {
                logger.error("Final flush failed", e);
            }
        }

        logger.info("AsyncWalWriter consumer stopped, events={}, batches={}, fsyncs={}",
                totalEvents.get(), totalBatches.get(), totalFsyncs.get());
    }

    /**
     * 批量刷新（P0-2 修复）
     *
     * 【P0-2 修复】更新 flushedSequence 追踪物理刷盘位置
     */
    private void flushBatch(List<WalEvent> batch) throws IOException {
        if (batch.isEmpty()) {
            return;
        }

        // 批量写入
        long lastSeq = -1;
        for (WalEvent event : batch) {
            syncWriter.append(
                    event.taskId,
                    event.bizKey,
                    event.eventType,
                    event.eventTime,
                    event.payload
            );
            lastSeq = Math.max(lastSeq, event.sequence);
        }

        // 单次 fsync（Group Commit 关键）
        syncWriter.sync();

        // 【P0-2 修复】关键：更新已刷盘序列号
        flushedSequence = Math.max(flushedSequence, lastSeq);

        totalBatches.incrementAndGet();
        totalFsyncs.incrementAndGet();

        logger.debug("Flushed batch, size={}, flushedSequence={}", batch.size(), flushedSequence);

        // 检查是否需要 checkpoint
        if (checkpointManager != null && checkpointManager.recordWrite()) {
            try {
                checkpointManager.checkpoint(
                        flushedSequence,
                        syncWriter.getSegmentSeq(),
                        syncWriter.getPosition(),
                        0  // taskCount 由外部更新
                );
            } catch (Exception e) {
                logger.error("Checkpoint failed", e);
            }
        }
    }

    /**
     * 设置 Checkpoint 管理器
     */
    public void setCheckpointManager(CheckpointManager checkpointManager) {
        this.checkpointManager = checkpointManager;
    }

    /**
     * 强制刷新
     */
    public void sync() throws IOException {
        // 等待消费追上
        long deadline = System.currentTimeMillis() + 5000;
        while (publishSequence.get() > consumeSequence.get() &&
                System.currentTimeMillis() < deadline) {
            Thread.yield();
        }
        syncWriter.sync();
    }

    /**
     * 等待指定序列号持久化（P0-2 修复）
     *
     * 【P0-2 修复】等待 flushedSequence >= sequence（物理刷盘）
     *
     * @param sequence 序列号
     * @param timeoutMs 超时时间
     * @return 是否成功持久化
     */
    public boolean awaitDurable(long sequence, long timeoutMs) throws IOException {
        long deadline = System.currentTimeMillis() + timeoutMs;

        // 【P0-2 修复】等待 flushedSequence >= sequence（物理刷盘）
        while (flushedSequence < sequence && System.currentTimeMillis() < deadline) {
            if (!running.get()) {
                throw new IOException("WAL writer is closed");
            }
            LockSupport.parkNanos(100_000);  // 100μs
        }

        return flushedSequence >= sequence;
    }

    /**
     * 获取当前已发布的序列号
     */
    public long getCurrentSequence() {
        return publishSequence.get();
    }

    /**
     * 获取统计
     */
    public WalStats getStats() {
        long pending = publishSequence.get() - consumeSequence.get();
        return new WalStats(
                totalEvents.get(),
                totalBatches.get(),
                totalFsyncs.get(),
                pending,
                BUFFER_SIZE,
                running.get()
        );
    }

    public record WalStats(
            long totalEvents,
            long totalBatches,
            long totalFsyncs,
            long ringBufferSize,      // 当前使用量
            int ringBufferCapacity,   // 总容量
            boolean running
    ) {}

    @Override
    public void close() {
        running.set(false);
        consumer.shutdown();
        try {
            consumer.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        syncWriter.close();
        logger.info("AsyncWalWriter closed");
    }

    // ========== 内部类 ==========

    /**
     * WAL 事件（包含序列号）
     */
    private static class WalEvent {
        final long sequence;      // 【P0-2 修复】添加序列号
        final String taskId;
        final String bizKey;
        final EventType eventType;
        final long eventTime;
        final byte[] payload;

        WalEvent(long sequence, String taskId, String bizKey, EventType eventType,
                 long eventTime, byte[] payload) {
            this.sequence = sequence;
            this.taskId = taskId;
            this.bizKey = bizKey;
            this.eventType = eventType;
            this.eventTime = eventTime;
            this.payload = payload;
        }
    }
}

package com.loomq.replication;

import com.loomq.cluster.ShardStateMachine;
import com.loomq.replication.protocol.CatchUpRequest;
import com.loomq.replication.protocol.CatchUpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * WAL 复制追赶管理器
 *
 * Week 4 Phase 4 - T4.1: 实现 replica 从 primary 追赶缺失的 WAL 记录
 *
 * 职责：
 * 1. 管理 replica 与 primary 之间的 offset 差距检测
 * 2. 从 primary 拉取缺失的 WAL 记录
 * 3. 按顺序应用追赶的记录
 * 4. 更新追赶进度到 ShardStateMachine
 *
 * 关键设计：
 * - 使用增量追赶而非全量同步
 * - 支持分批拉取，避免内存溢出
 * - 与 ShardStateMachine 集成，更新 REPLICA_CATCHING_UP 状态
 * - 追赶完成后自动切换到 REPLICA_SYNCED
 *
 * 状态流转：
 * ```
 * REPLICA_INIT
 *      │
 *      ▼ (启动追赶)
 * REPLICA_CATCHING_UP ◄──────────┐
 *      │                         │
 *      ▼ (追赶完成)               │ (收到新数据)
 * REPLICA_SYNCED ────────────────┤
 *      │                         │
 *      ▼ (primary 故障)           │
 * PROMOTING                      │
 * ```
 *
 * @author loomq
 * @since v0.4.8
 */
public class WalCatchUpManager implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(WalCatchUpManager.class);

    // ==================== 配置常量 ====================

    /**
     * 默认批量大小
     */
    public static final int DEFAULT_BATCH_SIZE = 100;

    /**
     * 最大批量大小
     */
    public static final int MAX_BATCH_SIZE = 1000;

    /**
     * 默认重试间隔
     */
    public static final long DEFAULT_RETRY_INTERVAL_MS = 1000;

    /**
     * 默认最大重试次数
     */
    public static final int DEFAULT_MAX_RETRIES = 3;

    /**
     * 追赶进度报告间隔
     */
    public static final long PROGRESS_REPORT_INTERVAL_MS = 5000;

    // ==================== 核心组件 ====================

    // 节点信息
    private final String nodeId;
    private final String shardId;

    // 分片状态机
    private final ShardStateMachine stateMachine;

    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean catchingUp = new AtomicBoolean(false);

    // 当前追赶状态
    private final AtomicReference<CatchUpState> currentState = new AtomicReference<>(CatchUpState.IDLE);

    // 当前应用的 offset
    private final AtomicLong currentOffset = new AtomicLong(0);

    // 目标 offset（primary 当前最新）
    private final AtomicLong targetOffset = new AtomicLong(0);

    // 追赶统计
    private final CatchUpStats stats = new CatchUpStats();

    // 调度器
    private final ScheduledExecutorService scheduler;

    // ==================== 回调函数 ====================

    // 发送追赶请求的函数 request -> response future
    private volatile Function<CatchUpRequest, CompletableFuture<CatchUpResponse>> catchUpRequestSender;

    // 记录应用器 (record) -> success
    private volatile Function<ReplicationRecord, Boolean> recordApplier;

    // 追赶完成回调
    private volatile Runnable onCatchUpComplete;

    // 追赶失败回调
    private volatile BiConsumer<CatchUpError, Throwable> onCatchUpError;

    // ==================== 构造函数 ====================

    /**
     * 创建追赶管理器
     *
     * @param nodeId 节点 ID
     * @param shardId 分片 ID
     * @param stateMachine 分片状态机
     */
    public WalCatchUpManager(String nodeId, String shardId, ShardStateMachine stateMachine) {
        this(nodeId, shardId, stateMachine, DEFAULT_BATCH_SIZE);
    }

    /**
     * 创建追赶管理器
     *
     * @param nodeId 节点 ID
     * @param shardId 分片 ID
     * @param stateMachine 分片状态机
     * @param batchSize 批量大小
     */
    public WalCatchUpManager(String nodeId, String shardId,
                              ShardStateMachine stateMachine, int batchSize) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId cannot be null");
        this.shardId = Objects.requireNonNull(shardId, "shardId cannot be null");
        this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine cannot be null");
        this.batchSize = Math.min(batchSize, MAX_BATCH_SIZE);

        this.scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "catchup-" + shardId);
            t.setDaemon(true);
            return t;
        });

        logger.info("WalCatchUpManager created: node={}, shard={}, batchSize={}",
            nodeId, shardId, this.batchSize);
    }

    // 批量大小
    private final int batchSize;

    // ==================== 生命周期 ====================

    /**
     * 启动追赶管理器
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            logger.warn("CatchUpManager already running");
            return;
        }

        logger.info("Starting WalCatchUpManager for shard {}...", shardId);

        // 启动进度报告任务
        scheduler.scheduleAtFixedRate(
            this::reportProgress,
            PROGRESS_REPORT_INTERVAL_MS,
            PROGRESS_REPORT_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        );

        logger.info("WalCatchUpManager started");
    }

    /**
     * 停止追赶管理器
     */
    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        logger.info("Stopping WalCatchUpManager...");

        // 停止正在进行的追赶
        stopCatchUp();

        // 关闭调度器
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("WalCatchUpManager stopped");
    }

    // ==================== 追赶核心逻辑 ====================

    /**
     * 启动追赶流程
     *
     * 从指定的 offset 开始追赶 primary 的数据
     *
     * @param startOffset 起始 offset（通常为本地最后应用的 offset）
     * @param targetOffset 目标 offset（primary 当前最新 offset）
     * @return CompletableFuture 追赶完成时返回 true
     */
    public CompletableFuture<Boolean> startCatchUp(long startOffset, long targetOffset) {
        if (!running.get()) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("CatchUpManager not running"));
        }

        // 检查是否已经在追赶中
        if (!catchingUp.compareAndSet(false, true)) {
            logger.warn("Catch up already in progress");
            return CompletableFuture.completedFuture(false);
        }

        // 更新状态机为 CATCHING_UP
        if (!stateMachine.toCatchingUp(targetOffset)) {
            catchingUp.set(false);
            return CompletableFuture.completedFuture(false);
        }

        // 初始化状态
        this.currentOffset.set(startOffset);
        this.targetOffset.set(targetOffset);
        this.currentState.set(CatchUpState.CATCHING_UP);
        this.stats.reset();
        this.stats.startTime = System.currentTimeMillis();
        this.stats.startOffset = startOffset;
        this.stats.targetOffset = targetOffset;

        logger.info("Starting catch up: shard={}, startOffset={}, targetOffset={}",
            shardId, startOffset, targetOffset);

        // 启动异步追赶
        return CompletableFuture.supplyAsync(this::doCatchUp);
    }

    /**
     * 执行追赶（内部方法）
     */
    private Boolean doCatchUp() {
        try {
            int retries = 0;

            while (running.get() && catchingUp.get()) {
                long current = currentOffset.get();
                long target = targetOffset.get();

                // 检查是否已完成
                if (current >= target) {
                    logger.info("Catch up completed: shard={}, reached target offset {}",
                        shardId, target);
                    onCatchUpSuccess();
                    return true;
                }

                // 发送追赶请求
                CatchUpRequest request = new CatchUpRequest(
                    shardId, current + 1, batchSize, nodeId);

                try {
                    CatchUpResponse response = sendCatchUpRequest(request);

                    if (response == null) {
                        throw new CatchUpException("Null response from primary");
                    }

                    if (!response.isSuccess()) {
                        throw new CatchUpException(
                            "Catch up request failed: " + response.getErrorMessage());
                    }

                    // 应用记录
                    List<ReplicationRecord> records = response.getRecords();
                    if (records != null && !records.isEmpty()) {
                        applyRecords(records);

                        // 更新统计
                        stats.recordsFetched += records.size();
                        stats.bytesFetched += records.stream()
                            .mapToInt(ReplicationRecord::getTotalSize)
                            .sum();

                        // 更新当前 offset
                        long lastOffset = records.get(records.size() - 1).getOffset();
                        currentOffset.set(lastOffset);

                        logger.debug("Applied {} records, current offset: {}",
                            records.size(), lastOffset);
                    }

                    // 更新目标 offset（primary 可能持续写入）
                    if (response.getPrimaryOffset() > targetOffset.get()) {
                        targetOffset.set(response.getPrimaryOffset());
                        stats.targetOffset = response.getPrimaryOffset();
                        stateMachine.setTargetOffset(response.getPrimaryOffset());
                    }

                    // 如果没有更多数据，等待后重试
                    if (!response.hasMore()) {
                        Thread.sleep(100);
                    }

                    retries = 0; // 重置重试计数

                } catch (Exception e) {
                    // WAL 追赶重试安全网：捕获 CatchUpException（下层 wrap）和网络/IO 异常
                    retries++;
                    stats.errors++;

                    if (retries > DEFAULT_MAX_RETRIES) {
                        logger.error("Catch up failed after {} retries", DEFAULT_MAX_RETRIES, e);
                        onCatchUpFailure(CatchUpError.REQUEST_FAILED, e);
                        return false;
                    }

                    logger.warn("Catch up request failed (retry {}/{}): {}",
                        retries, DEFAULT_MAX_RETRIES, e.getMessage());

                    Thread.sleep(DEFAULT_RETRY_INTERVAL_MS * retries);
                }
            }

            // 如果是因为停止而退出
            logger.info("Catch up stopped: running={}, catchingUp={}",
                running.get(), catchingUp.get());
            return false;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Catch up interrupted");
            onCatchUpFailure(CatchUpError.INTERRUPTED, e);
            return false;
        }
    }

    /**
     * 应用一批记录
     */
    private void applyRecords(List<ReplicationRecord> records) throws CatchUpException {
        for (ReplicationRecord record : records) {
            try {
                if (recordApplier != null) {
                    Boolean applied = recordApplier.apply(record);
                    if (applied == null || !applied) {
                        logger.warn("Record not applied: offset={}", record.getOffset());
                        stats.recordsSkipped++;
                    } else {
                        stats.recordsApplied++;
                    }
                }
            } catch (RuntimeException e) {
                logger.error("Failed to apply record: offset={}", record.getOffset(), e);
                throw new CatchUpException("Failed to apply record: " + record.getOffset(), e);
            }
        }
    }

    /**
     * 追赶成功处理
     */
    private void onCatchUpSuccess() {
        catchingUp.set(false);
        currentState.set(CatchUpState.COMPLETED);
        stats.endTime = System.currentTimeMillis();

        // 转换状态机到 SYNCED
        boolean synced = stateMachine.toSynced();
        logger.info("Catch up completed, state synced: {}", synced);

        // 触发回调
        if (onCatchUpComplete != null) {
            try {
                onCatchUpComplete.run();
            } catch (Exception e) {
                // 用户回调防御：回调异常不应影响追赶完成状态
                logger.error("Catch up complete callback error", e);
            }
        }
    }

    /**
     * 追赶失败处理
     */
    private void onCatchUpFailure(CatchUpError error, Throwable cause) {
        catchingUp.set(false);
        currentState.set(CatchUpState.FAILED);
        stats.endTime = System.currentTimeMillis();

        logger.error("Catch up failed: error={}, shard={}", error, shardId, cause);

        // 触发回调
        if (onCatchUpError != null) {
            try {
                onCatchUpError.accept(error, cause);
            } catch (Exception e) {
                // 用户回调防御：回调异常不应影响错误处理流程
                logger.error("Catch up error callback error", e);
            }
        }
    }

    /**
     * 停止当前追赶
     */
    public void stopCatchUp() {
        if (catchingUp.compareAndSet(true, false)) {
            logger.info("Catch up stopped for shard {}", shardId);
            currentState.set(CatchUpState.STOPPED);
        }
    }

    // ==================== 请求发送 ====================

    /**
     * 发送追赶请求到 primary
     */
    private CatchUpResponse sendCatchUpRequest(CatchUpRequest request) throws CatchUpException {
        if (catchUpRequestSender == null) {
            throw new CatchUpException("Catch up request sender not set");
        }

        CompletableFuture<CatchUpResponse> future = catchUpRequestSender.apply(request);
        if (future == null) {
            throw new CatchUpException("Catch up request sender returned null future");
        }

        try {
            return future.get(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new CatchUpException("Timed out waiting for catch up response", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CatchUpException("Interrupted while waiting for catch up response", e);
        } catch (ExecutionException e) {
            throw new CatchUpException("Failed to receive catch up response", e.getCause());
        }
    }

    // ==================== 进度报告 ====================

    /**
     * 报告追赶进度
     */
    private void reportProgress() {
        if (!catchingUp.get()) {
            return;
        }

        long current = currentOffset.get();
        long target = targetOffset.get();
        long start = stats.startOffset;

        if (target > start) {
            double progress = (double) (current - start) / (target - start);
            double percent = progress * 100;

            long elapsed = System.currentTimeMillis() - stats.startTime;
            long rate = elapsed > 0 ? (current - start) * 1000 / elapsed : 0;

            logger.info("Catch up progress: shard={}, {}/{} ({:.1f}%), rate={} records/s, " +
                       "fetched={}, applied={}, errors={}",
                shardId, current, target, percent, rate,
                stats.recordsFetched, stats.recordsApplied, stats.errors);
        }
    }

    // ==================== 回调设置 ====================

    /**
     * 设置追赶请求发送器
     */
    public void setCatchUpRequestSender(Function<CatchUpRequest, CompletableFuture<CatchUpResponse>> sender) {
        this.catchUpRequestSender = sender;
    }

    /**
     * 设置记录应用器
     */
    public void setRecordApplier(Function<ReplicationRecord, Boolean> applier) {
        this.recordApplier = applier;
    }

    /**
     * 设置追赶完成回调
     */
    public void setOnCatchUpComplete(Runnable callback) {
        this.onCatchUpComplete = callback;
    }

    /**
     * 设置追赶失败回调
     */
    public void setOnCatchUpError(BiConsumer<CatchUpError, Throwable> callback) {
        this.onCatchUpError = callback;
    }

    // ==================== 查询方法 ====================

    /**
     * 是否正在追赶中
     */
    public boolean isCatchingUp() {
        return catchingUp.get();
    }

    /**
     * 获取当前追赶状态
     */
    public CatchUpState getCurrentState() {
        return currentState.get();
    }

    /**
     * 获取当前 offset
     */
    public long getCurrentOffset() {
        return currentOffset.get();
    }

    /**
     * 获取目标 offset
     */
    public long getTargetOffset() {
        return targetOffset.get();
    }

    /**
     * 获取追赶进度（0.0 - 1.0）
     */
    public double getProgress() {
        long current = currentOffset.get();
        long target = targetOffset.get();
        long start = stats.startOffset;

        if (target <= start) {
            return current >= target ? 1.0 : 0.0;
        }

        return Math.min(1.0, (double) (current - start) / (target - start));
    }

    /**
     * 获取追赶统计
     */
    public CatchUpStats getStats() {
        return stats;
    }

    // ==================== 内部类 ====================

    /**
     * 追赶状态
     */
    public enum CatchUpState {
        IDLE,           // 空闲
        CATCHING_UP,    // 追赶中
        COMPLETED,      // 完成
        FAILED,         // 失败
        STOPPED         // 已停止
    }

    /**
     * 追赶错误类型
     */
    public enum CatchUpError {
        REQUEST_FAILED,     // 请求失败
        APPLY_FAILED,       // 应用失败
        TIMEOUT,            // 超时
        INTERRUPTED,        // 中断
        INVALID_STATE       // 无效状态
    }

    /**
     * 追赶统计
     */
    public static class CatchUpStats {
        public volatile long startTime;
        public volatile long endTime;
        public volatile long startOffset;
        public volatile long targetOffset;
        public volatile long recordsFetched;
        public volatile long recordsApplied;
        public volatile long recordsSkipped;
        public volatile long bytesFetched;
        public volatile long errors;

        void reset() {
            startTime = 0;
            endTime = 0;
            startOffset = 0;
            targetOffset = 0;
            recordsFetched = 0;
            recordsApplied = 0;
            recordsSkipped = 0;
            bytesFetched = 0;
            errors = 0;
        }

        @Override
        public String toString() {
            return String.format(
                "CatchUpStats{fetched=%d, applied=%d, skipped=%d, bytes=%d, errors=%d}",
                recordsFetched, recordsApplied, recordsSkipped, bytesFetched, errors);
        }
    }

    /**
     * 追赶异常
     */
    public static class CatchUpException extends Exception {
        public CatchUpException(String message) {
            super(message);
        }

        public CatchUpException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    @Override
    public String toString() {
        return String.format("WalCatchUpManager{shard=%s, state=%s, progress=%.1f%%}",
            shardId, currentState.get(), getProgress() * 100);
    }
}

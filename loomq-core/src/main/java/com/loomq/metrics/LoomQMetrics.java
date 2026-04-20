package com.loomq.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * LoomQ 可观测性指标 (v0.5)
 *
 * 提供完整的监控指标收集和查询能力
 *
 * @author loomq
 * @since v0.5.0
 */
public class LoomQMetrics {

    // ==================== Intent 指标 ====================
    private final LongAdder intentsCreated = new LongAdder();
    private final LongAdder intentsCompleted = new LongAdder();
    private final LongAdder intentsCancelled = new LongAdder();
    private final LongAdder intentsExpired = new LongAdder();
    private final LongAdder intentsDeadLettered = new LongAdder();

    // 按状态的 Intent 计数
    private final Map<String, AtomicLong> intentsByStatus = new ConcurrentHashMap<>();

    // ==================== 投递指标 ====================
    private final LongAdder deliveriesTotal = new LongAdder();
    private final LongAdder deliveriesSuccess = new LongAdder();
    private final LongAdder deliveriesFailed = new LongAdder();
    private final LongAdder deliveriesRetried = new LongAdder();

    // 延迟统计
    private final LongAdder deliveryLatencyTotalMs = new LongAdder();
    private final AtomicLong deliveryLatencyMaxMs = new AtomicLong(0);

    // ==================== 复制指标 ====================
    private final LongAdder replicationRecordsSent = new LongAdder();
    private final LongAdder replicationRecordsAcked = new LongAdder();
    private final LongAdder replicationBytesSent = new LongAdder();

    // 复制延迟
    private final AtomicLong replicationLagMs = new AtomicLong(0);

    // ==================== 存储指标 ====================
    private final LongAdder walRecordsWritten = new LongAdder();
    private final LongAdder walBytesWritten = new LongAdder();
    private final LongAdder snapshotsCreated = new LongAdder();

    // ==================== 系统指标 ====================
    private final AtomicLong pendingIntents = new AtomicLong(0);
    private final AtomicLong activeDispatches = new AtomicLong(0);
    private final AtomicLong memoryUsageBytes = new AtomicLong(0);

    // ==================== WAL 健康指标 ====================
    private final AtomicLong walLastFlushTime = new AtomicLong(0);
    private final LongAdder walFlushErrorCount = new LongAdder();
    private final LongAdder walFlushLatencyTotalMs = new LongAdder();
    private final AtomicLong walFlushLatencyMaxMs = new AtomicLong(0);
    private final AtomicLong walPendingWrites = new AtomicLong(0);
    private final AtomicLong walRingBufferSize = new AtomicLong(0);
    private final AtomicBoolean walHealthy = new AtomicBoolean(true);

    // ==================== WAL 健康指标更新 ====================

    public void updateWalLastFlushTime(long timestamp) {
        walLastFlushTime.set(timestamp);
    }

    public void incrementWalFlushErrorCount() {
        walFlushErrorCount.increment();
    }

    public void recordWalFlushLatency(long latencyMs) {
        walFlushLatencyTotalMs.add(latencyMs);
        walFlushLatencyMaxMs.updateAndGet(max -> Math.max(max, latencyMs));
    }

    public void updateWalPendingWrites(long count) {
        walPendingWrites.set(count);
    }

    public void updateWalRingBufferSize(long size) {
        walRingBufferSize.set(size);
    }

    public void updateWalHealth(boolean healthy) {
        walHealthy.set(healthy);
    }

    public boolean isWalHealthy() {
        return walHealthy.get();
    }

    public long getWalLastFlushTime() {
        return walLastFlushTime.get();
    }

    public long getWalIdleTimeMs() {
        long lastFlush = walLastFlushTime.get();
        if (lastFlush == 0) return 0;
        return System.currentTimeMillis() - lastFlush;
    }

    // ==================== 单例模式 ====================
    private static final LoomQMetrics INSTANCE = new LoomQMetrics();

    public static LoomQMetrics getInstance() {
        return INSTANCE;
    }

    private LoomQMetrics() {}

    // ==================== Intent 计数方法 ====================

    public void incrementIntentsCreated() {
        intentsCreated.increment();
    }

    public void incrementIntentsCompleted() {
        intentsCompleted.increment();
    }

    public void incrementIntentsCancelled() {
        intentsCancelled.increment();
    }

    public void incrementIntentsExpired() {
        intentsExpired.increment();
    }

    public void incrementIntentsDeadLettered() {
        intentsDeadLettered.increment();
    }

    public void updateIntentStatus(String status, long count) {
        intentsByStatus.computeIfAbsent(status, k -> new AtomicLong(0)).set(count);
    }

    // ==================== 投递计数方法 ====================

    public void recordDeliveryAttempt() {
        deliveriesTotal.increment();
    }

    public void recordDeliverySuccess(long latencyMs) {
        deliveriesSuccess.increment();
        recordLatency(latencyMs);
    }

    public void recordDeliveryFailure() {
        deliveriesFailed.increment();
    }

    public void recordDeliveryRetry() {
        deliveriesRetried.increment();
    }

    private void recordLatency(long latencyMs) {
        deliveryLatencyTotalMs.add(latencyMs);
        deliveryLatencyMaxMs.updateAndGet(max -> Math.max(max, latencyMs));
    }

    // ==================== 复制计数方法 ====================

    public void recordReplicationRecordSent(int bytes) {
        replicationRecordsSent.increment();
        replicationBytesSent.add(bytes);
    }

    public void recordReplicationRecordAcked() {
        replicationRecordsAcked.increment();
    }

    public void updateReplicationLag(long lagMs) {
        replicationLagMs.set(lagMs);
    }

    // ==================== 存储计数方法 ====================

    public void recordWalRecordWritten(int bytes) {
        walRecordsWritten.increment();
        walBytesWritten.add(bytes);
    }

    public void recordSnapshotCreated() {
        snapshotsCreated.increment();
    }

    // ==================== 系统状态更新 ====================

    public void updatePendingIntents(long count) {
        pendingIntents.set(count);
    }

    public void updateActiveDispatches(long count) {
        activeDispatches.set(count);
    }

    public void updateMemoryUsage(long bytes) {
        memoryUsageBytes.set(bytes);
    }

    // ==================== 指标查询 ====================

    public MetricsSnapshot snapshot() {
        return new MetricsSnapshot(
            intentsCreated.sum(),
            intentsCompleted.sum(),
            intentsCancelled.sum(),
            intentsExpired.sum(),
            intentsDeadLettered.sum(),
            new ConcurrentHashMap<>(intentsByStatus),
            deliveriesTotal.sum(),
            deliveriesSuccess.sum(),
            deliveriesFailed.sum(),
            deliveriesRetried.sum(),
            calculateAverageLatency(),
            deliveryLatencyMaxMs.get(),
            replicationRecordsSent.sum(),
            replicationRecordsAcked.sum(),
            replicationLagMs.get(),
            walRecordsWritten.sum(),
            snapshotsCreated.sum(),
            pendingIntents.get(),
            activeDispatches.get(),
            // WAL 健康指标
            walHealthy.get(),
            walLastFlushTime.get(),
            getWalIdleTimeMs(),
            walFlushErrorCount.sum(),
            calculateWalAverageFlushLatency(),
            walFlushLatencyMaxMs.get(),
            walPendingWrites.get(),
            walRingBufferSize.get()
        );
    }

    private double calculateWalAverageFlushLatency() {
        long total = walRecordsWritten.sum();
        if (total == 0) return 0.0;
        return (double) walFlushLatencyTotalMs.sum() / total;
    }

    private double calculateAverageLatency() {
        long total = deliveriesSuccess.sum();
        if (total == 0) return 0.0;
        return (double) deliveryLatencyTotalMs.sum() / total;
    }

    // ==================== 指标快照 ====================

    public record MetricsSnapshot(
        long intentsCreated,
        long intentsCompleted,
        long intentsCancelled,
        long intentsExpired,
        long intentsDeadLettered,
        Map<String, AtomicLong> intentsByStatus,
        long deliveriesTotal,
        long deliveriesSuccess,
        long deliveriesFailed,
        long deliveriesRetried,
        double avgDeliveryLatencyMs,
        long maxDeliveryLatencyMs,
        long replicationRecordsSent,
        long replicationRecordsAcked,
        long replicationLagMs,
        long walRecordsWritten,
        long snapshotsCreated,
        long pendingIntents,
        long activeDispatches,
        // WAL 健康指标
        boolean walHealthy,
        long walLastFlushTime,
        long walIdleTimeMs,
        long walFlushErrorCount,
        double avgWalFlushLatencyMs,
        long maxWalFlushLatencyMs,
        long walPendingWrites,
        long walRingBufferSize
    ) {}

    // ==================== 重置 ====================

    public void reset() {
        intentsCreated.reset();
        intentsCompleted.reset();
        intentsCancelled.reset();
        intentsExpired.reset();
        intentsDeadLettered.reset();
        intentsByStatus.clear();
        deliveriesTotal.reset();
        deliveriesSuccess.reset();
        deliveriesFailed.reset();
        deliveriesRetried.reset();
        deliveryLatencyTotalMs.reset();
        deliveryLatencyMaxMs.set(0);
        replicationRecordsSent.reset();
        replicationRecordsAcked.reset();
        replicationBytesSent.reset();
        replicationLagMs.set(0);
        walRecordsWritten.reset();
        walBytesWritten.reset();
        snapshotsCreated.reset();
        pendingIntents.set(0);
        activeDispatches.set(0);
        memoryUsageBytes.set(0);
        // WAL 健康指标重置
        walLastFlushTime.set(0);
        walFlushErrorCount.reset();
        walFlushLatencyTotalMs.reset();
        walFlushLatencyMaxMs.set(0);
        walPendingWrites.set(0);
        walRingBufferSize.set(0);
        walHealthy.set(true);
    }
}

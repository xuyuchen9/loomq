package com.loomq.metrics;

import com.loomq.common.MetricsCollector;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collections;

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
    private final IntentMetricsRegistry intentMetrics = new IntentMetricsRegistry();

    // ==================== 流水线指标 ====================
    private final PipelineMetricsRegistry pipelineMetrics = new PipelineMetricsRegistry();

    // ==================== WAL 健康指标 ====================
    private final WalHealthMetricsRegistry walHealthMetrics = new WalHealthMetricsRegistry();

    public void updateWalLastFlushTime(long timestamp) {
        walHealthMetrics.updateWalLastFlushTime(timestamp);
    }

    public void incrementWalFlushErrorCount() {
        walHealthMetrics.incrementWalFlushErrorCount();
    }

    public void recordWalFlushLatency(long latencyMs) {
        walHealthMetrics.recordWalFlushLatency(latencyMs);
    }

    public void updateWalPendingWrites(long count) {
        walHealthMetrics.updateWalPendingWrites(count);
    }

    public void updateWalRingBufferSize(long size) {
        walHealthMetrics.updateWalRingBufferSize(size);
    }

    public void updateWalHealth(boolean healthy) {
        walHealthMetrics.updateWalHealth(healthy);
    }

    public boolean isWalHealthy() {
        return walHealthMetrics.isWalHealthy();
    }

    public long getWalLastFlushTime() {
        return walHealthMetrics.getWalLastFlushTime();
    }

    public long getWalIdleTimeMs() {
        return walHealthMetrics.getWalIdleTimeMs();
    }

    // ==================== 单例模式 ====================
    private static final LoomQMetrics INSTANCE = new LoomQMetrics();

    public static LoomQMetrics getInstance() {
        return INSTANCE;
    }

    private LoomQMetrics() {}

    // ==================== Intent 计数方法 ====================

    public void incrementIntentsCreated() {
        intentMetrics.incrementIntentsCreated();
    }

    public void incrementIntentsCompleted() {
        intentMetrics.incrementIntentsCompleted();
    }

    public void incrementIntentsCancelled() {
        intentMetrics.incrementIntentsCancelled();
    }

    public void incrementIntentsExpired() {
        intentMetrics.incrementIntentsExpired();
    }

    public void incrementIntentsDeadLettered() {
        intentMetrics.incrementIntentsDeadLettered();
    }

    // ==================== 投递计数方法 ====================

    public void recordDeliveryAttempt() {
        pipelineMetrics.recordDeliveryAttempt();
    }

    public void recordDeliverySuccess(long latencyMs) {
        pipelineMetrics.recordDeliverySuccess(latencyMs);
    }

    public void recordDeliveryFailure() {
        pipelineMetrics.recordDeliveryFailure();
    }

    public void recordDeliveryRetry() {
        pipelineMetrics.recordDeliveryRetry();
    }

    // ==================== 复制计数方法 ====================

    public void recordReplicationRecordSent(int bytes) {
        pipelineMetrics.recordReplicationRecordSent(bytes);
    }

    public void recordReplicationRecordAcked() {
        pipelineMetrics.recordReplicationRecordAcked();
    }

    public void updateReplicationLag(long lagMs) {
        pipelineMetrics.updateReplicationLag(lagMs);
    }

    // ==================== 存储计数方法 ====================

    public void recordWalRecordWritten(int bytes) {
        pipelineMetrics.recordWalRecordWritten(bytes);
    }

    public void recordSnapshotCreated() {
        pipelineMetrics.recordSnapshotCreated();
    }

    // ==================== 系统状态更新 ====================

    public void updateActiveDispatches(long count) {
        pipelineMetrics.updateActiveDispatches(count);
    }

    public void updateMemoryUsage(long bytes) {
        pipelineMetrics.updateMemoryUsage(bytes);
    }

    // ==================== 指标查询 ====================

    public MetricsSnapshot snapshot() {
        Map<String, Long> statusSnapshot = new LinkedHashMap<>(MetricsCollector.getInstance().getIntentStatusCounts());
        long pendingIntents = MetricsCollector.getInstance().getPendingIntents();

        return new MetricsSnapshot(
            intentMetrics.getIntentsCreated(),
            intentMetrics.getIntentsCompleted(),
            intentMetrics.getIntentsCancelled(),
            intentMetrics.getIntentsExpired(),
            intentMetrics.getIntentsDeadLettered(),
            Collections.unmodifiableMap(statusSnapshot),
            pipelineMetrics.getDeliveriesTotal(),
            pipelineMetrics.getDeliveriesSuccess(),
            pipelineMetrics.getDeliveriesFailed(),
            pipelineMetrics.getDeliveriesRetried(),
            pipelineMetrics.calculateAverageDeliveryLatency(),
            pipelineMetrics.getDeliveryLatencyMaxMs(),
            pipelineMetrics.getReplicationRecordsSent(),
            pipelineMetrics.getReplicationRecordsAcked(),
            pipelineMetrics.getReplicationLagMs(),
            pipelineMetrics.getWalRecordsWritten(),
            pipelineMetrics.getSnapshotsCreated(),
            pendingIntents,
            pipelineMetrics.getActiveDispatches(),
            // WAL 健康指标
            walHealthMetrics.isWalHealthy(),
            walHealthMetrics.getWalLastFlushTime(),
            getWalIdleTimeMs(),
            walHealthMetrics.getWalFlushErrorCount(),
            walHealthMetrics.calculateAverageFlushLatency(pipelineMetrics.getWalRecordsWritten()),
            walHealthMetrics.getWalFlushLatencyMaxMs(),
            walHealthMetrics.getWalPendingWrites(),
            walHealthMetrics.getWalRingBufferSize()
        );
    }

    // ==================== 指标快照 ====================

    public record MetricsSnapshot(
        long intentsCreated,
        long intentsCompleted,
        long intentsCancelled,
        long intentsExpired,
        long intentsDeadLettered,
        Map<String, Long> intentsByStatus,
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
        intentMetrics.reset();
        pipelineMetrics.reset();
        // WAL 健康指标重置
        walHealthMetrics.reset();
        MetricsCollector.getInstance().resetRuntimeIntentMetrics();
    }
}

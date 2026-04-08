package com.loomq.replication;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;

/**
 * 复制链路指标收集
 *
 * 必须能观测的指标：
 * - replication.lag.ms - 复制延迟
 * - replication.ack.latency.ms - replica ACK 延迟
 * - replication.sent.bytes - 发送字节数
 * - replication.acked.offset - 已确认 offset
 * - failover.count - failover 次数
 * - catchup.progress - 追赶进度
 *
 * @author loomq
 * @since v0.4.8
 */
public class ReplicationMetrics {

    // ==================== Lag 指标 ====================

    /**
     * 复制延迟（offset 数量）
     */
    public static final Gauge REPLICATION_LAG_OFFSETS = Gauge.build()
        .name("loomq_replication_lag_offsets")
        .help("Number of offsets not yet acknowledged by replica")
        .labelNames("shard_id", "replica_id")
        .register();

    /**
     * 复制延迟（毫秒）- 基于时间戳差值
     */
    public static final Gauge REPLICATION_LAG_MS = Gauge.build()
        .name("loomq_replication_lag_ms")
        .help("Replication lag in milliseconds based on timestamp difference")
        .labelNames("shard_id", "replica_id")
        .register();

    // ==================== 吞吐量指标 ====================

    /**
     * 发送记录数
     */
    public static final Counter REPLICATION_SENT_RECORDS = Counter.build()
        .name("loomq_replication_sent_records_total")
        .help("Total number of replication records sent")
        .labelNames("shard_id", "replica_id")
        .register();

    /**
     * 发送字节数
     */
    public static final Counter REPLICATION_SENT_BYTES = Counter.build()
        .name("loomq_replication_sent_bytes_total")
        .help("Total number of bytes sent to replica")
        .labelNames("shard_id", "replica_id")
        .register();

    /**
     * 确认记录数（按状态分类）
     */
    public static final Counter REPLICATION_ACKED_RECORDS = Counter.build()
        .name("loomq_replication_acked_records_total")
        .help("Total number of records acknowledged by replica")
        .labelNames("shard_id", "replica_id", "status")
        .register();

    // ==================== 延迟指标 ====================

    /**
     * Replica ACK 延迟分布
     */
    public static final Histogram REPLICATION_ACK_LATENCY = Histogram.build()
        .name("loomq_replication_ack_latency_seconds")
        .help("Latency of replica acknowledgement")
        .labelNames("shard_id", "replica_id")
        .buckets(0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0)
        .register();

    /**
     * 复制延迟分布
     */
    public static final Histogram REPLICATION_LATENCY = Histogram.build()
        .name("loomq_replication_latency_seconds")
        .help("End-to-end replication latency")
        .labelNames("shard_id", "replica_id", "ack_level")
        .buckets(0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0)
        .register();

    // ==================== 连接状态指标 ====================

    /**
     * 连接状态（1=connected, 0=disconnected）
     */
    public static final Gauge REPLICATION_CONNECTED = Gauge.build()
        .name("loomq_replication_connected")
        .help("Connection status to replica (1=connected, 0=disconnected)")
        .labelNames("shard_id", "replica_id")
        .register();

    /**
     * 最后心跳时间戳
     */
    public static final Gauge REPLICATION_LAST_HEARTBEAT = Gauge.build()
        .name("loomq_replication_last_heartbeat_timestamp")
        .help("Timestamp of last heartbeat received from replica")
        .labelNames("shard_id", "replica_id")
        .register();

    // ==================== Failover 指标 ====================

    /**
     * Failover 次数
     */
    public static final Counter FAILOVER_COUNT = Counter.build()
        .name("loomq_failover_total")
        .help("Total number of failovers")
        .labelNames("shard_id", "reason")
        .register();

    /**
     * Promotion 次数
     */
    public static final Counter PROMOTION_COUNT = Counter.build()
        .name("loomq_promotion_total")
        .help("Total number of replica promotions to primary")
        .labelNames("shard_id", "result")
        .register();

    // ==================== Catch-up 指标 ====================

    /**
     * Catch-up 进度百分比
     */
    public static final Gauge CATCHUP_PROGRESS = Gauge.build()
        .name("loomq_catchup_progress_ratio")
        .help("Catch-up progress ratio (0.0 to 1.0)")
        .labelNames("shard_id")
        .register();

    /**
     * Catch-up 剩余 offset 数
     */
    public static final Gauge CATCHUP_REMAINING = Gauge.build()
        .name("loomq_catchup_remaining_offsets")
        .help("Number of offsets remaining to catch up")
        .labelNames("shard_id")
        .register();

    // ==================== 角色指标 ====================

    /**
     * 当前角色（1=PRIMARY, 0=REPLICA）
     */
    public static final Gauge REPLICATION_ROLE = Gauge.build()
        .name("loomq_replication_role")
        .help("Current role (1=PRIMARY, 0=REPLICA)")
        .labelNames("shard_id")
        .register();

    // ==================== 便捷方法 ====================

    /**
     * 记录发送指标
     */
    public static void recordSent(String shardId, String replicaId, int bytes) {
        REPLICATION_SENT_RECORDS.labels(shardId, replicaId).inc();
        REPLICATION_SENT_BYTES.labels(shardId, replicaId).inc(bytes);
    }

    /**
     * 记录 ACK 指标
     */
    public static void recordAck(String shardId, String replicaId, AckStatus status, double latencySeconds) {
        REPLICATION_ACKED_RECORDS.labels(shardId, replicaId, status.name()).inc();
        REPLICATION_ACK_LATENCY.labels(shardId, replicaId).observe(latencySeconds);
    }

    /**
     * 更新复制延迟
     */
    public static void updateLag(String shardId, String replicaId, long lagOffsets, long lagMs) {
        REPLICATION_LAG_OFFSETS.labels(shardId, replicaId).set(lagOffsets);
        REPLICATION_LAG_MS.labels(shardId, replicaId).set(lagMs);
    }

    /**
     * 更新连接状态
     */
    public static void updateConnectionStatus(String shardId, String replicaId, boolean connected) {
        REPLICATION_CONNECTED.labels(shardId, replicaId).set(connected ? 1 : 0);
    }

    /**
     * 记录 failover
     */
    public static void recordFailover(String shardId, String reason) {
        FAILOVER_COUNT.labels(shardId, reason).inc();
    }

    /**
     * 记录 promotion
     */
    public static void recordPromotion(String shardId, boolean success) {
        PROMOTION_COUNT.labels(shardId, success ? "success" : "failure").inc();
    }

    /**
     * 更新角色
     */
    public static void updateRole(String shardId, boolean isPrimary) {
        REPLICATION_ROLE.labels(shardId).set(isPrimary ? 1 : 0);
    }

    /**
     * 更新 catch-up 进度
     */
    public static void updateCatchupProgress(String shardId, double progress, long remaining) {
        CATCHUP_PROGRESS.labels(shardId).set(progress);
        CATCHUP_REMAINING.labels(shardId).set(remaining);
    }

    /**
     * 更新最后心跳时间
     */
    public static void updateLastHeartbeat(String shardId, String replicaId, long timestamp) {
        REPLICATION_LAST_HEARTBEAT.labels(shardId, replicaId).set(timestamp);
    }
}

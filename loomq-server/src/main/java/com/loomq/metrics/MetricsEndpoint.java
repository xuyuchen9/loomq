package com.loomq.metrics;

import io.javalin.Javalin;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 指标监控端点 (v0.5)
 *
 * 提供 HTTP 接口查询系统指标
 *
 * @author loomq
 * @since v0.5.0
 */
public class MetricsEndpoint {

    private static final Logger logger = LoggerFactory.getLogger(MetricsEndpoint.class);

    private final LoomQMetrics metrics;

    public MetricsEndpoint() {
        this.metrics = LoomQMetrics.getInstance();
    }

    /**
     * 注册指标端点到 Javalin 应用
     */
    public void register(Javalin app) {
        // Prometheus 格式指标
        app.get("/metrics", this::handlePrometheusMetrics);

        // JSON 格式指标
        app.get("/api/v1/metrics", this::handleJsonMetrics);

        // 健康状态
        app.get("/health/ready", this::handleReadiness);
        app.get("/health/live", this::handleLiveness);
        app.get("/health", this::handleHealth);

        // 组件健康检查
        app.get("/health/wal", this::handleWalHealth);
        app.get("/health/replica", this::handleReplicaHealth);

        logger.info("Metrics endpoints registered");
    }

    /**
     * Prometheus 格式指标
     */
    private void handlePrometheusMetrics(Context ctx) {
        LoomQMetrics.MetricsSnapshot snapshot = metrics.snapshot();

        StringBuilder sb = new StringBuilder();

        // Intent 指标
        sb.append("# HELP loomq_intents_created_total Total number of intents created\n");
        sb.append("# TYPE loomq_intents_created_total counter\n");
        sb.append("loomq_intents_created_total ").append(snapshot.intentsCreated()).append("\n\n");

        sb.append("# HELP loomq_intents_completed_total Total number of intents completed\n");
        sb.append("# TYPE loomq_intents_completed_total counter\n");
        sb.append("loomq_intents_completed_total ").append(snapshot.intentsCompleted()).append("\n\n");

        sb.append("# HELP loomq_intents_by_status Current number of intents by status\n");
        sb.append("# TYPE loomq_intents_by_status gauge\n");
        snapshot.intentsByStatus().forEach((status, count) -> {
            sb.append("loomq_intents_by_status{status=\"").append(status).append("\"} ")
              .append(count.get()).append("\n");
        });
        sb.append("\n");

        // 投递指标
        sb.append("# HELP loomq_deliveries_total Total number of delivery attempts\n");
        sb.append("# TYPE loomq_deliveries_total counter\n");
        sb.append("loomq_deliveries_total ").append(snapshot.deliveriesTotal()).append("\n\n");

        sb.append("# HELP loomq_deliveries_success_total Total number of successful deliveries\n");
        sb.append("# TYPE loomq_deliveries_success_total counter\n");
        sb.append("loomq_deliveries_success_total ").append(snapshot.deliveriesSuccess()).append("\n\n");

        sb.append("# HELP loomq_delivery_latency_avg_ms Average delivery latency in milliseconds\n");
        sb.append("# TYPE loomq_delivery_latency_avg_ms gauge\n");
        sb.append("loomq_delivery_latency_avg_ms ").append(snapshot.avgDeliveryLatencyMs()).append("\n\n");

        // 复制指标
        sb.append("# HELP loomq_replication_lag_ms Replication lag in milliseconds\n");
        sb.append("# TYPE loomq_replication_lag_ms gauge\n");
        sb.append("loomq_replication_lag_ms ").append(snapshot.replicationLagMs()).append("\n\n");

        sb.append("# HELP loomq_pending_intents Current number of pending intents\n");
        sb.append("# TYPE loomq_pending_intents gauge\n");
        sb.append("loomq_pending_intents ").append(snapshot.pendingIntents()).append("\n\n");

        // WAL 健康指标
        sb.append("# HELP loomq_wal_healthy WAL health status (1=healthy, 0=unhealthy)\n");
        sb.append("# TYPE loomq_wal_healthy gauge\n");
        sb.append("loomq_wal_healthy ").append(snapshot.walHealthy() ? 1 : 0).append("\n\n");

        sb.append("# HELP loomq_wal_idle_time_ms WAL idle time in milliseconds\n");
        sb.append("# TYPE loomq_wal_idle_time_ms gauge\n");
        sb.append("loomq_wal_idle_time_ms ").append(snapshot.walIdleTimeMs()).append("\n\n");

        sb.append("# HELP loomq_wal_flush_errors_total Total number of WAL flush errors\n");
        sb.append("# TYPE loomq_wal_flush_errors_total counter\n");
        sb.append("loomq_wal_flush_errors_total ").append(snapshot.walFlushErrorCount()).append("\n\n");

        sb.append("# HELP loomq_wal_flush_latency_avg_ms Average WAL flush latency\n");
        sb.append("# TYPE loomq_wal_flush_latency_avg_ms gauge\n");
        sb.append("loomq_wal_flush_latency_avg_ms ").append(String.format("%.2f", snapshot.avgWalFlushLatencyMs())).append("\n\n");

        sb.append("# HELP loomq_wal_flush_latency_max_ms Max WAL flush latency\n");
        sb.append("# TYPE loomq_wal_flush_latency_max_ms gauge\n");
        sb.append("loomq_wal_flush_latency_max_ms ").append(snapshot.maxWalFlushLatencyMs()).append("\n\n");

        sb.append("# HELP loomq_wal_pending_writes Current number of pending WAL writes\n");
        sb.append("# TYPE loomq_wal_pending_writes gauge\n");
        sb.append("loomq_wal_pending_writes ").append(snapshot.walPendingWrites()).append("\n\n");

        sb.append("# HELP loomq_wal_ring_buffer_size Current RingBuffer size\n");
        sb.append("# TYPE loomq_wal_ring_buffer_size gauge\n");
        sb.append("loomq_wal_ring_buffer_size ").append(snapshot.walRingBufferSize()).append("\n");

        ctx.contentType("text/plain");
        ctx.result(sb.toString());
    }

    /**
     * JSON 格式指标
     */
    private void handleJsonMetrics(Context ctx) {
        LoomQMetrics.MetricsSnapshot s = metrics.snapshot();

        Map<String, Object> metrics = Map.of(
            "intents", Map.of(
                "created", s.intentsCreated(),
                "completed", s.intentsCompleted(),
                "cancelled", s.intentsCancelled(),
                "expired", s.intentsExpired(),
                "deadLettered", s.intentsDeadLettered(),
                "byStatus", s.intentsByStatus()
            ),
            "deliveries", Map.of(
                "total", s.deliveriesTotal(),
                "success", s.deliveriesSuccess(),
                "failed", s.deliveriesFailed(),
                "retried", s.deliveriesRetried(),
                "avgLatencyMs", s.avgDeliveryLatencyMs(),
                "maxLatencyMs", s.maxDeliveryLatencyMs()
            ),
            "replication", Map.of(
                "recordsSent", s.replicationRecordsSent(),
                "recordsAcked", s.replicationRecordsAcked(),
                "lagMs", s.replicationLagMs()
            ),
            "system", Map.of(
                "pendingIntents", s.pendingIntents(),
                "activeDispatches", s.activeDispatches(),
                "walRecordsWritten", s.walRecordsWritten(),
                "snapshotsCreated", s.snapshotsCreated()
            ),
            "wal", Map.of(
                "healthy", s.walHealthy(),
                "lastFlushTime", s.walLastFlushTime(),
                "idleTimeMs", s.walIdleTimeMs(),
                "flushErrorCount", s.walFlushErrorCount(),
                "avgFlushLatencyMs", s.avgWalFlushLatencyMs(),
                "maxFlushLatencyMs", s.maxWalFlushLatencyMs(),
                "pendingWrites", s.walPendingWrites(),
                "ringBufferSize", s.walRingBufferSize()
            )
        );

        ctx.json(metrics);
    }

    /**
     * 就绪探针
     */
    private void handleReadiness(Context ctx) {
        // 检查是否可以接收流量
        boolean ready = checkReadiness();

        if (ready) {
            ctx.status(200);
            ctx.json(Map.of("status", "UP"));
        } else {
            ctx.status(503);
            ctx.json(Map.of("status", "DOWN"));
        }
    }

    /**
     * 存活探针
     */
    private void handleLiveness(Context ctx) {
        // 基本存活检查
        ctx.status(200);
        ctx.json(Map.of("status", "ALIVE"));
    }

    /**
     * 复制链路健康检查
     */
    private void handleReplicaHealth(Context ctx) {
        long lagMs = metrics.snapshot().replicationLagMs();

        // 如果复制延迟超过 5 秒，认为不健康
        if (lagMs > 5000) {
            ctx.status(503);
            ctx.json(Map.of(
                "status", "DEGRADED",
                "replicationLagMs", lagMs
            ));
        } else {
            ctx.status(200);
            ctx.json(Map.of(
                "status", "HEALTHY",
                "replicationLagMs", lagMs
            ));
        }
    }

    /**
     * WAL 健康检查
     */
    private void handleWalHealth(Context ctx) {
        LoomQMetrics.MetricsSnapshot s = metrics.snapshot();

        long maxIdleTime = 10000; // 10 秒未刷盘视为不健康
        boolean healthy = s.walHealthy() && s.walIdleTimeMs() < maxIdleTime;

        Map<String, Object> response = Map.of(
            "status", healthy ? "HEALTHY" : "DEGRADED",
            "healthy", s.walHealthy(),
            "idleTimeMs", s.walIdleTimeMs(),
            "flushErrorCount", s.walFlushErrorCount(),
            "pendingWrites", s.walPendingWrites(),
            "ringBufferSize", s.walRingBufferSize(),
            "avgFlushLatencyMs", s.avgWalFlushLatencyMs(),
            "maxFlushLatencyMs", s.maxWalFlushLatencyMs()
        );

        ctx.status(healthy ? 200 : 503);
        ctx.json(response);
    }

    /**
     * 综合健康检查
     */
    private void handleHealth(Context ctx) {
        LoomQMetrics.MetricsSnapshot s = metrics.snapshot();

        // 检查各个组件健康状态
        boolean walHealthy = s.walHealthy();
        boolean replicationHealthy = s.replicationLagMs() < 5000;

        Map<String, String> components = Map.of(
            "wal", walHealthy ? "UP" : "DEGRADED",
            "replication", replicationHealthy ? "UP" : "DEGRADED",
            "storage", "UP"
        );

        boolean overallHealthy = walHealthy && replicationHealthy;

        Map<String, Object> response = Map.of(
            "status", overallHealthy ? "UP" : "DEGRADED",
            "components", components
        );

        ctx.status(overallHealthy ? 200 : 503);
        ctx.json(response);
    }

    private boolean checkReadiness() {
        // 检查 WAL 健康
        return metrics.isWalHealthy();
    }
}

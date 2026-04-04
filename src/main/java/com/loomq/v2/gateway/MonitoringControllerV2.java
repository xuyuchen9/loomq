package com.loomq.v2.gateway;

import com.loomq.v2.LoomqEngineV2;
import com.loomq.v2.MonitoringServiceV2;
import io.javalin.http.Context;

import java.util.HashMap;
import java.util.Map;

/**
 * V0.2 监控端点
 *
 * 端点：
 * - GET /v2/health   - 健康检查 + 状态
 * - GET /v2/metrics  - Prometheus 格式指标
 * - GET /v2/alerts   - 告警状态
 */
public class MonitoringControllerV2 {

    private final LoomqEngineV2 engine;
    private final MonitoringServiceV2 monitoring;

    public MonitoringControllerV2(LoomqEngineV2 engine) {
        this.engine = engine;
        this.monitoring = MonitoringServiceV2.getInstance();
    }

    /**
     * 健康检查
     * GET /v2/health
     */
    public void health(Context ctx) {
        Map<String, Object> health = new HashMap<>();
        health.put("status", engine.getStats().running() ? "UP" : "DOWN");
        health.put("timestamp", System.currentTimeMillis());
        health.put("version", "V0.2");

        // 引擎统计
        var stats = engine.getStats();
        Map<String, Object> engineStats = new HashMap<>();
        engineStats.put("scheduler", Map.of(
                "bucketCount", stats.scheduler().bucketCount(),
                "readyQueueSize", stats.scheduler().readyQueueSize(),
                "totalScheduled", stats.scheduler().totalScheduled(),
                "totalExpired", stats.scheduler().totalExpired(),
                "running", stats.scheduler().running()
        ));
        engineStats.put("dispatcher", Map.of(
                "queueSize", stats.dispatcher().queueSize(),
                "availablePermits", stats.dispatcher().availablePermits(),
                "totalSubmitted", stats.dispatcher().totalSubmitted(),
                "totalExecuted", stats.dispatcher().totalExecuted(),
                "totalRejected", stats.dispatcher().totalRejected()
        ));
        engineStats.put("wal", Map.of(
                "totalEvents", stats.wal().totalEvents(),
                "totalBatches", stats.wal().totalBatches(),
                "totalFsyncs", stats.wal().totalFsyncs(),
                "ringBufferUsage", stats.wal().ringBufferSize() + "/" + stats.wal().ringBufferCapacity()
        ));
        // Checkpoint 信息
        if (stats.checkpoint() != null) {
            engineStats.put("checkpoint", Map.of(
                    "lastRecordSeq", stats.checkpoint().lastRecordSeq(),
                    "segmentSeq", stats.checkpoint().segmentSeq(),
                    "segmentPosition", stats.checkpoint().segmentPosition(),
                    "taskCount", stats.checkpoint().taskCount()
            ));
        }
        health.put("engine", engineStats);

        // 延迟指标
        Map<String, Object> latency = new HashMap<>();
        latency.put("wake_latency_ms_p95", monitoring.calculateP95WakeLatency());
        latency.put("webhook_latency_ms_p95", monitoring.calculateP95WebhookLatency());
        latency.put("total_latency_ms_p95", monitoring.calculateP95TotalLatency());
        health.put("latency", latency);

        // 资源使用
        Map<String, Object> resources = new HashMap<>();
        resources.put("ringbuffer_usage_percent", String.format("%.1f", monitoring.getRingBufferUsagePercent()));
        resources.put("dispatch_queue_usage_percent", String.format("%.1f", monitoring.getDispatchQueueUsagePercent()));
        resources.put("webhook_timeout_rate_percent", String.format("%.2f", monitoring.getWebhookTimeoutRate()));
        health.put("resources", resources);

        ctx.json(health);
    }

    /**
     * Prometheus 指标
     * GET /v2/metrics
     */
    public void metrics(Context ctx) {
        String prometheusMetrics = monitoring.exportPrometheusMetrics();
        ctx.contentType("text/plain; version=0.0.4").result(prometheusMetrics);
    }

    /**
     * 告警状态
     * GET /v2/alerts
     */
    public void alerts(Context ctx) {
        var status = monitoring.getAlertStatus();

        Map<String, Object> alerts = new HashMap<>();
        alerts.put("active", Map.of(
                "wake_latency", status.wakeLatencyAlert(),
                "total_latency", status.totalLatencyAlert(),
                "webhook_timeout", status.timeoutRateAlert(),
                "wal_size", status.walSizeAlert(),
                "backpressure", status.backpressureAlert()
        ));
        alerts.put("thresholds", Map.of(
                "wake_latency_ms", status.wakeLatencyThresholdMs(),
                "total_latency_ms", status.totalLatencyThresholdMs(),
                "webhook_timeout_percent", status.timeoutRateThreshold(),
                "wal_size_bytes", status.walSizeThresholdBytes(),
                "backpressure_percent", status.backpressureThreshold()
        ));

        ctx.json(alerts);
    }
}

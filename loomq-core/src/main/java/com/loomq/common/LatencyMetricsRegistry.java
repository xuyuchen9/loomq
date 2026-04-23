package com.loomq.common;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 通用延迟指标注册表。
 *
 * 负责触发、唤醒、webhook 和端到端延迟的分桶、P95 近似计算与 Prometheus 导出。
 */
final class LatencyMetricsRegistry {

    private static final int[] LATENCY_BOUNDS = {0, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000};

    private final ConcurrentHashMap<Integer, AtomicLong> triggerLatencyBuckets = new ConcurrentHashMap<>();
    private final AtomicLong triggerLatencySampleCount = new AtomicLong(0);

    private final ConcurrentHashMap<Integer, AtomicLong> wakeLatencyBuckets = new ConcurrentHashMap<>();
    private final AtomicLong wakeLatencySampleCount = new AtomicLong(0);

    private final ConcurrentHashMap<Integer, AtomicLong> webhookLatencyBuckets = new ConcurrentHashMap<>();
    private final AtomicLong webhookLatencySampleCount = new AtomicLong(0);

    private final ConcurrentHashMap<Integer, AtomicLong> totalLatencyBuckets = new ConcurrentHashMap<>();
    private final AtomicLong totalLatencySampleCount = new AtomicLong(0);

    LatencyMetricsRegistry() {
        for (int i = 0; i < LATENCY_BOUNDS.length; i++) {
            triggerLatencyBuckets.put(i, new AtomicLong(0));
            wakeLatencyBuckets.put(i, new AtomicLong(0));
            webhookLatencyBuckets.put(i, new AtomicLong(0));
            totalLatencyBuckets.put(i, new AtomicLong(0));
        }
    }

    void recordTriggerLatency(long latencyMs) {
        triggerLatencySampleCount.incrementAndGet();
        triggerLatencyBuckets.get(findBucket(latencyMs)).incrementAndGet();
    }

    void recordWakeLatency(long latencyMs) {
        wakeLatencySampleCount.incrementAndGet();
        wakeLatencyBuckets.get(findBucket(latencyMs)).incrementAndGet();
    }

    void recordWebhookLatency(long latencyMs) {
        webhookLatencySampleCount.incrementAndGet();
        webhookLatencyBuckets.get(findBucket(latencyMs)).incrementAndGet();
    }

    void recordTotalLatency(long latencyMs) {
        totalLatencySampleCount.incrementAndGet();
        totalLatencyBuckets.get(findBucket(latencyMs)).incrementAndGet();
    }

    long calculateP95Latency() {
        return calculateP95(triggerLatencyBuckets, triggerLatencySampleCount.get());
    }

    long calculateP95WakeLatency() {
        return calculateP95(wakeLatencyBuckets, wakeLatencySampleCount.get());
    }

    long calculateP95WebhookLatency() {
        return calculateP95(webhookLatencyBuckets, webhookLatencySampleCount.get());
    }

    long calculateP95TotalLatency() {
        return calculateP95(totalLatencyBuckets, totalLatencySampleCount.get());
    }

    void appendPrometheusMetrics(StringBuilder sb) {
        sb.append("# HELP loomq_trigger_latency_ms_p95 P95 trigger latency in milliseconds\n");
        sb.append("# TYPE loomq_trigger_latency_ms_p95 gauge\n");
        sb.append(formatMetric("loomq_trigger_latency_ms_p95", calculateP95Latency()));
        sb.append("\n");

        sb.append("# HELP loomq_trigger_latency_samples Total trigger latency samples\n");
        sb.append("# TYPE loomq_trigger_latency_samples counter\n");
        sb.append(formatMetric("loomq_trigger_latency_samples", triggerLatencySampleCount.get()));
        sb.append("\n");

        sb.append("# HELP loomq_wake_latency_ms_p95 P95 wake latency (sleep end to dispatch start) - internal scheduling precision\n");
        sb.append("# TYPE loomq_wake_latency_ms_p95 gauge\n");
        sb.append(formatMetric("loomq_wake_latency_ms_p95", calculateP95WakeLatency()));
        sb.append("\n");

        sb.append("# HELP loomq_wake_latency_samples Total wake latency samples\n");
        sb.append("# TYPE loomq_wake_latency_samples counter\n");
        sb.append(formatMetric("loomq_wake_latency_samples", wakeLatencySampleCount.get()));
        sb.append("\n");

        sb.append("# HELP loomq_webhook_latency_ms_p95 P95 webhook execution latency (request to response)\n");
        sb.append("# TYPE loomq_webhook_latency_ms_p95 gauge\n");
        sb.append(formatMetric("loomq_webhook_latency_ms_p95", calculateP95WebhookLatency()));
        sb.append("\n");

        sb.append("# HELP loomq_webhook_latency_samples Total webhook latency samples\n");
        sb.append("# TYPE loomq_webhook_latency_samples counter\n");
        sb.append(formatMetric("loomq_webhook_latency_samples", webhookLatencySampleCount.get()));
        sb.append("\n");

        sb.append("# HELP loomq_total_latency_ms_p95 P95 end-to-end latency (scheduled time to webhook complete) - user visible\n");
        sb.append("# TYPE loomq_total_latency_ms_p95 gauge\n");
        sb.append(formatMetric("loomq_total_latency_ms_p95", calculateP95TotalLatency()));
        sb.append("\n");

        sb.append("# HELP loomq_total_latency_samples Total end-to-end latency samples\n");
        sb.append("# TYPE loomq_total_latency_samples counter\n");
        sb.append(formatMetric("loomq_total_latency_samples", totalLatencySampleCount.get()));
        sb.append("\n");
    }

    private String formatMetric(String name, long value) {
        return name + " " + value + "\n";
    }

    private int findBucket(long latencyMs) {
        for (int i = LATENCY_BOUNDS.length - 1; i >= 0; i--) {
            if (latencyMs >= LATENCY_BOUNDS[i]) {
                return i;
            }
        }
        return 0;
    }

    private long calculateP95(ConcurrentHashMap<Integer, AtomicLong> buckets, long totalSamples) {
        return calculatePercentile(buckets, totalSamples, 0.95);
    }

    private long calculatePercentile(ConcurrentHashMap<Integer, AtomicLong> buckets,
                                     long totalSamples,
                                     double percentile) {
        if (totalSamples == 0) {
            return 0;
        }

        long target = (long) Math.ceil(totalSamples * percentile);
        long cumulative = 0;

        for (int i = 0; i < LATENCY_BOUNDS.length; i++) {
            cumulative += buckets.get(i).get();
            if (cumulative >= target) {
                return LATENCY_BOUNDS[i];
            }
        }

        return LATENCY_BOUNDS[LATENCY_BOUNDS.length - 1];
    }
}

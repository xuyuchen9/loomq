package com.loomq.common;

import com.loomq.domain.intent.PrecisionTier;
import org.HdrHistogram.Recorder;

import java.io.File;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 指标采集器
 * 收集并暴露 Prometheus 格式的指标
 */
public class MetricsCollector {

    // 计数器
    private final AtomicLong tasksCreatedTotal = new AtomicLong(0);
    private final AtomicLong tasksAckSuccessTotal = new AtomicLong(0);
    private final AtomicLong tasksFailedTerminalTotal = new AtomicLong(0);
    private final AtomicLong tasksCancelledTotal = new AtomicLong(0);
    private final AtomicLong tasksRetryTotal = new AtomicLong(0);
    private final AtomicLong tasksExpiredTotal = new AtomicLong(0);
    private final AtomicLong tasksDeadLetterTotal = new AtomicLong(0);
    private final AtomicLong webhookRequestsTotal = new AtomicLong(0);
    private final AtomicLong webhookTimeoutTotal = new AtomicLong(0);
    private final AtomicLong webhookErrorTotal = new AtomicLong(0);

    // Bucket 指标
    private volatile long bucketTaskCount = 0;
    private volatile long readyQueueSize = 0;

    // ========== 精度档位指标 (v0.5.1) ==========

    /**
     * 按精度档位的 Intent 创建计数
     */
    private final Map<PrecisionTier, AtomicLong> intentByTier = new EnumMap<>(PrecisionTier.class);

    /**
     * 按精度档位的到期 Intent 计数
     */
    private final Map<PrecisionTier, AtomicLong> intentDueByTier = new EnumMap<>(PrecisionTier.class);

    /**
     * 按精度档位的 Bucket 大小
     */
    private final Map<PrecisionTier, AtomicLong> bucketSizeByTier = new EnumMap<>(PrecisionTier.class);

    /**
     * 按精度档位的扫描耗时采样
     */
    private final Map<PrecisionTier, AtomicLong> scanDurationSamplesByTier = new EnumMap<>(PrecisionTier.class);

    /**
     * 按精度档位的背压事件计数（v0.6.2）
     */
    private final Map<PrecisionTier, AtomicLong> backpressureEventsByTier = new EnumMap<>(PrecisionTier.class);

    /**
     * 按精度档位的唤醒延迟采样
     */
    private final Map<PrecisionTier, ConcurrentHashMap<Integer, AtomicLong>> wakeupLatencyByTier = new EnumMap<>(PrecisionTier.class);
    private final Map<PrecisionTier, AtomicLong> wakeupLatencySampleCountByTier = new EnumMap<>(PrecisionTier.class);

    /**
     * 按精度档位的高精度唤醒延迟直方图 (HdrHistogram)
     * 用于精确计算 P99/P99.9，误差 <0.1%
     */
    private final Map<PrecisionTier, Recorder> wakeupLatencyRecorders = new EnumMap<>(PrecisionTier.class);

    // 恢复指标
    private final AtomicLong recoveryDurationMs = new AtomicLong(0);
    private final AtomicLong recoveryTasksTotal = new AtomicLong(0);

    // WAL 指标
    private volatile long walSizeBytes = 0;
    private volatile int walSegmentCount = 0;
    private volatile long walRecordCount = 0;

    // ========== 分层延迟指标 ==========
    // 1. 唤醒延迟 (wake_latency): sleep 结束 → 进入分发
    // 2. 队列等待 (queue_wait): 进入分发 → 开始执行 webhook
    // 3. webhook 延迟 (webhook_latency): 开始执行 → 收到响应
    // 4. 总延迟 (total_latency): 计划时间 → webhook 完成

    // 唤醒延迟采样
    private final ConcurrentHashMap<Integer, AtomicLong> wakeLatencyBuckets = new ConcurrentHashMap<>();
    private final AtomicLong wakeLatencySampleCount = new AtomicLong(0);

    // webhook 延迟采样
    private final ConcurrentHashMap<Integer, AtomicLong> webhookLatencyBuckets = new ConcurrentHashMap<>();
    private final AtomicLong webhookLatencySampleCount = new AtomicLong(0);

    // 总延迟采样 (计划时间 → webhook 完成)
    private final ConcurrentHashMap<Integer, AtomicLong> totalLatencyBuckets = new ConcurrentHashMap<>();
    private final AtomicLong totalLatencySampleCount = new AtomicLong(0);

    // 延迟桶边界 (ms): 0, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000+
    private static final int[] LATENCY_BOUNDS = {0, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000};

    // 保留旧字段以兼容
    private static final int LATENCY_WINDOW_SIZE = 10000;
    private final ConcurrentHashMap<Integer, AtomicLong> latencyBuckets = new ConcurrentHashMap<>();
    private final AtomicLong latencySampleCount = new AtomicLong(0);

    // WAL 数据目录
    private String walDataDir;

    private static final MetricsCollector INSTANCE = new MetricsCollector();

    private MetricsCollector() {
        // 初始化所有延迟桶
        for (int i = 0; i < LATENCY_BOUNDS.length; i++) {
            latencyBuckets.put(i, new AtomicLong(0));
            wakeLatencyBuckets.put(i, new AtomicLong(0));
            webhookLatencyBuckets.put(i, new AtomicLong(0));
            totalLatencyBuckets.put(i, new AtomicLong(0));
        }

        // 初始化精度档位指标
        for (PrecisionTier tier : PrecisionTier.values()) {
            intentByTier.put(tier, new AtomicLong(0));
            intentDueByTier.put(tier, new AtomicLong(0));
            bucketSizeByTier.put(tier, new AtomicLong(0));
            scanDurationSamplesByTier.put(tier, new AtomicLong(0));
            backpressureEventsByTier.put(tier, new AtomicLong(0));
            wakeupLatencyByTier.put(tier, new ConcurrentHashMap<>());
            wakeupLatencySampleCountByTier.put(tier, new AtomicLong(0));

            // 初始化每个档位的唤醒延迟桶
            for (int i = 0; i < LATENCY_BOUNDS.length; i++) {
                wakeupLatencyByTier.get(tier).put(i, new AtomicLong(0));
            }

            // 初始化 HdrHistogram Recorder（3位有效数字，支持 1ms 到 1小时）
            wakeupLatencyRecorders.put(tier, new Recorder(3));
        }
    }

    public static MetricsCollector getInstance() {
        return INSTANCE;
    }

    public void setWalDataDir(String walDataDir) {
        this.walDataDir = walDataDir;
    }

    // ========== 计数器更新 ==========

    public void incrementTasksCreated() {
        tasksCreatedTotal.incrementAndGet();
    }

    public void incrementTasksAckSuccess() {
        tasksAckSuccessTotal.incrementAndGet();
    }

    public void incrementTasksFailedTerminal() {
        tasksFailedTerminalTotal.incrementAndGet();
    }

    public void incrementTasksCancelled() {
        tasksCancelledTotal.incrementAndGet();
    }

    public void incrementTasksRetry() {
        tasksRetryTotal.incrementAndGet();
    }

    public void incrementWebhookRequests() {
        webhookRequestsTotal.incrementAndGet();
    }

    public void incrementWebhookTimeout() {
        webhookTimeoutTotal.incrementAndGet();
    }

    public void incrementWebhookError() {
        webhookErrorTotal.incrementAndGet();
    }

    public void incrementTasksExpired() {
        tasksExpiredTotal.incrementAndGet();
    }

    public void incrementTasksDeadLetter() {
        tasksDeadLetterTotal.incrementAndGet();
    }

    public void updateBucketMetrics(long bucketTaskCount, long readyQueueSize) {
        this.bucketTaskCount = bucketTaskCount;
        this.readyQueueSize = readyQueueSize;
    }

    // ========== 精度档位指标 (v0.5.1) ==========

    /**
     * 按精度档位增加 Intent 创建计数
     */
    public void incrementIntentByTier(PrecisionTier tier) {
        intentByTier.get(tier).incrementAndGet();
    }

    /**
     * 按精度档位增加到期 Intent 计数
     */
    public void incrementIntentDueByTier(PrecisionTier tier) {
        intentDueByTier.get(tier).incrementAndGet();
    }

    /**
     * 更新指定精度档位的 Bucket 大小
     */
    public void updateBucketSizeByTier(PrecisionTier tier, long size) {
        bucketSizeByTier.get(tier).set(size);
    }

    /**
     * 记录指定精度档位的扫描耗时
     */
    public void recordScanDurationByTier(PrecisionTier tier, long durationMs) {
        scanDurationSamplesByTier.get(tier).addAndGet(durationMs);
    }

    /**
     * 记录指定精度档位的唤醒延迟
     * 同时记录到手工分桶和 HdrHistogram（用于精确计算 P99/P99.9）
     */
    public void recordWakeupLatencyByTier(PrecisionTier tier, long latencyMs) {
        wakeupLatencySampleCountByTier.get(tier).incrementAndGet();
        int bucketIndex = findBucket(latencyMs);
        wakeupLatencyByTier.get(tier).get(bucketIndex).incrementAndGet();

        // 同时记录到 HdrHistogram（支持高精度百分位计算）
        Recorder recorder = wakeupLatencyRecorders.get(tier);
        if (recorder != null) {
            recorder.recordValue(Math.max(1, latencyMs)); // HdrHistogram 要求值 >= 1
        }
    }

    /**
     * 记录背压事件（v0.6.2）
     */
    public void incrementBackpressureEvent(PrecisionTier tier) {
        backpressureEventsByTier.get(tier).incrementAndGet();
    }

    /**
     * 获取按精度档位的背压事件计数
     */
    public Map<PrecisionTier, Long> getBackpressureEventsByTier() {
        Map<PrecisionTier, Long> result = new EnumMap<>(PrecisionTier.class);
        backpressureEventsByTier.forEach((tier, count) -> result.put(tier, count.get()));
        return result;
    }

    /**
     * 计算指定精度档位的 P95 唤醒延迟
     */
    public long calculateP95WakeupLatencyByTier(PrecisionTier tier) {
        ConcurrentHashMap<Integer, AtomicLong> buckets = wakeupLatencyByTier.get(tier);
        long totalSamples = wakeupLatencySampleCountByTier.get(tier).get();
        return calculateP95(buckets, totalSamples);
    }

    /**
     * 计算指定精度档位的 P99 唤醒延迟（使用 HdrHistogram，误差 <0.1%）
     */
    public long calculateP99WakeupLatencyByTier(PrecisionTier tier) {
        Recorder recorder = wakeupLatencyRecorders.get(tier);
        if (recorder == null) return 0;
        return recorder.getIntervalHistogram().getValueAtPercentile(99.0);
    }

    /**
     * 计算指定精度档位的 P99.9 唤醒延迟（使用 HdrHistogram，误差 <0.1%）
     */
    public long calculateP999WakeupLatencyByTier(PrecisionTier tier) {
        Recorder recorder = wakeupLatencyRecorders.get(tier);
        if (recorder == null) return 0;
        return recorder.getIntervalHistogram().getValueAtPercentile(99.9);
    }

    /**
     * 计算指定档位的理论最大 QPS
     * 理论最大 QPS = 并发数 × (1000ms / 平均HTTP响应延迟)
     *
     * @param tier 精度档位
     * @param avgHttpLatencyMs 平均HTTP响应延迟（毫秒）
     * @return 理论最大 QPS
     */
    public double calculateTheoreticalMaxQps(PrecisionTier tier, double avgHttpLatencyMs) {
        int concurrency = tier.getMaxConcurrency();
        if (avgHttpLatencyMs <= 0 || concurrency <= 0) return 0;
        return concurrency * (1000.0 / avgHttpLatencyMs);
    }

    /**
     * 计算实际资源利用率（效率）
     * 效率 = 实际 QPS / 理论最大 QPS
     *
     * @param tier 精度档位
     * @param measuredQps 实测 QPS
     * @param avgHttpLatencyMs 平均HTTP响应延迟（毫秒）
     * @return 资源利用率（0-1之间）
     */
    public double calculateEfficiency(PrecisionTier tier, double measuredQps, double avgHttpLatencyMs) {
        double theoreticalMaxQps = calculateTheoreticalMaxQps(tier, avgHttpLatencyMs);
        if (theoreticalMaxQps <= 0) return 0;
        return measuredQps / theoreticalMaxQps;
    }

    /**
     * 获取按精度档位的 Intent 创建计数
     */
    public Map<PrecisionTier, Long> getIntentCountsByTier() {
        Map<PrecisionTier, Long> result = new EnumMap<>(PrecisionTier.class);
        intentByTier.forEach((tier, counter) -> result.put(tier, counter.get()));
        return result;
    }

    /**
     * 获取按精度档位的 Bucket 大小
     */
    public Map<PrecisionTier, Long> getBucketSizesByTier() {
        Map<PrecisionTier, Long> result = new EnumMap<>(PrecisionTier.class);
        bucketSizeByTier.forEach((tier, counter) -> result.put(tier, counter.get()));
        return result;
    }

    // ========== 恢复指标 ==========

    public void recordRecovery(long durationMs, long tasksRecovered) {
        recoveryDurationMs.set(durationMs);
        recoveryTasksTotal.set(tasksRecovered);
    }

    // ========== WAL 指标 ==========

    public void updateWalMetrics(long sizeBytes, int segmentCount, long recordCount) {
        this.walSizeBytes = sizeBytes;
        this.walSegmentCount = segmentCount;
        this.walRecordCount = recordCount;
    }

    public void refreshWalSize() {
        if (walDataDir != null) {
            File dir = new File(walDataDir);
            if (dir.exists() && dir.isDirectory()) {
                long totalSize = 0;
                File[] files = dir.listFiles((d, name) -> name.endsWith(".wal"));
                if (files != null) {
                    for (File file : files) {
                        totalSize += file.length();
                    }
                    walSizeBytes = totalSize;
                    walSegmentCount = files.length;
                }
            }
        }
    }

    // ========== 触发延迟 ==========

    /**
     * 记录触发延迟
     * @param latencyMs 从 trigger_time 到实际执行的时间差 (可能为负数表示提前执行)
     */
    public void recordTriggerLatency(long latencyMs) {
        latencySampleCount.incrementAndGet();

        // 找到对应的桶
        int bucketIndex = findBucket(latencyMs);
        latencyBuckets.get(bucketIndex).incrementAndGet();
    }

    /**
     * 记录唤醒延迟 (sleep 结束 → 进入分发)
     * 这是纯粹的系统内部调度延迟，与 webhook 无关
     */
    public void recordWakeLatency(long latencyMs) {
        wakeLatencySampleCount.incrementAndGet();
        int bucketIndex = findBucket(latencyMs);
        wakeLatencyBuckets.get(bucketIndex).incrementAndGet();
    }

    /**
     * 记录 webhook 执行延迟 (开始执行 → 收到响应)
     */
    public void recordWebhookLatency(long latencyMs) {
        webhookLatencySampleCount.incrementAndGet();
        int bucketIndex = findBucket(latencyMs);
        webhookLatencyBuckets.get(bucketIndex).incrementAndGet();
    }

    /**
     * 记录总延迟 (计划时间 → webhook 完成)
     * 这是用户可见的端到端延迟
     */
    public void recordTotalLatency(long latencyMs) {
        totalLatencySampleCount.incrementAndGet();
        int bucketIndex = findBucket(latencyMs);
        totalLatencyBuckets.get(bucketIndex).incrementAndGet();
    }

    private int findBucket(long latencyMs) {
        for (int i = LATENCY_BOUNDS.length - 1; i >= 0; i--) {
            if (latencyMs >= LATENCY_BOUNDS[i]) {
                return i;
            }
        }
        return 0;
    }

    /**
     * 计算 P95 触发延迟
     */
    public long calculateP95Latency() {
        return calculateP95(latencyBuckets, latencySampleCount.get());
    }

    /**
     * 计算 P95 唤醒延迟 (系统内部调度精度)
     */
    public long calculateP95WakeLatency() {
        return calculateP95(wakeLatencyBuckets, wakeLatencySampleCount.get());
    }

    /**
     * 计算 P95 webhook 延迟
     */
    public long calculateP95WebhookLatency() {
        return calculateP95(webhookLatencyBuckets, webhookLatencySampleCount.get());
    }

    /**
     * 计算 P95 总延迟 (用户可见)
     */
    public long calculateP95TotalLatency() {
        return calculateP95(totalLatencyBuckets, totalLatencySampleCount.get());
    }

    private long calculateP95(ConcurrentHashMap<Integer, AtomicLong> buckets, long totalSamples) {
        if (totalSamples == 0) {
            return 0;
        }

        long p95Target = (long) (totalSamples * 0.95);
        long cumulative = 0;

        for (int i = 0; i < LATENCY_BOUNDS.length; i++) {
            cumulative += buckets.get(i).get();
            if (cumulative >= p95Target) {
                return LATENCY_BOUNDS[i];
            }
        }

        return LATENCY_BOUNDS[LATENCY_BOUNDS.length - 1];
    }

    // ========== 导出指标 ==========

    /**
     * 导出 Prometheus 格式的指标
     */
    public String exportPrometheusMetrics(Map<String, Long> taskStats) {
        StringBuilder sb = new StringBuilder();

        // 刷新 WAL 大小
        refreshWalSize();

        // 任务统计
        sb.append("# HELP loomq_tasks_total Total number of tasks\n");
        sb.append("# TYPE loomq_tasks_total gauge\n");
        sb.append(formatMetric("loomq_tasks_total", taskStats.getOrDefault("total", 0L)));
        sb.append("\n");

        sb.append("# HELP loomq_tasks_pending Number of pending tasks\n");
        sb.append("# TYPE loomq_tasks_pending gauge\n");
        sb.append(formatMetric("loomq_tasks_pending", taskStats.getOrDefault("pending", 0L)));
        sb.append("\n");

        sb.append("# HELP loomq_tasks_scheduled Number of scheduled tasks\n");
        sb.append("# TYPE loomq_tasks_scheduled gauge\n");
        sb.append(formatMetric("loomq_tasks_scheduled", taskStats.getOrDefault("scheduled", 0L)));
        sb.append("\n");

        sb.append("# HELP loomq_tasks_dispatching Number of dispatching tasks\n");
        sb.append("# TYPE loomq_tasks_dispatching gauge\n");
        sb.append(formatMetric("loomq_tasks_dispatching", taskStats.getOrDefault("dispatching", 0L)));
        sb.append("\n");

        // 计数器
        sb.append("# HELP loomq_tasks_created_total Total tasks created\n");
        sb.append("# TYPE loomq_tasks_created_total counter\n");
        sb.append(formatMetric("loomq_tasks_created_total", tasksCreatedTotal.get()));
        sb.append("\n");

        sb.append("# HELP loomq_tasks_ack_success_total Total tasks acknowledged success\n");
        sb.append("# TYPE loomq_tasks_ack_success_total counter\n");
        sb.append(formatMetric("loomq_tasks_ack_success_total", tasksAckSuccessTotal.get()));
        sb.append("\n");

        sb.append("# HELP loomq_tasks_failed_terminal_total Total tasks failed terminal\n");
        sb.append("# TYPE loomq_tasks_failed_terminal_total counter\n");
        sb.append(formatMetric("loomq_tasks_failed_terminal_total", tasksFailedTerminalTotal.get()));
        sb.append("\n");

        sb.append("# HELP loomq_tasks_cancelled_total Total tasks cancelled\n");
        sb.append("# TYPE loomq_tasks_cancelled_total counter\n");
        sb.append(formatMetric("loomq_tasks_cancelled_total", tasksCancelledTotal.get()));
        sb.append("\n");

        sb.append("# HELP loomq_tasks_retry_total Total task retries\n");
        sb.append("# TYPE loomq_tasks_retry_total counter\n");
        sb.append(formatMetric("loomq_tasks_retry_total", tasksRetryTotal.get()));
        sb.append("\n");

        sb.append("# HELP loomq_tasks_expired_total Total tasks expired\n");
        sb.append("# TYPE loomq_tasks_expired_total counter\n");
        sb.append(formatMetric("loomq_tasks_expired_total", tasksExpiredTotal.get()));
        sb.append("\n");

        sb.append("# HELP loomq_tasks_dead_letter_total Total tasks in dead letter\n");
        sb.append("# TYPE loomq_tasks_dead_letter_total counter\n");
        sb.append(formatMetric("loomq_tasks_dead_letter_total", tasksDeadLetterTotal.get()));
        sb.append("\n");

        // Bucket 指标
        sb.append("# HELP loomq_bucket_task_count Number of tasks in time buckets\n");
        sb.append("# TYPE loomq_bucket_task_count gauge\n");
        sb.append(formatMetric("loomq_bucket_task_count", bucketTaskCount));
        sb.append("\n");

        sb.append("# HELP loomq_ready_queue_size Size of ready queue\n");
        sb.append("# TYPE loomq_ready_queue_size gauge\n");
        sb.append(formatMetric("loomq_ready_queue_size", readyQueueSize));
        sb.append("\n");

        // Webhook 指标
        sb.append("# HELP loomq_webhook_requests_total Total webhook requests\n");
        sb.append("# TYPE loomq_webhook_requests_total counter\n");
        sb.append(formatMetric("loomq_webhook_requests_total", webhookRequestsTotal.get()));
        sb.append("\n");

        sb.append("# HELP loomq_webhook_timeout_total Total webhook timeouts\n");
        sb.append("# TYPE loomq_webhook_timeout_total counter\n");
        sb.append(formatMetric("loomq_webhook_timeout_total", webhookTimeoutTotal.get()));
        sb.append("\n");

        sb.append("# HELP loomq_webhook_error_total Total webhook errors\n");
        sb.append("# TYPE loomq_webhook_error_total counter\n");
        sb.append(formatMetric("loomq_webhook_error_total", webhookErrorTotal.get()));
        sb.append("\n");

        // Webhook 超时率
        long totalRequests = webhookRequestsTotal.get();
        long totalTimeouts = webhookTimeoutTotal.get();
        double timeoutRate = totalRequests > 0 ? (double) totalTimeouts / totalRequests * 100 : 0;
        sb.append("# HELP loomq_webhook_timeout_rate_percent Webhook timeout rate in percent\n");
        sb.append("# TYPE loomq_webhook_timeout_rate_percent gauge\n");
        sb.append(String.format("loomq_webhook_timeout_rate_percent %.2f\n", timeoutRate));
        sb.append("\n");

        // 触发延迟 (旧指标，保留兼容)
        sb.append("# HELP loomq_trigger_latency_ms_p95 P95 trigger latency in milliseconds\n");
        sb.append("# TYPE loomq_trigger_latency_ms_p95 gauge\n");
        sb.append(formatMetric("loomq_trigger_latency_ms_p95", calculateP95Latency()));
        sb.append("\n");

        sb.append("# HELP loomq_trigger_latency_samples Total trigger latency samples\n");
        sb.append("# TYPE loomq_trigger_latency_samples counter\n");
        sb.append(formatMetric("loomq_trigger_latency_samples", latencySampleCount.get()));
        sb.append("\n");

        // ========== 分层延迟指标 ==========

        // 唤醒延迟 (系统内部调度精度)
        sb.append("# HELP loomq_wake_latency_ms_p95 P95 wake latency (sleep end to dispatch start) - internal scheduling precision\n");
        sb.append("# TYPE loomq_wake_latency_ms_p95 gauge\n");
        sb.append(formatMetric("loomq_wake_latency_ms_p95", calculateP95WakeLatency()));
        sb.append("\n");

        sb.append("# HELP loomq_wake_latency_samples Total wake latency samples\n");
        sb.append("# TYPE loomq_wake_latency_samples counter\n");
        sb.append(formatMetric("loomq_wake_latency_samples", wakeLatencySampleCount.get()));
        sb.append("\n");

        // webhook 延迟 (执行 → 响应)
        sb.append("# HELP loomq_webhook_latency_ms_p95 P95 webhook execution latency (request to response)\n");
        sb.append("# TYPE loomq_webhook_latency_ms_p95 gauge\n");
        sb.append(formatMetric("loomq_webhook_latency_ms_p95", calculateP95WebhookLatency()));
        sb.append("\n");

        sb.append("# HELP loomq_webhook_latency_samples Total webhook latency samples\n");
        sb.append("# TYPE loomq_webhook_latency_samples counter\n");
        sb.append(formatMetric("loomq_webhook_latency_samples", webhookLatencySampleCount.get()));
        sb.append("\n");

        // 总延迟 (计划 → 完成，用户可见)
        sb.append("# HELP loomq_total_latency_ms_p95 P95 end-to-end latency (scheduled time to webhook complete) - user visible\n");
        sb.append("# TYPE loomq_total_latency_ms_p95 gauge\n");
        sb.append(formatMetric("loomq_total_latency_ms_p95", calculateP95TotalLatency()));
        sb.append("\n");

        sb.append("# HELP loomq_total_latency_samples Total end-to-end latency samples\n");
        sb.append("# TYPE loomq_total_latency_samples counter\n");
        sb.append(formatMetric("loomq_total_latency_samples", totalLatencySampleCount.get()));
        sb.append("\n");

        // 恢复指标
        sb.append("# HELP loomq_recovery_duration_ms Recovery duration in milliseconds\n");
        sb.append("# TYPE loomq_recovery_duration_ms gauge\n");
        sb.append(formatMetric("loomq_recovery_duration_ms", recoveryDurationMs.get()));
        sb.append("\n");

        sb.append("# HELP loomq_recovery_tasks_total Total tasks recovered\n");
        sb.append("# TYPE loomq_recovery_tasks_total counter\n");
        sb.append(formatMetric("loomq_recovery_tasks_total", recoveryTasksTotal.get()));
        sb.append("\n");

        // WAL 指标
        sb.append("# HELP loomq_wal_size_bytes WAL total size in bytes\n");
        sb.append("# TYPE loomq_wal_size_bytes gauge\n");
        sb.append(formatMetric("loomq_wal_size_bytes", walSizeBytes));
        sb.append("\n");

        sb.append("# HELP loomq_wal_segment_count Number of WAL segments\n");
        sb.append("# TYPE loomq_wal_segment_count gauge\n");
        sb.append(formatMetric("loomq_wal_segment_count", walSegmentCount));
        sb.append("\n");

        sb.append("# HELP loomq_wal_record_count Total WAL records written\n");
        sb.append("# TYPE loomq_wal_record_count counter\n");
        sb.append(formatMetric("loomq_wal_record_count", walRecordCount));
        sb.append("\n");

        // ========== 精度档位指标 (v0.5.1) ==========

        // 按精度档位的 Intent 创建计数
        sb.append("# HELP loomq_intent_total Total intents created by precision tier\n");
        sb.append("# TYPE loomq_intent_total counter\n");
        intentByTier.forEach((tier, count) -> {
            sb.append("loomq_intent_total{precision_tier=\"").append(tier.name().toLowerCase()).append("\"} ")
              .append(count.get()).append("\n");
        });
        sb.append("\n");

        // 按精度档位的到期 Intent 计数
        sb.append("# HELP loomq_intent_due_total Total intents due by precision tier\n");
        sb.append("# TYPE loomq_intent_due_total counter\n");
        intentDueByTier.forEach((tier, count) -> {
            sb.append("loomq_intent_due_total{precision_tier=\"").append(tier.name().toLowerCase()).append("\"} ")
              .append(count.get()).append("\n");
        });
        sb.append("\n");

        // 按精度档位的 Bucket 大小
        sb.append("# HELP loomq_scheduler_bucket_size Current bucket size by precision tier\n");
        sb.append("# TYPE loomq_scheduler_bucket_size gauge\n");
        bucketSizeByTier.forEach((tier, count) -> {
            sb.append("loomq_scheduler_bucket_size{precision_tier=\"").append(tier.name().toLowerCase()).append("\"} ")
              .append(count.get()).append("\n");
        });
        sb.append("\n");

        // 按精度档位的 P95 唤醒延迟
        sb.append("# HELP loomq_scheduler_wakeup_late_ms_p95 P95 wakeup latency by precision tier\n");
        sb.append("# TYPE loomq_scheduler_wakeup_late_ms_p95 gauge\n");
        for (PrecisionTier tier : PrecisionTier.values()) {
            sb.append("loomq_scheduler_wakeup_late_ms_p95{precision_tier=\"").append(tier.name().toLowerCase()).append("\"} ")
              .append(calculateP95WakeupLatencyByTier(tier)).append("\n");
        }
        sb.append("\n");

        // 按精度档位的 P99 唤醒延迟（使用 HdrHistogram）
        sb.append("# HELP loomq_scheduler_wakeup_latency_ms_p99 P99 wakeup latency by precision tier\n");
        sb.append("# TYPE loomq_scheduler_wakeup_latency_ms_p99 gauge\n");
        for (PrecisionTier tier : PrecisionTier.values()) {
            sb.append("loomq_scheduler_wakeup_latency_ms_p99{precision_tier=\"").append(tier.name().toLowerCase()).append("\"} ")
              .append(calculateP99WakeupLatencyByTier(tier)).append("\n");
        }
        sb.append("\n");

        // 按精度档位的 P99.9 唤醒延迟（使用 HdrHistogram）
        sb.append("# HELP loomq_scheduler_wakeup_latency_ms_p999 P99.9 wakeup latency by precision tier\n");
        sb.append("# TYPE loomq_scheduler_wakeup_latency_ms_p999 gauge\n");
        for (PrecisionTier tier : PrecisionTier.values()) {
            sb.append("loomq_scheduler_wakeup_latency_ms_p999{precision_tier=\"").append(tier.name().toLowerCase()).append("\"} ")
              .append(calculateP999WakeupLatencyByTier(tier)).append("\n");
        }
        sb.append("\n");

        return sb.toString();
    }

    private String formatMetric(String name, long value) {
        return name + " " + value + "\n";
    }

    // ========== 获取器 (用于告警检查) ==========

    public long getP95LatencyMs() {
        return calculateP95Latency();
    }

    public double getWebhookTimeoutRate() {
        long total = webhookRequestsTotal.get();
        if (total == 0) return 0;
        return (double) webhookTimeoutTotal.get() / total * 100;
    }

    public long getWalSizeBytes() {
        refreshWalSize();
        return walSizeBytes;
    }

    public long getRecoveryDurationMs() {
        return recoveryDurationMs.get();
    }

    // ========== 系统状态更新 (从 LoomQMetrics 迁移) ==========

    private final AtomicLong pendingIntents = new AtomicLong(0);
    private final ConcurrentHashMap<String, AtomicLong> intentStatus = new ConcurrentHashMap<>();

    public void updatePendingIntents(long count) {
        pendingIntents.set(count);
    }

    public void updateIntentStatus(String status, long count) {
        intentStatus.computeIfAbsent(status, k -> new AtomicLong(0)).set(count);
    }

    public long getPendingIntents() {
        return pendingIntents.get();
    }

    public Map<String, Long> getIntentStatusCounts() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        intentStatus.forEach((status, count) -> result.put(status, count.get()));
        return result;
    }
}

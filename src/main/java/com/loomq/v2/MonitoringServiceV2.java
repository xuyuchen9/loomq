package com.loomq.v2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * V0.2 监控服务
 *
 * 核心功能：
 * 1. 指标采集：分层延迟、吞吐量、背压状态
 * 2. 告警检测：延迟超标、超时率过高、WAL 过大
 * 3. Prometheus 导出：标准格式指标
 *
 * 告警阈值（来自需求文档）：
 * - P95 唤醒延迟 > 50ms（系统内部调度精度）
 * - P95 总延迟 > 5s（用户可见端到端延迟）
 * - webhook 超时率 > 5%
 * - WAL 大小超过阈值
 * - RingBuffer 使用率 > 75%（背压预警）
 * - 分发队列使用率 > 80%
 */
public class MonitoringServiceV2 {

    private static final Logger logger = LoggerFactory.getLogger(MonitoringServiceV2.class);
    private static final Logger alertLogger = LoggerFactory.getLogger("ALERT");

    // ========== 延迟采样（必须在 INSTANCE 之前初始化）==========
    private static final int[] LATENCY_BOUNDS = {0, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000};

    // 单例（在所有静态依赖之后初始化）
    private static final MonitoringServiceV2 INSTANCE = new MonitoringServiceV2();

    // ========== 告警阈值 ==========
    // 唤醒延迟：系统内部调度精度
    private static final long WAKE_LATENCY_P95_THRESHOLD_MS = 50;
    // 总延迟：用户可见的端到端延迟
    private static final long TOTAL_LATENCY_P95_THRESHOLD_MS = 5000;
    private static final double WEBHOOK_TIMEOUT_RATE_THRESHOLD = 5.0;
    private static final double BACKPRESSURE_THRESHOLD = 75.0;  // RingBuffer 75%
    private static final double DISPATCH_QUEUE_THRESHOLD = 80.0; // 分发队列 80%
    private long walSizeThresholdBytes = 1024 * 1024 * 1024;    // WAL > 1GB

    // 告警冷却期
    private static final long ALERT_COOLDOWN_MS = 60000;

    // ========== 计数器 ==========
    private final AtomicLong tasksCreatedTotal = new AtomicLong(0);
    private final AtomicLong tasksAckSuccessTotal = new AtomicLong(0);
    private final AtomicLong tasksFailedTerminalTotal = new AtomicLong(0);
    private final AtomicLong tasksCancelledTotal = new AtomicLong(0);
    private final AtomicLong tasksRetryTotal = new AtomicLong(0);
    private final AtomicLong webhookRequestsTotal = new AtomicLong(0);
    private final AtomicLong webhookTimeoutTotal = new AtomicLong(0);
    private final AtomicLong webhookErrorTotal = new AtomicLong(0);

    // ========== 延迟采样 ==========

    // 唤醒延迟（sleep 结束 → 进入分发）
    private final ConcurrentHashMap<Integer, AtomicLong> wakeLatencyBuckets = new ConcurrentHashMap<>();
    private final AtomicLong wakeLatencySampleCount = new AtomicLong(0);

    // Webhook 延迟（请求 → 响应）
    private final ConcurrentHashMap<Integer, AtomicLong> webhookLatencyBuckets = new ConcurrentHashMap<>();
    private final AtomicLong webhookLatencySampleCount = new AtomicLong(0);

    // 总延迟（计划时间 → 完成，用户可见）
    private final ConcurrentHashMap<Integer, AtomicLong> totalLatencyBuckets = new ConcurrentHashMap<>();
    private final AtomicLong totalLatencySampleCount = new AtomicLong(0);

    // ========== V2 特有指标 ==========
    private volatile int bucketCount = 0;
    private volatile int readyQueueSize = 0;
    private volatile int dispatchQueueSize = 0;
    private volatile int dispatchQueueCapacity = 0;
    private volatile int availablePermits = 0;
    private volatile int maxConcurrency = 0;
    private volatile long ringBufferUsage = 0;      // RingBuffer 使用量
    private volatile long ringBufferCapacity = 0;   // RingBuffer 容量
    private volatile long walSizeBytes = 0;
    private volatile int walSegmentCount = 0;
    private volatile long walRecordCount = 0;

    // 恢复指标
    private final AtomicLong recoveryDurationMs = new AtomicLong(0);
    private final AtomicLong recoveryTasksTotal = new AtomicLong(0);

    // 调度延迟（scheduler_lag）
    private final AtomicLong schedulerLagMs = new AtomicLong(0);

    // 重复执行计数（duplicate_rate）
    private final AtomicLong duplicateExecutions = new AtomicLong(0);
    private final AtomicLong totalExecutions = new AtomicLong(0);

    // 告警状态
    private final AtomicLong lastWakeLatencyAlert = new AtomicLong(0);
    private final AtomicLong lastTotalLatencyAlert = new AtomicLong(0);
    private final AtomicLong lastTimeoutRateAlert = new AtomicLong(0);
    private final AtomicLong lastWalSizeAlert = new AtomicLong(0);
    private final AtomicLong lastBackpressureAlert = new AtomicLong(0);

    private final AtomicBoolean wakeLatencyAlertActive = new AtomicBoolean(false);
    private final AtomicBoolean totalLatencyAlertActive = new AtomicBoolean(false);
    private final AtomicBoolean timeoutRateAlertActive = new AtomicBoolean(false);
    private final AtomicBoolean walSizeAlertActive = new AtomicBoolean(false);
    private final AtomicBoolean backpressureAlertActive = new AtomicBoolean(false);

    // 调度器
    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;

    // WAL 数据目录
    private String walDataDir;

    private MonitoringServiceV2() {
        // 初始化延迟桶
        for (int i = 0; i < LATENCY_BOUNDS.length; i++) {
            wakeLatencyBuckets.put(i, new AtomicLong(0));
            webhookLatencyBuckets.put(i, new AtomicLong(0));
            totalLatencyBuckets.put(i, new AtomicLong(0));
        }
    }

    public static MonitoringServiceV2 getInstance() {
        return INSTANCE;
    }

    public void setWalDataDir(String walDataDir) {
        this.walDataDir = walDataDir;
    }

    public void setWalSizeThreshold(long bytes) {
        this.walSizeThresholdBytes = bytes;
    }

    // ========== 启动/停止 ==========

    public void start() {
        if (running) return;
        running = true;

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "v2-monitor");
            t.setDaemon(true);
            return t;
        });

        // 每 10 秒检查告警
        scheduler.scheduleAtFixedRate(this::checkAlerts, 10, 10, TimeUnit.SECONDS);
        // 每 30 秒刷新 WAL 大小
        scheduler.scheduleAtFixedRate(this::refreshWalSize, 30, 30, TimeUnit.SECONDS);

        logger.info("V2 Monitoring service started");
    }

    public void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdown();
        }
        logger.info("V2 Monitoring service stopped");
    }

    /**
     * 重置所有监控计数器（测试用）
     */
    public void reset() {
        // 重置计数器
        tasksCreatedTotal.set(0);
        tasksAckSuccessTotal.set(0);
        tasksFailedTerminalTotal.set(0);
        tasksCancelledTotal.set(0);
        tasksRetryTotal.set(0);
        webhookRequestsTotal.set(0);
        webhookTimeoutTotal.set(0);
        webhookErrorTotal.set(0);

        // 重置延迟桶
        for (int i = 0; i < LATENCY_BOUNDS.length; i++) {
            wakeLatencyBuckets.get(i).set(0);
            webhookLatencyBuckets.get(i).set(0);
            totalLatencyBuckets.get(i).set(0);
        }

        // 重置样本计数
        wakeLatencySampleCount.set(0);
        webhookLatencySampleCount.set(0);
        totalLatencySampleCount.set(0);
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

    // ========== 延迟记录 ==========

    public void recordWakeLatency(long latencyMs) {
        wakeLatencySampleCount.incrementAndGet();
        int bucketIndex = findBucket(latencyMs);
        wakeLatencyBuckets.get(bucketIndex).incrementAndGet();
    }

    public void recordWebhookLatency(long latencyMs) {
        webhookLatencySampleCount.incrementAndGet();
        int bucketIndex = findBucket(latencyMs);
        webhookLatencyBuckets.get(bucketIndex).incrementAndGet();
    }

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

    // ========== V2 状态更新 ==========

    public void updateSchedulerStats(int bucketCount, int readyQueueSize) {
        this.bucketCount = bucketCount;
        this.readyQueueSize = readyQueueSize;
    }

    public void updateDispatcherStats(int queueSize, int queueCapacity, int availablePermits, int maxConcurrency) {
        this.dispatchQueueSize = queueSize;
        this.dispatchQueueCapacity = queueCapacity;
        this.availablePermits = availablePermits;
        this.maxConcurrency = maxConcurrency;
    }

    public void updateWalStats(long ringBufferUsage, long ringBufferCapacity, long recordCount) {
        this.ringBufferUsage = ringBufferUsage;
        this.ringBufferCapacity = ringBufferCapacity;
        this.walRecordCount = recordCount;
    }

    public void recordRecovery(long durationMs, long tasksRecovered) {
        recoveryDurationMs.set(durationMs);
        recoveryTasksTotal.set(tasksRecovered);
    }

    // ========== 新增指标：调度延迟和重复执行 ==========

    /**
     * 更新调度延迟
     */
    public void updateSchedulerLag(long lagMs) {
        schedulerLagMs.set(lagMs);
    }

    /**
     * 记录执行（用于计算重复率）
     */
    public void recordExecution(boolean isDuplicate) {
        totalExecutions.incrementAndGet();
        if (isDuplicate) {
            duplicateExecutions.incrementAndGet();
        }
    }

    /**
     * 获取调度延迟
     */
    public long getSchedulerLagMs() {
        return schedulerLagMs.get();
    }

    /**
     * 获取重复执行率
     */
    public double getDuplicateRate() {
        long total = totalExecutions.get();
        return total > 0 ? (double) duplicateExecutions.get() / total * 100 : 0;
    }

    // ========== P95 计算 ==========

    public long calculateP95WakeLatency() {
        return calculateP95(wakeLatencyBuckets, wakeLatencySampleCount.get());
    }

    public long calculateP95WebhookLatency() {
        return calculateP95(webhookLatencyBuckets, webhookLatencySampleCount.get());
    }

    public long calculateP95TotalLatency() {
        return calculateP95(totalLatencyBuckets, totalLatencySampleCount.get());
    }

    private long calculateP95(ConcurrentHashMap<Integer, AtomicLong> buckets, long totalSamples) {
        if (totalSamples == 0) return 0;

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

    // ========== 告警检查 ==========

    private void checkAlerts() {
        try {
            // 1. P95 唤醒延迟（系统内部调度精度）
            checkWakeLatency(calculateP95WakeLatency());

            // 2. P95 总延迟（用户可见）
            checkTotalLatency(calculateP95TotalLatency());

            // 3. Webhook 超时率
            checkTimeoutRate(getWebhookTimeoutRate());

            // 4. WAL 大小
            checkWalSize(walSizeBytes);

            // 5. 背压状态
            checkBackpressure();

        } catch (Exception e) {
            logger.error("Error checking alerts", e);
        }
    }

    private void checkWakeLatency(long p95Ms) {
        boolean shouldAlert = p95Ms > WAKE_LATENCY_P95_THRESHOLD_MS;

        if (shouldAlert && canAlert(lastWakeLatencyAlert)) {
            wakeLatencyAlertActive.set(true);
            alertLogger.warn("ALERT_WAKE_LATENCY|P95唤醒延迟超过阈值|current={}ms|threshold={}ms|说明=系统内部调度精度",
                    p95Ms, WAKE_LATENCY_P95_THRESHOLD_MS);
            lastWakeLatencyAlert.set(System.currentTimeMillis());
        } else if (!shouldAlert && wakeLatencyAlertActive.compareAndSet(true, false)) {
            alertLogger.info("ALERT_RESOLVED|P95唤醒延迟恢复正常|current={}ms", p95Ms);
        }
    }

    private void checkTotalLatency(long p95Ms) {
        boolean shouldAlert = p95Ms > TOTAL_LATENCY_P95_THRESHOLD_MS;

        if (shouldAlert && canAlert(lastTotalLatencyAlert)) {
            totalLatencyAlertActive.set(true);
            alertLogger.warn("ALERT_TOTAL_LATENCY|P95总延迟超过阈值|current={}ms|threshold={}ms|说明=用户可见端到端延迟",
                    p95Ms, TOTAL_LATENCY_P95_THRESHOLD_MS);
            lastTotalLatencyAlert.set(System.currentTimeMillis());
        } else if (!shouldAlert && totalLatencyAlertActive.compareAndSet(true, false)) {
            alertLogger.info("ALERT_RESOLVED|P95总延迟恢复正常|current={}ms", p95Ms);
        }
    }

    private void checkTimeoutRate(double rate) {
        boolean shouldAlert = rate > WEBHOOK_TIMEOUT_RATE_THRESHOLD;

        if (shouldAlert && canAlert(lastTimeoutRateAlert)) {
            timeoutRateAlertActive.set(true);
            alertLogger.warn("ALERT_WEBHOOK_TIMEOUT|Webhook超时率超过阈值|current={}%|threshold={}%",
                    String.format("%.2f", rate), WEBHOOK_TIMEOUT_RATE_THRESHOLD);
            lastTimeoutRateAlert.set(System.currentTimeMillis());
        } else if (!shouldAlert && timeoutRateAlertActive.compareAndSet(true, false)) {
            alertLogger.info("ALERT_RESOLVED|Webhook超时率恢复正常|current={}%",
                    String.format("%.2f", rate));
        }
    }

    private void checkWalSize(long sizeBytes) {
        boolean shouldAlert = sizeBytes > walSizeThresholdBytes;

        if (shouldAlert && canAlert(lastWalSizeAlert)) {
            walSizeAlertActive.set(true);
            alertLogger.warn("ALERT_WAL_SIZE|WAL大小超过阈值|current={}MB|threshold={}MB",
                    sizeBytes / 1024 / 1024, walSizeThresholdBytes / 1024 / 1024);
            lastWalSizeAlert.set(System.currentTimeMillis());
        } else if (!shouldAlert && walSizeAlertActive.compareAndSet(true, false)) {
            alertLogger.info("ALERT_RESOLVED|WAL大小恢复正常|current={}MB", sizeBytes / 1024 / 1024);
        }
    }

    private void checkBackpressure() {
        // 检查 RingBuffer 使用率
        if (ringBufferCapacity > 0) {
            double ringUsage = (double) ringBufferUsage / ringBufferCapacity * 100;

            if (ringUsage > BACKPRESSURE_THRESHOLD && canAlert(lastBackpressureAlert)) {
                backpressureAlertActive.set(true);
                alertLogger.warn("ALERT_BACKPRESSURE|RingBuffer使用率过高|current={}%|threshold={}%",
                        String.format("%.1f", ringUsage), BACKPRESSURE_THRESHOLD);
                lastBackpressureAlert.set(System.currentTimeMillis());
            } else if (ringUsage <= BACKPRESSURE_THRESHOLD * 0.8) {
                // 低于阈值 80% 时恢复
                backpressureAlertActive.compareAndSet(true, false);
            }
        }

        // 检查分发队列使用率
        if (dispatchQueueCapacity > 0) {
            double queueUsage = (double) dispatchQueueSize / dispatchQueueCapacity * 100;

            if (queueUsage > DISPATCH_QUEUE_THRESHOLD && canAlert(lastBackpressureAlert)) {
                alertLogger.warn("ALERT_DISPATCH_QUEUE|分发队列使用率过高|current={}%|threshold={}%",
                        String.format("%.1f", queueUsage), DISPATCH_QUEUE_THRESHOLD);
            }
        }
    }

    private boolean canAlert(AtomicLong lastAlertTime) {
        return System.currentTimeMillis() - lastAlertTime.get() > ALERT_COOLDOWN_MS;
    }

    // ========== 指标获取 ==========

    public double getWebhookTimeoutRate() {
        long total = webhookRequestsTotal.get();
        return total > 0 ? (double) webhookTimeoutTotal.get() / total * 100 : 0;
    }

    public double getRingBufferUsagePercent() {
        return ringBufferCapacity > 0 ? (double) ringBufferUsage / ringBufferCapacity * 100 : 0;
    }

    public double getDispatchQueueUsagePercent() {
        return dispatchQueueCapacity > 0 ? (double) dispatchQueueSize / dispatchQueueCapacity * 100 : 0;
    }

    private void refreshWalSize() {
        if (walDataDir == null) return;

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

    // ========== Prometheus 导出 ==========

    public String exportPrometheusMetrics() {
        StringBuilder sb = new StringBuilder();

        // 刷新 WAL 大小
        refreshWalSize();

        // 任务计数器
        sb.append("# HELP loomq_v2_tasks_created_total Total tasks created\n");
        sb.append("# TYPE loomq_v2_tasks_created_total counter\n");
        sb.append("loomq_v2_tasks_created_total ").append(tasksCreatedTotal.get()).append("\n\n");

        sb.append("# HELP loomq_v2_tasks_ack_total Total tasks acknowledged\n");
        sb.append("# TYPE loomq_v2_tasks_ack_total counter\n");
        sb.append("loomq_v2_tasks_ack_total ").append(tasksAckSuccessTotal.get()).append("\n\n");

        sb.append("# HELP loomq_v2_tasks_failed_total Total tasks failed\n");
        sb.append("# TYPE loomq_v2_tasks_failed_total counter\n");
        sb.append("loomq_v2_tasks_failed_total ").append(tasksFailedTerminalTotal.get()).append("\n\n");

        sb.append("# HELP loomq_v2_tasks_cancelled_total Total tasks cancelled\n");
        sb.append("# TYPE loomq_v2_tasks_cancelled_total counter\n");
        sb.append("loomq_v2_tasks_cancelled_total ").append(tasksCancelledTotal.get()).append("\n\n");

        // Webhook 指标
        sb.append("# HELP loomq_v2_webhook_requests_total Total webhook requests\n");
        sb.append("# TYPE loomq_v2_webhook_requests_total counter\n");
        sb.append("loomq_v2_webhook_requests_total ").append(webhookRequestsTotal.get()).append("\n\n");

        sb.append("# HELP loomq_v2_webhook_timeout_total Total webhook timeouts\n");
        sb.append("# TYPE loomq_v2_webhook_timeout_total counter\n");
        sb.append("loomq_v2_webhook_timeout_total ").append(webhookTimeoutTotal.get()).append("\n\n");

        sb.append("# HELP loomq_v2_webhook_timeout_rate_percent Webhook timeout rate\n");
        sb.append("# TYPE loomq_v2_webhook_timeout_rate_percent gauge\n");
        sb.append(String.format("loomq_v2_webhook_timeout_rate_percent %.2f\n\n", getWebhookTimeoutRate()));

        // 延迟指标
        sb.append("# HELP loomq_v2_wake_latency_ms_p95 P95 wake latency (internal scheduling)\n");
        sb.append("# TYPE loomq_v2_wake_latency_ms_p95 gauge\n");
        sb.append("loomq_v2_wake_latency_ms_p95 ").append(calculateP95WakeLatency()).append("\n\n");

        sb.append("# HELP loomq_v2_webhook_latency_ms_p95 P95 webhook latency\n");
        sb.append("# TYPE loomq_v2_webhook_latency_ms_p95 gauge\n");
        sb.append("loomq_v2_webhook_latency_ms_p95 ").append(calculateP95WebhookLatency()).append("\n\n");

        sb.append("# HELP loomq_v2_total_latency_ms_p95 P95 end-to-end latency (user visible)\n");
        sb.append("# TYPE loomq_v2_total_latency_ms_p95 gauge\n");
        sb.append("loomq_v2_total_latency_ms_p95 ").append(calculateP95TotalLatency()).append("\n\n");

        // V2 特有指标
        sb.append("# HELP loomq_v2_bucket_count Number of time buckets\n");
        sb.append("# TYPE loomq_v2_bucket_count gauge\n");
        sb.append("loomq_v2_bucket_count ").append(bucketCount).append("\n\n");

        sb.append("# HELP loomq_v2_ready_queue_size Ready queue size\n");
        sb.append("# TYPE loomq_v2_ready_queue_size gauge\n");
        sb.append("loomq_v2_ready_queue_size ").append(readyQueueSize).append("\n\n");

        sb.append("# HELP loomq_v2_dispatch_queue_size Dispatch queue size\n");
        sb.append("# TYPE loomq_v2_dispatch_queue_size gauge\n");
        sb.append("loomq_v2_dispatch_queue_size ").append(dispatchQueueSize).append("\n\n");

        sb.append("# HELP loomq_v2_dispatch_queue_usage_percent Dispatch queue usage\n");
        sb.append("# TYPE loomq_v2_dispatch_queue_usage_percent gauge\n");
        sb.append(String.format("loomq_v2_dispatch_queue_usage_percent %.1f\n\n", getDispatchQueueUsagePercent()));

        sb.append("# HELP loomq_v2_available_permits Available concurrency permits\n");
        sb.append("# TYPE loomq_v2_available_permits gauge\n");
        sb.append("loomq_v2_available_permits ").append(availablePermits).append("\n\n");

        sb.append("# HELP loomq_v2_max_concurrency Max concurrency\n");
        sb.append("# TYPE loomq_v2_max_concurrency gauge\n");
        sb.append("loomq_v2_max_concurrency ").append(maxConcurrency).append("\n\n");

        sb.append("# HELP loomq_v2_ringbuffer_usage_percent RingBuffer usage\n");
        sb.append("# TYPE loomq_v2_ringbuffer_usage_percent gauge\n");
        sb.append(String.format("loomq_v2_ringbuffer_usage_percent %.1f\n\n", getRingBufferUsagePercent()));

        // WAL 指标
        sb.append("# HELP loomq_v2_wal_size_bytes WAL size in bytes\n");
        sb.append("# TYPE loomq_v2_wal_size_bytes gauge\n");
        sb.append("loomq_v2_wal_size_bytes ").append(walSizeBytes).append("\n\n");

        sb.append("# HELP loomq_v2_wal_segment_count WAL segment count\n");
        sb.append("# TYPE loomq_v2_wal_segment_count gauge\n");
        sb.append("loomq_v2_wal_segment_count ").append(walSegmentCount).append("\n\n");

        sb.append("# HELP loomq_v2_wal_record_count WAL record count\n");
        sb.append("# TYPE loomq_v2_wal_record_count counter\n");
        sb.append("loomq_v2_wal_record_count ").append(walRecordCount).append("\n\n");

        // 恢复指标
        sb.append("# HELP loomq_v2_recovery_duration_ms Recovery duration\n");
        sb.append("# TYPE loomq_v2_recovery_duration_ms gauge\n");
        sb.append("loomq_v2_recovery_duration_ms ").append(recoveryDurationMs.get()).append("\n\n");

        sb.append("# HELP loomq_v2_recovery_tasks_total Tasks recovered\n");
        sb.append("# TYPE loomq_v2_recovery_tasks_total counter\n");
        sb.append("loomq_v2_recovery_tasks_total ").append(recoveryTasksTotal.get()).append("\n\n");

        // 新增指标：调度延迟和重复执行率
        sb.append("# HELP loomq_v2_scheduler_lag_ms Scheduler lag (time since earliest pending task)\n");
        sb.append("# TYPE loomq_v2_scheduler_lag_ms gauge\n");
        sb.append("loomq_v2_scheduler_lag_ms ").append(schedulerLagMs.get()).append("\n\n");

        sb.append("# HELP loomq_v2_duplicate_executions_total Total duplicate task executions\n");
        sb.append("# TYPE loomq_v2_duplicate_executions_total counter\n");
        sb.append("loomq_v2_duplicate_executions_total ").append(duplicateExecutions.get()).append("\n\n");

        sb.append("# HELP loomq_v2_total_executions_total Total task executions\n");
        sb.append("# TYPE loomq_v2_total_executions_total counter\n");
        sb.append("loomq_v2_total_executions_total ").append(totalExecutions.get()).append("\n\n");

        sb.append("# HELP loomq_v2_duplicate_rate_percent Duplicate execution rate\n");
        sb.append("# TYPE loomq_v2_duplicate_rate_percent gauge\n");
        sb.append(String.format("loomq_v2_duplicate_rate_percent %.2f\n\n", getDuplicateRate()));

        // 告警状态
        sb.append("# HELP loomq_v2_alert_active Alert active status\n");
        sb.append("# TYPE loomq_v2_alert_active gauge\n");
        sb.append("loomq_v2_alert_active{type=\"wake_latency\"} ").append(wakeLatencyAlertActive.get() ? 1 : 0).append("\n");
        sb.append("loomq_v2_alert_active{type=\"total_latency\"} ").append(totalLatencyAlertActive.get() ? 1 : 0).append("\n");
        sb.append("loomq_v2_alert_active{type=\"webhook_timeout\"} ").append(timeoutRateAlertActive.get() ? 1 : 0).append("\n");
        sb.append("loomq_v2_alert_active{type=\"wal_size\"} ").append(walSizeAlertActive.get() ? 1 : 0).append("\n");
        sb.append("loomq_v2_alert_active{type=\"backpressure\"} ").append(backpressureAlertActive.get() ? 1 : 0).append("\n");

        return sb.toString();
    }

    // ========== 告警状态 ==========

    public AlertStatus getAlertStatus() {
        return new AlertStatus(
                wakeLatencyAlertActive.get(),
                totalLatencyAlertActive.get(),
                timeoutRateAlertActive.get(),
                walSizeAlertActive.get(),
                backpressureAlertActive.get(),
                WAKE_LATENCY_P95_THRESHOLD_MS,
                TOTAL_LATENCY_P95_THRESHOLD_MS,
                WEBHOOK_TIMEOUT_RATE_THRESHOLD,
                walSizeThresholdBytes,
                BACKPRESSURE_THRESHOLD
        );
    }

    public record AlertStatus(
            boolean wakeLatencyAlert,
            boolean totalLatencyAlert,
            boolean timeoutRateAlert,
            boolean walSizeAlert,
            boolean backpressureAlert,
            long wakeLatencyThresholdMs,
            long totalLatencyThresholdMs,
            double timeoutRateThreshold,
            long walSizeThresholdBytes,
            double backpressureThreshold
    ) {}
}

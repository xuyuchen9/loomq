package com.loomq.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 告警服务
 * 监控关键指标并在超过阈值时触发告警
 */
public class AlertService {
    private static final Logger logger = LoggerFactory.getLogger(AlertService.class);
    private static final Logger alertLogger = LoggerFactory.getLogger("ALERT");

    private static final AlertService INSTANCE = new AlertService();

    // 告警阈值 (来自需求文档冻结值)
    // 唤醒延迟：系统内部调度精度，应该在 50ms 以内
    private static final long WAKE_LATENCY_P95_THRESHOLD_MS = 50;
    // 总延迟：用户可见的端到端延迟，告警阈值 5s
    private static final long TOTAL_LATENCY_P95_THRESHOLD_MS = 5000;
    private static final double WEBHOOK_TIMEOUT_RATE_THRESHOLD = 5.0;   // webhook 超时率 > 5%
    private long walSizeThresholdBytes = 1024 * 1024 * 1024;            // WAL 大小 > 1GB (默认)

    // 告警冷却期 (避免重复告警)
    private static final long ALERT_COOLDOWN_MS = 60000; // 1 分钟
    private final AtomicLong lastWakeLatencyAlert = new AtomicLong(0);
    private final AtomicLong lastTotalLatencyAlert = new AtomicLong(0);
    private final AtomicLong lastTimeoutRateAlert = new AtomicLong(0);
    private final AtomicLong lastWalSizeAlert = new AtomicLong(0);

    // 告警状态
    private final AtomicBoolean wakeLatencyAlertActive = new AtomicBoolean(false);
    private final AtomicBoolean totalLatencyAlertActive = new AtomicBoolean(false);
    private final AtomicBoolean timeoutRateAlertActive = new AtomicBoolean(false);
    private final AtomicBoolean walSizeAlertActive = new AtomicBoolean(false);

    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;

    private AlertService() {
    }

    public static AlertService getInstance() {
        return INSTANCE;
    }

    /**
     * 设置 WAL 大小告警阈值
     */
    public void setWalSizeThreshold(long bytes) {
        this.walSizeThresholdBytes = bytes;
    }

    /**
     * 启动告警检查
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "alert-checker");
            t.setDaemon(true);
            return t;
        });

        // 每 10 秒检查一次
        scheduler.scheduleAtFixedRate(this::checkAlerts, 10, 10, TimeUnit.SECONDS);

        logger.info("Alert service started, checking every 10s");
    }

    /**
     * 停止告警检查
     */
    public void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdown();
        }
        logger.info("Alert service stopped");
    }

    /**
     * 检查所有告警条件
     */
    private void checkAlerts() {
        try {
            MetricsCollector metrics = MetricsCollector.getInstance();

            // 检查 P95 唤醒延迟 (系统内部调度精度)
            checkWakeLatency(metrics.calculateP95WakeLatency());

            // 检查 P95 总延迟 (用户可见)
            checkTotalLatency(metrics.calculateP95TotalLatency());

            // 检查 webhook 超时率
            checkTimeoutRate(metrics.getWebhookTimeoutRate());

            // 检查 WAL 大小
            checkWalSize(metrics.getWalSizeBytes());

        } catch (Exception e) {
            logger.error("Error checking alerts", e);
        }
    }

    /**
     * 检查 P95 唤醒延迟 (系统内部调度精度)
     */
    private void checkWakeLatency(long p95LatencyMs) {
        boolean shouldAlert = p95LatencyMs > WAKE_LATENCY_P95_THRESHOLD_MS;

        if (shouldAlert && canAlert(lastWakeLatencyAlert)) {
            wakeLatencyAlertActive.set(true);
            alertLogger.warn("ALERT_WAKE_LATENCY|P95唤醒延迟超过阈值|current={}ms|threshold={}ms|说明=系统内部调度精度",
                    p95LatencyMs, WAKE_LATENCY_P95_THRESHOLD_MS);
            lastWakeLatencyAlert.set(System.currentTimeMillis());
        } else if (!shouldAlert) {
            if (wakeLatencyAlertActive.compareAndSet(true, false)) {
                alertLogger.info("ALERT_RESOLVED|P95唤醒延迟恢复正常|current={}ms", p95LatencyMs);
            }
        }
    }

    /**
     * 检查 P95 总延迟 (用户可见的端到端延迟)
     */
    private void checkTotalLatency(long p95LatencyMs) {
        boolean shouldAlert = p95LatencyMs > TOTAL_LATENCY_P95_THRESHOLD_MS;

        if (shouldAlert && canAlert(lastTotalLatencyAlert)) {
            totalLatencyAlertActive.set(true);
            alertLogger.warn("ALERT_TOTAL_LATENCY|P95总延迟超过阈值|current={}ms|threshold={}ms|说明=用户可见端到端延迟",
                    p95LatencyMs, TOTAL_LATENCY_P95_THRESHOLD_MS);
            lastTotalLatencyAlert.set(System.currentTimeMillis());
        } else if (!shouldAlert) {
            if (totalLatencyAlertActive.compareAndSet(true, false)) {
                alertLogger.info("ALERT_RESOLVED|P95总延迟恢复正常|current={}ms", p95LatencyMs);
            }
        }
    }

    /**
     * 检查 webhook 超时率
     */
    private void checkTimeoutRate(double timeoutRate) {
        boolean shouldAlert = timeoutRate > WEBHOOK_TIMEOUT_RATE_THRESHOLD;

        if (shouldAlert && canAlert(lastTimeoutRateAlert)) {
            timeoutRateAlertActive.set(true);
            alertLogger.warn("ALERT_WEBHOOK_TIMEOUT|Webhook超时率超过阈值|current={}%|threshold={}%",
                    String.format("%.2f", timeoutRate), WEBHOOK_TIMEOUT_RATE_THRESHOLD);
            lastTimeoutRateAlert.set(System.currentTimeMillis());
        } else if (!shouldAlert) {
            if (timeoutRateAlertActive.compareAndSet(true, false)) {
                alertLogger.info("ALERT_RESOLVED|Webhook超时率恢复正常|current={}%",
                        String.format("%.2f", timeoutRate));
            }
        }
    }

    /**
     * 检查 WAL 大小
     */
    private void checkWalSize(long walSizeBytes) {
        boolean shouldAlert = walSizeBytes > walSizeThresholdBytes;

        if (shouldAlert && canAlert(lastWalSizeAlert)) {
            walSizeAlertActive.set(true);
            alertLogger.warn("ALERT_WAL_SIZE|WAL大小超过阈值|current={}bytes|threshold={}bytes|currentMB={}MB|thresholdMB={}MB",
                    walSizeBytes, walSizeThresholdBytes,
                    walSizeBytes / 1024 / 1024, walSizeThresholdBytes / 1024 / 1024);
            lastWalSizeAlert.set(System.currentTimeMillis());
        } else if (!shouldAlert) {
            if (walSizeAlertActive.compareAndSet(true, false)) {
                alertLogger.info("ALERT_RESOLVED|WAL大小恢复正常|current={}bytes", walSizeBytes);
            }
        }
    }

    /**
     * 检查是否可以发送告警 (冷却期检查)
     */
    private boolean canAlert(AtomicLong lastAlertTime) {
        long last = lastAlertTime.get();
        return System.currentTimeMillis() - last > ALERT_COOLDOWN_MS;
    }

    /**
     * 获取告警状态
     */
    public AlertStatus getStatus() {
        return new AlertStatus(
                wakeLatencyAlertActive.get(),
                totalLatencyAlertActive.get(),
                timeoutRateAlertActive.get(),
                walSizeAlertActive.get(),
                WAKE_LATENCY_P95_THRESHOLD_MS,
                TOTAL_LATENCY_P95_THRESHOLD_MS,
                WEBHOOK_TIMEOUT_RATE_THRESHOLD,
                walSizeThresholdBytes
        );
    }

    public record AlertStatus(
            boolean wakeLatencyAlert,
            boolean totalLatencyAlert,
            boolean timeoutRateAlert,
            boolean walSizeAlert,
            long wakeLatencyThresholdMs,
            long totalLatencyThresholdMs,
            double timeoutRateThreshold,
            long walSizeThresholdBytes
    ) {}
}

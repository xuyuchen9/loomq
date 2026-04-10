package com.loomq.scheduler.v5;

import com.loomq.common.MetricsCollector;
import com.loomq.entity.v5.Intent;
import com.loomq.entity.v5.IntentStatus;
import com.loomq.entity.v5.PrecisionTier;
import com.loomq.spi.DeliveryContext;
import com.loomq.spi.DefaultRedeliveryDecider;
import com.loomq.spi.RedeliveryDecider;
import com.loomq.store.IntentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.*;

/**
 * 精度调度器 (v0.5.1)
 *
 * 支持多精度档位的任务调度，每个档位独立的扫描线程。
 * 核心架构：虚拟线程独立休眠 + 分层 Bucket 唤醒。
 *
 * @author loomq
 * @since v0.5.1
 */
public class PrecisionScheduler {

    private static final Logger logger = LoggerFactory.getLogger(PrecisionScheduler.class);

    private final IntentStore intentStore;
    private final BucketGroupManager bucketGroupManager;
    private final HttpClient httpClient;
    private final RedeliveryDecider redeliveryDecider;
    private final ExecutorService dispatchExecutor;
    private final ExecutorService sleepExecutor;

    // 按精度档位的扫描任务调度器
    private final Map<PrecisionTier, ScheduledExecutorService> scanSchedulers;
    private final Map<PrecisionTier, ScheduledFuture<?>> scanTasks;

    private volatile boolean running = false;
    private volatile boolean paused = false;

    // Metrics
    private final MetricsCollector metrics = MetricsCollector.getInstance();

    public PrecisionScheduler(IntentStore intentStore) {
        this.intentStore = intentStore;
        this.bucketGroupManager = new BucketGroupManager();
        this.scanSchedulers = new ConcurrentHashMap<>();
        this.scanTasks = new ConcurrentHashMap<>();

        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

        ServiceLoader<RedeliveryDecider> loader = ServiceLoader.load(RedeliveryDecider.class);
        this.redeliveryDecider = loader.findFirst().orElseGet(DefaultRedeliveryDecider::new);

        this.dispatchExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.sleepExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * 启动调度器
     */
    public void start() {
        if (running) return;
        running = true;

        logger.info("PrecisionScheduler starting...");

        // 为每个精度档位启动独立的扫描任务
        for (PrecisionTier tier : PrecisionTier.values()) {
            startScanTask(tier);
        }

        logger.info("PrecisionScheduler started with {} precision tiers", PrecisionTier.values().length);
    }

    /**
     * 启动指定精度档位的扫描任务
     */
    private void startScanTask(PrecisionTier tier) {
        long intervalMs = tier.getPrecisionWindowMs();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "scan-" + tier.name().toLowerCase());
            t.setDaemon(true);
            return t;
        });
        scanSchedulers.put(tier, scheduler);

        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(
            () -> scanAndDispatch(tier),
            intervalMs,  // 初始延迟
            intervalMs,  // 扫描间隔
            TimeUnit.MILLISECONDS
        );

        scanTasks.put(tier, future);
        logger.info("Started scan task for tier {} with interval {}ms", tier, intervalMs);
    }

    /**
     * 停止调度器
     */
    public void stop() {
        running = false;

        // 取消所有扫描任务
        for (ScheduledFuture<?> future : scanTasks.values()) {
            future.cancel(false);
        }
        scanTasks.clear();

        // 关闭所有扫描调度器
        for (ScheduledExecutorService scheduler : scanSchedulers.values()) {
            scheduler.shutdown();
        }
        scanSchedulers.clear();

        dispatchExecutor.shutdown();
        sleepExecutor.shutdown();

        try {
            if (!dispatchExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                dispatchExecutor.shutdownNow();
            }
            if (!sleepExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                sleepExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            dispatchExecutor.shutdownNow();
            sleepExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("PrecisionScheduler stopped");
    }

    /**
     * 暂停调度器
     */
    public void pause() {
        if (!paused) {
            paused = true;
            logger.info("PrecisionScheduler paused");
        }
    }

    /**
     * 恢复调度器
     */
    public void resume() {
        if (paused) {
            paused = false;
            logger.info("PrecisionScheduler resumed");
        }
    }

    public boolean isPaused() {
        return paused;
    }

    /**
     * 调度 Intent
     *
     * 根据 executeAt 和 precisionTier 计算休眠时间，然后添加到对应桶。
     *
     * @param intent Intent 实例
     */
    public void schedule(Intent intent) {
        if (intent.getStatus() != IntentStatus.CREATED) {
            logger.warn("Cannot schedule intent {} with status {}",
                intent.getIntentId(), intent.getStatus());
            return;
        }

        Instant executeAt = intent.getExecuteAt();
        Instant now = Instant.now();
        long delayMs = Duration.between(now, executeAt).toMillis();

        PrecisionTier tier = intent.getPrecisionTier();
        BucketGroup group = bucketGroupManager.getBucketGroup(tier);

        if (delayMs <= 0) {
            // 已到期，直接投递
            addToBucketAndDispatch(intent);
        } else {
            // 计算休眠时间并异步等待
            long sleepMs = group.calculateSleepMs(delayMs);

            if (sleepMs > 0) {
                // 长延迟：先休眠
                sleepExecutor.submit(() -> {
                    try {
                        Thread.sleep(sleepMs);
                        addToBucketAndDispatch(intent);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.debug("Sleep interrupted for intent {}", intent.getIntentId());
                    }
                });
            } else {
                // 短延迟：直接入桶
                addToBucketAndDispatch(intent);
            }
        }

        // 更新状态
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.update(intent);

        logger.debug("Scheduled intent {} with tier {}, delay {}ms, sleep {}ms",
            intent.getIntentId(), tier, delayMs,
            delayMs <= 0 ? 0 : Math.max(0, delayMs - tier.getPrecisionWindowMs()));
    }

    /**
     * 添加到桶并等待调度
     */
    private void addToBucketAndDispatch(Intent intent) {
        bucketGroupManager.add(intent);
    }

    /**
     * 扫描并投递指定精度档位的到期任务
     */
    private void scanAndDispatch(PrecisionTier tier) {
        if (paused) return;

        long startTime = System.nanoTime();
        Instant now = Instant.now();

        try {
            List<Intent> dueIntents = bucketGroupManager.scanDue(tier, now);

            if (!dueIntents.isEmpty()) {
                logger.debug("Found {} due intents for tier {}", dueIntents.size(), tier);

                // 记录指标
                for (int i = 0; i < dueIntents.size(); i++) {
                    metrics.incrementIntentDueByTier(tier);
                }

                for (Intent intent : dueIntents) {
                    // 记录唤醒延迟
                    recordWakeupLatency(intent, now);

                    // 提交投递任务
                    dispatchExecutor.submit(() -> dispatch(intent));
                }
            }

            // 更新桶大小指标
            metrics.updateBucketSizeByTier(tier, bucketGroupManager.getBucketGroup(tier).getPendingCount());

            // 检查过期任务
            checkExpiredIntents(tier);

        } catch (Exception e) {
            logger.error("Error scanning tier {}", tier, e);
        }

        // 记录扫描耗时
        long durationMs = (System.nanoTime() - startTime) / 1_000_000;
        metrics.recordScanDurationByTier(tier, durationMs);
    }

    /**
     * 记录唤醒延迟
     */
    private void recordWakeupLatency(Intent intent, Instant actualTime) {
        Instant executeAt = intent.getExecuteAt();
        long latencyMs = Duration.between(executeAt, actualTime).toMillis();

        metrics.recordWakeupLatencyByTier(intent.getPrecisionTier(), latencyMs);
    }

    /**
     * 检查过期任务
     */
    private void checkExpiredIntents(PrecisionTier tier) {
        // 从 IntentStore 检查该档位的过期任务
        for (Intent intent : intentStore.getAllIntents().values()) {
            if (intent.getPrecisionTier() != tier) continue;

            if ((intent.getStatus() == IntentStatus.DUE ||
                 intent.getStatus() == IntentStatus.DELIVERED) &&
                intent.isExpired()) {
                handleExpired(intent);
            }
        }
    }

    /**
     * 投递 Intent
     */
    private void dispatch(Intent intent) {
        String deliveryId = generateDeliveryId(intent);
        int attempt = intent.getAttempts() + 1;

        DeliveryContext context = new DeliveryContext(deliveryId, intent.getIntentId(), attempt);

        try {
            intent.transitionTo(IntentStatus.DUE);
            intent.transitionTo(IntentStatus.DISPATCHING);
            intent.incrementAttempts();
            intent.setLastDeliveryId(deliveryId);
            intentStore.update(intent);

            HttpRequest request = buildHttpRequest(intent);
            HttpResponse<String> response = httpClient.send(request,
                HttpResponse.BodyHandlers.ofString());

            context.markSuccess(response.statusCode(), Map.of(), response.body());

            if (redeliveryDecider.shouldRedeliver(context)) {
                scheduleRedelivery(intent);
            } else if (response.statusCode() >= 200 && response.statusCode() < 300) {
                intent.transitionTo(IntentStatus.DELIVERED);
                intentStore.update(intent);

                intent.transitionTo(IntentStatus.ACKED);
                intentStore.update(intent);

                logger.debug("Intent {} delivered successfully", intent.getIntentId());
            } else {
                handleDeliveryFailure(intent);
            }

        } catch (Exception e) {
            context.markFailure(e);

            if (redeliveryDecider.shouldRedeliver(context)) {
                scheduleRedelivery(intent);
            } else {
                handleDeliveryFailure(intent);
            }
        }
    }

    /**
     * 构建 HTTP 请求
     */
    private HttpRequest buildHttpRequest(Intent intent) {
        HttpRequest.BodyPublisher bodyPublisher;
        if (intent.getCallback().getBody() != null) {
            bodyPublisher = HttpRequest.BodyPublishers.ofString(
                intent.getCallback().getBody().toString());
        } else {
            bodyPublisher = HttpRequest.BodyPublishers.noBody();
        }

        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(intent.getCallback().getUrl()))
            .timeout(Duration.ofSeconds(30))
            .header("Content-Type", "application/json")
            .header("X-LoomQ-Intent-Id", intent.getIntentId());

        if (intent.getCallback().getHeaders() != null) {
            intent.getCallback().getHeaders().forEach(builder::header);
        }

        String method = intent.getCallback().getMethod();
        if (method == null || "POST".equalsIgnoreCase(method)) {
            builder.POST(bodyPublisher);
        } else if ("PUT".equalsIgnoreCase(method)) {
            builder.PUT(bodyPublisher);
        } else if ("PATCH".equalsIgnoreCase(method)) {
            builder.method("PATCH", bodyPublisher);
        } else {
            builder.POST(bodyPublisher);
        }

        return builder.build();
    }

    /**
     * 调度重投
     */
    private void scheduleRedelivery(Intent intent) {
        long delayMs = intent.getRedelivery() != null
            ? intent.getRedelivery().calculateDelay(intent.getAttempts())
            : 5000;

        logger.info("Scheduling redelivery for intent={}, attempt={}, delay={}ms",
            intent.getIntentId(), intent.getAttempts(), delayMs);

        intent.setExecuteAt(Instant.now().plusMillis(delayMs));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.update(intent);

        // 重新调度
        schedule(intent);
    }

    /**
     * 处理投递失败
     */
    private void handleDeliveryFailure(Intent intent) {
        int maxAttempts = intent.getRedelivery() != null
            ? intent.getRedelivery().getMaxAttempts()
            : 5;

        if (intent.getAttempts() >= maxAttempts) {
            intent.transitionTo(IntentStatus.DEAD_LETTERED);
            logger.warn("Intent dead-lettered after max attempts: id={}", intent.getIntentId());
        } else {
            intent.transitionTo(IntentStatus.DEAD_LETTERED);
        }
        intentStore.update(intent);
    }

    /**
     * 处理过期任务
     */
    private void handleExpired(Intent intent) {
        logger.info("Intent expired: id={}, deadline={}", intent.getIntentId(), intent.getDeadline());

        switch (intent.getExpiredAction()) {
            case DISCARD:
                intent.transitionTo(IntentStatus.EXPIRED);
                break;
            case DEAD_LETTER:
                intent.transitionTo(IntentStatus.DEAD_LETTERED);
                break;
        }
        intentStore.update(intent);
    }

    /**
     * 生成投递 ID
     */
    private String generateDeliveryId(Intent intent) {
        return "delivery_" + intent.getIntentId() + "_" + intent.getAttempts();
    }

    /**
     * 获取桶组管理器
     */
    public BucketGroupManager getBucketGroupManager() {
        return bucketGroupManager;
    }
}

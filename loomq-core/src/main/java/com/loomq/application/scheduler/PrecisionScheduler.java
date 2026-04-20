package com.loomq.application.scheduler;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.DefaultRedeliveryDecider;
import com.loomq.spi.DeliveryContext;
import com.loomq.spi.RedeliveryDecider;
import com.loomq.store.IntentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 精度调度器 (v0.5.1)
 *
 * 支持多精度档位的任务调度，每个档位独立的扫描线程。
 * 核心架构：虚拟线程独立休眠 + 分层 Bucket 唤醒。
 *
 * v0.7.1: 迁移至 loomq-core，使用 DeliveryHandler SPI 接口投递
 *
 * @author loomq
 * @since v0.5.1
 */
public class PrecisionScheduler {

    private static final Logger logger = LoggerFactory.getLogger(PrecisionScheduler.class);

    private final IntentStore intentStore;
    private final BucketGroupManager bucketGroupManager;
    private final DeliveryHandler deliveryHandler;
    private final RedeliveryDecider redeliveryDecider;

    // 共享虚拟线程池（所有档位共享）
    private final ExecutorService sharedExecutor;

    // 档位级并发控制（信号量）
    private final Map<PrecisionTier, Semaphore> tierSemaphores;

    // 档位级批量队列（使用LinkedBlockingDeque支持队首插入和阻塞操作）
    private final Map<PrecisionTier, LinkedBlockingDeque<Intent>> tierDispatchQueues;

    // 按精度档位的扫描任务调度器
    private final Map<PrecisionTier, ScheduledExecutorService> scanSchedulers;
    private final Map<PrecisionTier, ScheduledFuture<?>> scanTasks;

    private volatile boolean running = false;
    private volatile boolean paused = false;

    // Metrics
    private final MetricsCollector metrics = MetricsCollector.getInstance();

    /**
     * 创建调度器（使用默认投递处理器）
     *
     * @param intentStore Intent 存储
     */
    public PrecisionScheduler(IntentStore intentStore) {
        this(intentStore, null, null);
    }

    /**
     * 创建调度器（指定投递处理器）
     *
     * @param intentStore     Intent 存储
     * @param deliveryHandler 投递处理器（null 则使用 ServiceLoader 加载）
     */
    public PrecisionScheduler(IntentStore intentStore, DeliveryHandler deliveryHandler) {
        this(intentStore, deliveryHandler, null);
    }

    /**
     * 创建调度器（完整参数）
     *
     * @param intentStore       Intent 存储
     * @param deliveryHandler   投递处理器（null 则使用 ServiceLoader 加载）
     * @param redeliveryDecider 重投决策器（null 则使用默认）
     */
    public PrecisionScheduler(IntentStore intentStore, DeliveryHandler deliveryHandler, RedeliveryDecider redeliveryDecider) {
        this.intentStore = intentStore;
        this.bucketGroupManager = new BucketGroupManager();
        this.scanSchedulers = new ConcurrentHashMap<>();
        this.scanTasks = new ConcurrentHashMap<>();

        // 加载投递处理器
        if (deliveryHandler != null) {
            this.deliveryHandler = deliveryHandler;
        } else {
            ServiceLoader<DeliveryHandler> handlerLoader = ServiceLoader.load(DeliveryHandler.class);
            this.deliveryHandler = handlerLoader.findFirst().orElse(null);
        }

        // 加载重投决策器
        if (redeliveryDecider != null) {
            this.redeliveryDecider = redeliveryDecider;
        } else {
            ServiceLoader<RedeliveryDecider> deciderLoader = ServiceLoader.load(RedeliveryDecider.class);
            this.redeliveryDecider = deciderLoader.findFirst().orElseGet(DefaultRedeliveryDecider::new);
        }

        // 初始化共享虚拟线程池
        this.sharedExecutor = Executors.newVirtualThreadPerTaskExecutor();

        // 初始化档位级信号量和队列
        this.tierSemaphores = new EnumMap<>(PrecisionTier.class);
        this.tierDispatchQueues = new EnumMap<>(PrecisionTier.class);

        for (PrecisionTier tier : PrecisionTier.values()) {
            tierSemaphores.put(tier, new Semaphore(tier.getMaxConcurrency()));
            tierDispatchQueues.put(tier, new LinkedBlockingDeque<>());
        }

        if (this.deliveryHandler == null) {
            logger.warn("No DeliveryHandler configured - intents will not be delivered!");
        }
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

        // 启动档位级批量消费者
        for (PrecisionTier tier : PrecisionTier.values()) {
            startBatchConsumers(tier);
        }

        logger.info("PrecisionScheduler started with {} precision tiers", PrecisionTier.values().length);
    }

    /**
     * 启动指定档位的批量消费者
     */
    private void startBatchConsumers(PrecisionTier tier) {
        int consumerCount = tier.getConsumerCount();

        for (int i = 0; i < consumerCount; i++) {
            final int consumerId = i;
            sharedExecutor.submit(() -> {
                Thread.currentThread().setName("batch-consumer-" + tier.name().toLowerCase() + "-" + consumerId);
                runBatchConsumer(tier);
            });
        }

        logger.info("Started {} batch consumers for tier {}", consumerCount, tier);
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

        // 关闭共享虚拟线程池
        sharedExecutor.shutdown();
        try {
            if (!sharedExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                sharedExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            sharedExecutor.shutdownNow();
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
        // 接受 CREATED 或 SCHEDULED 状态的任务
        if (intent.getStatus() != IntentStatus.CREATED && intent.getStatus() != IntentStatus.SCHEDULED) {
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
            Instant scheduledExecuteAt = intent.getExecuteAt();

            if (sleepMs > 0) {
                // 长延迟：先休眠（使用共享虚拟线程池）
                sharedExecutor.submit(() -> {
                    try {
                        Thread.sleep(sleepMs);
                        // 期间若 intent 已被重定向、取消或重新调度，则跳过旧任务
                        if (!scheduledExecuteAt.equals(intent.getExecuteAt()) ||
                            intent.getStatus().isTerminal()) {
                            logger.debug("Skip stale schedule for intent {}", intent.getIntentId());
                            return;
                        }
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
     * 从调度桶中移除 Intent。
     *
     * @param intent Intent 实例
     */
    public void unschedule(Intent intent) {
        bucketGroupManager.remove(intent);
    }

    /**
     * 恢复 Intent 到调度桶。
     *
     * 恢复路径会直接重建桶状态，不走创建时的状态机约束。
     */
    public void restore(Intent intent) {
        if (intent == null || intent.getExecuteAt() == null || intent.getStatus().isTerminal()) {
            return;
        }

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

                    // 提交到档位队列
                    LinkedBlockingDeque<Intent> queue = tierDispatchQueues.get(tier);
                    if (!queue.offerLast(intent)) {
                        metrics.incrementBackpressureEvent(tier);
                        logger.warn("Backpressure triggered for tier {}: queue full", tier);
                    }
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
     * 批量消费者循环
     */
    private void runBatchConsumer(PrecisionTier tier) {
        LinkedBlockingDeque<Intent> queue = tierDispatchQueues.get(tier);
        Semaphore semaphore = tierSemaphores.get(tier);
        int batchSize = tier.getBatchSize();
        int batchWindowMs = tier.getBatchWindowMs();

        while (running) {
            try {
                Intent first = queue.poll(batchWindowMs, TimeUnit.MILLISECONDS);
                if (first == null) continue;

                // 批量攒批
                List<Intent> batch = new ArrayList<>(batchSize);
                batch.add(first);
                for (int i = 1; i < batchSize; i++) {
                    Intent next = queue.pollFirst();
                    if (next == null) break;
                    batch.add(next);
                }

                // 申请信号量
                int permitsToAcquire = batch.size();
                boolean acquired = false;

                while (permitsToAcquire > 0) {
                    acquired = semaphore.tryAcquire(permitsToAcquire);
                    if (acquired) break;
                    permitsToAcquire--;
                }

                if (!acquired) {
                    metrics.incrementBackpressureEvent(tier);
                    logger.warn("Backpressure for tier {}: semaphore exhausted, batch {} tasks dropped",
                        tier, batch.size());
                    Thread.sleep(100);
                    continue;
                }

                // 退回多余任务
                if (permitsToAcquire < batch.size()) {
                    List<Intent> toDispatch = new ArrayList<>(batch.subList(0, permitsToAcquire));
                    List<Intent> toReturn = batch.subList(permitsToAcquire, batch.size());

                    for (int i = toReturn.size() - 1; i >= 0; i--) {
                        Intent intent = toReturn.get(i);
                        if (!queue.offerFirst(intent)) {
                            logger.debug("Failed to put intent {} back to queue", intent.getIntentId());
                        }
                    }
                    batch = toDispatch;
                }

                // 并行发送批次
                final List<Intent> finalBatch = batch;

                List<CompletableFuture<Void>> futures = finalBatch.stream()
                    .map(intent -> CompletableFuture.runAsync(() -> {
                        try {
                            dispatchWithMetrics(intent, tier);
                        } catch (Exception e) {
                            logger.error("Dispatch failed for intent {}: {}",
                                intent.getIntentId(), e.getMessage());
                        } finally {
                            semaphore.release();
                        }
                    }, sharedExecutor))
                    .toList();

                // 等待整批完成
                try {
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .orTimeout(30, TimeUnit.SECONDS)
                        .join();
                } catch (Exception e) {
                    logger.warn("Batch dispatch timeout or error for tier {}: {}", tier, e.getMessage());
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in batch consumer for tier {}", tier, e);
            }
        }
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
        if (deliveryHandler == null) {
            logger.warn("No DeliveryHandler configured, cannot deliver intent {}", intent.getIntentId());
            return;
        }

        String deliveryId = generateDeliveryId(intent);
        int attempt = intent.getAttempts() + 1;

        try {
            intent.transitionTo(IntentStatus.DUE);
            intent.transitionTo(IntentStatus.DISPATCHING);
            intent.incrementAttempts();
            intent.setLastDeliveryId(deliveryId);
            intentStore.update(intent);

            // 使用 SPI 接口投递
            DeliveryResult result = deliveryHandler.deliver(intent);

            switch (result) {
                case SUCCESS:
                    intent.transitionTo(IntentStatus.DELIVERED);
                    intentStore.update(intent);
                    intent.transitionTo(IntentStatus.ACKED);
                    intentStore.update(intent);
                    logger.debug("Intent {} delivered successfully", intent.getIntentId());
                    break;

                case RETRY:
                    scheduleRedelivery(intent);
                    break;

                case DEAD_LETTER:
                    intent.transitionTo(IntentStatus.DEAD_LETTERED);
                    intentStore.update(intent);
                    logger.warn("Intent {} dead-lettered", intent.getIntentId());
                    break;

                case EXPIRED:
                    intent.transitionTo(IntentStatus.EXPIRED);
                    intentStore.update(intent);
                    logger.info("Intent {} expired", intent.getIntentId());
                    break;
            }

        } catch (Exception e) {
            logger.error("Unexpected error dispatching intent {}: {}",
                intent.getIntentId(), e.getMessage(), e);
            handleDeliveryFailure(intent);
        }
    }

    /**
     * 带指标记录的投递
     */
    private void dispatchWithMetrics(Intent intent, PrecisionTier tier) {
        long startTime = System.nanoTime();

        try {
            dispatch(intent);

            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
            metrics.recordWebhookLatency(durationMs);
            metrics.incrementIntentByTier(tier);

        } catch (Exception e) {
            logger.error("Dispatch failed for intent {} in tier {}",
                intent.getIntentId(), tier, e);
            throw e;
        }
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
     * 检查档位是否处于背压状态
     */
    public boolean isTierUnderBackpressure(PrecisionTier tier) {
        Semaphore semaphore = tierSemaphores.get(tier);
        LinkedBlockingDeque<Intent> queue = tierDispatchQueues.get(tier);

        boolean semaphoreExhausted = semaphore.availablePermits() == 0;
        boolean queueBackedUp = queue.size() >= tier.getMaxConcurrency() * 2;

        return semaphoreExhausted || queueBackedUp;
    }

    /**
     * 获取档位背压信息
     */
    public Map<PrecisionTier, BackpressureInfo> getBackpressureStatus() {
        Map<PrecisionTier, BackpressureInfo> status = new EnumMap<>(PrecisionTier.class);

        for (PrecisionTier tier : PrecisionTier.values()) {
            Semaphore semaphore = tierSemaphores.get(tier);
            LinkedBlockingDeque<Intent> queue = tierDispatchQueues.get(tier);

            int availablePermits = semaphore.availablePermits();
            int queueSize = queue.size();
            boolean underPressure = isTierUnderBackpressure(tier);

            status.put(tier, new BackpressureInfo(
                tier.getMaxConcurrency(),
                availablePermits,
                queueSize,
                underPressure
            ));
        }

        return status;
    }

    /**
     * 背压信息记录
     */
    public record BackpressureInfo(
        int maxConcurrency,
        int availablePermits,
        int queueSize,
        boolean underBackpressure
    ) {}

    /**
     * 获取桶组管理器
     */
    public BucketGroupManager getBucketGroupManager() {
        return bucketGroupManager;
    }
}

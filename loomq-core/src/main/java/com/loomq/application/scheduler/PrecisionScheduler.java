package com.loomq.application.scheduler;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.DefaultRedeliveryDecider;
import com.loomq.spi.RedeliveryDecider;
import com.loomq.store.IntentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * 精度调度器。
 *
 * 支持多精度档位的 Intent 调度，每个档位独立的扫描线程。
 * 核心架构：虚拟线程独立休眠 + 分层 Bucket 唤醒。
 *
 * @author loomq
 */
public class PrecisionScheduler {

    private static final Logger logger = LoggerFactory.getLogger(PrecisionScheduler.class);

    private static final long BACKPRESSURE_LOG_INTERVAL_MS = 1000;

    private final IntentStore intentStore;
    private final PrecisionTierCatalog precisionTierCatalog;
    private final BucketGroupManager bucketGroupManager;
    private final DeliveryHandler deliveryHandler;
    private final RedeliveryDecider redeliveryDecider;

    // 共享虚拟线程池（所有档位共享）
    private final ExecutorService sharedExecutor;

    // 档位级并发控制（信号量）
    private final Map<PrecisionTier, Semaphore> tierSemaphores;

    // 档位级无锁队列（fire-and-forget 模式下 semaphore 硬限并发，队列自然受控）
    private final Map<PrecisionTier, ConcurrentLinkedDeque<Intent>> tierDispatchQueues;

    // 按精度档位的扫描调度器
    private final Map<PrecisionTier, ScheduledExecutorService> scanSchedulers;
    private final Map<PrecisionTier, ScheduledFuture<?>> scanFutures;

    // due→dispatch lag 追踪（key=intentId, value=enqueueTimeNanos）
    private final ConcurrentHashMap<String, Long> enqueueTimeNanos = new ConcurrentHashMap<>();

    // 过期检查分频计数器
    private final Map<PrecisionTier, AtomicLong> expiredCheckCounters;

    // 限频日志时间戳
    private final AtomicLong lastBackpressureLogTimeMs = new AtomicLong(0);

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
        this(intentStore, null, null, null);
    }

    /**
     * 创建调度器（指定投递处理器）
     *
     * @param intentStore     Intent 存储
     * @param deliveryHandler 投递处理器（null 则使用 ServiceLoader 加载）
     */
    public PrecisionScheduler(IntentStore intentStore, DeliveryHandler deliveryHandler) {
        this(intentStore, deliveryHandler, null, null);
    }

    /**
     * 创建调度器（完整参数）
     *
     * @param intentStore       Intent 存储
     * @param deliveryHandler   投递处理器（null 则使用 ServiceLoader 加载）
     * @param redeliveryDecider 重投决策器（null 则使用默认）
     */
    public PrecisionScheduler(IntentStore intentStore, DeliveryHandler deliveryHandler, RedeliveryDecider redeliveryDecider) {
        this(intentStore, deliveryHandler, redeliveryDecider, null);
    }

    /**
     * 创建调度器（完整参数）
     *
     * @param intentStore       Intent 存储
     * @param deliveryHandler   投递处理器（null 则使用 ServiceLoader 加载）
     * @param redeliveryDecider 重投决策器（null 则使用默认）
     * @param precisionTierCatalog 精度档位目录
     */
    public PrecisionScheduler(IntentStore intentStore,
                              DeliveryHandler deliveryHandler,
                              RedeliveryDecider redeliveryDecider,
                              PrecisionTierCatalog precisionTierCatalog) {
        this.intentStore = intentStore;
        this.precisionTierCatalog = precisionTierCatalog != null
            ? precisionTierCatalog
            : PrecisionTierCatalog.defaultCatalog();
        this.bucketGroupManager = new BucketGroupManager(this.precisionTierCatalog);
        this.scanSchedulers = new ConcurrentHashMap<>();
        this.scanFutures = new ConcurrentHashMap<>();

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
        this.expiredCheckCounters = new EnumMap<>(PrecisionTier.class);

        for (PrecisionTier tier : this.precisionTierCatalog.supportedTiers()) {
            tierSemaphores.put(tier, new Semaphore(this.precisionTierCatalog.maxConcurrency(tier)));
            tierDispatchQueues.put(tier, new ConcurrentLinkedDeque<>());
            expiredCheckCounters.put(tier, new AtomicLong(0));
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

        // 为每个精度档位启动独立的扫描循环
        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            startScanCycle(tier);
        }

        // 启动档位级批量消费者
        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            startBatchConsumers(tier);
        }

        logger.info("PrecisionScheduler started with {} precision tiers", precisionTierCatalog.tierCount());
    }

    /**
     * 启动指定档位的批量消费者
     */
    private void startBatchConsumers(PrecisionTier tier) {
        int consumerCount = precisionTierCatalog.consumerCount(tier);

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
    private void startScanCycle(PrecisionTier tier) {
        long intervalMs = precisionTierCatalog.precisionWindowMs(tier);

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

        scanFutures.put(tier, future);
        logger.info("Started scan cycle for tier {} with interval {}ms", tier, intervalMs);
    }

    /**
     * 停止调度器
     */
    public void stop() {
        running = false;

        // 取消所有扫描循环
        for (ScheduledFuture<?> future : scanFutures.values()) {
            future.cancel(false);
        }
        scanFutures.clear();

        // 关闭所有扫描调度器
        for (ScheduledExecutorService scheduler : scanSchedulers.values()) {
            scheduler.shutdown();
        }
        scanSchedulers.clear();

        // 排空 in-flight dispatch: 获取全部 permit 证明所有投递已完成
        for (Map.Entry<PrecisionTier, Semaphore> entry : tierSemaphores.entrySet()) {
            PrecisionTier tier = entry.getKey();
            Semaphore sem = entry.getValue();
            int max = precisionTierCatalog.maxConcurrency(tier);
            try {
                if (!sem.tryAcquire(max, 5, TimeUnit.SECONDS)) {
                    logger.warn("Tier {} has {} intents still in-flight after drain timeout",
                        tier, max - sem.availablePermits());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

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
        BucketGroup group = resolveBucketGroup(tier);

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
            delayMs <= 0 ? 0 : Math.max(0, delayMs - precisionTierCatalog.precisionWindowMs(tier)));
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
        BucketGroup group = resolveBucketGroup(tier);

        try {
            List<Intent> dueIntents = group.scanDue(now);

            if (!dueIntents.isEmpty()) {
                logger.debug("Found {} due intents for tier {}", dueIntents.size(), tier);

                // 记录指标
                for (int i = 0; i < dueIntents.size(); i++) {
                    metrics.incrementIntentDueByTier(tier);
                }

                for (Intent intent : dueIntents) {
                    // 记录唤醒延迟
                    recordWakeupLatency(intent, now);

                    // 追踪入队时间（用于 due→dispatch lag 计算）
                    enqueueTimeNanos.put(intent.getIntentId(), System.nanoTime());

                    // 提交到档位队列（有界，带短重试）
                    ConcurrentLinkedDeque<Intent> queue = tierDispatchQueues.get(tier);
                    boolean offered = false;
                    int offerRetries = 0;
                    while (!offered && offerRetries < 3) {
                        offered = queue.offerLast(intent);
                        if (!offered) {
                            offerRetries++;
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                        }
                    }
                    if (!offered) {
                        metrics.incrementDispatchQueueOfferFailed(tier);
                        metrics.incrementBackpressureEvent(tier);
                        logRateLimited("Backpressure: dispatch queue full for tier {}, dropping intent {} after retries",
                            tier, intent.getIntentId());
                    }
                }
            }

            // 更新桶大小指标
            metrics.updateBucketSizeByTier(tier, group.getPendingCount());

            // 更新队列深度指标
            ConcurrentLinkedDeque<Intent> queue = tierDispatchQueues.get(tier);
            metrics.updateDispatchQueueSizeByTier(tier, queue.size());

            // 检查过期任务（分频）
            if (shouldCheckExpired(tier)) {
                checkExpiredIntents(tier);
            }

        } catch (Exception e) {
            // 扫描循环安全网：单个 tier 的异常不应杀死整个扫描线程
            logger.error("Error scanning tier {}", tier, e);
        }

        // 记录扫描耗时
        long durationMs = (System.nanoTime() - startTime) / 1_000_000;
        metrics.recordScanDurationByTier(tier, durationMs);
    }

    /**
     * 限频日志：每秒最多 1 条，避免 I/O 阻塞扫描线程
     */
    private void logRateLimited(String format, Object... args) {
        long now = System.currentTimeMillis();
        long last = lastBackpressureLogTimeMs.get();
        if (now - last >= BACKPRESSURE_LOG_INTERVAL_MS && lastBackpressureLogTimeMs.compareAndSet(last, now)) {
            logger.warn(format, args);
        }
    }

    /**
     * 判断当前 scan cycle 是否需要执行过期检查（分频策略）
     *
     * ULTRA/FAST: 每 cycle（延迟敏感）
     * HIGH: 每 3 cycle（最大过期延迟 400ms）
     * STANDARD: 每 5 cycle（最大过期延迟 3000ms）
     * ECONOMY: 每 10 cycle（最大过期延迟 11000ms）
     */
    private boolean shouldCheckExpired(PrecisionTier tier) {
        long count = expiredCheckCounters.get(tier).incrementAndGet();
        int interval = switch (tier) {
            case ULTRA, FAST -> 1;
            case HIGH -> 3;
            case STANDARD -> 5;
            case ECONOMY -> 10;
        };
        return count % interval == 0;
    }

    /**
     * 投递消费者循环 (fire-and-forget 模式)。
     *
     * Semaphore 是唯一的并发控制器。消费者先获取 permit，
     * 然后从队列取 intent 异步投递，permit 在 HTTP 回调中释放。
     * 吞吐上限 = maxConcurrency / avgDownstreamLatency。
     */
    private void runBatchConsumer(PrecisionTier tier) {
        ConcurrentLinkedDeque<Intent> queue = tierDispatchQueues.get(tier);
        Semaphore semaphore = tierSemaphores.get(tier);

        while (running) {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            Intent intent = queue.pollFirst();
            if (intent == null) {
                semaphore.release();
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                continue;
            }

            recordDispatchQueueLag(intent, tier);

            sharedExecutor.submit(() -> {
                try {
                    dispatchWithMetrics(intent, tier);
                } catch (Exception e) {
                    logger.error("Dispatch failed for intent {}: {}",
                        intent.getIntentId(), e.getMessage(), e);
                } finally {
                    semaphore.release();
                }
            });
        }
    }

    /**
     * 记录 due→dispatch lag
     */
    private void recordDispatchQueueLag(Intent intent, PrecisionTier tier) {
        Long enqueueNanos = enqueueTimeNanos.remove(intent.getIntentId());
        if (enqueueNanos != null) {
            long lagMs = (System.nanoTime() - enqueueNanos) / 1_000_000;
            metrics.recordDispatchQueueLagByTier(tier, lagMs);
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
     * 检查过期任务。
     *
     * 扫描所有非终态且已过期的 intent（包括未入队的 SCHEDULED），
     * 适配轻量级 dispatch() 中中间态不持久化的设计。
     */
    private void checkExpiredIntents(PrecisionTier tier) {
        for (Intent intent : intentStore.getAllIntents().values()) {
            if (intent.getPrecisionTier() != tier) continue;
            if (!intent.isExpired()) continue;
            if (intent.getStatus().isTerminal()) continue;

            if (intent.getStatus() == IntentStatus.DUE ||
                intent.getStatus() == IntentStatus.DELIVERED ||
                intent.getStatus() == IntentStatus.SCHEDULED ||
                intent.getStatus() == IntentStatus.DISPATCHING) {
                handleExpired(intent);
            }
        }
    }

    /**
     * 投递 Intent (轻量级内存状态流 + 终态单次持久化)。
     *
     * 中间态 (DUE→DISPATCHING→DELIVERED) 仅在内存中流过，
     * 只在终态做一次 intentStore.update()，消除双重 CHM.compute 开销。
     */
    private void dispatch(Intent intent) {
        if (deliveryHandler == null) {
            logger.warn("No DeliveryHandler configured, cannot deliver intent {}", intent.getIntentId());
            return;
        }

        String deliveryId = generateDeliveryId(intent);
        int attempt = intent.getAttempts() + 1;

        try {
            // 内存中状态转换（不持久化 — 终态才做一次 upsert）
            intent.transitionTo(IntentStatus.DUE);
            intent.transitionTo(IntentStatus.DISPATCHING);
            intent.incrementAttempts();
            intent.setLastDeliveryId(deliveryId);

            DeliveryResult result = deliveryHandler.deliver(intent);

            switch (result) {
                case SUCCESS:
                    intent.transitionTo(IntentStatus.DELIVERED);
                    intent.transitionTo(IntentStatus.ACKED);
                    intentStore.update(intent);
                    logger.debug("Intent {} delivered successfully", intent.getIntentId());
                    break;

                case RETRY: {
                    long delayMs = intent.getRedelivery() != null
                        ? intent.getRedelivery().calculateDelay(intent.getAttempts())
                        : 5000;
                    logger.info("Scheduling redelivery for intent={}, attempt={}, delay={}ms",
                        intent.getIntentId(), intent.getAttempts(), delayMs);
                    intent.setExecuteAt(Instant.now().plusMillis(delayMs));
                    intent.transitionTo(IntentStatus.SCHEDULED);
                    intentStore.update(intent);
                    schedule(intent);
                    break;
                }

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
        dispatch(intent);
        long durationMs = (System.nanoTime() - startTime) / 1_000_000;
        metrics.recordWebhookLatency(durationMs);
        metrics.incrementIntentByTier(tier);
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
        ConcurrentLinkedDeque<Intent> queue = tierDispatchQueues.get(tier);

        boolean semaphoreExhausted = semaphore.availablePermits() == 0;
        boolean queueBackedUp = queue.size() >= precisionTierCatalog.maxConcurrency(tier) * 2;

        return semaphoreExhausted || queueBackedUp;
    }

    /**
     * 获取档位背压信息
     */
    public Map<PrecisionTier, BackpressureInfo> getBackpressureStatus() {
        Map<PrecisionTier, BackpressureInfo> status = new EnumMap<>(PrecisionTier.class);

        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            Semaphore semaphore = tierSemaphores.get(tier);
            ConcurrentLinkedDeque<Intent> queue = tierDispatchQueues.get(tier);

            int availablePermits = semaphore.availablePermits();
            int queueSize = queue.size();
            boolean underPressure = isTierUnderBackpressure(tier);

            status.put(tier, new BackpressureInfo(
                precisionTierCatalog.maxConcurrency(tier),
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

    private BucketGroup resolveBucketGroup(PrecisionTier tier) {
        BucketGroup group = bucketGroupManager.getBucketGroup(tier);
        if (group == null) {
            group = bucketGroupManager.getBucketGroup(precisionTierCatalog.defaultTier());
        }
        return group;
    }
}

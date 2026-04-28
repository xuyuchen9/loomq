package com.loomq.application.dispatcher;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.exception.BackPressureException;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.DefaultRedeliveryDecider;
import com.loomq.spi.RedeliveryDecider;
import com.loomq.store.IntentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 批量同步投递器。
 *
 * 第一性原理实现：
 * 1. 虚拟线程使同步调用成本极低
 * 2. 批量处理摊平连接开销
 * 3. 同步投递消除异步状态机复杂度
 * 4. 背压通过队列满拒绝实现
 *
 * @author loomq
 */
public class BatchDispatcher implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(BatchDispatcher.class);

    // ========== 配置常量 ==========
    private static final int BATCH_SIZE = 100;
    private static final int QUEUE_CAPACITY = 10000;
    private static final long POLL_TIMEOUT_MS = 10;
    private static final long BATCH_TIMEOUT_MS = 30000;

    // ========== 依赖 ==========
    private final IntentStore intentStore;
    private final DeliveryHandler deliveryHandler;
    private final RedeliveryDecider redeliveryDecider;
    private final PrecisionScheduler scheduler;

    // ========== 执行器 ==========
    private final ExecutorService virtualThreadExecutor;
    private final Thread consumerThread;
    private final AtomicBoolean running = new AtomicBoolean(false);

    // ========== 队列 ==========
    private final BlockingQueue<Intent> dispatchQueue;

    // ========== 统计 ==========
    private final Stats stats = new Stats();

    public static class Stats {
        private final AtomicLong submittedCount = new AtomicLong(0);
        private final AtomicLong successCount = new AtomicLong(0);
        private final AtomicLong failureCount = new AtomicLong(0);
        private final AtomicLong rescheduledCount = new AtomicLong(0);

        public void recordSubmit() { submittedCount.incrementAndGet(); }
        public void recordSuccess() { successCount.incrementAndGet(); }
        public void recordFailure() { failureCount.incrementAndGet(); }
        public void recordRescheduled() { rescheduledCount.incrementAndGet(); }

        public long getSubmitted() { return submittedCount.get(); }
        public long getSuccess() { return successCount.get(); }
        public long getFailure() { return failureCount.get(); }
        public long getRescheduled() { return rescheduledCount.get(); }
    }

    /**
     * 创建批量投递器
     *
     * @param intentStore Intent 存储
     * @param scheduler   调度器（用于重投）
     */
    public BatchDispatcher(IntentStore intentStore, PrecisionScheduler scheduler) {
        this.intentStore = intentStore;
        this.scheduler = scheduler;
        this.dispatchQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

        // 加载投递处理器
        ServiceLoader<DeliveryHandler> handlerLoader = ServiceLoader.load(DeliveryHandler.class);
        this.deliveryHandler = handlerLoader.findFirst().orElse(null);

        // 加载重投决策器
        ServiceLoader<RedeliveryDecider> deciderLoader = ServiceLoader.load(RedeliveryDecider.class);
        this.redeliveryDecider = deciderLoader.findFirst().orElseGet(DefaultRedeliveryDecider::new);

        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.consumerThread = Thread.ofPlatform()
            .name("batch-dispatcher")
            .daemon(true)
            .unstarted(this::dispatchLoop);

        logger.info("BatchDispatcher created, batchSize={}, queueCapacity={}", BATCH_SIZE, QUEUE_CAPACITY);

        if (this.deliveryHandler == null) {
            logger.warn("No DeliveryHandler configured - intents will not be delivered!");
        }
    }

    /**
     * 创建批量投递器（指定投递处理器）
     *
     * @param intentStore     Intent 存储
     * @param scheduler       调度器
     * @param deliveryHandler 投递处理器
     */
    public BatchDispatcher(IntentStore intentStore, PrecisionScheduler scheduler, DeliveryHandler deliveryHandler) {
        this.intentStore = intentStore;
        this.scheduler = scheduler;
        this.deliveryHandler = deliveryHandler;
        this.dispatchQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

        ServiceLoader<RedeliveryDecider> deciderLoader = ServiceLoader.load(RedeliveryDecider.class);
        this.redeliveryDecider = deciderLoader.findFirst().orElseGet(DefaultRedeliveryDecider::new);

        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.consumerThread = Thread.ofPlatform()
            .name("batch-dispatcher")
            .daemon(true)
            .unstarted(this::dispatchLoop);

        logger.info("BatchDispatcher created with custom DeliveryHandler");
    }

    /**
     * 启动投递器
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            consumerThread.start();
            logger.info("BatchDispatcher started");
        }
    }

    /**
     * 提交 Intent 到投递队列
     *
     * @param intent Intent 实例
     * @throws BackPressureException 如果队列已满
     */
    public void submit(Intent intent) throws BackPressureException {
        if (!running.get()) {
            throw new IllegalStateException("Dispatcher not started");
        }

        if (!dispatchQueue.offer(intent)) {
            throw new BackPressureException("Dispatch queue full (capacity=" + QUEUE_CAPACITY + ")", null, 1000);
        }

        stats.recordSubmit();
        logger.debug("Submitted intent {} to dispatch queue", intent.getIntentId());
    }

    /**
     * 投递循环
     */
    private void dispatchLoop() {
        logger.info("Dispatch loop started");

        List<Intent> batch = new ArrayList<>(BATCH_SIZE);

        while (running.get()) {
            try {
                batch.clear();
                Intent first = dispatchQueue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                if (first == null) {
                    continue;
                }

                batch.add(first);
                dispatchQueue.drainTo(batch, BATCH_SIZE - 1);

                if (batch.isEmpty()) {
                    continue;
                }

                processBatch(batch);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Dispatch loop interrupted");
                break;
            } catch (Exception e) {
                logger.error("Dispatch loop error", e);
            }
        }

        logger.info("Dispatch loop ended");
    }

    /**
     * 处理批量投递
     */
    private void processBatch(List<Intent> batch) {
        logger.debug("Processing batch of {} intents", batch.size());

        List<CompletableFuture<Void>> futures = new ArrayList<>(batch.size());

        for (Intent intent : batch) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(
                () -> dispatchSingle(intent),
                virtualThreadExecutor
            );
            futures.add(future);
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .orTimeout(BATCH_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .join();
        } catch (CompletionException e) {
            logger.debug("Batch completed with some failures");
        }
    }

    /**
     * 投递单个 Intent
     */
    private void dispatchSingle(Intent intent) {
        if (deliveryHandler == null) {
            logger.warn("No DeliveryHandler configured, cannot deliver intent {}", intent.getIntentId());
            return;
        }

        String deliveryId = generateDeliveryId(intent);
        int attempt = intent.getAttempts() + 1;

        try {
            // 状态转换
            updateStatus(intent, IntentStatus.DUE);
            updateStatus(intent, IntentStatus.DISPATCHING);
            intent.incrementAttempts();
            intent.setLastDeliveryId(deliveryId);

            // 使用 SPI 异步接口投递
            DeliveryResult result = deliveryHandler.deliverAsync(intent)
                .orTimeout(30, TimeUnit.SECONDS)
                .join();

            switch (result) {
                case SUCCESS:
                    updateStatus(intent, IntentStatus.DELIVERED);
                    updateStatus(intent, IntentStatus.ACKED);
                    stats.recordSuccess();
                    logger.debug("Intent {} delivered successfully", intent.getIntentId());
                    break;

                case RETRY:
                    scheduleRedelivery(intent);
                    break;

                case DEAD_LETTER:
                    updateStatus(intent, IntentStatus.DEAD_LETTERED);
                    stats.recordFailure();
                    logger.warn("Intent {} dead-lettered", intent.getIntentId());
                    break;

                case EXPIRED:
                    updateStatus(intent, IntentStatus.EXPIRED);
                    stats.recordFailure();
                    logger.info("Intent {} expired", intent.getIntentId());
                    break;
            }

        } catch (Exception e) {
            logger.error("Unexpected error dispatching intent {}: {}",
                intent.getIntentId(), e.getMessage(), e);
            handleFailure(intent, "unexpected_error");
        }
    }

    /**
     * 处理投递失败
     */
    private void handleFailure(Intent intent, String reason) {
        stats.recordFailure();
        logger.warn("Intent {} delivery failed: {}", intent.getIntentId(), reason);

        int maxAttempts = intent.getRedelivery() != null
            ? intent.getRedelivery().getMaxAttempts()
            : 5;

        if (intent.getAttempts() >= maxAttempts) {
            updateStatus(intent, IntentStatus.DEAD_LETTERED);
            logger.error("Intent {} dead-lettered after {} attempts",
                intent.getIntentId(), intent.getAttempts());
        } else {
            scheduleRedelivery(intent);
        }
    }

    /**
     * 调度重投
     */
    private void scheduleRedelivery(Intent intent) {
        stats.recordRescheduled();

        long delayMs = intent.getRedelivery() != null
            ? intent.getRedelivery().calculateDelay(intent.getAttempts())
            : 5000;

        logger.info("Scheduling redelivery for intent={}, attempt={}, delay={}ms",
            intent.getIntentId(), intent.getAttempts(), delayMs);

        intent.setExecuteAt(Instant.now().plusMillis(delayMs));
        updateStatus(intent, IntentStatus.SCHEDULED);

        if (scheduler != null) {
            scheduler.schedule(intent);
        }
    }

    /**
     * 更新状态
     */
    private void updateStatus(Intent intent, IntentStatus status) {
        intent.transitionTo(status);
        intentStore.update(intent);
    }

    /**
     * 生成投递 ID
     */
    private String generateDeliveryId(Intent intent) {
        return "delivery_" + intent.getIntentId() + "_" + intent.getAttempts();
    }

    // ========== 状态查询 ==========

    public Stats getStats() {
        return stats;
    }

    public int getQueueSize() {
        return dispatchQueue.size();
    }

    public boolean isRunning() {
        return running.get();
    }

    // ========== 关闭 ==========

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        logger.info("Closing BatchDispatcher...");

        consumerThread.interrupt();
        try {
            consumerThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        virtualThreadExecutor.shutdown();
        try {
            if (!virtualThreadExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                virtualThreadExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            virtualThreadExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("BatchDispatcher closed, submitted={}, success={}, failure={}, rescheduled={}",
            stats.getSubmitted(), stats.getSuccess(), stats.getFailure(), stats.getRescheduled());
    }
}

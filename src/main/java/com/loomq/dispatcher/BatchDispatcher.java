package com.loomq.dispatcher;

import com.loomq.entity.v5.Intent;
import com.loomq.entity.v5.IntentStatus;
import com.loomq.scheduler.v5.PrecisionScheduler;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 批量同步投递器
 *
 * 第一性原理实现：
 * 1. 虚拟线程使同步调用成本极低
 * 2. 批量处理摊平连接开销
 * 3. 同步投递消除异步状态机复杂度
 * 4. 背压通过队列满拒绝实现
 *
 * 架构：
 * - 单消费者虚拟线程循环
 * - 批量取出（最多100条，最多等10ms）
 * - 并行同步发送（虚拟线程池）
 * - 整批等待完成（带超时）
 *
 * @author loomq
 * @since v0.6.1
 */
public class BatchDispatcher implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(BatchDispatcher.class);

    // ========== 配置常量 ==========
    private static final int BATCH_SIZE = 100;
    private static final int QUEUE_CAPACITY = 10000;
    private static final long POLL_TIMEOUT_MS = 10;
    private static final long BATCH_TIMEOUT_MS = 30000;
    private static final int DISPATCH_TIMEOUT_SEC = 30;

    // ========== 依赖 ==========
    private final IntentStore intentStore;
    private final HttpClient httpClient;
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

    public BatchDispatcher(IntentStore intentStore, PrecisionScheduler scheduler) {
        this.intentStore = intentStore;
        this.scheduler = scheduler;
        this.dispatchQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

        ServiceLoader<RedeliveryDecider> loader = ServiceLoader.load(RedeliveryDecider.class);
        this.redeliveryDecider = loader.findFirst().orElseGet(DefaultRedeliveryDecider::new);

        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.consumerThread = Thread.ofPlatform()
            .name("batch-dispatcher")
            .daemon(true)
            .unstarted(this::dispatchLoop);

        logger.info("BatchDispatcher created, batchSize={}, queueCapacity={}", BATCH_SIZE, QUEUE_CAPACITY);
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

        // 尝试入队，满时立即拒绝（背压）
        if (!dispatchQueue.offer(intent)) {
            throw new BackPressureException("Dispatch queue full (capacity=" + QUEUE_CAPACITY + ")");
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
                // 批量取出
                batch.clear();
                Intent first = dispatchQueue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                if (first == null) {
                    continue;  // 超时，继续循环
                }

                batch.add(first);
                dispatchQueue.drainTo(batch, BATCH_SIZE - 1);

                if (batch.isEmpty()) {
                    continue;
                }

                // 处理批量
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

        // 为每个 Intent 创建投递任务
        List<CompletableFuture<Void>> futures = new ArrayList<>(batch.size());

        for (Intent intent : batch) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(
                () -> dispatchSingle(intent),
                virtualThreadExecutor
            );
            futures.add(future);
        }

        // 等待整批完成（带超时）
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .orTimeout(BATCH_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .join();
        } catch (CompletionException e) {
            // 超时或异常，已在单个任务中处理
            logger.debug("Batch completed with some failures");
        }
    }

    /**
     * 投递单个 Intent
     */
    private void dispatchSingle(Intent intent) {
        String deliveryId = generateDeliveryId(intent);
        int attempt = intent.getAttempts() + 1;

        DeliveryContext context = new DeliveryContext(deliveryId, intent.getIntentId(), attempt);

        try {
            // 状态转换
            updateStatus(intent, IntentStatus.DUE);
            updateStatus(intent, IntentStatus.DISPATCHING);
            intent.incrementAttempts();
            intent.setLastDeliveryId(deliveryId);

            // 构建并发送请求
            HttpRequest request = buildHttpRequest(intent);
            HttpResponse<String> response = httpClient.send(
                request,
                HttpResponse.BodyHandlers.ofString()
            );

            // 处理响应
            context.markSuccess(response.statusCode(), Map.of(), response.body());

            if (redeliveryDecider.shouldRedeliver(context)) {
                scheduleRedelivery(intent);
            } else if (response.statusCode() >= 200 && response.statusCode() < 300) {
                updateStatus(intent, IntentStatus.DELIVERED);
                updateStatus(intent, IntentStatus.ACKED);
                stats.recordSuccess();
                logger.debug("Intent {} delivered successfully", intent.getIntentId());
            } else {
                handleFailure(intent, "HTTP " + response.statusCode());
            }

        } catch (Exception e) {
            context.markFailure(e);

            if (redeliveryDecider.shouldRedeliver(context)) {
                scheduleRedelivery(intent);
            } else {
                handleFailure(intent, e.getMessage());
            }
        }
    }

    /**
     * 构建 HTTP 请求
     */
    private HttpRequest buildHttpRequest(Intent intent) {
        Object body = intent.getCallback().getBody();
        HttpRequest.BodyPublisher bodyPublisher = body != null
            ? HttpRequest.BodyPublishers.ofString(body.toString())
            : HttpRequest.BodyPublishers.noBody();

        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(intent.getCallback().getUrl()))
            .timeout(Duration.ofSeconds(DISPATCH_TIMEOUT_SEC))
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

        // 更新执行时间
        intent.setExecuteAt(java.time.Instant.now().plusMillis(delayMs));
        updateStatus(intent, IntentStatus.SCHEDULED);

        // 重新调度
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

    /**
     * 背压异常
     */
    public static class BackPressureException extends RuntimeException {
        public BackPressureException(String message) {
            super(message);
        }
    }
}

package com.loomq.scheduler.v5;

import com.loomq.entity.v5.Intent;
import com.loomq.entity.v5.IntentStatus;
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
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.*;

/**
 * Intent 调度器 (v0.5)
 *
 * 负责任务到期检测和 HTTP 投递
 *
 * @author loomq
 * @since v0.5.0
 */
public class IntentScheduler {

    private static final Logger logger = LoggerFactory.getLogger(IntentScheduler.class);

    private final IntentStore intentStore;
    private final HttpClient httpClient;
    private final RedeliveryDecider redeliveryDecider;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService dispatchExecutor;

    private volatile boolean running = false;
    private volatile boolean paused = false;

    public IntentScheduler(IntentStore intentStore) {
        this.intentStore = intentStore;
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

        // 加载 SPI 实现的 RedeliveryDecider，否则使用默认
        ServiceLoader<RedeliveryDecider> loader = ServiceLoader.load(RedeliveryDecider.class);
        this.redeliveryDecider = loader.findFirst().orElseGet(DefaultRedeliveryDecider::new);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "intent-scheduler");
            t.setDaemon(true);
            return t;
        });

        this.dispatchExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * 启动调度器
     */
    public void start() {
        if (running) return;
        running = true;

        logger.info("IntentScheduler starting...");

        // 每 100ms 检查一次到期任务
        scheduler.scheduleAtFixedRate(this::checkDueIntents, 0, 100, TimeUnit.MILLISECONDS);
    }

    /**
     * 停止调度器
     */
    public void stop() {
        running = false;
        scheduler.shutdown();
        dispatchExecutor.shutdown();

        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            if (!dispatchExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                dispatchExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            dispatchExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("IntentScheduler stopped");
    }

    /**
     * 暂停调度器（失去 Primary 时调用）
     */
    public void pause() {
        if (!paused) {
            paused = true;
            logger.info("IntentScheduler paused");
        }
    }

    /**
     * 恢复调度器（成为 Primary 时调用）
     */
    public void resume() {
        if (paused) {
            paused = false;
            logger.info("IntentScheduler resumed");
        }
    }

    /**
     * 检查是否暂停
     */
    public boolean isPaused() {
        return paused;
    }

    /**
     * 检查到期任务
     */
    private void checkDueIntents() {
        if (paused) {
            return;
        }
        try {
            Instant now = Instant.now();

            for (Intent intent : intentStore.getAllIntents().values()) {
                if (intent.getStatus() == IntentStatus.SCHEDULED
                    && !intent.getExecuteAt().isAfter(now)) {

                    // 转换为 DUE 状态
                    intent.transitionTo(IntentStatus.DUE);
                    intentStore.update(intent);

                    // 提交投递任务
                    dispatchExecutor.submit(() -> dispatch(intent));
                }

                // 检查过期任务
                if ((intent.getStatus() == IntentStatus.DUE ||
                     intent.getStatus() == IntentStatus.DELIVERED) &&
                    intent.isExpired()) {
                    handleExpired(intent);
                }
            }
        } catch (Exception e) {
            logger.error("Error checking due intents", e);
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
            // 更新状态为 DISPATCHING
            intent.transitionTo(IntentStatus.DISPATCHING);
            intent.incrementAttempts();
            intent.setLastDeliveryId(deliveryId);
            intentStore.update(intent);

            // 构建 HTTP 请求
            HttpRequest request = buildHttpRequest(intent);

            // 发送请求
            HttpResponse<String> response = httpClient.send(request,
                HttpResponse.BodyHandlers.ofString());

            // 标记完成
            context.markSuccess(
                response.statusCode(),
                Map.of(), // 简化处理
                response.body()
            );

            // 判断是否继续重投
            if (redeliveryDecider.shouldRedeliver(context)) {
                scheduleRedelivery(intent);
            } else if (response.statusCode() >= 200 && response.statusCode() < 300) {
                // 成功
                intent.transitionTo(IntentStatus.DELIVERED);
                intentStore.update(intent);

                // TODO: 等待 ACK 或自动确认
                // 简化：立即转为 ACKED
                intent.transitionTo(IntentStatus.ACKED);
                intentStore.update(intent);
            } else {
                // 不继续重投，进入终态
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

        // 更新执行时间并重置状态
        intent.setExecuteAt(Instant.now().plusMillis(delayMs));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.update(intent);
    }

    /**
     * 处理投递失败
     */
    private void handleDeliveryFailure(Intent intent) {
        if (intent.getAttempts() >= (intent.getRedelivery() != null
            ? intent.getRedelivery().getMaxAttempts()
            : 5)) {
            // 达到最大重试次数
            intent.transitionTo(IntentStatus.DEAD_LETTERED);
            logger.warn("Intent dead-lettered after max attempts: id={}", intent.getIntentId());
        } else {
            // 其他失败原因
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
}

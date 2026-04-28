package com.loomq.embedded;

import com.loomq.application.scheduler.BucketGroupManager;
import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.replication.AckLevel;
import com.loomq.store.IntentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * LoomQ 嵌入式使用示例
 *
 * 演示如何在不依赖 HTTP/Netty 的情况下使用 LoomQ 核心功能。
 * 适用于需要在应用内部集成延迟任务调度的场景。
 *
 * <p>核心组件：</p>
 * <ul>
 *   <li>{@link IntentStore} - Intent 存储（内存）</li>
 *   <li>{@link BucketGroupManager} - 时间桶管理</li>
 *   <li>{@link PrecisionScheduler} - 精度调度器</li>
 * </ul>
 *
 * <p>使用场景：</p>
 * <ul>
 *   <li>单机应用内部延迟任务</li>
 *   <li>嵌入式设备任务调度</li>
 *   <li>测试环境快速验证</li>
 *   <li>loomoq-core 模块独立使用</li>
 * </ul>
 *
 * @author loomq
 * @since v0.6.0-final
 */
public class EmbeddedDemo {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedDemo.class);

    // 核心组件
    private IntentStore intentStore;
    private PrecisionScheduler scheduler;

    // 嵌入式回调注册表（替代 HTTP 回调）
    private final Map<String, Consumer<Intent>> callbackRegistry = new ConcurrentHashMap<>();

    // 统计
    private final AtomicInteger triggeredCount = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);

    /**
     * 启动嵌入式引擎
     */
    public void start() {
        logger.info("╔════════════════════════════════════════════════════════╗");
        logger.info("║       LoomQ Embedded Mode (No HTTP Required)           ║");
        logger.info("╚════════════════════════════════════════════════════════╝");

        // 1. 创建存储
        intentStore = new IntentStore();
        logger.info("[1/3] IntentStore initialized");

        // 2. 创建调度器
        scheduler = new PrecisionScheduler(intentStore,
            intent -> java.util.concurrent.CompletableFuture.completedFuture(com.loomq.spi.DeliveryHandler.DeliveryResult.DEAD_LETTER),
            null);
        scheduler.start();
        logger.info("[2/3] PrecisionScheduler started with {} tiers", PrecisionTier.values().length);

        // 3. 启动嵌入式投递线程（替代 HTTP 回调）
        startEmbeddedDispatcher();
        logger.info("[3/3] Embedded dispatcher started");

        logger.info("LoomQ Embedded ready - No HTTP/Netty dependencies");
    }

    /**
     * 停止嵌入式引擎
     */
    public void stop() {
        logger.info("Stopping LoomQ Embedded...");
        if (scheduler != null) {
            scheduler.stop();
        }
        if (intentStore != null) {
            intentStore.shutdown();
        }
        logger.info("LoomQ Embedded stopped");
    }

    /**
     * 创建 Intent（嵌入式版本）
     *
     * @param delayMs     延迟毫秒数
     * @param tier        精度档位
     * @param callback    回调函数（本地 Java 函数，非 HTTP）
     * @return 创建的 Intent
     */
    public Intent createIntent(long delayMs, PrecisionTier tier, Consumer<Intent> callback) {
        Intent intent = new Intent();
        intent.setExecuteAt(Instant.now().plusMillis(delayMs));
        intent.setDeadline(Instant.now().plusMillis(delayMs + 60000)); // 默认1分钟过期
        intent.setPrecisionTier(tier);
        intent.setAckLevel(AckLevel.ASYNC); // 嵌入式模式使用 ASYNC

        // 使用特殊 URL 格式标识嵌入式回调
        String callbackId = "embedded://" + intent.getIntentId();
        Callback cb = new Callback(callbackId);
        intent.setCallback(cb);

        // 注册回调函数
        callbackRegistry.put(callbackId, callback);

        // 保存到存储
        intentStore.save(intent);
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.update(intent);

        // 提交到调度器
        scheduler.schedule(intent);

        logger.info("Created intent: id={}, delay={}ms, tier={}",
            intent.getIntentId(), delayMs, tier);

        return intent;
    }

    /**
     * 创建简单 Intent（使用默认 STANDARD 档位）
     *
     * @param delayMs  延迟毫秒数
     * @param callback 回调函数
     * @return 创建的 Intent
     */
    public Intent createIntent(long delayMs, Consumer<Intent> callback) {
        return createIntent(delayMs, PrecisionTier.STANDARD, callback);
    }

    /**
     * 启动嵌入式投递线程
     *
     * 模拟调度器的投递过程，但使用本地回调函数替代 HTTP 请求
     */
    private void startEmbeddedDispatcher() {
        Thread dispatcher = new Thread(this::dispatchLoop, "embedded-dispatcher");
        dispatcher.setDaemon(true);
        dispatcher.start();
    }

    /**
     * 投递循环
     */
    private void dispatchLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // 扫描所有档位的到期任务
                BucketGroupManager bucketManager = scheduler.getBucketGroupManager();
                Instant now = Instant.now();

                for (PrecisionTier tier : PrecisionTier.values()) {
                    var dueIntents = bucketManager.scanDue(tier, now);

                    for (Intent intent : dueIntents) {
                        dispatchEmbedded(intent);
                    }
                }

                // 100ms 扫描间隔
                Thread.sleep(100);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in embedded dispatcher", e);
            }
        }
    }

    /**
     * 执行嵌入式投递
     */
    private void dispatchEmbedded(Intent intent) {
        String callbackUrl = intent.getCallback().getUrl();
        Consumer<Intent> callback = callbackRegistry.get(callbackUrl);

        if (callback == null) {
            logger.warn("No callback registered for intent: {}", intent.getIntentId());
            return;
        }

        triggeredCount.incrementAndGet();
        logger.info("Triggering intent: id={}, executeAt={}, actualDelayMs={}",
            intent.getIntentId(),
            intent.getExecuteAt(),
            Duration.between(intent.getExecuteAt(), Instant.now()).toMillis());

        try {
            // 执行本地回调
            callback.accept(intent);

            // 更新状态
            intent.transitionTo(IntentStatus.DELIVERED);
            intent.transitionTo(IntentStatus.ACKED);
            intentStore.update(intent);

            successCount.incrementAndGet();

        } catch (Exception e) {
            logger.error("Callback failed for intent: {}", intent.getIntentId(), e);
            intent.transitionTo(IntentStatus.DEAD_LETTERED);
            intentStore.update(intent);
        }
    }

    /**
     * 获取统计信息
     */
    public Map<String, Object> getStats() {
        return Map.of(
            "triggered", triggeredCount.get(),
            "success", successCount.get(),
            "pending", intentStore.getPendingCount()
        );
    }

    // ==================== 演示主方法 ====================

    public static void main(String[] args) throws Exception {
        EmbeddedDemo demo = new EmbeddedDemo();

        try {
            demo.start();

            // 演示 1: 基本延迟任务
            logger.info("\n--- Demo 1: Basic Delayed Intent ---");
            CountDownLatch latch1 = new CountDownLatch(1);
            long startTime1 = System.currentTimeMillis();

            demo.createIntent(500, intent -> {
                long actualDelay = System.currentTimeMillis() - startTime1;
                logger.info("✓ Intent executed after {}ms (expected 500ms)", actualDelay);
                latch1.countDown();
            });

            latch1.await(2, TimeUnit.SECONDS);

            // 演示 2: 多档位对比
            logger.info("\n--- Demo 2: Precision Tier Comparison ---");
            CountDownLatch latch2 = new CountDownLatch(3);

            long startTime2 = System.currentTimeMillis();

            // ULTRA 档位 - 高精度
            demo.createIntent(200, PrecisionTier.ULTRA, intent -> {
                long delay = System.currentTimeMillis() - startTime2;
                logger.info("✓ ULTRA intent executed after {}ms (window: 10ms)", delay);
                latch2.countDown();
            });

            // STANDARD 档位 - 标准精度（推荐）
            demo.createIntent(200, PrecisionTier.STANDARD, intent -> {
                long delay = System.currentTimeMillis() - startTime2;
                logger.info("✓ STANDARD intent executed after {}ms (window: 500ms)", delay);
                latch2.countDown();
            });

            // ECONOMY 档位 - 经济型
            demo.createIntent(200, PrecisionTier.ECONOMY, intent -> {
                long delay = System.currentTimeMillis() - startTime2;
                logger.info("✓ ECONOMY intent executed after {}ms (window: 1000ms)", delay);
                latch2.countDown();
            });

            latch2.await(3, TimeUnit.SECONDS);

            // 演示 3: 批量任务
            logger.info("\n--- Demo 3: Batch Intents (100 intents) ---");
            int batchSize = 100;
            CountDownLatch latch3 = new CountDownLatch(batchSize);
            AtomicInteger counter = new AtomicInteger(0);
            long startTime3 = System.currentTimeMillis();

            for (int i = 0; i < batchSize; i++) {
                demo.createIntent(300 + (i * 10), PrecisionTier.STANDARD, intent -> {
                    int count = counter.incrementAndGet();
                    if (count == batchSize) {
                        long totalTime = System.currentTimeMillis() - startTime3;
                        logger.info("✓ All {} intents completed in {}ms", batchSize, totalTime);
                    }
                    latch3.countDown();
                });
            }

            latch3.await(10, TimeUnit.SECONDS);

            // 打印统计
            logger.info("\n--- Final Statistics ---");
            demo.getStats().forEach((key, value) ->
                logger.info("  {}: {}", key, value));

            logger.info("\n✓ Embedded Demo completed successfully!");
            logger.info("✓ No HTTP server, no Netty, no Jackson required");

        } finally {
            demo.stop();
        }
    }
}

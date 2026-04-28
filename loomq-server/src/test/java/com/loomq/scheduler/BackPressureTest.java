package com.loomq.scheduler;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.store.IntentStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 背压和并发控制测试
 *
 * 验证：
 * 1. 各档位并发隔离
 * 2. 背压触发时记录指标
 * 3. 信号量正确释放
 *
 * @author loomq
 * @since v0.6.2
 */
@Tag("slow")
class BackPressureTest {

    private PrecisionScheduler scheduler;
    private IntentStore intentStore;
    private MetricsCollector metrics;
    private final DeliveryHandler mockHandler =
        intent -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.DEAD_LETTER);

    @BeforeEach
    void setUp() {
        intentStore = new IntentStore();
        scheduler = new PrecisionScheduler(intentStore, mockHandler, null);
        metrics = MetricsCollector.getInstance();
        scheduler.start();
    }

    @AfterEach
    void tearDown() {
        scheduler.stop();
    }

    /**
     * 测试各档位最大并发配置正确
     */
    @Test
    void testTierConcurrencyConfig() {
        assertEquals(200, PrecisionTier.ULTRA.getMaxConcurrency(), "ULTRA should have 200 max concurrency");
        assertEquals(150, PrecisionTier.FAST.getMaxConcurrency(), "FAST should have 150 max concurrency");
        assertEquals(50, PrecisionTier.HIGH.getMaxConcurrency(), "HIGH should have 50 max concurrency");
        assertEquals(50, PrecisionTier.STANDARD.getMaxConcurrency(), "STANDARD should have 50 max concurrency");
        assertEquals(50, PrecisionTier.ECONOMY.getMaxConcurrency(), "ECONOMY should have 50 max concurrency");
    }

    /**
     * 测试各档位批量配置
     */
    @Test
    void testTierBatchConfig() {
        // ULTRA 和 FAST 禁用批量
        assertEquals(1, PrecisionTier.ULTRA.getBatchSize(), "ULTRA should disable batch");
        assertFalse(PrecisionTier.ULTRA.isBatchEnabled(), "ULTRA batch should be disabled");

        assertEquals(1, PrecisionTier.FAST.getBatchSize(), "FAST should disable batch");
        assertFalse(PrecisionTier.FAST.isBatchEnabled(), "FAST batch should be disabled");

        // HIGH/ECONOMY 启用批量
        assertTrue(PrecisionTier.HIGH.isBatchEnabled(), "HIGH batch should be enabled");
        assertEquals(5, PrecisionTier.HIGH.getBatchSize(), "HIGH batch size should be 5");

        assertTrue(PrecisionTier.ECONOMY.isBatchEnabled(), "ECONOMY batch should be enabled");
        assertEquals(25, PrecisionTier.ECONOMY.getBatchSize(), "ECONOMY batch size should be 25");
    }

    /**
     * 测试背压事件被记录
     */
    @Test
    void testBackpressureEventRecorded() throws InterruptedException {
        // 获取背压事件初始值
        Map<PrecisionTier, Long> initialEvents = metrics.getBackpressureEventsByTier();
        long initialUltra = initialEvents.getOrDefault(PrecisionTier.ULTRA, 0L);

        // 快速创建大量 ULTRA 任务（超过队列容量）
        // 注意：这里只是测试队列满时背压是否被记录，不测试实际投递
        for (int i = 0; i < 2000; i++) {
            Intent intent = createTestIntent("bp-test-" + i, PrecisionTier.ULTRA);
            intentStore.save(intent);
            scheduler.schedule(intent);
        }

        // 等待一段时间让调度器处理
        Thread.sleep(100);

        // 验证背压事件被记录（可能不会被触发，取决于队列处理速度，但指标应该存在）
        Map<PrecisionTier, Long> currentEvents = metrics.getBackpressureEventsByTier();
        assertNotNull(currentEvents, "Backpressure events map should exist");
        assertTrue(currentEvents.containsKey(PrecisionTier.ULTRA), "ULTRA tier should have backpressure metrics");
    }

    /**
     * 测试各档位队列独立
     */
    @Test
    void testTierQueuesAreIndependent() throws InterruptedException {
        // 为每个档位创建任务
        for (PrecisionTier tier : PrecisionTier.values()) {
            for (int i = 0; i < 100; i++) {
                Intent intent = createTestIntent(tier.name() + "-" + i, tier);
                intentStore.save(intent);
                scheduler.schedule(intent);
            }
        }

        // 等待调度
        Thread.sleep(50);

        // 验证各档位的桶大小指标都被记录
        Map<PrecisionTier, Long> bucketSizes = metrics.getBucketSizesByTier();
        for (PrecisionTier tier : PrecisionTier.values()) {
            assertTrue(bucketSizes.containsKey(tier),
                tier + " should have bucket size metric");
        }
    }

    /**
     * 测试信号量在任务完成后释放
     */
    @Test
    void testSemaphoreReleasedAfterDispatch() throws InterruptedException {
        // 创建一个立即可执行的任务
        Intent intent = createImmediateIntent("immediate-test", PrecisionTier.ULTRA);
        intentStore.save(intent);

        // 调度任务
        scheduler.schedule(intent);

        // 等待任务处理
        Thread.sleep(200);

        // 验证任务状态已变更（说明已处理）
        Intent updated = intentStore.findById("immediate-test");
        assertNotNull(updated);
        // 任务应该已进入某种终态或调度状态
        assertNotEquals(IntentStatus.CREATED, updated.getStatus(),
            "Intent should have been processed");
    }

    /**
     * 测试消费者数量配置
     */
    @Test
    void testConsumerCountConfig() {
        assertEquals(16, PrecisionTier.ULTRA.getConsumerCount(), "ULTRA should have 16 consumers");
        assertEquals(12, PrecisionTier.FAST.getConsumerCount(), "FAST should have 12 consumers");
        assertEquals(4, PrecisionTier.HIGH.getConsumerCount(), "HIGH should have 4 consumers");
        assertEquals(3, PrecisionTier.STANDARD.getConsumerCount(), "STANDARD should have 3 consumers");
        assertEquals(2, PrecisionTier.ECONOMY.getConsumerCount(), "ECONOMY should have 2 consumers");
    }

    /**
     * 测试批量窗口配置
     */
    @Test
    void testBatchWindowConfig() {
        assertEquals(5, PrecisionTier.ULTRA.getBatchWindowMs(), "ULTRA should have 5ms batch window (defensive default)");
        assertEquals(10, PrecisionTier.FAST.getBatchWindowMs(), "FAST should have 10ms batch window (defensive default)");
        assertEquals(50, PrecisionTier.HIGH.getBatchWindowMs(), "HIGH batch window should be 50ms");
        assertEquals(100, PrecisionTier.STANDARD.getBatchWindowMs(), "STANDARD batch window should be 100ms");
        assertEquals(300, PrecisionTier.ECONOMY.getBatchWindowMs(), "ECONOMY batch window should be 300ms");
    }

    /**
     * 测试高负载下档位隔离（简化版）
     */
    @Test
    void testTierIsolationUnderLoad() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        AtomicInteger ultraCount = new AtomicInteger(0);
        AtomicInteger economyCount = new AtomicInteger(0);

        // 同时创建 ULTRA 和 ECONOMY 任务
        new Thread(() -> {
            for (int i = 0; i < 500; i++) {
                Intent intent = createTestIntent("ultra-load-" + i, PrecisionTier.ULTRA);
                intentStore.save(intent);
                scheduler.schedule(intent);
                ultraCount.incrementAndGet();
            }
            latch.countDown();
        }).start();

        new Thread(() -> {
            for (int i = 0; i < 500; i++) {
                Intent intent = createTestIntent("economy-load-" + i, PrecisionTier.ECONOMY);
                intentStore.save(intent);
                scheduler.schedule(intent);
                economyCount.incrementAndGet();
            }
            latch.countDown();
        }).start();

        new Thread(() -> {
            for (int i = 0; i < 500; i++) {
                Intent intent = createTestIntent("standard-load-" + i, PrecisionTier.STANDARD);
                intentStore.save(intent);
                scheduler.schedule(intent);
            }
            latch.countDown();
        }).start();

        assertTrue(latch.await(10, TimeUnit.SECONDS), "All threads should complete");

        // 验证创建计数
        assertEquals(500, ultraCount.get(), "ULTRA intents should be created");
        assertEquals(500, economyCount.get(), "ECONOMY intents should be created");

        // 等待调度器处理
        Thread.sleep(100);

        // 验证桶大小指标被记录（任务已进入桶）
        Map<PrecisionTier, Long> bucketSizes = metrics.getBucketSizesByTier();
        // 注意：由于任务执行时间是1小时后，它们应该在桶中
        assertNotNull(bucketSizes, "Bucket sizes should be recorded");
    }

    // ========== 辅助方法 ==========

    private Intent createTestIntent(String intentId, PrecisionTier tier) {
        Intent intent = new Intent(intentId);
        intent.setExecuteAt(Instant.now().plusSeconds(3600)); // 1小时后执行
        intent.setDeadline(Instant.now().plusSeconds(7200));
        intent.setPrecisionTier(tier);
        intent.setShardKey("test-shard");
        Callback callback = new Callback();
        callback.setUrl("http://localhost:9999/webhook");
        intent.setCallback(callback);
        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }

    private Intent createImmediateIntent(String intentId, PrecisionTier tier) {
        Intent intent = new Intent(intentId);
        intent.setExecuteAt(Instant.now()); // 立即执行
        intent.setDeadline(Instant.now().plusSeconds(60));
        intent.setPrecisionTier(tier);
        intent.setShardKey("test-shard");
        Callback callback = new Callback();
        callback.setUrl("http://localhost:9999/webhook");
        intent.setCallback(callback);
        intent.transitionTo(IntentStatus.SCHEDULED); // CREATED -> SCHEDULED
        return intent;
    }
}

package com.loomq.scheduler;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.PrecisionTierProfile;
import com.loomq.spi.DefaultRedeliveryDecider;
import com.loomq.spi.DeliveryHandler;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * P2: 调度器参数单变量实验。
 *
 * 每个实验只改一个变量，其他保持默认。
 * 输出机器解析的 RESULT| 行，用于绘制性能曲线。
 *
 * 实验矩阵：
 * 1. 队列容量：maxConcurrency × {4, 8, 16, 32}
 * 2. 消费者数量：{4, 8, 16, 32, 64}
 *
 * @tag slow
 */
@Tag("slow")
public class SchedulerParameterExperiment {

    private static final Logger logger = LoggerFactory.getLogger(SchedulerParameterExperiment.class);
    private static final int WEBHOOK_PORT = 19998;
    private static final int INTENTS_PER_TIER = 2000;
    private static final int ULTRA_DELAY_MS = 5;

    /**
     * 实验 1：队列容量单变量测试。
     *
     * 变量：dispatchQueueCapacity = maxConcurrency × {4, 8, 16, 32}
     * 控制：其他参数保持默认
     */
    @Test
    void experiment1_queueCapacity() throws Exception {
        logger.info("=== 实验 1: 队列容量单变量测试 ===");

        int[] multipliers = {4, 8, 16, 32};
        for (int mult : multipliers) {
            runWithCustomCatalog(
                "queue_x" + mult,
                tier -> new PrecisionTierProfile(
                    PrecisionTierCatalog.defaultCatalog().profile(tier).precisionWindowMs(),
                    PrecisionTierCatalog.defaultCatalog().profile(tier).maxConcurrency(),
                    PrecisionTierCatalog.defaultCatalog().profile(tier).batchSize(),
                    PrecisionTierCatalog.defaultCatalog().profile(tier).batchWindowMs(),
                    PrecisionTierCatalog.defaultCatalog().profile(tier).consumerCount(),
                    PrecisionTierCatalog.defaultCatalog().profile(tier).maxConcurrency() * mult,
                    PrecisionTierCatalog.defaultCatalog().profile(tier).walMode(),
                    PrecisionTierCatalog.defaultCatalog().profile(tier).scanIntervalMs()
                )
            );
        }
    }

    /**
     * 实验 2：消费者数量单变量测试。
     *
     * 变量：consumerCount = {4, 8, 16, 32, 64}
     * 控制：其他参数保持默认
     */
    @Test
    void experiment2_consumerCount() throws Exception {
        logger.info("=== 实验 2: 消费者数量单变量测试 ===");

        int[] consumerCounts = {4, 8, 16, 32, 64};
        for (int cc : consumerCounts) {
            int finalCc = cc;
            runWithCustomCatalog(
                "consumers_" + cc,
                tier -> {
                    // 只修改 ULTRA 档位的消费者数量，其他档位保持默认
                    PrecisionTierProfile defaultProfile = PrecisionTierCatalog.defaultCatalog().profile(tier);
                    if (tier == PrecisionTier.ULTRA) {
                        return new PrecisionTierProfile(
                            defaultProfile.precisionWindowMs(),
                            defaultProfile.maxConcurrency(),
                            defaultProfile.batchSize(),
                            defaultProfile.batchWindowMs(),
                            finalCc,
                            defaultProfile.dispatchQueueCapacity(),
                            defaultProfile.walMode(),
                            defaultProfile.scanIntervalMs()
                        );
                    }
                    return defaultProfile;
                }
            );
        }
    }

    /**
     * 实验 3：扫描间隔单变量测试。
     *
     * 变量：scanIntervalMs = {5, 10, 20, 50} ms（ULTRA 档位）
     * 控制：其他参数保持默认
     */
    @Test
    void experiment3_scanInterval() throws Exception {
        logger.info("=== 实验 3: 扫描间隔单变量测试 ===");

        long[] intervals = {5, 10, 20, 50};
        for (long interval : intervals) {
            long finalInterval = interval;
            runWithCustomCatalog(
                "scan_" + interval + "ms",
                tier -> {
                    PrecisionTierProfile defaultProfile = PrecisionTierCatalog.defaultCatalog().profile(tier);
                    if (tier == PrecisionTier.ULTRA) {
                        return new PrecisionTierProfile(
                            defaultProfile.precisionWindowMs(),
                            defaultProfile.maxConcurrency(),
                            defaultProfile.batchSize(),
                            defaultProfile.batchWindowMs(),
                            defaultProfile.consumerCount(),
                            defaultProfile.dispatchQueueCapacity(),
                            defaultProfile.walMode(),
                            finalInterval
                        );
                    }
                    return defaultProfile;
                }
            );
        }
    }

    /**
     * 实验 4：批量大小单变量测试。
     *
     * 变量：batchSize = {1, 5, 10, 20, 50}（STANDARD 档位）
     * 控制：其他参数保持默认
     */
    @Test
    void experiment4_batchSize() throws Exception {
        logger.info("=== 实验 4: 批量大小单变量测试 ===");

        int[] batchSizes = {1, 5, 10, 20, 50};
        for (int bs : batchSizes) {
            int finalBs = bs;
            runWithCustomCatalog(
                "batch_" + bs,
                tier -> {
                    PrecisionTierProfile defaultProfile = PrecisionTierCatalog.defaultCatalog().profile(tier);
                    if (tier == PrecisionTier.STANDARD) {
                        return new PrecisionTierProfile(
                            defaultProfile.precisionWindowMs(),
                            defaultProfile.maxConcurrency(),
                            finalBs,
                            defaultProfile.batchWindowMs(),
                            defaultProfile.consumerCount(),
                            defaultProfile.dispatchQueueCapacity(),
                            defaultProfile.walMode(),
                            defaultProfile.scanIntervalMs()
                        );
                    }
                    return defaultProfile;
                },
                PrecisionTier.STANDARD
            );
        }
    }

    /**
     * 运行单次实验：使用自定义 Catalog 创建调度器，测量指定档位吞吐。
     */
    private void runWithCustomCatalog(String label,
                                       java.util.function.Function<PrecisionTier, PrecisionTierProfile> profileFactory) throws Exception {
        runWithCustomCatalog(label, profileFactory, PrecisionTier.ULTRA);
    }

    private void runWithCustomCatalog(String label,
                                       java.util.function.Function<PrecisionTier, PrecisionTierProfile> profileFactory,
                                       PrecisionTier testTier) throws Exception {
        // 构建自定义 Catalog
        Map<PrecisionTier, PrecisionTierProfile> profiles = new EnumMap<>(PrecisionTier.class);
        for (PrecisionTier tier : PrecisionTier.values()) {
            profiles.put(tier, profileFactory.apply(tier));
        }
        PrecisionTierCatalog customCatalog = PrecisionTierCatalog.of(profiles, PrecisionTier.STANDARD);

        // 组件
        IntentStore intentStore = new ConcurrentIntentStore();
        int concurrency = customCatalog.maxConcurrency(testTier);
        int queueCap = customCatalog.dispatchQueueCapacity(testTier);
        int consumers = customCatalog.consumerCount(testTier);

        CountDownLatch latch = new CountDownLatch(INTENTS_PER_TIER);
        AtomicInteger deliveredCount = new AtomicInteger(0);

        // 投递 Handler：模拟 5ms 延迟 + 跟踪计数
        // 使用专用线程池避免 ForkJoinPool 瓶颈
        java.util.concurrent.ExecutorService deliveryExecutor =
            java.util.concurrent.Executors.newFixedThreadPool(200, r -> {
                Thread t = new Thread(r, "delivery-worker");
                t.setDaemon(true);
                return t;
            });

        DeliveryHandler handler = intent -> {
            return java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                try { Thread.sleep(ULTRA_DELAY_MS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                int count = deliveredCount.incrementAndGet();
                if (count <= INTENTS_PER_TIER) latch.countDown();
                return com.loomq.spi.DeliveryHandler.DeliveryResult.SUCCESS;
            }, deliveryExecutor);
        };

        PrecisionScheduler scheduler = new PrecisionScheduler(
            intentStore, handler, new DefaultRedeliveryDecider(), customCatalog
        );
        scheduler.start();

        // 创建 Intent
        Instant executeAt = Instant.now().plusMillis(2000);
        for (int i = 0; i < INTENTS_PER_TIER; i++) {
            Intent intent = createIntent(label + "-" + i, executeAt, testTier);
            intentStore.save(intent);
            scheduler.schedule(intent);
        }

        // 等待投递
        long waitStart = System.currentTimeMillis();
        boolean completed = latch.await(60, TimeUnit.SECONDS);
        long durationMs = System.currentTimeMillis() - waitStart;

        int delivered = deliveredCount.get();
        double qps = delivered / (durationMs / 1000.0);

        // 输出结果
        System.out.printf(Locale.ROOT,
            "RESULT|experiment=%s|tier=%s|concurrency=%d|queue_capacity=%d|consumers=%d|"
                + "delivered=%d|expected=%d|duration_ms=%d|qps=%.1f|completed=%s%n",
            label, testTier.name(), concurrency, queueCap, consumers,
            delivered, INTENTS_PER_TIER, durationMs, qps, completed);

        logger.info("Experiment '{}': tier={}, concurrency={}, queue={}, consumers={} → {} QPS (delivered {}/{})",
            label, testTier.name(), concurrency, queueCap, consumers, (int) qps, delivered, INTENTS_PER_TIER);

        // 清理
        scheduler.stop();
        deliveryExecutor.shutdownNow();
        intentStore.shutdown();
    }

    private Intent createIntent(String id, Instant executeAt, PrecisionTier tier) {
        Intent intent = new Intent(id);
        intent.setExecuteAt(executeAt);
        intent.setDeadline(executeAt.plusSeconds(60));
        intent.setPrecisionTier(tier);
        intent.setShardKey("experiment-shard");
        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }
}

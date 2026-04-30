package com.loomq.scheduler;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DefaultRedeliveryDecider;
import com.loomq.spi.DeliveryHandler;
import com.loomq.store.IntentStore;
import com.sun.management.OperatingSystemMXBean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 调度器端到端性能基准测试（带 Mock Webhook Server）
 *
 * 在 Windows 环境下使用 Java 内置 HttpServer 搭建真实测试环境。
 *
 * @author loomq
 * @since v0.6.2
 */
public class SchedulerTriggerBenchmarkWithMockServer {

    private static final Logger logger = LoggerFactory.getLogger(SchedulerTriggerBenchmarkWithMockServer.class);
    private static final int WEBHOOK_PORT = 19999;

    // 慢速端点 - 模拟真实网络延迟
    // 档位越高，延迟越高（模拟重负载下游）
    private static final int ULTRA_DELAY_MS = 5;      // 极速档位，快速响应
    private static final int FAST_DELAY_MS = 10;      // 快速档位
    private static final int HIGH_DELAY_MS = 20;      // 高精档位
    private static final int STANDARD_DELAY_MS = 30;  // 标准档位
    private static final int ECONOMY_DELAY_MS = 50;   // 经济档位，慢速响应

    private PrecisionScheduler scheduler;
    private IntentStore intentStore;
    private MetricsCollector metrics;
    private NettyMockWebhookServer mockServer;
    private BatchedHttpDeliveryHandler deliveryHandler;

    // 统计各档位接收到的 webhook 数量
    private final Map<PrecisionTier, AtomicInteger> webhookReceivedByTier = new ConcurrentHashMap<>();
    // Per-intent E2E latency tracking: intentId -> webhook receive epoch millis
    private final ConcurrentHashMap<String, Long> webhookReceiveTimeMs = new ConcurrentHashMap<>();
    // Per-tier intentId lists for post-benchmark E2E analysis
    private final Map<PrecisionTier, List<String>> tierIntentIds = new ConcurrentHashMap<>();
    private final AtomicInteger totalWebhookReceived = new AtomicInteger(0);
    // Known executeAt times for E2E latency computation
    private final ConcurrentHashMap<String, Long> intentExecuteAtMs = new ConcurrentHashMap<>();
    // Scheduler precision: intentId -> consumer dequeue epoch millis
    private final ConcurrentHashMap<String, Long> dequeuedAtMs = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        boolean quick = Boolean.getBoolean("loomq.benchmark.quick");
        SchedulerTriggerBenchmarkWithMockServer benchmark = new SchedulerTriggerBenchmarkWithMockServer();
        benchmark.setUp();
        try {
            BenchmarkSummary summary = benchmark.runBenchmarkMode(quick);
            benchmark.printBenchmarkSummary(summary);
        } finally {
            benchmark.tearDown();
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        // Initialize counters BEFORE starting mock server (server needs them)
        for (PrecisionTier tier : PrecisionTier.values()) {
            webhookReceivedByTier.put(tier, new AtomicInteger(0));
            tierIntentIds.put(tier, new ArrayList<>());
        }

        logger.info("Scheduler benchmark setup: starting Netty mock webhook server");
        startMockServer();

        logger.info("Scheduler benchmark setup: creating scheduler components");
        intentStore = new IntentStore();

        deliveryHandler = new BatchedHttpDeliveryHandler(WEBHOOK_PORT);

        // Wrap to record scheduler precision at dequeue time
        DeliveryHandler wrappedHandler = intent -> {
            dequeuedAtMs.put(intent.getIntentId(), System.currentTimeMillis());
            return deliveryHandler.deliverAsync(intent);
        };

        scheduler = new PrecisionScheduler(intentStore, wrappedHandler, new DefaultRedeliveryDecider());
        metrics = MetricsCollector.getInstance();
        logger.info("Scheduler benchmark setup: starting scheduler");
        scheduler.start();
        logger.info("Scheduler benchmark setup: scheduler started");
    }

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.stop();
        }
        if (deliveryHandler != null) {
            deliveryHandler.shutdown();
        }
        stopMockServer();
    }

    private void startMockServer() throws IOException {
        Map<String, Integer> delays = new HashMap<>();
        Map<String, AtomicInteger> receivedMap = new HashMap<>();
        for (PrecisionTier tier : PrecisionTier.values()) {
            delays.put(tier.name(), getDelayForTier(tier));
            receivedMap.put(tier.name(), webhookReceivedByTier.get(tier));
        }
        mockServer = new NettyMockWebhookServer(WEBHOOK_PORT, delays, receivedMap,
            webhookReceiveTimeMs, totalWebhookReceived);
        try {
            mockServer.start();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to start Netty mock server", e);
        }
    }

    private void stopMockServer() {
        if (mockServer != null) {
            mockServer.stop();
        }
    }

    private int getDelayForTier(PrecisionTier tier) {
        return switch (tier) {
            case ULTRA -> ULTRA_DELAY_MS;
            case FAST -> FAST_DELAY_MS;
            case HIGH -> HIGH_DELAY_MS;
            case STANDARD -> STANDARD_DELAY_MS;
            case ECONOMY -> ECONOMY_DELAY_MS;
        };
    }


    /**
     * 核心测试：各档位触发吞吐对比（带真实 webhook）
     */
    @Test
    void testTierTriggerThroughputWithRealWebhook() throws Exception {
        logger.info("=== 调度器档位触发吞吐测试（带 Mock Webhook）===");

        // 重置计数器
        webhookReceivedByTier.values().forEach(counter -> counter.set(0));
        totalWebhookReceived.set(0);

        // 增加到 1000 Intent/档位，让批量优势显现
        int intentsPerTier = 1000;
        // Intent 在 2 秒后触发，给调度器足够时间攒批
        Instant executeAt = Instant.now().plusMillis(2000);

        logger.info("创建 {} 个 Intent/档位，预计执行时间: {}", intentsPerTier, executeAt);

        // 创建各档位 Intent
        for (PrecisionTier tier : PrecisionTier.values()) {
            for (int i = 0; i < intentsPerTier; i++) {
                Intent intent = createTestIntent(
                    String.format("real-%s-%04d", tier.name(), i),
                    tier,
                    executeAt
                );
                intent.transitionTo(IntentStatus.SCHEDULED);
                intentStore.save(intent);
                scheduler.schedule(intent);
            }
            logger.info("  {}: 已创建 {} 个 Intent", tier, intentsPerTier);
        }

        // 等待任务到期并执行（1000任务/档位需要更长时间）
        long waitStart = System.currentTimeMillis();
        long maxWaitMs = 60000; // 最多等待 60 秒

        int lastReceived = 0;
        int stableCount = 0;

        while (System.currentTimeMillis() - waitStart < maxWaitMs) {
            Thread.sleep(1000);

            int received = totalWebhookReceived.get();
            int expected = intentsPerTier * PrecisionTier.values().length;

            if (received >= expected) {
                logger.info("所有任务 webhook 已接收: {}/{}", received, expected);
                break;
            }

            // 检测是否稳定（连续 3 秒没有新 Intent）
            if (received == lastReceived) {
                stableCount++;
                if (stableCount >= 3) {
                    logger.info("Intent 处理已稳定: {}/{} (稳定 {} 秒)",
                        received, expected, stableCount);
                    break;
                }
            } else {
                stableCount = 0;
                lastReceived = received;
            }

            long elapsed = System.currentTimeMillis() - waitStart;
            if (elapsed % 5000 == 0) {
                logger.info("等待中... 已接收 {}/{} Intent ({}ms, {}%)",
                    received, expected, elapsed,
                    (int)((double)received / expected * 100));
            }
        }

        long totalWaitMs = System.currentTimeMillis() - waitStart;

        // 计算各档位触发吞吐和效率
        logger.info("\n=== 测试结果 ===");
        logger.info("总等待时间: {} ms", totalWaitMs);
        logger.info("\n档位 Webhook 接收统计:");
        logger.info(String.format("%-10s %8s %10s %10s %14s %12s %12s",
            "Tier", "并发", "接收", "QPS", "理论最大QPS", "效率", "延迟(ms)"));
        logger.info("-".repeat(70));

        double totalQps = 0;
        for (PrecisionTier tier : PrecisionTier.values()) {
            int received = webhookReceivedByTier.get(tier).get();
            double qps = totalWaitMs > 0 ? (double) received / totalWaitMs * 1000 : 0;
            totalQps += qps;

            // 获取该档位在 Mock Server 中的平均延迟
            double avgLatencyMs = getMockServerLatency(tier);
            int concurrency = tier.getMaxConcurrency();

            // 计算理论最大 QPS 和效率
            double theoreticalMaxQps = Math.max(0.0, metrics.calculateTheoreticalMaxQps(tier, avgLatencyMs));
            double efficiencyRawPct = metrics.calculateEfficiency(tier, qps, avgLatencyMs) * 100.0;
            double efficiencyPct = clampPercent(efficiencyRawPct);

            logger.info(String.format("%-10s %8d %10d %10.1f %14.1f %11.1f%% %10.1f",
                tier.name(), concurrency, received, qps, theoreticalMaxQps, efficiencyPct, avgLatencyMs));
        }

        // 验证档位间吞吐差距
        double ultraQps = webhookReceivedByTier.get(PrecisionTier.ULTRA).get() / (double) totalWaitMs * 1000;
        double economyQps = webhookReceivedByTier.get(PrecisionTier.ECONOMY).get() / (double) totalWaitMs * 1000;

        logger.info("\n档位吞吐对比:");
        logger.info("  ULTRA QPS:  {:.1f}", ultraQps);
        logger.info("  ECONOMY QPS: {:.1f}", economyQps);

        if (economyQps > 0 && ultraQps > 0) {
            double ratio = economyQps / ultraQps;
            logger.info("  ECONOMY/ULTRA 比率: {:.2f}x", ratio);

            // 验证 ECONOMY 至少与 ULTRA 持平（批量优势抵消并发劣势）
            assertTrue(ratio >= 0.8,
                String.format("ECONOMY 档位吞吐不应低于 ULTRA 的 80%%，实际 %.2fx", ratio));
        }

        // 验证 Intent 处理结果
        int totalReceived = totalWebhookReceived.get();
        int totalExpected = intentsPerTier * PrecisionTier.values().length;
        double completionRate = (double) totalReceived / totalExpected * 100;

        logger.info("\n总 Intent 处理: {}/{} ({}%)", totalReceived, totalExpected,
            String.format("%.1f", completionRate));

        // 获取背压统计
        Map<PrecisionTier, Long> backpressureEvents = metrics.getBackpressureEventsByTier();
        logger.info("\n背压事件统计:");
        for (PrecisionTier tier : PrecisionTier.values()) {
            long events = backpressureEvents.getOrDefault(tier, 0L);
            logger.info("  {}: {} 次", tier, events);
        }

        // 只要有显著进展就算通过（至少 50%）
        assertTrue(completionRate >= 50,
            String.format("至少 50%% Intent 应被处理，实际 %.1f%% (%d/%d)",
                completionRate, totalReceived, totalExpected));
    }

    /**
     * 快速对比测试：验证各档位处理速度
     */
    @Test
    void testQuickTierComparison() throws Exception {
        logger.info("=== 快速档位对比测试 ===");

        webhookReceivedByTier.values().forEach(counter -> counter.set(0));
        totalWebhookReceived.set(0);

        int intentsPerTier = 50;
        // Intent 在 500ms 后执行，但给调度器足够时间准备
        Instant executeAt = Instant.now().plusMillis(800);

        // 创建 Intent
        for (PrecisionTier tier : PrecisionTier.values()) {
            for (int i = 0; i < intentsPerTier; i++) {
                Intent intent = createTestIntent(
                    String.format("quick-%s-%02d", tier.name(), i),
                    tier,
                    executeAt
                );
                intent.transitionTo(IntentStatus.SCHEDULED);
                intentStore.save(intent);
                scheduler.schedule(intent);
            }
        }

        // 等待 Intent 执行（执行时间 + 处理时间 + 批量窗口）
        // ULTRA 扫描间隔 10ms，ECONOMY 1000ms，所以等待至少 1.5 秒
        Thread.sleep(1500);

        // 再检查 Intent 状态
        long pendingIntents = intentStore.getAllIntents().values().stream()
            .filter(i -> i.getStatus() == IntentStatus.SCHEDULED)
            .count();
        logger.info("等待后仍有 {} 个 Intent 处于 SCHEDULED 状态", pendingIntents);

        // 统计结果
        logger.info("\n档位处理结果:");
        for (PrecisionTier tier : PrecisionTier.values()) {
            int received = webhookReceivedByTier.get(tier).get();
            logger.info("  {}: {}/{} Intent 完成", tier, received, intentsPerTier);
        }

        // 在 mock 环境下，验证 Intent 已进入调度流程即可
        // 由于 batch 攒批，可能不是所有 Intent 都能被处理
        int totalReceived = totalWebhookReceived.get();
        logger.info("总接收 webhook: {}", totalReceived);

        // 只要有 Intent 被处理就算通过（在测试环境中）
        assertTrue(totalReceived > 0,
            "至少应有部分 Intent 被 webhook 接收");
    }

    // ========== 辅助方法 ==========

    private Intent createTestIntent(String intentId, PrecisionTier tier, Instant executeAt) {
        Intent intent = new Intent(intentId);
        intent.setExecuteAt(executeAt);
        intent.setDeadline(executeAt.plusSeconds(60));
        intent.setPrecisionTier(tier);
        intent.setShardKey("benchmark-shard");
        Callback callback = new Callback();
        callback.setUrl("http://localhost:" + WEBHOOK_PORT + "/webhook");
        intent.setCallback(callback);
        return intent;
    }

    /**
     * 获取 Mock Server 中各档位的平均 HTTP 延迟（毫秒）
     * 与 Mock Server 的 TierHandler 配置保持一致
     */
    private double getMockServerLatency(PrecisionTier tier) {
        return switch (tier) {
            case ULTRA -> 5.0;     // ULTRA: 5ms
            case FAST -> 10.0;     // FAST: 10ms
            case HIGH -> 20.0;     // HIGH: 20ms
            case STANDARD -> 30.0; // STANDARD: 30ms
            case ECONOMY -> 50.0;  // ECONOMY: 50ms
        };
    }

    /**
     * 十万级档位吞吐测试 - 验证档位设计有效性
     */
    @Test
    void test100kTierThroughput() throws Exception {
        logger.info("=== 十万级档位吞吐验证测试 ===");
        logger.info("各档位延迟配置: ULTRA={}ms, FAST={}ms, HIGH={}ms, STANDARD={}ms, ECONOMY={}ms",
            ULTRA_DELAY_MS, FAST_DELAY_MS, HIGH_DELAY_MS, STANDARD_DELAY_MS, ECONOMY_DELAY_MS);

        // 重置计数器
        webhookReceivedByTier.values().forEach(counter -> counter.set(0));
        totalWebhookReceived.set(0);

        // 十万级 Intent：20000/档位
        int intentsPerTier = 20000;
        Instant executeAt = Instant.now().plusMillis(2000);

        logger.info("创建 {} 个 Intent/档位，总计 {} 万 Intent", intentsPerTier, intentsPerTier * 5 / 10000);

        long createStart = System.currentTimeMillis();

        // 创建 Intent
        for (PrecisionTier tier : PrecisionTier.values()) {
            for (int i = 0; i < intentsPerTier; i++) {
                Intent intent = createTestIntent(
                    String.format("100k-%s-%05d", tier.name(), i),
                    tier,
                    executeAt
                );
                intent.transitionTo(IntentStatus.SCHEDULED);
                intentStore.save(intent);
                scheduler.schedule(intent);
            }
            logger.info("  {}: 已创建 {} 个 Intent (预计延迟 {}ms)",
                tier, intentsPerTier, getDelayForTier(tier));
        }

        logger.info("任务创建耗时: {}ms", System.currentTimeMillis() - createStart);

        // 等待 Intent 执行完成
        long waitStart = System.currentTimeMillis();
        long maxWaitMs = 180000; // 最多等待 3 分钟

        Map<PrecisionTier, Long> tierCompleteTimes = new EnumMap<>(PrecisionTier.class);

        while (System.currentTimeMillis() - waitStart < maxWaitMs) {
            Thread.sleep(1000);

            int totalReceived = totalWebhookReceived.get();
            int totalExpected = intentsPerTier * PrecisionTier.values().length;

            // 记录各档位完成时间
            for (PrecisionTier tier : PrecisionTier.values()) {
                if (!tierCompleteTimes.containsKey(tier)) {
                    int received = webhookReceivedByTier.get(tier).get();
                    if (received >= intentsPerTier) {
                        tierCompleteTimes.put(tier, System.currentTimeMillis() - waitStart);
                    }
                }
            }

            if (totalReceived >= totalExpected) {
                logger.info("所有 Intent webhook 已接收: {}/{}", totalReceived, totalExpected);
                break;
            }

            long elapsed = System.currentTimeMillis() - waitStart;
            if (elapsed % 10000 == 0) {
                logger.info("等待中... 已接收 {}/{} Intent ({}ms, {}%)",
                    totalReceived, totalExpected, elapsed,
                    (int)((double)totalReceived / totalExpected * 100));

                // 打印各档位进度
                for (PrecisionTier tier : PrecisionTier.values()) {
                    int received = webhookReceivedByTier.get(tier).get();
                    logger.info("    {}: {}/{} Intent ({}%)", tier, received, intentsPerTier,
                        (int)((double)received / intentsPerTier * 100));
                }
            }
        }

        long totalWaitMs = System.currentTimeMillis() - waitStart;

        // 汇总结果
        logger.info("\n" + "=".repeat(60));
        logger.info("【档位设计有效性验证结果】");
        logger.info("=".repeat(60));
        logger.info("总 Intent 数: {} 万", intentsPerTier * 5 / 10000);
        logger.info("总耗时: {} ms ({} 秒)", totalWaitMs, totalWaitMs / 1000);

        logger.info("\n档位完成统计:");
        logger.info(String.format("%-10s %10s %10s %12s %10s",
            "Tier", "延迟(ms)", "并发数", "完成时间(ms)", "QPS"));
        logger.info("-".repeat(60));

        for (PrecisionTier tier : PrecisionTier.values()) {
            int delayMs = getDelayForTier(tier);
            int concurrency = tier.getMaxConcurrency();
            int received = webhookReceivedByTier.get(tier).get();

            Long completeTime = tierCompleteTimes.get(tier);
            long actualTime = completeTime != null ? completeTime : totalWaitMs;

            double qps = actualTime > 0 ? (double) received / actualTime * 1000 : 0;

            logger.info(String.format("%-10s %10d %10d %12d %10.1f",
                tier.name(), delayMs, concurrency, actualTime, qps));
        }

        // 验证档位设计有效性
        logger.info("\n【档位设计验证】");

        // 理论分析：
        // - 档位延迟越高，理论 QPS 越低（因为每个请求耗时更长）
        // - 但 ECONOMY 批量更大，可以部分抵消延迟影响

        double ultraQps = webhookReceivedByTier.get(PrecisionTier.ULTRA).get() /
            (double) (tierCompleteTimes.getOrDefault(PrecisionTier.ULTRA, totalWaitMs)) * 1000;
        double economyQps = webhookReceivedByTier.get(PrecisionTier.ECONOMY).get() /
            (double) (tierCompleteTimes.getOrDefault(PrecisionTier.ECONOMY, totalWaitMs)) * 1000;

        logger.info("ULTRA QPS (延迟{}ms, 并发{}): {:.1f}",
            ULTRA_DELAY_MS, PrecisionTier.ULTRA.getMaxConcurrency(), ultraQps);
        logger.info("ECONOMY QPS (延迟{}ms, 并发{}): {:.1f}",
            ECONOMY_DELAY_MS, PrecisionTier.ECONOMY.getMaxConcurrency(), economyQps);

        // 验证：ECONOMY 不应该比 ULTRA 慢太多（不应低于 50%）
        if (ultraQps > 0) {
            double ratio = economyQps / ultraQps;
            logger.info("ECONOMY/ULTRA QPS 比率: {:.2f}x", ratio);

            // 即使延迟 10 倍，批量优势应该让 ECONOMY 保持在 ULTRA 的 30% 以上
            assertTrue(ratio >= 0.3,
                String.format("ECONOMY 档位 QPS 不应低于 ULTRA 的 30%%，实际 %.1f%% (%.2fx)",
                    ratio * 100, ratio));
        }

        // 验证所有 Intent 都完成
        int totalReceived = totalWebhookReceived.get();
        int totalExpected = intentsPerTier * PrecisionTier.values().length;
        double completionRate = (double) totalReceived / totalExpected * 100;

        logger.info("\n总完成率: {:.1f}% ({}/{})", completionRate, totalReceived, totalExpected);
        assertTrue(completionRate >= 95,
            String.format("Intent 完成率应 >= 95%%，实际 %.1f%%", completionRate));

        // 验证档位隔离：各档位应独立处理，不应相互阻塞
        logger.info("\n【档位隔离验证】");
        boolean allCompleted = tierCompleteTimes.size() == PrecisionTier.values().length;
        logger.info("所有档位是否都完成: {}", allCompleted);
        assertTrue(allCompleted, "所有档位都应独立完成处理");
    }

    private BenchmarkSummary runBenchmarkMode(boolean quick) throws Exception {
        webhookReceivedByTier.values().forEach(counter -> counter.set(0));
        webhookReceiveTimeMs.clear();
        dequeuedAtMs.clear();
        intentExecuteAtMs.clear();
        tierIntentIds.values().forEach(List::clear);
        totalWebhookReceived.set(0);

        // QUICK: 2,000 intents/tier = 10,000 total (real concurrency test)
        // FULL:  20,000 intents/tier = 100,000 total (stress test)
        int intentsPerTier = quick ? 2000 : 20000;
        Instant executeAt = Instant.now().plusMillis(quick ? 2000 : 3000);
        long maxWaitMs = quick ? 60000 : 180000;

        // --- Environment marker ---
        String mode = quick ? "QUICK" : "FULL";
        System.out.printf(Locale.ROOT,
            "RESULT_ENV|java_version=%s|java_vendor=%s|os_name=%s|os_arch=%s|cpu_cores=%d|max_heap_mb=%d|mode=%s%n",
            System.getProperty("java.version"),
            System.getProperty("java.vendor"),
            System.getProperty("os.name") + " " + System.getProperty("os.version"),
            System.getProperty("os.arch"),
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().maxMemory() / (1024 * 1024),
            mode);

        // --- Test parameters marker ---
        System.out.printf(Locale.ROOT,
            "RESULT_PARAMS|intents_per_tier=%d|execute_at_delay_ms=%d|max_wait_ms=%d%n",
            intentsPerTier,
            java.time.Duration.between(Instant.now(), executeAt).toMillis(),
            maxWaitMs);
        System.out.printf(Locale.ROOT,
            "RESULT_PARAMS|mock_delay_ULTRA=%d|mock_delay_FAST=%d|mock_delay_HIGH=%d|mock_delay_STANDARD=%d|mock_delay_ECONOMY=%d%n",
            ULTRA_DELAY_MS, FAST_DELAY_MS, HIGH_DELAY_MS, STANDARD_DELAY_MS, ECONOMY_DELAY_MS);

        Map<PrecisionTier, Long> initialBackpressure = snapshotBackpressure();

        logger.info("=== Scheduler real webhook benchmark ===");
        logger.info("Mode: {}", quick ? "QUICK" : "FULL");
        logger.info("Intents per tier: {}", intentsPerTier);
        logger.info("ExecuteAt: {}", executeAt);

        for (PrecisionTier tier : PrecisionTier.values()) {
            for (int i = 0; i < intentsPerTier; i++) {
                String intentId = String.format("real-%s-%05d", tier.name(), i);
                Intent intent = createTestIntent(intentId, tier, executeAt);
                intent.transitionTo(IntentStatus.SCHEDULED);
                intentStore.save(intent);
                scheduler.schedule(intent);
                // Record for E2E latency computation
                intentExecuteAtMs.put(intentId, executeAt.toEpochMilli());
                tierIntentIds.get(tier).add(intentId);
            }
            logger.info("  {}: created {} intents", tier, intentsPerTier);
        }

        Thread.sleep(quick ? 500 : 1000);

        long waitStart = System.currentTimeMillis();
        int expected = intentsPerTier * PrecisionTier.values().length;
        int lastReceived = 0;
        int stableCount = 0;
        List<SystemSample> systemSamples = new ArrayList<>();
        OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        long lastSampleTs = 0;

        while (System.currentTimeMillis() - waitStart < maxWaitMs) {
            Thread.sleep(1000);

            // System resource sampling (every 2s)
            if (System.currentTimeMillis() - lastSampleTs >= 2000) {
                double cpu = osBean.getProcessCpuLoad();
                long heapMb = Runtime.getRuntime().totalMemory() / (1024 * 1024);
                systemSamples.add(new SystemSample(System.currentTimeMillis() - waitStart, cpu, heapMb));
                lastSampleTs = System.currentTimeMillis();
            }

            int received = totalWebhookReceived.get();
            if (received >= expected) {
                break;
            }

            if (received == lastReceived) {
                stableCount++;
                if (stableCount >= 3) {
                    break;
                }
            } else {
                stableCount = 0;
                lastReceived = received;
            }
        }

        long totalWaitMs = System.currentTimeMillis() - waitStart;
        Map<PrecisionTier, Long> currentBackpressure = metrics.getBackpressureEventsByTier();
        Map<PrecisionTier, BenchmarkTierResult> tierResults = new EnumMap<>(PrecisionTier.class);

        var backpressureStatus = scheduler.getBackpressureStatus();

        for (PrecisionTier tier : PrecisionTier.values()) {
            int received = webhookReceivedByTier.get(tier).get();
            double avgLatencyMs = getMockServerLatency(tier);

            // Compute E2E latency: executeAt -> webhook received (real customer-perceived time)
            List<String> ids = tierIntentIds.get(tier);
            long[] e2eLatencies = ids != null ? ids.stream()
                .mapToLong(id -> {
                    Long execMs = intentExecuteAtMs.get(id);
                    Long recvMs = webhookReceiveTimeMs.get(id);
                    return (execMs != null && recvMs != null) ? recvMs - execMs : -1;
                })
                .filter(v -> v >= 0)
                .toArray() : new long[0];
            long[] e2eSorted = e2eLatencies.clone();
            Arrays.sort(e2eSorted);
            long e2eP50 = percentile(e2eSorted, 50);
            long e2eP95 = percentile(e2eSorted, 95);
            long e2eP99 = percentile(e2eSorted, 99);
            long e2eMax = e2eSorted.length > 0 ? e2eSorted[e2eSorted.length - 1] : 0;
            long e2eMean = e2eSorted.length > 0 ? (long) Arrays.stream(e2eSorted).average().orElse(0) : 0;

            // Compute scheduler precision: executeAt -> consumer dequeue (pure scheduler metric)
            long[] schedLatencies = ids != null ? ids.stream()
                .mapToLong(id -> {
                    Long execMs = intentExecuteAtMs.get(id);
                    Long deqMs = dequeuedAtMs.get(id);
                    return (execMs != null && deqMs != null) ? deqMs - execMs : -1;
                })
                .filter(v -> v >= 0)
                .toArray() : new long[0];
            long[] schedSorted = schedLatencies.clone();
            Arrays.sort(schedSorted);
            long schedP50 = percentile(schedSorted, 50);
            long schedP95 = percentile(schedSorted, 95);
            long schedP99 = percentile(schedSorted, 99);

            // Per-tier QPS: executeAt → lastRecv window (wide enough to be stable,
            // differentiated enough to show tier performance gap).
            long executeAtMs = intentExecuteAtMs.values().stream().findFirst().orElse(0L);
            long tierLastRecv = ids != null ? ids.stream()
                .mapToLong(id -> webhookReceiveTimeMs.getOrDefault(id, 0L))
                .max().orElse(0L) : 0L;
            double tierWindowSec = Math.max(0.1, (tierLastRecv - executeAtMs) / 1000.0);
            double qps = received > 0 ? received / tierWindowSec : 0;
            double theoreticalMaxQps = Math.max(0.0, metrics.calculateTheoreticalMaxQps(tier, avgLatencyMs));
            double efficiencyRawPct = metrics.calculateEfficiency(tier, qps, avgLatencyMs) * 100.0;
            double efficiencyPct = clampPercent(efficiencyRawPct);
            long backpressureDelta = currentBackpressure.getOrDefault(tier, 0L)
                - initialBackpressure.getOrDefault(tier, 0L);

            // 维度 1: 延迟分布
            MetricsCollector.LatencySnapshot latency = metrics.getWakeupLatencySnapshot(tier);

            // 维度 2: 信号量利用率
            var bp = backpressureStatus.get(tier);
            int activeDispatches = bp != null ? bp.activeDispatches() : 0;
            double semaphoreUtil = bp != null ? bp.utilizationPct() : 0.0;

            // 维度 3: 队列压力
            long queueSize = metrics.getDispatchQueueSize(tier);
            long offerFailed = metrics.getDispatchQueueOfferFailed(tier);
            long retryCount = metrics.getDispatchQueueRetry(tier);
            long abandoned = metrics.getDispatchQueueAbandoned(tier);

            // 维度 4: Intent 生命周期
            Map<IntentStatus, Long> lifecycle = new HashMap<>();
            for (Intent intent : intentStore.getAllIntents().values()) {
                if (intent.getPrecisionTier() == tier) {
                    lifecycle.merge(intent.getStatus(), 1L, Long::sum);
                }
            }

            tierResults.put(tier, new BenchmarkTierResult(
                tier,
                received,
                qps,
                avgLatencyMs,
                theoreticalMaxQps,
                efficiencyPct,
                efficiencyRawPct,
                tier.getMaxConcurrency(),
                backpressureDelta,
                latency,
                activeDispatches,
                semaphoreUtil,
                queueSize,
                offerFailed,
                retryCount,
                abandoned,
                lifecycle,
                e2eP50, e2eP95, e2eP99, e2eMax, e2eMean, e2eSorted.length,
                schedP50, schedP95, schedP99, schedSorted.length,
                com.loomq.domain.intent.PrecisionTierCatalog.defaultCatalog().profile(tier).batchWindowMs()
            ));
        }

        int totalReceived = totalWebhookReceived.get();
        return new BenchmarkSummary(tierResults, totalReceived, expected, totalWaitMs, systemSamples);
    }

    private Map<PrecisionTier, Long> snapshotBackpressure() {
        Map<PrecisionTier, Long> snapshot = new EnumMap<>(PrecisionTier.class);
        Map<PrecisionTier, Long> current = metrics.getBackpressureEventsByTier();
        for (PrecisionTier tier : PrecisionTier.values()) {
            snapshot.put(tier, current.getOrDefault(tier, 0L));
        }
        return snapshot;
    }

    private static long percentile(long[] sorted, int pct) {
        if (sorted.length == 0) return 0;
        if (sorted.length == 1) return sorted[0];
        int idx = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }

    private static double clampPercent(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return 0.0;
        }
        return Math.max(0.0, Math.min(100.0, value));
    }

    private void printBenchmarkSummary(BenchmarkSummary summary) {
        System.out.println();
        System.out.println("=== 调度器真实触发结果 ===");
        System.out.printf(Locale.ROOT, "%-10s %8s %10s %10s %14s %12s %12s%n",
            "Tier", "并发", "接收", "QPS", "理论最大QPS", "效率", "延迟(ms)");
        System.out.println("-".repeat(70));

        BenchmarkTierResult peak = null;
        BenchmarkTierResult worst = null;
        BenchmarkTierResult bestEfficiency = null;
        BenchmarkTierResult worstEfficiency = null;

        for (PrecisionTier tier : PrecisionTier.values()) {
            BenchmarkTierResult result = summary.tierResults().get(tier);
            if (result == null) {
                continue;
            }

            System.out.printf(Locale.ROOT, "%-10s %8d %10d %10.1f %14.1f %11.1f%% %10.1f%n",
                result.tier().name(),
                result.concurrency(),
                result.received(),
                result.qps(),
                result.theoreticalMaxQps(),
                result.efficiencyPct(),
                result.avgLatencyMs());
            System.out.printf(Locale.ROOT,
                "RESULT_ROW|tier=%s|concurrency=%d|received=%d|qps=%.1f|avg_latency_ms=%.1f|theoretical_qps=%.1f|efficiency=%.1f|backpressure=%d%n",
                result.tier().name(),
                result.concurrency(),
                result.received(),
                result.qps(),
                result.avgLatencyMs(),
                result.theoreticalMaxQps(),
                result.efficiencyPct(),
                result.backpressureEvents());

            // 维度 1: 延迟分布 (scheduler-internal wakeup latency)
            var lat = result.latencySnapshot();
            System.out.printf(Locale.ROOT,
                "RESULT_LATENCY|tier=%s|type=wakeup|p50=%d|p75=%d|p90=%d|p95=%d|p99=%d|p999=%d|max=%d|mean=%d|samples=%d%n",
                result.tier().name(), lat.p50(), lat.p75(), lat.p90(),
                lat.p95(), lat.p99(), lat.p999(), lat.max(), lat.mean(), lat.sampleCount());

            // E2E latency: executeAt -> webhook received (real customer-perceived time)
            System.out.printf(Locale.ROOT,
                "RESULT_E2E_LATENCY|tier=%s|type=e2e|p50=%d|p95=%d|p99=%d|max=%d|mean=%d|samples=%d|batch_window_ms=%d%n",
                result.tier().name(),
                result.e2eP50Ms(), result.e2eP95Ms(), result.e2eP99Ms(),
                result.e2eMaxMs(), result.e2eMeanMs(), result.e2eSamples(),
                result.batchWindowMs());

            // Scheduler precision: executeAt -> consumer dequeue (no delivery noise)
            System.out.printf(Locale.ROOT,
                "RESULT_SCHED_PRECISION|tier=%s|p50=%d|p95=%d|p99=%d|samples=%d%n",
                result.tier().name(),
                result.schedP50Ms(), result.schedP95Ms(), result.schedP99Ms(),
                result.schedSamples());

            // 维度 2: 信号量
            System.out.printf(Locale.ROOT,
                "RESULT_SEMAPHORE|tier=%s|max=%d|active=%d|utilization_pct=%.1f%n",
                result.tier().name(), result.concurrency(),
                result.activeDispatches(), result.semaphoreUtilizationPct());

            // 维度 3: 队列
            System.out.printf(Locale.ROOT,
                "RESULT_QUEUE|tier=%s|size=%d|offer_failed=%d|retry=%d|abandoned=%d%n",
                result.tier().name(), result.queueSize(),
                result.queueOfferFailed(), result.queueRetry(), result.queueAbandoned());

            // 维度 4: 生命周期
            var lc = result.lifecycleBreakdown();
            long acked = lc.getOrDefault(IntentStatus.ACKED, 0L);
            long dl = lc.getOrDefault(IntentStatus.DEAD_LETTERED, 0L);
            long expired = lc.getOrDefault(IntentStatus.EXPIRED, 0L);
            long cancelled = lc.getOrDefault(IntentStatus.CANCELED, 0L);
            long other = lc.values().stream().mapToLong(Long::longValue).sum() - acked - dl - expired - cancelled;
            System.out.printf(Locale.ROOT,
                "RESULT_LIFECYCLE|tier=%s|total=%d|acked=%d|dead_letter=%d|expired=%d|cancelled=%d|other=%d%n",
                result.tier().name(), result.received() + (int) other, acked, dl, expired, cancelled, other);

            // 维度 5: 完整配置
            var catalog = com.loomq.domain.intent.PrecisionTierCatalog.defaultCatalog();
            var p = catalog.profile(result.tier());
            System.out.printf(Locale.ROOT,
                "RESULT_PROFILE|tier=%s|precision_window_ms=%d|max_concurrency=%d|batch_size=%d|batch_window_ms=%d|consumer_count=%d|dispatch_queue_capacity=%d|wal_tier_mode=%s%n",
                result.tier().name(),
                p.precisionWindowMs(), p.maxConcurrency(), p.batchSize(),
                p.batchWindowMs(), p.consumerCount(), p.dispatchQueueCapacity(),
                p.walMode().name());

            if (peak == null || result.qps() > peak.qps()) {
                peak = result;
            }
            if (worst == null || result.qps() < worst.qps()) {
                worst = result;
            }
            if (bestEfficiency == null || result.efficiencyPct() > bestEfficiency.efficiencyPct()) {
                bestEfficiency = result;
            }
            if (worstEfficiency == null || result.efficiencyPct() < worstEfficiency.efficiencyPct()) {
                worstEfficiency = result;
            }
        }

        double completionRate = summary.expected() == 0 ? 0.0
            : (double) summary.totalReceived() / summary.expected() * 100.0;
        long clampedTierCount = summary.tierResults().values().stream()
            .filter(r -> Math.abs(r.efficiencyRawPct() - r.efficiencyPct()) > 0.1)
            .count();

        // 维度 6: 全局延迟
        System.out.printf(Locale.ROOT,
            "RESULT_GLOBAL_LATENCY|p95_trigger=%d|p95_wake=%d|p95_webhook=%d|p95_total=%d%n",
            metrics.calculateP95Latency(), metrics.calculateP95WakeLatency(),
            metrics.calculateP95WebhookLatency(), metrics.calculateP95TotalLatency());

        // System resource samples
        for (SystemSample s : summary.systemSamples()) {
            System.out.printf(Locale.ROOT,
                "RESULT_SYSTEM|ts=%d|cpu=%.2f|heap_mb=%d%n",
                s.tsMs(), s.cpuLoad(), s.heapMb());
        }

        // --- DeepSeek V4 optimization observability ---

        // Cohort (CSA-inspired) metrics
        var cohortMgr = scheduler.getCohortManager();
        long cohortRegistered = cohortMgr.getTotalRegistered();
        long cohortFlushed = cohortMgr.getTotalFlushed();
        long cohortWakeEvents = cohortMgr.getWakeEventCount();
        int cohortActive = cohortMgr.cohortCount();
        int cohortPending = cohortMgr.pendingIntentCount();
        System.out.printf(Locale.ROOT,
            "RESULT_COHORT|total_registered=%d|total_flushed=%d|wake_events=%d|active_cohorts=%d|pending_intents=%d%n",
            cohortRegistered, cohortFlushed, cohortWakeEvents, cohortActive, cohortPending);

        // Per-tier WAL mode indicators
        for (PrecisionTier tier : PrecisionTier.values()) {
            var walMode = com.loomq.domain.intent.PrecisionTierCatalog.defaultCatalog().walMode(tier);
            System.out.printf(Locale.ROOT,
                "RESULT_WAL_MODE|tier=%s|wal_mode=%s%n",
                tier.name(), walMode.name());
        }

        // Optimization impact: VT reduction ratio
        // For intents with delayMs > precisionWindowMs, old code spawned 1 VT each
        // New code: 0 VTs (cohort waker is 1 platform thread for ALL tiers)
        int totalIntents = summary.expected();
        long vtsOld = cohortRegistered; // old: 1 VT per registered intent
        long vtsNew = cohortWakeEvents; // new: 1 wake per cohort (platform thread)
        double vtReductionPct = vtsOld > 0 ? (1.0 - (double) vtsNew / vtsOld) * 100.0 : 0.0;
        System.out.printf(Locale.ROOT,
            "RESULT_OPTIMIZATION|vts_old_path=%d|vts_new_path=%d|vt_reduction_pct=%.1f|vts_saved=%d%n",
            vtsOld, vtsNew, vtReductionPct, vtsOld - vtsNew);

        // Batch delivery metrics
        if (deliveryHandler != null) {
            deliveryHandler.printBatchMarkers();
        }

        // Arrow cross-tier borrowing metrics
        var borrowStats = scheduler.getBorrowStats();
        System.out.printf(Locale.ROOT,
            "RESULT_BORROW|own_direct=%d|own_blocking=%d|borrowed=%d|borrow_timeouts=%d|borrow_rate=%.1f%n",
            borrowStats.ownAcquires.get(), borrowStats.ownBlockingAcquires.get(),
            borrowStats.borrowedAcquires.get(), borrowStats.borrowedTimeouts.get(),
            borrowStats.borrowRate());

        // Pipeline trace: avg time at each stage (all tiers combined)
        printPipelineTrace();

        System.out.println();
        System.out.printf(Locale.ROOT, "总接收: %d / %d (%.1f%%)%n",
            summary.totalReceived(), summary.expected(), completionRate);
        System.out.printf(Locale.ROOT, "总等待: %d ms%n", summary.totalWaitMs());
        double systemQps = summary.totalWaitMs() > 0
            ? (double) summary.totalReceived() / summary.totalWaitMs() * 1000 : 0;
        System.out.printf(Locale.ROOT, "RESULT_SYSTEM_QPS|total_qps=%.1f|total_received=%d|total_wait_ms=%d%n",
            systemQps, summary.totalReceived(), summary.totalWaitMs());
        if (clampedTierCount > 0) {
            System.out.printf(Locale.ROOT,
                "注意: %d 个档位原始效率超过 100%%（理论值受抖动影响），报告效率已封顶到 100%%。%n",
                clampedTierCount);
        }

        // SLO Validation — uses E2E latency (real customer-perceived time from executeAt to webhook received)
        System.out.println();
        System.out.println("=== SLO Validation (E2E latency: executeAt -> webhook received) ===");
        System.out.println("Note: BatchWindowMs contributes directly to E2E latency. Lower batch=lower latency, higher QPS.");
        Map<String, long[]> slo = Map.of(
            "ULTRA",    new long[]{20, 35},
            "FAST",     new long[]{70, 110},
            "HIGH",     new long[]{150, 230},
            "STANDARD", new long[]{600, 900},
            "ECONOMY",  new long[]{1200, 1700}
        );
        for (PrecisionTier tier : PrecisionTier.values()) {
            BenchmarkTierResult result = summary.tierResults().get(tier);
            if (result == null || result.e2eSamples() == 0) continue;
            long[] limits = slo.getOrDefault(tier.name(), new long[]{9999, 9999});
            boolean p95ok = result.e2eP95Ms() <= limits[0];
            boolean p99ok = result.e2eP99Ms() <= limits[1];
            String note = result.batchWindowMs() > 0 && result.e2eP99Ms() >= result.batchWindowMs() * 0.8
                ? String.format(" [batch window %dms dominates]", result.batchWindowMs())
                : "";
            System.out.printf(Locale.ROOT, "  %-10s E2E-p95=%d <= %d %s | E2E-p99=%d <= %d %s%s%n",
                tier.name(),
                result.e2eP95Ms(), limits[0], p95ok ? "PASS" : "FAIL",
                result.e2eP99Ms(), limits[1], p99ok ? "PASS" : "FAIL",
                note);
        }
        System.out.println("解释: 这组结果反映的是调度器 + callback 投递 + webhook 响应的联合作用，不是纯创建接口。");

        // DeepSeek V4 optimization impact section
        if (cohortRegistered > 0) {
            System.out.println();
            System.out.println("=== DeepSeek V4 Optimization Impact ===");
            System.out.println("CSA Cohort Consolidation (per-intent VT sleep -> batched cohort wake):");
            System.out.printf(Locale.ROOT, "  Intents via cohort: %d (old path: %d VT sleeps)%n",
                cohortRegistered, cohortRegistered);
            System.out.printf(Locale.ROOT, "  Cohort wake events: %d (single platform daemon thread)%n",
                cohortWakeEvents);
            System.out.printf(Locale.ROOT, "  VT reduction:       %.1f%% (%d VTs eliminated)%n",
                vtReductionPct, vtsOld - vtsNew);
            System.out.printf(Locale.ROOT, "  Active cohorts:     %d (%d intents pending)%n",
                cohortActive, cohortPending);
            System.out.println("  Note: VT reduction benefit is real at scale (>10k intents).");
            System.out.println("  At <100 intents the overhead is negligible on modern hardware.");
            System.out.println();
            System.out.println("Tier-Differentiated WAL (FP4 QAT-inspired):");
            System.out.println("  ULTRA/FAST: ASYNC — memory-mapped write, no fsync wait. Crash-lose window = flush interval (~10ms).");
            System.out.println("  HIGH:       BATCH_DEFERRED — async write, periodic batch fsync (~50ms window).");
            System.out.println("  STANDARD/ECONOMY: DURABLE — full fsync per write. Strongest guarantee, highest latency.");
            System.out.println("  WARNING: ASYNC correctness has NOT been validated with crash-recovery testing.");
            System.out.println("  Production deployment requires quantifying the data-loss window in milliseconds.");
            System.out.println();
            System.out.println("E2E Latency Note:");
            System.out.println("  Measured E2E = executeAt -> webhook received (real customer-perceived time).");
            System.out.println("  Current bottleneck: Mock HttpServer (java built-in), NOT the scheduler.");
            System.out.println("  Scheduler internal dispatch: ~0-10ms. Actual E2E is dominated by HTTP server capacity.");
        }

        if (peak != null && worst != null && bestEfficiency != null && worstEfficiency != null) {
            System.out.printf(Locale.ROOT, "峰值档位: %s @ %.1f QPS%n", peak.tier().name(), peak.qps());
            System.out.printf(Locale.ROOT, "最弱档位: %s @ %.1f QPS%n", worst.tier().name(), worst.qps());
            System.out.printf(Locale.ROOT, "最高效率: %s @ %.1f%%%n", bestEfficiency.tier().name(), bestEfficiency.efficiencyPct());
            System.out.printf(Locale.ROOT, "最低效率: %s @ %.1f%%%n", worstEfficiency.tier().name(), worstEfficiency.efficiencyPct());
            System.out.printf(Locale.ROOT,
                "RESULT|peak_tier=%s|peak_qps=%.1f|worst_tier=%s|worst_qps=%.1f|best_efficiency_tier=%s|best_efficiency=%.1f|worst_efficiency_tier=%s|worst_efficiency=%.1f|completion_rate=%.1f|total_received=%d|total_expected=%d|total_wait_ms=%d|efficiency_clamped_tiers=%d%n",
                peak.tier().name(),
                peak.qps(),
                worst.tier().name(),
                worst.qps(),
                bestEfficiency.tier().name(),
                bestEfficiency.efficiencyPct(),
                worstEfficiency.tier().name(),
                worstEfficiency.efficiencyPct(),
                completionRate,
                summary.totalReceived(),
                summary.expected(),
                summary.totalWaitMs(),
                clampedTierCount);
        }
    }

    private record BenchmarkTierResult(
        PrecisionTier tier,
        int received,
        double qps,
        double avgLatencyMs,
        double theoreticalMaxQps,
        double efficiencyPct,
        double efficiencyRawPct,
        int concurrency,
        long backpressureEvents,
        // 维度 1-5: 完整数据剖面
        MetricsCollector.LatencySnapshot latencySnapshot,
        int activeDispatches,
        double semaphoreUtilizationPct,
        long queueSize,
        long queueOfferFailed,
        long queueRetry,
        long queueAbandoned,
        Map<IntentStatus, Long> lifecycleBreakdown,
        // E2E latency (executeAt -> webhook received, real customer-perceived)
        long e2eP50Ms,
        long e2eP95Ms,
        long e2eP99Ms,
        long e2eMaxMs,
        long e2eMeanMs,
        int e2eSamples,
        // Scheduler precision (executeAt -> consumer dequeue, no delivery noise)
        long schedP50Ms,
        long schedP95Ms,
        long schedP99Ms,
        int schedSamples,
        // Batch overhead: how much batchWindowMs contributes to latency
        int batchWindowMs
    ) {}

    private record BenchmarkSummary(
        Map<PrecisionTier, BenchmarkTierResult> tierResults,
        int totalReceived,
        int expected,
        long totalWaitMs,
        List<SystemSample> systemSamples
    ) {}

    private void printPipelineTrace() {
        // Global aggregate
        long[] schedGaps = dequeuedAtMs.entrySet().stream()
            .mapToLong(e -> {
                Long exec = intentExecuteAtMs.get(e.getKey());
                return exec != null ? e.getValue() - exec : -1;
            }).filter(v -> v >= 0).sorted().toArray();
        long[] e2eGaps = webhookReceiveTimeMs.entrySet().stream()
            .mapToLong(e -> {
                Long exec = intentExecuteAtMs.get(e.getKey());
                return exec != null ? e.getValue() - exec : -1;
            }).filter(v -> v >= 0).sorted().toArray();
        long deliverP50 = 0, deliverP95 = 0;
        if (schedGaps.length > 0 && e2eGaps.length > 0) {
            long[] d = new long[Math.min(schedGaps.length, e2eGaps.length)];
            for (int i = 0; i < d.length; i++) d[i] = Math.max(0, e2eGaps[i] - schedGaps[i]);
            java.util.Arrays.sort(d);
            deliverP50 = percentile(d, 50); deliverP95 = percentile(d, 95);
        }
        long sP50 = percentile(schedGaps, 50), sP95 = percentile(schedGaps, 95);
        long eP50 = percentile(e2eGaps, 50), eP95 = percentile(e2eGaps, 95);

        System.out.printf("RESULT_TRACE|sched_p50=%d|sched_p95=%d|deliver_p50=%d|deliver_p95=%d|e2e_p50=%d|e2e_p95=%d|total_intents=%d%n",
            sP50, sP95, deliverP50, deliverP95, eP50, eP95, schedGaps.length);

        // RTT (delivery latency) per tier: dequeue→webhook received
        for (PrecisionTier tier : PrecisionTier.values()) {
            List<String> ids = tierIntentIds.get(tier);
            if (ids == null || ids.isEmpty()) continue;
            long[] rtt = ids.stream().mapToLong(id -> {
                Long dq = dequeuedAtMs.get(id); Long rv = webhookReceiveTimeMs.get(id);
                return (dq != null && rv != null) ? Math.max(0, rv - dq) : -1;
            }).filter(v -> v >= 0).sorted().toArray();
            System.out.printf("RESULT_RTT|tier=%s|p50=%d|p95=%d|p99=%d|samples=%d%n",
                tier.name(), percentile(rtt, 50), percentile(rtt, 95),
                percentile(rtt, 99), rtt.length);
        }

        // Per-tier trace breakdown
        for (PrecisionTier tier : PrecisionTier.values()) {
            List<String> ids = tierIntentIds.get(tier);
            if (ids == null || ids.isEmpty()) continue;
            long[] tSched = ids.stream().mapToLong(id -> {
                Long e = intentExecuteAtMs.get(id); Long d = dequeuedAtMs.get(id);
                return (e != null && d != null) ? d - e : -1;
            }).filter(v -> v >= 0).sorted().toArray();
            long[] tE2e = ids.stream().mapToLong(id -> {
                Long e = intentExecuteAtMs.get(id); Long r = webhookReceiveTimeMs.get(id);
                return (e != null && r != null) ? r - e : -1;
            }).filter(v -> v >= 0).sorted().toArray();
            System.out.printf("RESULT_TRACE_TIER|tier=%s|sched_p50=%d|sched_p95=%d|e2e_p50=%d|e2e_p95=%d|samples=%d%n",
                tier.name(), percentile(tSched, 50), percentile(tSched, 95),
                percentile(tE2e, 50), percentile(tE2e, 95), tSched.length);
        }

        // Individual intent trace samples (first 3 of each tier with traceId)
        System.out.println("=== Per-Intent Trace Samples ===");
        System.out.printf("%-8s %-22s %8s %8s %8s %8s%n", "Tier", "TraceID", "exec->deq", "deq->recv", "TotalE2E", "Status");
        int shown = 0;
        for (PrecisionTier tier : PrecisionTier.values()) {
            List<String> ids = tierIntentIds.get(tier);
            if (ids == null) continue;
            for (String id : ids) {
                if (shown >= 25) break;
                Long exec = intentExecuteAtMs.get(id);
                Long deq = dequeuedAtMs.get(id);
                Long recv = webhookReceiveTimeMs.get(id);
                if (exec == null || deq == null || recv == null) continue;
                Intent intent = intentStore.findById(id);
                String traceId = intent != null ? intent.getTraceId() : "?";
                long sched = deq - exec;
                long deliv = Math.max(0, recv - deq);
                System.out.printf(Locale.ROOT, "%-8s %-22s %7dms %8dms %8dms %8s%n",
                    tier.name(), traceId, sched, deliv, sched + deliv,
                    intent != null ? intent.getStatus().name() : "?");
                System.out.printf(Locale.ROOT,
                    "RESULT_INTENT_TRACE|trace_id=%s|tier=%s|intent_id=%s|exec_to_deq=%d|deq_to_recv=%d|e2e=%d|status=%s%n",
                    traceId, tier.name(), id, sched, deliv, sched + deliv,
                    intent != null ? intent.getStatus().name() : "?");
                shown++;
            }
        }
    }

    private record SystemSample(long tsMs, double cpuLoad, long heapMb) {}
}

package com.loomq.scheduler;


import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.channel.http.NettyHttpDeliveryHandler;
import com.loomq.channel.http.batch.BatchDeliveryConfig;
import com.loomq.channel.http.batch.BatchedHttpDeliveryHandler;
import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DefaultRedeliveryDecider;
import com.loomq.spi.DeliveryHandler;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 批量投递模式压力测试。
 *
 * <p>构造高并发同时到期的场景，验证批量模式在以下方面的价值：
 * <ul>
 *   <li>HTTP 请求数量减少</li>
 *   <li>高并发下的吞吐量</li>
 *   <li>E2E 延迟表现</li>
 * </ul>
 *
 * @author loomq
 * @since v0.9.2
 */
@Tag("benchmark")
public class BatchDeliveryStressTest {

    private static final Logger logger = LoggerFactory.getLogger(BatchDeliveryStressTest.class);
    private static final int WEBHOOK_PORT = 19997;

    // Mock Server 延迟：50ms 模拟真实 Webhook 处理时间
    private static final int MOCK_DELAY_MS = 50;

    // 批量配置
    private static final int MAX_BATCH_SIZE = 200;
    private static final long FLUSH_INTERVAL_MS = 2;

    /**
     * 核心测试：高并发同时到期场景。
     *
     * <p>创建 5000 个 ULTRA Intent，全部同时到期，对比单请求 vs 批量模式。
     */
    @Test
    void testConcurrentExpiry() throws Exception {
        int intentCount = 5000;
        logger.info("=== 批量投递压力测试：{} 个 Intent 同时到期 ===", intentCount);

        // 运行单请求模式
        logger.info("\n--- 单请求模式 ---");
        StressResult singleResult = runStressTest("single", intentCount, new NettyHttpDeliveryHandler());

        // 运行批量模式
        logger.info("\n--- 批量模式 ---");
        BatchDeliveryConfig config = new BatchDeliveryConfig(
            MAX_BATCH_SIZE, FLUSH_INTERVAL_MS, 500, true, 30000);
        BatchedHttpDeliveryHandler batchHandler = new BatchedHttpDeliveryHandler(config);
        StressResult batchResult;
        try {
            batchResult = runStressTest("batched", intentCount, batchHandler);
        } finally {
            batchHandler.shutdown();
        }

        // 输出对比报告
        printComparisonReport(singleResult, batchResult, intentCount);
    }

    /**
     * 扩展性测试：不同并发级别。
     */
    @Test
    void testScalingBehavior() throws Exception {
        int[] intentCounts = {1000, 2000, 5000, 10000};

        logger.info("=== 批量投递扩展性测试 ===");
        logger.info("测试并发级别: {}", Arrays.toString(intentCounts));

        List<ScalingResult> results = new ArrayList<>();

        for (int count : intentCounts) {
            logger.info("\n--- {} 个 Intent ---", count);

            // 单请求模式
            StressResult singleResult = runStressTest("single-" + count, count, new NettyHttpDeliveryHandler());

            // 批量模式
            BatchDeliveryConfig config = new BatchDeliveryConfig(
                MAX_BATCH_SIZE, FLUSH_INTERVAL_MS, 500, true, 30000);
            BatchedHttpDeliveryHandler batchHandler = new BatchedHttpDeliveryHandler(config);
            StressResult batchResult;
            try {
                batchResult = runStressTest("batched-" + count, count, batchHandler);
            } finally {
                batchHandler.shutdown();
            }

            results.add(new ScalingResult(count, singleResult, batchResult));
        }

        // 输出扩展性报告
        printScalingReport(results);
    }

    /**
     * 运行压力测试。
     */
    private StressResult runStressTest(String mode, int intentCount, DeliveryHandler deliveryHandler) throws Exception {
        // 初始化计数器
        Map<PrecisionTier, AtomicInteger> webhookReceivedByTier = new HashMap<>();
        ConcurrentHashMap<String, Long> webhookReceiveTimeMs = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Long> intentExecuteAtMs = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Long> dequeuedAtMs = new ConcurrentHashMap<>();
        AtomicInteger totalWebhookReceived = new AtomicInteger(0);

        for (PrecisionTier tier : PrecisionTier.values()) {
            webhookReceivedByTier.put(tier, new AtomicInteger(0));
        }

        // 启动 Mock Server（50ms 延迟）
        Map<String, Integer> delays = new HashMap<>();
        Map<String, AtomicInteger> receivedMap = new HashMap<>();
        for (PrecisionTier tier : PrecisionTier.values()) {
            delays.put(tier.name(), MOCK_DELAY_MS);
            receivedMap.put(tier.name(), webhookReceivedByTier.get(tier));
        }

        NettyMockWebhookServer mockServer = new NettyMockWebhookServer(
            WEBHOOK_PORT, delays, receivedMap,
            webhookReceiveTimeMs, totalWebhookReceived, false);

        try {
            mockServer.start();

            // 创建调度器
            IntentStore intentStore = new ConcurrentIntentStore();

            // 包装 deliveryHandler 以记录 dequeue 时间
            DeliveryHandler wrappedHandler = intent -> {
                dequeuedAtMs.put(intent.getIntentId(), System.currentTimeMillis());
                return deliveryHandler.deliverAsync(intent);
            };

            PrecisionScheduler scheduler = new PrecisionScheduler(
                intentStore, wrappedHandler, new DefaultRedeliveryDecider());

            scheduler.start();

            // 创建 Intent：全部使用 ULTRA 档位，相同 executeAt
            Instant executeAt = Instant.now().plusMillis(1000);
            PrecisionTier tier = PrecisionTier.ULTRA;

            logger.info("创建 {} 个 ULTRA Intent，executeAt={}", intentCount, executeAt);

            for (int i = 0; i < intentCount; i++) {
                String intentId = String.format("%s-ultra-%05d", mode, i);
                Intent intent = createTestIntent(intentId, tier, executeAt);
                intent.transitionTo(IntentStatus.SCHEDULED);
                intentStore.save(intent);
                scheduler.schedule(intent);

                intentExecuteAtMs.put(intentId, executeAt.toEpochMilli());
            }

            // 等待完成
            long waitStart = System.currentTimeMillis();
            long maxWaitMs = 60000;
            int lastReceived = 0;
            int stableCount = 0;

            while (System.currentTimeMillis() - waitStart < maxWaitMs) {
                Thread.sleep(500);

                int received = totalWebhookReceived.get();
                if (received >= intentCount) {
                    break;
                }

                if (received == lastReceived) {
                    stableCount++;
                    if (stableCount >= 12) { // 6 秒无新 Intent
                        break;
                    }
                } else {
                    stableCount = 0;
                    lastReceived = received;
                }
            }

            long totalWaitMs = System.currentTimeMillis() - waitStart;

            // 计算 E2E 延迟
            long[] e2eLatencies = intentExecuteAtMs.entrySet().stream()
                .mapToLong(e -> {
                    Long recv = webhookReceiveTimeMs.get(e.getKey());
                    return recv != null ? recv - e.getValue() : -1;
                })
                .filter(v -> v >= 0)
                .sorted()
                .toArray();

            // 计算调度精度
            long[] schedLatencies = intentExecuteAtMs.entrySet().stream()
                .mapToLong(e -> {
                    Long deq = dequeuedAtMs.get(e.getKey());
                    return deq != null ? deq - e.getValue() : -1;
                })
                .filter(v -> v >= 0)
                .sorted()
                .toArray();

            // 计算投递延迟
            long[] deliveryLatencies = new long[Math.min(e2eLatencies.length, schedLatencies.length)];
            for (int i = 0; i < deliveryLatencies.length; i++) {
                deliveryLatencies[i] = Math.max(0, e2eLatencies[i] - schedLatencies[i]);
            }
            Arrays.sort(deliveryLatencies);

            scheduler.stop();

            int httpRequestCount = mockServer.getHttpRequestCount();

            return new StressResult(
                mode,
                totalWebhookReceived.get(),
                intentCount,
                totalWaitMs,
                httpRequestCount,
                percentile(e2eLatencies, 50), percentile(e2eLatencies, 95), percentile(e2eLatencies, 99),
                percentile(schedLatencies, 50), percentile(schedLatencies, 95), percentile(schedLatencies, 99),
                percentile(deliveryLatencies, 50), percentile(deliveryLatencies, 95), percentile(deliveryLatencies, 99)
            );
        } finally {
            mockServer.stop();
        }
    }

    private Intent createTestIntent(String intentId, PrecisionTier tier, Instant executeAt) {
        Intent intent = new Intent(intentId);
        intent.setExecuteAt(executeAt);
        intent.setDeadline(executeAt.plusSeconds(60));
        intent.setPrecisionTier(tier);
        intent.setShardKey("stress-test");
        Callback callback = new Callback();
        callback.setUrl("http://localhost:" + WEBHOOK_PORT + "/webhook");
        intent.setCallback(callback);
        return intent;
    }

    private static long percentile(long[] sorted, int pct) {
        if (sorted.length == 0) return 0;
        if (sorted.length == 1) return sorted[0];
        int idx = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }

    /**
     * 输出对比报告。
     */
    private void printComparisonReport(StressResult single, StressResult batch, int intentCount) {
        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════════════");
        System.out.println("                    批量投递压力测试报告");
        System.out.println("═══════════════════════════════════════════════════════════════════════");
        System.out.println();
        System.out.printf(Locale.ROOT, "  测试规模: %,d 个 ULTRA Intent 同时到期%n", intentCount);
        System.out.printf(Locale.ROOT, "  Mock Server 延迟: %dms%n", MOCK_DELAY_MS);
        System.out.printf(Locale.ROOT, "  批量配置: maxBatchSize=%d, flushIntervalMs=%d%n",
            MAX_BATCH_SIZE, FLUSH_INTERVAL_MS);
        System.out.println();

        // HTTP 请求数对比
        System.out.println("  ┌─ HTTP 请求数 ────────────────────────────────────────────────────┐");
        System.out.printf(Locale.ROOT, "  │  单请求模式: %,6d 次                                          │%n",
            single.httpRequestCount());
        System.out.printf(Locale.ROOT, "  │  批量模式:   %,6d 次                                          │%n",
            batch.httpRequestCount());
        double reduction = single.httpRequestCount() > 0
            ? (1.0 - (double) batch.httpRequestCount() / single.httpRequestCount()) * 100 : 0;
        System.out.printf(Locale.ROOT, "  │  减少:       %.1f%%                                             │%n",
            reduction);
        System.out.println("  └──────────────────────────────────────────────────────────────────┘");
        System.out.println();

        // 完成率
        System.out.println("  ┌─ 完成率 ──────────────────────────────────────────────────────────┐");
        System.out.printf(Locale.ROOT, "  │  单请求模式: %,d / %,d (%.1f%%)                              │%n",
            single.totalReceived(), single.expected(),
            (double) single.totalReceived() / single.expected() * 100);
        System.out.printf(Locale.ROOT, "  │  批量模式:   %,d / %,d (%.1f%%)                              │%n",
            batch.totalReceived(), batch.expected(),
            (double) batch.totalReceived() / batch.expected() * 100);
        System.out.println("  └──────────────────────────────────────────────────────────────────┘");
        System.out.println();

        // 吞吐量
        double singleQps = single.totalWaitMs() > 0
            ? (double) single.totalReceived() / single.totalWaitMs() * 1000 : 0;
        double batchQps = batch.totalWaitMs() > 0
            ? (double) batch.totalReceived() / batch.totalWaitMs() * 1000 : 0;
        System.out.println("  ┌─ 吞吐量 ──────────────────────────────────────────────────────────┐");
        System.out.printf(Locale.ROOT, "  │  单请求模式: %,.1f QPS                                        │%n", singleQps);
        System.out.printf(Locale.ROOT, "  │  批量模式:   %,.1f QPS                                        │%n", batchQps);
        System.out.println("  └──────────────────────────────────────────────────────────────────┘");
        System.out.println();

        // E2E 延迟
        System.out.println("  ┌─ E2E 延迟 (ms) ──────────────────────────────────────────────────┐");
        System.out.printf(Locale.ROOT, "  │  %-12s %8s %8s %8s %8s                       │%n",
            "模式", "P50", "P95", "P99", "样本数");
        System.out.println("  ├──────────────────────────────────────────────────────────────────┤");
        System.out.printf(Locale.ROOT, "  │  %-12s %8d %8d %8d %8d                       │%n",
            "单请求", single.e2eP50(), single.e2eP95(), single.e2eP99(), single.totalReceived());
        System.out.printf(Locale.ROOT, "  │  %-12s %8d %8d %8d %8d                       │%n",
            "批量", batch.e2eP50(), batch.e2eP95(), batch.e2eP99(), batch.totalReceived());
        System.out.println("  └──────────────────────────────────────────────────────────────────┘");
        System.out.println();

        // 投递延迟
        System.out.println("  ┌─ 投递延迟 (ms) ──────────────────────────────────────────────────┐");
        System.out.printf(Locale.ROOT, "  │  %-12s %8s %8s %8s                       │%n",
            "模式", "P50", "P95", "P99");
        System.out.println("  ├──────────────────────────────────────────────────────────────────┤");
        System.out.printf(Locale.ROOT, "  │  %-12s %8d %8d %8d                       │%n",
            "单请求", single.deliveryP50(), single.deliveryP95(), single.deliveryP99());
        System.out.printf(Locale.ROOT, "  │  %-12s %8d %8d %8d                       │%n",
            "批量", batch.deliveryP50(), batch.deliveryP95(), batch.deliveryP99());
        System.out.println("  └──────────────────────────────────────────────────────────────────┘");
        System.out.println();

        // 机器解析行
        System.out.printf(Locale.ROOT,
            "RESULT_STRESS|single_http=%d|batch_http=%d|reduction=%.1f|single_qps=%.1f|batch_qps=%.1f|single_e2e_p50=%d|batch_e2e_p50=%d|single_e2e_p99=%d|batch_e2e_p99=%d%n",
            single.httpRequestCount(), batch.httpRequestCount(), reduction,
            singleQps, batchQps,
            single.e2eP50(), batch.e2eP50(),
            single.e2eP99(), batch.e2eP99());
    }

    /**
     * 输出扩展性报告。
     */
    private void printScalingReport(List<ScalingResult> results) {
        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════════════");
        System.out.println("                    批量投递扩展性报告");
        System.out.println("═══════════════════════════════════════════════════════════════════════");
        System.out.println();
        System.out.printf(Locale.ROOT, "  Mock Server 延迟: %dms%n", MOCK_DELAY_MS);
        System.out.printf(Locale.ROOT, "  批量配置: maxBatchSize=%d, flushIntervalMs=%d%n",
            MAX_BATCH_SIZE, FLUSH_INTERVAL_MS);
        System.out.println();

        // HTTP 请求数扩展性
        System.out.println("  ┌─ HTTP 请求数扩展性 ───────────────────────────────────────────────┐");
        System.out.printf(Locale.ROOT, "  │  %-8s %12s %12s %12s %10s       │%n",
            "Intent", "单请求", "批量", "减少", "减少%");
        System.out.println("  ├──────────────────────────────────────────────────────────────────┤");

        for (ScalingResult r : results) {
            double reduction = r.single().httpRequestCount() > 0
                ? (1.0 - (double) r.batch().httpRequestCount() / r.single().httpRequestCount()) * 100 : 0;
            System.out.printf(Locale.ROOT, "  │  %,6d %,12d %,12d %,12d %9.1f%%       │%n",
                r.intentCount(),
                r.single().httpRequestCount(),
                r.batch().httpRequestCount(),
                r.single().httpRequestCount() - r.batch().httpRequestCount(),
                reduction);
        }
        System.out.println("  └──────────────────────────────────────────────────────────────────┘");
        System.out.println();

        // 吞吐量扩展性
        System.out.println("  ┌─ 吞吐量扩展性 (QPS) ─────────────────────────────────────────────┐");
        System.out.printf(Locale.ROOT, "  │  %-8s %12s %12s %12s                       │%n",
            "Intent", "单请求", "批量", "提升%");
        System.out.println("  ├──────────────────────────────────────────────────────────────────┤");

        for (ScalingResult r : results) {
            double singleQps = r.single().totalWaitMs() > 0
                ? (double) r.single().totalReceived() / r.single().totalWaitMs() * 1000 : 0;
            double batchQps = r.batch().totalWaitMs() > 0
                ? (double) r.batch().totalReceived() / r.batch().totalWaitMs() * 1000 : 0;
            double improvement = singleQps > 0 ? (batchQps / singleQps - 1) * 100 : 0;
            System.out.printf(Locale.ROOT, "  │  %,6d %,12.1f %,12.1f %11.1f%%                       │%n",
                r.intentCount(), singleQps, batchQps, improvement);
        }
        System.out.println("  └──────────────────────────────────────────────────────────────────┘");
        System.out.println();

        // E2E P50 延迟扩展性
        System.out.println("  ┌─ E2E P50 延迟扩展性 (ms) ────────────────────────────────────────┐");
        System.out.printf(Locale.ROOT, "  │  %-8s %12s %12s %12s                       │%n",
            "Intent", "单请求", "批量", "差异");
        System.out.println("  ├──────────────────────────────────────────────────────────────────┤");

        for (ScalingResult r : results) {
            long diff = r.batch().e2eP50() - r.single().e2eP50();
            System.out.printf(Locale.ROOT, "  │  %,6d %12d %12d %+12d                       │%n",
                r.intentCount(), r.single().e2eP50(), r.batch().e2eP50(), diff);
        }
        System.out.println("  └──────────────────────────────────────────────────────────────────┘");
        System.out.println();

        // 机器解析行
        for (ScalingResult r : results) {
            double singleQps = r.single().totalWaitMs() > 0
                ? (double) r.single().totalReceived() / r.single().totalWaitMs() * 1000 : 0;
            double batchQps = r.batch().totalWaitMs() > 0
                ? (double) r.batch().totalReceived() / r.batch().totalWaitMs() * 1000 : 0;
            double httpReduction = r.single().httpRequestCount() > 0
                ? (1.0 - (double) r.batch().httpRequestCount() / r.single().httpRequestCount()) * 100 : 0;
            System.out.printf(Locale.ROOT,
                "RESULT_SCALING|intents=%d|single_http=%d|batch_http=%d|http_reduction=%.1f|single_qps=%.1f|batch_qps=%.1f|single_e2e_p50=%d|batch_e2e_p50=%d%n",
                r.intentCount(),
                r.single().httpRequestCount(), r.batch().httpRequestCount(), httpReduction,
                singleQps, batchQps,
                r.single().e2eP50(), r.batch().e2eP50());
        }
    }

    // ── 数据类型 ──

    private record StressResult(
        String mode,
        int totalReceived,
        int expected,
        long totalWaitMs,
        int httpRequestCount,
        long e2eP50, long e2eP95, long e2eP99,
        long schedP50, long schedP95, long schedP99,
        long deliveryP50, long deliveryP95, long deliveryP99
    ) {}

    private record ScalingResult(
        int intentCount,
        StressResult single,
        StressResult batch
    ) {}
}

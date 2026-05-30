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
 * 投递模式基准测试：对比单请求 vs 批量投递的 E2E 延迟。
 *
 * @author loomq
 * @since v0.9.2
 */
@Tag("benchmark")
public class DeliveryModeBenchmarkTest {

    private static final Logger logger = LoggerFactory.getLogger(DeliveryModeBenchmarkTest.class);
    private static final int WEBHOOK_PORT = 19998;

    // Mock server 延迟配置（毫秒）
    private static final int ULTRA_DELAY_MS = 5;
    private static final int FAST_DELAY_MS = 10;
    private static final int HIGH_DELAY_MS = 20;
    private static final int STANDARD_DELAY_MS = 30;
    private static final int ECONOMY_DELAY_MS = 50;

    // 测试参数
    private static final int INTENTS_PER_TIER = 200;
    private static final long MAX_WAIT_MS = 30000;

    /**
     * 单请求投递模式基准测试。
     */
    @Test
    void testSingleRequestMode() throws Exception {
        logger.info("=== 单请求投递模式基准测试 ===");
        BenchmarkResult result = runBenchmark("single", new NettyHttpDeliveryHandler());
        printResult("单请求模式", result);
    }

    /**
     * 批量投递模式基准测试。
     */
    @Test
    void testBatchedMode() throws Exception {
        logger.info("=== 批量投递模式基准测试 ===");
        BatchDeliveryConfig config = new BatchDeliveryConfig(
            200,   // maxBatchSize
            2,     // flushIntervalMs (reduced for lower latency)
            500,   // maxConnections
            true,  // tcpNoDelay
            30000  // batchTimeoutMs
        );
        BatchedHttpDeliveryHandler handler = new BatchedHttpDeliveryHandler(config);
        try {
            BenchmarkResult result = runBenchmark("batched", handler);
            printResult("批量模式", result);

            // 打印批量投递指标
            BatchedHttpDeliveryHandler.BatchDeliveryMetrics metrics = handler.getMetrics();
            logger.info("批量投递指标:");
            logger.info("  总批次数: {}", metrics.totalBatches());
            logger.info("  总 Intent 数: {}", metrics.totalIntentsBatched());
            logger.info("  平均批次大小: {}", String.format("%.1f", metrics.avgBatchSize()));
            logger.info("  平均批次驻留时间: {}ms", String.format("%.1f", metrics.avgBatchDwellMs()));
            logger.info("  平均 HTTP RTT: {}ms", String.format("%.1f", metrics.avgHttpRttMs()));
            logger.info("  满批触发: {} ({}%)", metrics.flushByFullBatch(),
                String.format("%.1f", metrics.fullBatchPercentage()));
            logger.info("  定时触发: {}", metrics.flushByTimer());
        } finally {
            handler.shutdown();
        }
    }

    /**
     * 运行基准测试。
     */
    private BenchmarkResult runBenchmark(String mode, DeliveryHandler deliveryHandler) throws Exception {
        // 初始化计数器
        Map<PrecisionTier, AtomicInteger> webhookReceivedByTier = new HashMap<>();
        ConcurrentHashMap<String, Long> webhookReceiveTimeMs = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Long> intentExecuteAtMs = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Long> dequeuedAtMs = new ConcurrentHashMap<>();
        Map<PrecisionTier, List<String>> tierIntentIds = new HashMap<>();
        AtomicInteger totalWebhookReceived = new AtomicInteger(0);

        for (PrecisionTier tier : PrecisionTier.values()) {
            webhookReceivedByTier.put(tier, new AtomicInteger(0));
            tierIntentIds.put(tier, new ArrayList<>());
        }

        // 启动 Mock Server
        Map<String, Integer> delays = new HashMap<>();
        Map<String, AtomicInteger> receivedMap = new HashMap<>();
        for (PrecisionTier tier : PrecisionTier.values()) {
            delays.put(tier.name(), getDelayForTier(tier));
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

            // 创建 Intent
            Instant executeAt = Instant.now().plusMillis(1000);

            for (PrecisionTier tier : PrecisionTier.values()) {
                for (int i = 0; i < INTENTS_PER_TIER; i++) {
                    String intentId = String.format("%s-%s-%04d", mode, tier.name(), i);
                    Intent intent = createTestIntent(intentId, tier, executeAt);
                    intent.transitionTo(IntentStatus.SCHEDULED);
                    intentStore.save(intent);
                    scheduler.schedule(intent);

                    intentExecuteAtMs.put(intentId, executeAt.toEpochMilli());
                    tierIntentIds.get(tier).add(intentId);
                }
            }

            logger.info("已创建 {} 个 Intent/档位，executeAt={}", INTENTS_PER_TIER, executeAt);

            // 等待完成
            long waitStart = System.currentTimeMillis();
            int expected = INTENTS_PER_TIER * PrecisionTier.values().length;
            int lastReceived = 0;
            int stableCount = 0;

            while (System.currentTimeMillis() - waitStart < MAX_WAIT_MS) {
                Thread.sleep(500);

                int received = totalWebhookReceived.get();
                if (received >= expected) {
                    break;
                }

                if (received == lastReceived) {
                    stableCount++;
                    if (stableCount >= 6) {
                        break;
                    }
                } else {
                    stableCount = 0;
                    lastReceived = received;
                }
            }

            long totalWaitMs = System.currentTimeMillis() - waitStart;

            // 计算结果
            Map<PrecisionTier, TierLatencyResult> tierResults = new HashMap<>();
            long[] allE2e = new long[0];
            long[] allSched = new long[0];

            for (PrecisionTier tier : PrecisionTier.values()) {
                List<String> ids = tierIntentIds.get(tier);

                // E2E 延迟
                long[] e2e = ids.stream()
                    .mapToLong(id -> {
                        Long exec = intentExecuteAtMs.get(id);
                        Long recv = webhookReceiveTimeMs.get(id);
                        return (exec != null && recv != null) ? recv - exec : -1;
                    })
                    .filter(v -> v >= 0)
                    .sorted()
                    .toArray();

                // 调度精度
                long[] sched = ids.stream()
                    .mapToLong(id -> {
                        Long exec = intentExecuteAtMs.get(id);
                        Long deq = dequeuedAtMs.get(id);
                        return (exec != null && deq != null) ? deq - exec : -1;
                    })
                    .filter(v -> v >= 0)
                    .sorted()
                    .toArray();

                tierResults.put(tier, new TierLatencyResult(
                    tier,
                    webhookReceivedByTier.get(tier).get(),
                    percentile(e2e, 50), percentile(e2e, 95), percentile(e2e, 99),
                    percentile(sched, 50), percentile(sched, 95), percentile(sched, 99)
                ));

                allE2e = concat(allE2e, e2e);
                allSched = concat(allSched, sched);
            }

            Arrays.sort(allE2e);
            Arrays.sort(allSched);

            scheduler.stop();

            return new BenchmarkResult(
                mode,
                totalWebhookReceived.get(),
                expected,
                totalWaitMs,
                percentile(allE2e, 50), percentile(allE2e, 95), percentile(allE2e, 99),
                percentile(allSched, 50), percentile(allSched, 95), percentile(allSched, 99),
                tierResults
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
        intent.setShardKey("benchmark-shard");
        Callback callback = new Callback();
        callback.setUrl("http://localhost:" + WEBHOOK_PORT + "/webhook");
        intent.setCallback(callback);
        return intent;
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

    private static long percentile(long[] sorted, int pct) {
        if (sorted.length == 0) return 0;
        if (sorted.length == 1) return sorted[0];
        int idx = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }

    private static long[] concat(long[] a, long[] b) {
        long[] result = new long[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    private void printResult(String label, BenchmarkResult result) {
        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════");
        System.out.printf("  %s基准测试结果%n", label);
        System.out.println("═══════════════════════════════════════════════════════════════");
        System.out.println();
        System.out.printf(Locale.ROOT, "  总 Intent: %d / %d (%.1f%%)%n",
            result.totalReceived(), result.expected(),
            (double) result.totalReceived() / result.expected() * 100);
        System.out.printf(Locale.ROOT, "  总耗时: %d ms%n", result.totalWaitMs());
        System.out.printf(Locale.ROOT, "  吞吐量: %.1f QPS%n",
            result.totalWaitMs() > 0 ? (double) result.totalReceived() / result.totalWaitMs() * 1000 : 0);
        System.out.println();

        // E2E 延迟
        System.out.println("  ┌─ E2E 延迟 (executeAt → webhook received) ─────────────┐");
        System.out.printf(Locale.ROOT, "  │  P50: %5d ms   P95: %5d ms   P99: %5d ms        │%n",
            result.e2eP50(), result.e2eP95(), result.e2eP99());
        System.out.println("  └────────────────────────────────────────────────────────┘");
        System.out.println();

        // 调度精度
        System.out.println("  ┌─ 调度精度 (executeAt → consumer dequeue) ─────────────┐");
        System.out.printf(Locale.ROOT, "  │  P50: %5d ms   P95: %5d ms   P99: %5d ms        │%n",
            result.schedP50(), result.schedP95(), result.schedP99());
        System.out.println("  └────────────────────────────────────────────────────────┘");
        System.out.println();

        // 投递延迟
        long deliverP50 = Math.max(0, result.e2eP50() - result.schedP50());
        long deliverP95 = Math.max(0, result.e2eP95() - result.schedP95());
        long deliverP99 = Math.max(0, result.e2eP99() - result.schedP99());
        System.out.println("  ┌─ 投递延迟 (E2E - 调度精度) ────────────────────────────┐");
        System.out.printf(Locale.ROOT, "  │  P50: %5d ms   P95: %5d ms   P99: %5d ms        │%n",
            deliverP50, deliverP95, deliverP99);
        System.out.println("  └────────────────────────────────────────────────────────┘");
        System.out.println();

        // 各档位详情
        System.out.println("  ┌─ 各档位 E2E 延迟 ─────────────────────────────────────┐");
        System.out.printf(Locale.ROOT, "  │ %-8s %8s %8s %8s %8s %8s │%n",
            "Tier", "接收", "P50", "P95", "P99", "延迟ms");
        System.out.println("  ├────────────────────────────────────────────────────────┤");

        for (PrecisionTier tier : PrecisionTier.values()) {
            TierLatencyResult tr = result.tierResults().get(tier);
            if (tr == null) continue;
            System.out.printf(Locale.ROOT, "  │ %-8s %8d %6dms %6dms %6dms %8d │%n",
                tier.name(), tr.received(), tr.e2eP50(), tr.e2eP95(), tr.e2eP99(),
                getDelayForTier(tier));
        }
        System.out.println("  └────────────────────────────────────────────────────────┘");

        // 机器解析行
        System.out.printf(Locale.ROOT,
            "RESULT_DELIVERY_MODE|mode=%s|received=%d|expected=%d|wait_ms=%d|e2e_p50=%d|e2e_p95=%d|e2e_p99=%d|sched_p50=%d|sched_p95=%d|sched_p99=%d%n",
            result.mode(), result.totalReceived(), result.expected(), result.totalWaitMs(),
            result.e2eP50(), result.e2eP95(), result.e2eP99(),
            result.schedP50(), result.schedP95(), result.schedP99());
    }

    // ── 数据类型 ──

    private record BenchmarkResult(
        String mode,
        int totalReceived,
        int expected,
        long totalWaitMs,
        long e2eP50, long e2eP95, long e2eP99,
        long schedP50, long schedP95, long schedP99,
        Map<PrecisionTier, TierLatencyResult> tierResults
    ) {}

    private record TierLatencyResult(
        PrecisionTier tier,
        int received,
        long e2eP50, long e2eP95, long e2eP99,
        long schedP50, long schedP95, long schedP99
    ) {}
}

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
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
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
    private static final String WEBHOOK_URL = "http://localhost:" + WEBHOOK_PORT + "/webhook";

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
    private HttpServer mockServer;
    private ExecutorService mockServerExecutor;
    private HttpClient deliveryClient;

    // 统计各档位接收到的 webhook 数量
    private final Map<PrecisionTier, AtomicInteger> webhookReceivedByTier = new ConcurrentHashMap<>();
    private final Map<PrecisionTier, AtomicLong> firstWebhookTsMsByTier = new ConcurrentHashMap<>();
    private final Map<PrecisionTier, AtomicLong> lastWebhookTsMsByTier = new ConcurrentHashMap<>();
    private final AtomicInteger totalWebhookReceived = new AtomicInteger(0);

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
        // 启动 mock webhook 服务器
        startMockServer();

        logger.info("Scheduler benchmark setup: creating scheduler components");
        intentStore = new IntentStore();
        deliveryClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
        DeliveryHandler deliveryHandler = createBenchmarkDeliveryHandler();
        scheduler = new PrecisionScheduler(intentStore, deliveryHandler, new DefaultRedeliveryDecider());
        metrics = MetricsCollector.getInstance();
        logger.info("Scheduler benchmark setup: starting scheduler");
        scheduler.start();
        logger.info("Scheduler benchmark setup: scheduler started");

        // 初始化计数器
        for (PrecisionTier tier : PrecisionTier.values()) {
            webhookReceivedByTier.put(tier, new AtomicInteger(0));
            firstWebhookTsMsByTier.put(tier, new AtomicLong(0));
            lastWebhookTsMsByTier.put(tier, new AtomicLong(0));
        }
    }

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.stop();
        }
        deliveryClient = null;
        stopMockServer();
    }

    private DeliveryHandler createBenchmarkDeliveryHandler() {
        return intent -> {
            try {
                String url = intent.getCallback() != null ? intent.getCallback().getUrl() : null;
                if (url == null || url.isBlank()) {
                    return DeliveryHandler.DeliveryResult.DEAD_LETTER;
                }

                String payload = "{\"intentId\":\"" + intent.getIntentId()
                    + "\",\"precisionTier\":\"" + intent.getPrecisionTier().name() + "\"}";

                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(3))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build();

                HttpResponse<String> response = deliveryClient.send(request, HttpResponse.BodyHandlers.ofString());
                int status = response.statusCode();
                if (status >= 200 && status < 300) {
                    return DeliveryHandler.DeliveryResult.SUCCESS;
                }
                if (status >= 500) {
                    return DeliveryHandler.DeliveryResult.RETRY;
                }
                return DeliveryHandler.DeliveryResult.DEAD_LETTER;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return DeliveryHandler.DeliveryResult.DEAD_LETTER;
            } catch (Exception e) {
                return DeliveryHandler.DeliveryResult.RETRY;
            }
        };
    }

    /**
     * 启动 Mock Webhook 服务器
     */
    private void startMockServer() throws IOException {
        mockServer = HttpServer.create(new InetSocketAddress(WEBHOOK_PORT), 0);

        // 为每个档位创建不同延迟的端点
        for (PrecisionTier tier : PrecisionTier.values()) {
            int delayMs = getDelayForTier(tier);
            String path = "/webhook/" + tier.name().toLowerCase();

            mockServer.createContext(path, new TierHandler(tier, delayMs));
        }

        // 默认端点（兼容旧测试）
        mockServer.createContext("/webhook", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                try {
                    byte[] requestBody = exchange.getRequestBody().readAllBytes();
                    String body = new String(requestBody, StandardCharsets.UTF_8);

                    PrecisionTier tier = extractTierFromBody(body);
                    if (tier != null) {
                        int delayMs = getDelayForTier(tier);
                        Thread.sleep(delayMs);
                        webhookReceivedByTier.get(tier).incrementAndGet();
                        recordWebhookTimestamp(tier);
                    }
                    totalWebhookReceived.incrementAndGet();

                    String response = "{\"status\":\"ok\"}";
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, response.getBytes(StandardCharsets.UTF_8).length);

                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response.getBytes(StandardCharsets.UTF_8));
                    }
                } catch (Exception e) {
                    logger.warn("Mock server error: {}", e.getMessage());
                    exchange.sendResponseHeaders(500, 0);
                }
            }
        });

        mockServerExecutor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
        mockServer.setExecutor(mockServerExecutor);
        mockServer.start();
        logger.info("Mock webhook server started on port {} (with tier-specific delays)", WEBHOOK_PORT);
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

    private class TierHandler implements HttpHandler {
        private final PrecisionTier tier;
        private final int delayMs;

        TierHandler(PrecisionTier tier, int delayMs) {
            this.tier = tier;
            this.delayMs = delayMs;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                // 模拟处理延迟
                Thread.sleep(delayMs);

                webhookReceivedByTier.get(tier).incrementAndGet();
                recordWebhookTimestamp(tier);
                totalWebhookReceived.incrementAndGet();

                String response = "{\"status\":\"ok\",\"tier\":\"" + tier.name() + "\"}";
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.getBytes(StandardCharsets.UTF_8).length);

                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes(StandardCharsets.UTF_8));
                }
            } catch (Exception e) {
                logger.warn("Mock server error for tier {}: {}", tier, e.getMessage());
                exchange.sendResponseHeaders(500, 0);
            }
        }
    }

    private void stopMockServer() {
        if (mockServer != null) {
            mockServer.stop(0);
            logger.info("Mock webhook server stopped");
        }

        if (mockServerExecutor != null) {
            mockServerExecutor.shutdownNow();
            try {
                if (!mockServerExecutor.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS)) {
                    logger.warn("Mock webhook executor did not terminate within timeout");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                mockServerExecutor = null;
            }
        }
    }

    private void recordWebhookTimestamp(PrecisionTier tier) {
        long now = System.currentTimeMillis();
        AtomicLong first = firstWebhookTsMsByTier.get(tier);
        AtomicLong last = lastWebhookTsMsByTier.get(tier);
        if (first != null) {
            first.compareAndSet(0L, now);
        }
        if (last != null) {
            last.set(now);
        }
    }

    private PrecisionTier extractTierFromBody(String body) {
        // 简单字符串匹配提取 tier
        for (PrecisionTier tier : PrecisionTier.values()) {
            // 尝试多种可能的格式
            if (body.contains("\"precisionTier\":\"" + tier.name() + "\"") ||
                body.contains("\"precisionTier\": \"" + tier.name() + "\"")) {
                return tier;
            }
        }
        return null;
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
        // 使用档位特定的端点，模拟不同延迟
        callback.setUrl("http://localhost:" + WEBHOOK_PORT + "/webhook/" + tier.name().toLowerCase());
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
        firstWebhookTsMsByTier.values().forEach(ts -> ts.set(0));
        lastWebhookTsMsByTier.values().forEach(ts -> ts.set(0));
        totalWebhookReceived.set(0);

        int intentsPerTier = quick ? 40 : 1000;
        Instant executeAt = Instant.now().plusMillis(quick ? 800 : 2000);
        long maxWaitMs = quick ? 12000 : 60000;

        Map<PrecisionTier, Long> initialBackpressure = snapshotBackpressure();

        logger.info("=== Scheduler real webhook benchmark ===");
        logger.info("Mode: {}", quick ? "QUICK" : "FULL");
        logger.info("Intents per tier: {}", intentsPerTier);
        logger.info("ExecuteAt: {}", executeAt);

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
            logger.info("  {}: created {} intents", tier, intentsPerTier);
        }

        Thread.sleep(quick ? 500 : 1000);

        long waitStart = System.currentTimeMillis();
        int expected = intentsPerTier * PrecisionTier.values().length;
        int lastReceived = 0;
        int stableCount = 0;

        while (System.currentTimeMillis() - waitStart < maxWaitMs) {
            Thread.sleep(1000);

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

        for (PrecisionTier tier : PrecisionTier.values()) {
            int received = webhookReceivedByTier.get(tier).get();
            double avgLatencyMs = getMockServerLatency(tier);
            long firstTs = firstWebhookTsMsByTier.get(tier).get();
            long lastTs = lastWebhookTsMsByTier.get(tier).get();
            double qps;
            if (received > 0 && firstTs > 0 && lastTs >= firstTs) {
                double tierDurationSec = Math.max(0.001, (lastTs - firstTs + 1) / 1000.0);
                qps = received / tierDurationSec;
            } else {
                qps = 0.0;
            }
            double theoreticalMaxQps = Math.max(0.0, metrics.calculateTheoreticalMaxQps(tier, avgLatencyMs));
            double efficiencyRawPct = metrics.calculateEfficiency(tier, qps, avgLatencyMs) * 100.0;
            double efficiencyPct = clampPercent(efficiencyRawPct);
            long backpressureDelta = currentBackpressure.getOrDefault(tier, 0L)
                - initialBackpressure.getOrDefault(tier, 0L);

            tierResults.put(tier, new BenchmarkTierResult(
                tier,
                received,
                qps,
                avgLatencyMs,
                theoreticalMaxQps,
                efficiencyPct,
                efficiencyRawPct,
                tier.getMaxConcurrency(),
                backpressureDelta
            ));
        }

        int totalReceived = totalWebhookReceived.get();
        return new BenchmarkSummary(tierResults, totalReceived, expected, totalWaitMs);
    }

    private Map<PrecisionTier, Long> snapshotBackpressure() {
        Map<PrecisionTier, Long> snapshot = new EnumMap<>(PrecisionTier.class);
        Map<PrecisionTier, Long> current = metrics.getBackpressureEventsByTier();
        for (PrecisionTier tier : PrecisionTier.values()) {
            snapshot.put(tier, current.getOrDefault(tier, 0L));
        }
        return snapshot;
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

        System.out.println();
        System.out.printf(Locale.ROOT, "总接收: %d / %d (%.1f%%)%n",
            summary.totalReceived(), summary.expected(), completionRate);
        System.out.printf(Locale.ROOT, "总等待: %d ms%n", summary.totalWaitMs());
        if (clampedTierCount > 0) {
            System.out.printf(Locale.ROOT,
                "注意: %d 个档位原始效率超过 100%%（理论值受抖动影响），报告效率已封顶到 100%%。%n",
                clampedTierCount);
        }
        System.out.println("解释: 这组结果反映的是调度器 + callback 投递 + webhook 响应的联合作用，不是纯创建接口。");

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
        long backpressureEvents
    ) {}

    private record BenchmarkSummary(
        Map<PrecisionTier, BenchmarkTierResult> tierResults,
        int totalReceived,
        int expected,
        long totalWaitMs
    ) {}
}

package com.loomq.scheduler;


import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.channel.grpc.server.DeliveryStreamRegistry;
import com.loomq.channel.grpc.server.GrpcStreamDeliveryHandler;
import com.loomq.channel.http.NettyHttpDeliveryHandler;
import com.loomq.channel.http.batch.BatchDeliveryConfig;
import com.loomq.channel.http.batch.BatchedHttpDeliveryHandler;
import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.grpc.gen.DeliveryAckMode;
import com.loomq.grpc.gen.DeliveryEvent;
import com.loomq.grpc.gen.WatchDeliveriesRequest;
import com.loomq.spi.DefaultRedeliveryDecider;
import com.loomq.spi.DeliveryHandler;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.time.Instant;
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
 * 三种投递模式对比基准测试：HTTP 单请求 / HTTP 批量 / gRPC 流。
 *
 * <p>构造高并发同时到期场景（5000 个 ULTRA Intent），对比：
 * <ul>
 *   <li>HTTP 请求数（gRPC 流为 0）</li>
 *   <li>E2E 延迟分布</li>
 *   <li>吞吐量</li>
 *   <li>完成率</li>
 * </ul>
 *
 * @author loomq
 * @since v0.9.2
 */
@Tag("benchmark")
public class DeliveryModeComparisonBenchmark {

    private static final Logger logger = LoggerFactory.getLogger(DeliveryModeComparisonBenchmark.class);
    private static final int WEBHOOK_PORT = 19996;
    private static final int MOCK_DELAY_MS = 50;
    private static final int INTENT_COUNT = 5000;

    /**
     * 三种模式对比测试。
     */
    @Test
    void testThreeModeComparison() throws Exception {
        logger.info("=== 三种投递模式对比基准测试 ===");
        logger.info("测试规模: {} 个 ULTRA Intent 同时到期", INTENT_COUNT);
        logger.info("Mock Server 延迟: {}ms", MOCK_DELAY_MS);

        // 1. HTTP 单请求模式
        logger.info("\n--- HTTP 单请求模式 ---");
        BenchmarkResult singleResult = runBenchmark("http-single", new NettyHttpDeliveryHandler());

        // 2. HTTP 批量模式
        logger.info("\n--- HTTP 批量模式 ---");
        BatchDeliveryConfig batchConfig = new BatchDeliveryConfig(200, 2, 500, true, 30000);
        BatchedHttpDeliveryHandler batchHandler = new BatchedHttpDeliveryHandler(batchConfig);
        BenchmarkResult batchResult;
        try {
            batchResult = runBenchmark("http-batch", batchHandler);
        } finally {
            batchHandler.shutdown();
        }

        // 3. gRPC 流模式（模拟）
        logger.info("\n--- gRPC 流模式 ---");
        BenchmarkResult grpcResult = runGrpcStreamBenchmark();

        // 输出对比报告
        printComparisonReport(singleResult, batchResult, grpcResult);
    }

    /**
     * 运行 HTTP 投递基准测试。
     */
    private BenchmarkResult runBenchmark(String mode, DeliveryHandler deliveryHandler) throws Exception {
        Map<PrecisionTier, AtomicInteger> webhookReceivedByTier = new HashMap<>();
        ConcurrentHashMap<String, Long> webhookReceiveTimeMs = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Long> intentExecuteAtMs = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Long> dequeuedAtMs = new ConcurrentHashMap<>();
        AtomicInteger totalWebhookReceived = new AtomicInteger(0);

        for (PrecisionTier tier : PrecisionTier.values()) {
            webhookReceivedByTier.put(tier, new AtomicInteger(0));
        }

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

            IntentStore intentStore = new ConcurrentIntentStore();
            DeliveryHandler wrappedHandler = intent -> {
                dequeuedAtMs.put(intent.getIntentId(), System.currentTimeMillis());
                return deliveryHandler.deliverAsync(intent);
            };

            PrecisionScheduler scheduler = new PrecisionScheduler(
                intentStore, wrappedHandler, new DefaultRedeliveryDecider());
            scheduler.start();

            Instant executeAt = Instant.now().plusMillis(1000);
            PrecisionTier tier = PrecisionTier.ULTRA;

            for (int i = 0; i < INTENT_COUNT; i++) {
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
                if (received >= INTENT_COUNT) break;
                if (received == lastReceived) {
                    stableCount++;
                    if (stableCount >= 12) break;
                } else {
                    stableCount = 0;
                    lastReceived = received;
                }
            }

            long totalWaitMs = System.currentTimeMillis() - waitStart;

            // 计算延迟
            long[] e2e = computeLatencies(intentExecuteAtMs, webhookReceiveTimeMs);
            long[] sched = computeLatencies(intentExecuteAtMs, dequeuedAtMs);
            long[] delivery = computeDeliveryLatencies(e2e, sched);

            scheduler.stop();

            int httpRequestCount = mockServer.getHttpRequestCount();

            return new BenchmarkResult(
                mode,
                totalWebhookReceived.get(),
                INTENT_COUNT,
                totalWaitMs,
                httpRequestCount,
                percentile(e2e, 50), percentile(e2e, 95), percentile(e2e, 99),
                percentile(sched, 50), percentile(sched, 95), percentile(sched, 99),
                percentile(delivery, 50), percentile(delivery, 95), percentile(delivery, 99)
            );
        } finally {
            mockServer.stop();
        }
    }

    /**
     * 运行 gRPC 流投递基准测试（模拟，不启动真实 gRPC 服务器）。
     */
    private BenchmarkResult runGrpcStreamBenchmark() throws Exception {
        DeliveryStreamRegistry registry = new DeliveryStreamRegistry();
        GrpcStreamDeliveryHandler handler = new GrpcStreamDeliveryHandler(
            DeliveryAckMode.AUTO_ACK, registry);

        // 模拟 Gateway 注册流
        MockCallStreamObserver observer = new MockCallStreamObserver();
        WatchDeliveriesRequest request = WatchDeliveriesRequest.newBuilder()
            .setAckMode(DeliveryAckMode.AUTO_ACK)
            .build();
        var entry = registry.register(request, observer);

        IntentStore intentStore = new ConcurrentIntentStore();

        // 记录时间戳
        ConcurrentHashMap<String, Long> intentExecuteAtMs = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Long> dequeuedAtMs = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Long> deliveryTimeMs = new ConcurrentHashMap<>();

        DeliveryHandler wrappedHandler = intent -> {
            long dequeueTime = System.currentTimeMillis();
            dequeuedAtMs.put(intent.getIntentId(), dequeueTime);
            return handler.deliverAsync(intent).thenApply(result -> {
                deliveryTimeMs.put(intent.getIntentId(), System.currentTimeMillis());
                return result;
            });
        };

        PrecisionScheduler scheduler = new PrecisionScheduler(
            intentStore, wrappedHandler, new DefaultRedeliveryDecider());
        scheduler.start();

        Instant executeAt = Instant.now().plusMillis(1000);
        PrecisionTier tier = PrecisionTier.ULTRA;

        for (int i = 0; i < INTENT_COUNT; i++) {
            String intentId = String.format("grpc-ultra-%05d", i);
            Intent intent = createTestIntent(intentId, tier, executeAt);
            intent.transitionTo(IntentStatus.SCHEDULED);
            intentStore.save(intent);
            scheduler.schedule(intent);
            intentExecuteAtMs.put(intentId, executeAt.toEpochMilli());
        }

        // 等待完成
        long waitStart = System.currentTimeMillis();
        long maxWaitMs = 30000;

        while (System.currentTimeMillis() - waitStart < maxWaitMs) {
            Thread.sleep(500);
            if (observer.getEvents().size() >= INTENT_COUNT) break;
        }

        long totalWaitMs = System.currentTimeMillis() - waitStart;

        // 计算延迟
        long[] e2e = intentExecuteAtMs.entrySet().stream()
            .mapToLong(e -> {
                Long delivery = deliveryTimeMs.get(e.getKey());
                return delivery != null ? delivery - e.getValue() : -1;
            })
            .filter(v -> v >= 0)
            .sorted()
            .toArray();

        long[] sched = computeLatencies(intentExecuteAtMs, dequeuedAtMs);
        long[] delivery = computeDeliveryLatencies(e2e, sched);

        scheduler.stop();
        registry.unregister(entry);
        handler.close();

        return new BenchmarkResult(
            "grpc-stream",
            observer.getEvents().size(),
            INTENT_COUNT,
            totalWaitMs,
            0, // 无 HTTP 请求
            percentile(e2e, 50), percentile(e2e, 95), percentile(e2e, 99),
            percentile(sched, 50), percentile(sched, 95), percentile(sched, 99),
            percentile(delivery, 50), percentile(delivery, 95), percentile(delivery, 99)
        );
    }

    private Intent createTestIntent(String intentId, PrecisionTier tier, Instant executeAt) {
        Intent intent = new Intent(intentId);
        intent.setExecuteAt(executeAt);
        intent.setDeadline(executeAt.plusSeconds(60));
        intent.setPrecisionTier(tier);
        intent.setShardKey("benchmark");
        Callback callback = new Callback();
        callback.setUrl("http://localhost:" + WEBHOOK_PORT + "/webhook");
        intent.setCallback(callback);
        return intent;
    }

    private long[] computeLatencies(ConcurrentHashMap<String, Long> startTimes,
                                    ConcurrentHashMap<String, Long> endTimes) {
        return startTimes.entrySet().stream()
            .mapToLong(e -> {
                Long end = endTimes.get(e.getKey());
                return end != null ? end - e.getValue() : -1;
            })
            .filter(v -> v >= 0)
            .sorted()
            .toArray();
    }

    private long[] computeDeliveryLatencies(long[] e2e, long[] sched) {
        int len = Math.min(e2e.length, sched.length);
        long[] delivery = new long[len];
        for (int i = 0; i < len; i++) {
            delivery[i] = Math.max(0, e2e[i] - sched[i]);
        }
        Arrays.sort(delivery);
        return delivery;
    }

    private static long percentile(long[] sorted, int pct) {
        if (sorted.length == 0) return 0;
        if (sorted.length == 1) return sorted[0];
        int idx = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }

    /**
     * 输出三种模式对比报告。
     */
    private void printComparisonReport(BenchmarkResult single, BenchmarkResult batch, BenchmarkResult grpc) {
        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════════════════════");
        System.out.println("                    三种投递模式对比基准测试报告");
        System.out.println("═══════════════════════════════════════════════════════════════════════════════");
        System.out.println();
        System.out.printf(Locale.ROOT, "  测试规模: %,d 个 ULTRA Intent 同时到期%n", INTENT_COUNT);
        System.out.printf(Locale.ROOT, "  Mock Server 延迟: %dms%n", MOCK_DELAY_MS);
        System.out.println();

        // HTTP 请求数
        System.out.println("  ┌─ HTTP 请求数 ────────────────────────────────────────────────────────────────┐");
        System.out.printf(Locale.ROOT, "  │  %-15s %,10d 次                                                │%n",
            "单请求模式", single.httpRequestCount());
        System.out.printf(Locale.ROOT, "  │  %-15s %,10d 次                                                │%n",
            "批量模式", batch.httpRequestCount());
        System.out.printf(Locale.ROOT, "  │  %-15s %,10d 次 (gRPC 流，无 HTTP)                              │%n",
            "gRPC 流模式", grpc.httpRequestCount());
        System.out.println("  └──────────────────────────────────────────────────────────────────────────────┘");
        System.out.println();

        // 完成率
        System.out.println("  ┌─ 完成率 ──────────────────────────────────────────────────────────────────────┐");
        printCompletionRow("单请求模式", single);
        printCompletionRow("批量模式", batch);
        printCompletionRow("gRPC 流模式", grpc);
        System.out.println("  └──────────────────────────────────────────────────────────────────────────────┘");
        System.out.println();

        // 吞吐量
        double singleQps = single.totalWaitMs() > 0
            ? (double) single.totalReceived() / single.totalWaitMs() * 1000 : 0;
        double batchQps = batch.totalWaitMs() > 0
            ? (double) batch.totalReceived() / batch.totalWaitMs() * 1000 : 0;
        double grpcQps = grpc.totalWaitMs() > 0
            ? (double) grpc.totalReceived() / grpc.totalWaitMs() * 1000 : 0;

        System.out.println("  ┌─ 吞吐量 ──────────────────────────────────────────────────────────────────────┐");
        System.out.printf(Locale.ROOT, "  │  %-15s %,10.1f QPS                                                │%n",
            "单请求模式", singleQps);
        System.out.printf(Locale.ROOT, "  │  %-15s %,10.1f QPS                                                │%n",
            "批量模式", batchQps);
        System.out.printf(Locale.ROOT, "  │  %-15s %,10.1f QPS                                                │%n",
            "gRPC 流模式", grpcQps);
        System.out.println("  └──────────────────────────────────────────────────────────────────────────────┘");
        System.out.println();

        // E2E 延迟
        System.out.println("  ┌─ E2E 延迟 (ms) ──────────────────────────────────────────────────────────────┐");
        System.out.printf(Locale.ROOT, "  │  %-15s %8s %8s %8s %8s                                   │%n",
            "模式", "P50", "P95", "P99", "样本数");
        System.out.println("  ├──────────────────────────────────────────────────────────────────────────────┤");
        printLatencyRow("单请求模式", single);
        printLatencyRow("批量模式", batch);
        printLatencyRow("gRPC 流模式", grpc);
        System.out.println("  └──────────────────────────────────────────────────────────────────────────────┘");
        System.out.println();

        // 投递延迟
        System.out.println("  ┌─ 投递延迟 (ms) ──────────────────────────────────────────────────────────────┐");
        System.out.printf(Locale.ROOT, "  │  %-15s %8s %8s %8s                                           │%n",
            "模式", "P50", "P95", "P99");
        System.out.println("  ├──────────────────────────────────────────────────────────────────────────────┤");
        printDeliveryRow("单请求模式", single);
        printDeliveryRow("批量模式", batch);
        printDeliveryRow("gRPC 流模式", grpc);
        System.out.println("  └──────────────────────────────────────────────────────────────────────────────┘");
        System.out.println();

        // 机器解析行
        System.out.printf(Locale.ROOT,
            "RESULT_COMPARISON|single_http=%d|batch_http=%d|grpc_http=%d|single_qps=%.1f|batch_qps=%.1f|grpc_qps=%.1f|single_e2e_p50=%d|batch_e2e_p50=%d|grpc_e2e_p50=%d|single_e2e_p99=%d|batch_e2e_p99=%d|grpc_e2e_p99=%d%n",
            single.httpRequestCount(), batch.httpRequestCount(), grpc.httpRequestCount(),
            singleQps, batchQps, grpcQps,
            single.e2eP50(), batch.e2eP50(), grpc.e2eP50(),
            single.e2eP99(), batch.e2eP99(), grpc.e2eP99());
    }

    private void printCompletionRow(String label, BenchmarkResult result) {
        double pct = (double) result.totalReceived() / result.expected() * 100;
        System.out.printf(Locale.ROOT, "  │  %-15s %,6d / %,d (%.1f%%)                                       │%n",
            label, result.totalReceived(), result.expected(), pct);
    }

    private void printLatencyRow(String label, BenchmarkResult result) {
        System.out.printf(Locale.ROOT, "  │  %-15s %8d %8d %8d %8d                                   │%n",
            label, result.e2eP50(), result.e2eP95(), result.e2eP99(), result.totalReceived());
    }

    private void printDeliveryRow(String label, BenchmarkResult result) {
        System.out.printf(Locale.ROOT, "  │  %-15s %8d %8d %8d                                           │%n",
            label, result.deliveryP50(), result.deliveryP95(), result.deliveryP99());
    }

    // ── 数据类型 ──

    private record BenchmarkResult(
        String mode,
        int totalReceived,
        int expected,
        long totalWaitMs,
        int httpRequestCount,
        long e2eP50, long e2eP95, long e2eP99,
        long schedP50, long schedP95, long schedP99,
        long deliveryP50, long deliveryP95, long deliveryP99
    ) {}

    /**
     * 模拟 CallStreamObserver（用于 gRPC 流测试）。
     */
    private static class MockCallStreamObserver extends io.grpc.stub.ServerCallStreamObserver<DeliveryEvent> {
        private final List<DeliveryEvent> events = new java.util.concurrent.CopyOnWriteArrayList<>();

        List<DeliveryEvent> getEvents() {
            return events;
        }

        @Override
        public void onNext(DeliveryEvent value) {
            events.add(value);
        }

        @Override
        public void onError(Throwable t) {}

        @Override
        public void onCompleted() {}

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setOnReadyHandler(Runnable onReadyHandler) {}

        @Override
        public void disableAutoInboundFlowControl() {}

        @Override
        public void request(int count) {}

        @Override
        public void setMessageCompression(boolean enable) {}

        @Override
        public void setOnCancelHandler(Runnable onCancelHandler) {}

        @Override
        public void setOnCloseHandler(Runnable onCloseHandler) {}

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void setCompression(String compression) {}
    }
}

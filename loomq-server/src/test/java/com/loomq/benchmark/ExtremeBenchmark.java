package com.loomq.benchmark;

import com.loomq.domain.intent.PrecisionTier;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 极限性能测试 (v0.7.0)
 *
 * 测试场景：
 * 1. 各精度档位的峰值创建吞吐量
 * 2. 混合档位并发创建吞吐量
 * 3. 内存占用与 GC 行为
 * 4. 延迟分布（P50/P90/P99/P999）
 *
 * @author loomq
 * @since v0.5.1
 */
public class ExtremeBenchmark {

    private static final String BASE_URL = "http://localhost:8080";
    private static final HttpClient httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();

    // 测试参数
    private static final int WARMUP_COUNT = 100;
    private static final int[] THREAD_COUNTS = {10, 20, 50, 100};
    private static final int TEST_DURATION_SEC = 10;

    public static void main(String[] args) throws Exception {
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║         LoomQ 极限性能测试                                    ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println();

        // 健康检查
        if (!healthCheck()) {
            System.err.println("❌ 服务不可用，请先启动 LoomQ: java -jar loomq-server/target/loomq-server-0.7.0-SNAPSHOT.jar");
            return;
        }
        System.out.println("✅ 服务健康检查通过");
        System.out.println();

        // 预热
        System.out.println("🔥 预热中 (" + WARMUP_COUNT + " 请求)...");
        warmup();
        System.out.println("✅ 预热完成");
        System.out.println();

        // 测试结果收集
        // 1. 各精度档位吞吐量测试
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println("测试 1: 各精度档位峰值吞吐量 (10线程, 10秒)");
        System.out.println("══════════════════════════════════════════════════════════════");
        Map<PrecisionTier, TierResult> tierResults = new EnumMap<>(PrecisionTier.class);
        for (PrecisionTier tier : PrecisionTier.values()) {
            TierResult result = testTierThroughput(tier, 10, TEST_DURATION_SEC);
            tierResults.put(tier, result);
        }
        printTierResults(tierResults);
        System.out.println();

        // 2. 最佳线程数探索
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println("测试 2: 最佳线程数探索 (STANDARD 档位)");
        System.out.println("══════════════════════════════════════════════════════════════");
        Map<Integer, TierResult> threadResults = new LinkedHashMap<>();
        for (int threads : THREAD_COUNTS) {
            TierResult result = testTierThroughput(PrecisionTier.STANDARD, threads, TEST_DURATION_SEC);
            threadResults.put(threads, result);
        }
        printThreadResults(threadResults);
        System.out.println();

        // 3. 混合档位测试
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println("测试 3: 混合档位并发吞吐量 (20线程, 10秒)");
        System.out.println("══════════════════════════════════════════════════════════════");
        TierResult mixedResult = testMixedTierThroughput(20, TEST_DURATION_SEC);
        System.out.printf("混合档位 QPS: %.0f%n", mixedResult.qps);
        System.out.printf("平均延迟: %.2f ms%n", mixedResult.avgLatency);
        System.out.printf("P99 延迟: %d ms%n", mixedResult.p99);
        System.out.println();

        // 4. 短延迟场景测试
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println("测试 4: 短延迟场景 (delay < precisionWindow)");
        System.out.println("══════════════════════════════════════════════════════════════");
        TierResult shortDelayResult = testShortDelay(10, 5);
        System.out.printf("短延迟 QPS: %.0f%n", shortDelayResult.qps);
        System.out.println();

        // 5. 查询吞吐量测试
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println("测试 5: 查询吞吐量");
        System.out.println("══════════════════════════════════════════════════════════════");
        long queryQps = testQueryThroughput(10, 5);
        System.out.printf("查询 QPS: %d%n", queryQps);
        System.out.println();

        // 6. 内存占用估算
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println("测试 6: 内存占用估算");
        System.out.println("══════════════════════════════════════════════════════════════");
        long memBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        int intentCount = 10000;
        createIntentsForMemoryTest(intentCount);
        System.gc();
        Thread.sleep(500);
        long memAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long memPerIntent = (memAfter - memBefore) / intentCount;
        System.out.printf("创建 Intent 数: %d%n", intentCount);
        System.out.printf("内存增量: %.2f MB%n", (memAfter - memBefore) / 1024.0 / 1024.0);
        System.out.printf("每 Intent 内存: ~%d bytes%n", memPerIntent);
        System.out.println();

        // 汇总报告
        printSummaryReport(tierResults, threadResults, mixedResult, queryQps, memPerIntent);

        // 生成 Markdown 报告
        generateMarkdownReport(tierResults, threadResults, mixedResult, queryQps, memPerIntent);
    }

    private static void warmup() throws Exception {
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        for (int i = 0; i < WARMUP_COUNT; i++) {
            executor.submit(() -> {
                try {
                    createIntent(PrecisionTier.STANDARD, 3600);
                } catch (Exception e) {
                    // ignore
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
    }

    private static TierResult testTierThroughput(PrecisionTier tier, int threads, int durationSec) throws Exception {
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        CountDownLatch latch = new CountDownLatch(threads);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + durationSec * 1000L;

        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    while (System.currentTimeMillis() < endTime) {
                        long reqStart = System.nanoTime();
                        try {
                            createIntent(tier, 3600);
                            long latency = (System.nanoTime() - reqStart) / 1_000_000;
                            latencies.add(latency);
                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            failCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        long actualDuration = System.currentTimeMillis() - startTime;
        Collections.sort(latencies);

        double qps = (double) successCount.get() / actualDuration * 1000;
        double avgLatency = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
        long p50 = getPercentile(latencies, 0.50);
        long p90 = getPercentile(latencies, 0.90);
        long p99 = getPercentile(latencies, 0.99);
        long p999 = getPercentile(latencies, 0.999);

        return new TierResult(qps, avgLatency, p50, p90, p99, p999, successCount.get(), failCount.get());
    }

    private static TierResult testMixedTierThroughput(int threads, int durationSec) throws Exception {
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        PrecisionTier[] tiers = PrecisionTier.values();

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        CountDownLatch latch = new CountDownLatch(threads);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + durationSec * 1000L;

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    int tierIndex = 0;
                    while (System.currentTimeMillis() < endTime) {
                        PrecisionTier tier = tiers[tierIndex % tiers.length];
                        tierIndex++;
                        long reqStart = System.nanoTime();
                        try {
                            createIntent(tier, 3600);
                            long latency = (System.nanoTime() - reqStart) / 1_000_000;
                            latencies.add(latency);
                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            failCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        long actualDuration = System.currentTimeMillis() - startTime;
        Collections.sort(latencies);

        double qps = (double) successCount.get() / actualDuration * 1000;
        double avgLatency = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
        long p99 = getPercentile(latencies, 0.99);

        return new TierResult(qps, avgLatency, 0, 0, p99, 0, successCount.get(), failCount.get());
    }

    private static TierResult testShortDelay(int threads, int durationSec) throws Exception {
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        CountDownLatch latch = new CountDownLatch(threads);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + durationSec * 1000L;

        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    while (System.currentTimeMillis() < endTime) {
                        long reqStart = System.nanoTime();
                        try {
                            // 短延迟：delay < precisionWindow
                            createIntent(PrecisionTier.STANDARD, 0); // 立即执行
                            long latency = (System.nanoTime() - reqStart) / 1_000_000;
                            latencies.add(latency);
                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            failCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        long actualDuration = System.currentTimeMillis() - startTime;
        Collections.sort(latencies);

        double qps = (double) successCount.get() / actualDuration * 1000;
        double avgLatency = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
        long p99 = getPercentile(latencies, 0.99);

        return new TierResult(qps, avgLatency, 0, 0, p99, 0, successCount.get(), failCount.get());
    }

    private static long testQueryThroughput(int threads, int durationSec) throws Exception {
        // 先创建一些任务用于查询
        Set<String> intentIds = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < 100; i++) {
            String id = "query-test-" + i;
            try {
                createIntentWithId(PrecisionTier.STANDARD, 3600, id);
                intentIds.add(id);
            } catch (Exception e) {
                // ignore
            }
        }

        AtomicInteger successCount = new AtomicInteger(0);
        List<String> idList = new ArrayList<>(intentIds);

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        CountDownLatch latch = new CountDownLatch(threads);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + durationSec * 1000L;
        AtomicLong index = new AtomicLong(0);

        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    while (System.currentTimeMillis() < endTime) {
                        try {
                            int idx = (int) (index.getAndIncrement() % idList.size());
                            getIntent(idList.get(idx));
                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            // ignore
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        long actualDuration = System.currentTimeMillis() - startTime;
        return (long) ((double) successCount.get() / actualDuration * 1000);
    }

    private static void createIntentsForMemoryTest(int count) throws Exception {
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        CountDownLatch latch = new CountDownLatch(count);

        for (int i = 0; i < count; i++) {
            executor.submit(() -> {
                try {
                    createIntent(PrecisionTier.STANDARD, 3600);
                } catch (Exception e) {
                    // ignore
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
    }

    // ========== 辅助方法 ==========

    private static boolean healthCheck() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + "/health"))
                .GET()
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return response.body().contains("UP");
        } catch (Exception e) {
            return false;
        }
    }

    private static String createIntent(PrecisionTier tier, long delaySeconds) throws IOException, InterruptedException {
        Instant executeAt = Instant.now().plus(delaySeconds, ChronoUnit.SECONDS);
        Instant deadline = executeAt.plus(5, ChronoUnit.MINUTES);

        String body = String.format("""
            {
                "executeAt": "%s",
                "deadline": "%s",
                "precisionTier": "%s",
                "shardKey": "benchmark",
                "callback": {
                    "url": "http://localhost:9999/webhook"
                }
            }
            """,
            executeAt.toString(),
            deadline.toString(),
            tier.name()
        );

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + "/v1/intents"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    private static String createIntentWithId(PrecisionTier tier, long delaySeconds, String intentId) throws IOException, InterruptedException {
        Instant executeAt = Instant.now().plus(delaySeconds, ChronoUnit.SECONDS);
        Instant deadline = executeAt.plus(5, ChronoUnit.MINUTES);

        String body = String.format("""
            {
                "intentId": "%s",
                "executeAt": "%s",
                "deadline": "%s",
                "precisionTier": "%s",
                "shardKey": "benchmark",
                "callback": {
                    "url": "http://localhost:9999/webhook"
                }
            }
            """,
            intentId,
            executeAt.toString(),
            deadline.toString(),
            tier.name()
        );

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + "/v1/intents"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    private static String getIntent(String intentId) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + "/v1/intents/" + intentId))
            .GET()
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    private static long getPercentile(List<Long> sortedLatencies, double percentile) {
        if (sortedLatencies.isEmpty()) return 0;
        int index = (int) (sortedLatencies.size() * percentile);
        index = Math.min(index, sortedLatencies.size() - 1);
        return sortedLatencies.get(index);
    }

    // ========== 打印方法 ==========

    private static void printTierResults(Map<PrecisionTier, TierResult> results) {
        System.out.println();
        System.out.printf("%-12s %12s %10s %8s %8s %8s %8s%n",
            "Tier", "QPS", "Avg(ms)", "P50", "P90", "P99", "P99.9");
        System.out.println("-".repeat(75));
        for (Map.Entry<PrecisionTier, TierResult> entry : results.entrySet()) {
            TierResult r = entry.getValue();
            System.out.printf("%-12s %,12.0f %10.2f %8d %8d %8d %8d%n",
                entry.getKey().name(), r.qps, r.avgLatency, r.p50, r.p90, r.p99, r.p999);
        }
    }

    private static void printThreadResults(Map<Integer, TierResult> results) {
        System.out.println();
        System.out.printf("%-10s %12s %10s %8s%n", "Threads", "QPS", "Avg(ms)", "P99(ms)");
        System.out.println("-".repeat(45));
        for (Map.Entry<Integer, TierResult> entry : results.entrySet()) {
            TierResult r = entry.getValue();
            System.out.printf("%-10d %,12.0f %10.2f %8d%n",
                entry.getKey(), r.qps, r.avgLatency, r.p99);
        }
    }

    private static void printSummaryReport(
            Map<PrecisionTier, TierResult> tierResults,
            Map<Integer, TierResult> threadResults,
            TierResult mixedResult,
            long queryQps,
            long memPerIntent) {

        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║                      性能测试汇总报告                        ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println();

        // 峰值吞吐量
        TierResult bestTier = tierResults.values().stream()
            .max(Comparator.comparingDouble(r -> r.qps))
            .orElse(null);
        if (bestTier != null) {
            System.out.printf("📊 峰值写入吞吐量: %,.0f QPS%n", bestTier.qps);
        }

        // 混合档位
        System.out.printf("📊 混合档位吞吐量: %,.0f QPS%n", mixedResult.qps);

        // 查询吞吐量
        System.out.printf("📊 查询吞吐量: %,d QPS%n", queryQps);

        // 延迟
        System.out.printf("📊 创建延迟 P99: %d ms%n", bestTier != null ? bestTier.p99 : 0);

        // 内存
        System.out.printf("📊 每任务内存: ~%d bytes%n", memPerIntent);

        // SLO 验证
        System.out.println();
        System.out.println("📋 SLO 验证:");
        for (Map.Entry<PrecisionTier, TierResult> entry : tierResults.entrySet()) {
            PrecisionTier tier = entry.getKey();
            TierResult r = entry.getValue();
            long slo = getSlo(tier);
            boolean passed = r.p99 <= slo;
            System.out.printf("  %s: P99=%dms, SLO=≤%dms %s%n",
                tier.name(), r.p99, slo, passed ? "✅" : "❌");
        }
    }

    private static long getSlo(PrecisionTier tier) {
        return switch (tier) {
            case ULTRA -> 15;
            case FAST -> 60;
            case HIGH -> 120;
            case STANDARD -> 550;
            case ECONOMY -> 1100;
        };
    }

    private static void generateMarkdownReport(
            Map<PrecisionTier, TierResult> tierResults,
            Map<Integer, TierResult> threadResults,
            TierResult mixedResult,
            long queryQps,
            long memPerIntent) {

        System.out.println();
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println("Markdown 报告 (可复制到 README):");
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println();

        // 性能指标表格
        System.out.println("### Performance Metrics");
        System.out.println();
        System.out.println("| Metric | Value |");
        System.out.println("|--------|-------|");

        TierResult bestTier = tierResults.values().stream()
            .max(Comparator.comparingDouble(r -> r.qps))
            .orElse(null);

        if (bestTier != null) {
            System.out.printf("| **Peak Write Throughput** | **%,.0f QPS** |%n", bestTier.qps);
        }
        System.out.printf("| **Mixed Tier Throughput** | %,.0f QPS |%n", mixedResult.qps);
        System.out.printf("| **Query Throughput** | **%,d QPS** |%n", queryQps);
        if (bestTier != null) {
            System.out.printf("| **Create Latency P99** | %d ms |%n", bestTier.p99);
        }
        System.out.printf("| **Memory per Intent** | **~%d bytes** |%n", memPerIntent);
        System.out.println();

        // 精度档位性能
        System.out.println("### Precision Tier Throughput Comparison");
        System.out.println();
        System.out.println("| Tier | Window | QPS | P99 (ms) | SLO |");
        System.out.println("|------|--------|-----|----------|-----|");
        for (Map.Entry<PrecisionTier, TierResult> entry : tierResults.entrySet()) {
            PrecisionTier tier = entry.getKey();
            TierResult r = entry.getValue();
            long slo = getSlo(tier);
            boolean passed = r.p99 <= slo;
            System.out.printf("| %s | %dms | %,.0f | %d | ≤%dms %s |%n",
                tier.name(), tier.getPrecisionWindowMs(), r.qps, r.p99, slo, passed ? "✅" : "❌");
        }
    }

    private record TierResult(
        double qps,
        double avgLatency,
        long p50,
        long p90,
        long p99,
        long p999,
        int successCount,
        int failCount
    ) {}
}

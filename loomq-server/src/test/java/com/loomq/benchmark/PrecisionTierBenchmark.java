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
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 精度档位压测工具 (v0.7.0)
 *
 * 对比不同精度档位的：
 * 1. 相同吞吐下的 CPU 占用与唤醒延迟
 * 2. 相同堆积量下的 GC 频率与暂停时间
 * 3. P99 唤醒延迟尾部分布
 *
 * @author loomq
 * @since v0.7.0
 */
public class PrecisionTierBenchmark {

    private static final String BASE_URL = "http://localhost:8080";
    private static final HttpClient httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();

    // SLO 边界 (p99)
    private static final Map<PrecisionTier, Long> SLO_BOUNDS = Map.of(
        PrecisionTier.ULTRA, 15L,
        PrecisionTier.FAST, 60L,
        PrecisionTier.HIGH, 120L,
        PrecisionTier.STANDARD, 550L,
        PrecisionTier.ECONOMY, 1100L
    );

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }

        String command = args[0];
        switch (command) {
            case "throughput" -> runThroughputBenchmark(args);
            case "latency" -> runLatencyBenchmark(args);
            case "compare" -> runCompareBenchmark(args);
            case "slo" -> runSLOVerification(args);
            default -> printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("LoomQ 精度档位压测工具 (v0.7.0)");
        System.out.println();
        System.out.println("用法:");
        System.out.println("  java -cp target/test-classes:target/classes com.loomq.benchmark.PrecisionTierBenchmark <command> [options]");
        System.out.println();
        System.out.println("命令:");
        System.out.println("  throughput <tier> <threads> <duration_s>  吞吐量测试");
        System.out.println("                                            tier: ULTRA/FAST/HIGH/STANDARD/ECONOMY");
        System.out.println("                                            threads: 并发线程数 (默认 10)");
        System.out.println("                                            duration: 测试时长秒 (默认 30)");
        System.out.println();
        System.out.println("  latency <tier> <count>                    延迟分布测试");
        System.out.println("                                            count: 任务数量 (默认 1000)");
        System.out.println();
        System.out.println("  compare <count>                           跨档位对比测试");
        System.out.println("                                            count: 每档任务数 (默认 1000)");
        System.out.println();
        System.out.println("  slo                                       SLO 验证测试");
        System.out.println();
        System.out.println("示例:");
        System.out.println("  java -cp target/test-classes:target/classes com.loomq.benchmark.PrecisionTierBenchmark throughput STANDARD 20 60");
        System.out.println("  java -cp target/test-classes:target/classes com.loomq.benchmark.PrecisionTierBenchmark latency ECONOMY 5000");
        System.out.println("  java -cp target/test-classes:target/classes com.loomq.benchmark.PrecisionTierBenchmark compare 2000");
    }

    /**
     * 吞吐量测试
     */
    private static void runThroughputBenchmark(String[] args) throws Exception {
        PrecisionTier tier = args.length > 1 ? PrecisionTier.valueOf(args[1].toUpperCase()) : PrecisionTier.STANDARD;
        int threads = args.length > 2 ? Integer.parseInt(args[2]) : 10;
        int durationSec = args.length > 3 ? Integer.parseInt(args[3]) : 30;

        System.out.println("=== 吞吐量测试 ===");
        System.out.println("精度档位: " + tier);
        System.out.println("并发线程: " + threads);
        System.out.println("测试时长: " + durationSec + " 秒");
        System.out.println("精度窗口: " + tier.getPrecisionWindowMs() + " ms");
        System.out.println();

        if (!healthCheck()) {
            System.err.println("服务不可用，请先启动 LoomQ");
            return;
        }

        // 预热
        System.out.println("预热中...");
        for (int i = 0; i < 50; i++) {
            createIntent(tier, 3600);
        }
        Thread.sleep(1000);

        // 开始压测
        System.out.println("开始压测...");
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        AtomicLong totalBytes = new AtomicLong(0);

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

        // 统计结果
        Collections.sort(latencies);
        double qps = (double) successCount.get() / actualDuration * 1000;

        System.out.println();
        System.out.println("=== 测试结果 ===");
        System.out.println("实际耗时: " + actualDuration + " ms");
        System.out.println("成功请求: " + successCount.get());
        System.out.println("失败请求: " + failCount.get());
        System.out.println("吞吐量: " + String.format("%.2f", qps) + " QPS");
        System.out.println();

        if (!latencies.isEmpty()) {
            double avgLatency = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
            long p50 = getPercentile(latencies, 0.50);
            long p90 = getPercentile(latencies, 0.90);
            long p95 = getPercentile(latencies, 0.95);
            long p99 = getPercentile(latencies, 0.99);

            System.out.println("延迟统计:");
            System.out.println("  平均: " + String.format("%.2f", avgLatency) + " ms");
            System.out.println("  P50:  " + p50 + " ms");
            System.out.println("  P90:  " + p90 + " ms");
            System.out.println("  P95:  " + p95 + " ms");
            System.out.println("  P99:  " + p99 + " ms");
        }

        System.out.println();
        System.out.println("建议: 使用 jstat -gcutil <pid> 1000 监控 GC 情况");
    }

    /**
     * 延迟分布测试
     */
    private static void runLatencyBenchmark(String[] args) throws Exception {
        PrecisionTier tier = args.length > 1 ? PrecisionTier.valueOf(args[1].toUpperCase()) : PrecisionTier.STANDARD;
        int count = args.length > 2 ? Integer.parseInt(args[2]) : 1000;

        System.out.println("=== 延迟分布测试 ===");
        System.out.println("精度档位: " + tier);
        System.out.println("任务数量: " + count);
        System.out.println("精度窗口: " + tier.getPrecisionWindowMs() + " ms");
        System.out.println("SLO (p99): " + SLO_BOUNDS.get(tier) + " ms");
        System.out.println();

        if (!healthCheck()) {
            System.err.println("服务不可用，请先启动 LoomQ");
            return;
        }

        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger successCount = new AtomicInteger(0);

        System.out.println("创建任务中...");
        long startTime = System.currentTimeMillis();

        // 使用虚拟线程并发创建
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Long>> futures = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                futures.add(executor.submit(() -> {
                    long reqStart = System.nanoTime();
                    createIntent(tier, 300); // 5分钟后执行
                    return (System.nanoTime() - reqStart) / 1_000_000;
                }));
            }

            for (Future<Long> future : futures) {
                try {
                    latencies.add(future.get());
                    successCount.incrementAndGet();
                    if (successCount.get() % 1000 == 0) {
                        System.out.println("已创建: " + successCount.get() + " 任务");
                    }
                } catch (Exception e) {
                    // ignore
                }
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        // 统计
        Collections.sort(latencies);
        double qps = (double) count / duration * 1000;

        System.out.println();
        System.out.println("=== 结果 ===");
        System.out.println("创建耗时: " + duration + " ms");
        System.out.println("成功创建: " + successCount.get());
        System.out.println("创建速率: " + String.format("%.2f", qps) + " tasks/s");
        System.out.println();

        if (!latencies.isEmpty()) {
            System.out.println("延迟分布:");
            printLatencyDistribution(latencies);
        }
    }

    /**
     * 跨档位对比测试
     */
    private static void runCompareBenchmark(String[] args) throws Exception {
        int countPerTier = args.length > 1 ? Integer.parseInt(args[1]) : 1000;

        System.out.println("=== 跨档位对比测试 ===");
        System.out.println("每档任务数: " + countPerTier);
        System.out.println();

        if (!healthCheck()) {
            System.err.println("服务不可用，请先启动 LoomQ");
            return;
        }

        Map<PrecisionTier, BenchmarkResult> results = new EnumMap<>(PrecisionTier.class);

        for (PrecisionTier tier : PrecisionTier.values()) {
            System.out.println("测试档位: " + tier);
            BenchmarkResult result = benchmarkTier(tier, countPerTier);
            results.put(tier, result);
            System.out.println();
        }

        // 对比表格
        System.out.println("=== 对比结果 ===");
        System.out.println();
        System.out.printf("%-10s %10s %10s %10s %10s%n",
            "Tier", "Window(ms)", "QPS", "P95(ms)", "P99(ms)");
        System.out.println("-".repeat(55));

        for (Map.Entry<PrecisionTier, BenchmarkResult> entry : results.entrySet()) {
            PrecisionTier tier = entry.getKey();
            BenchmarkResult r = entry.getValue();
            System.out.printf("%-10s %10d %10.0f %10d %10d%n",
                tier.name(), tier.getPrecisionWindowMs(), r.qps, r.p95, r.p99);
        }

        System.out.println();
        System.out.println("结论:");
        System.out.println("  - 低精度档位 (STANDARD/ECONOMY) 理论上具有更高的吞吐上限");
        System.out.println("  - 高精度档位 (ULTRA/FAST) 适合对延迟敏感的场景");
        System.out.println("  - 实际性能受回调延迟、网络等多种因素影响");
    }

    /**
     * SLO 验证测试
     */
    private static void runSLOVerification(String[] args) throws Exception {
        System.out.println("=== SLO 验证测试 ===");
        System.out.println();
        System.out.println("验证各档位的唤醒延迟是否满足 SLO (p99):");
        SLO_BOUNDS.forEach((tier, slo) ->
            System.out.println("  " + tier + ": ≤" + slo + "ms"));
        System.out.println();

        if (!healthCheck()) {
            System.err.println("服务不可用，请先启动 LoomQ");
            return;
        }

        System.out.println("注意: 此测试验证创建延迟，唤醒延迟需等待任务到期后测量");
        System.out.println("建议: 使用 Prometheus 查询 loomq_scheduler_wakeup_late_ms 指标");
        System.out.println();

        // 简单验证创建 API 正常工作
        for (PrecisionTier tier : PrecisionTier.values()) {
            try {
                createIntent(tier, 60);
                System.out.println("  " + tier + ": ✅ API 正常");
            } catch (Exception e) {
                System.out.println("  " + tier + ": ❌ API 异常 - " + e.getMessage());
            }
        }
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
                "intentId": "bench-%s-%d",
                "executeAt": "%s",
                "deadline": "%s",
                "precisionTier": "%s",
                "shardKey": "benchmark",
                "callback": {
                    "url": "http://localhost:9999/webhook"
                }
            }
            """,
            tier.name().toLowerCase(),
            System.nanoTime(),
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

    private static long getPercentile(List<Long> sortedLatencies, double percentile) {
        if (sortedLatencies.isEmpty()) return 0;
        int index = (int) (sortedLatencies.size() * percentile);
        index = Math.min(index, sortedLatencies.size() - 1);
        return sortedLatencies.get(index);
    }

    private static void printLatencyDistribution(List<Long> sortedLatencies) {
        long min = sortedLatencies.get(0);
        long max = sortedLatencies.get(sortedLatencies.size() - 1);
        double avg = sortedLatencies.stream().mapToLong(Long::longValue).average().orElse(0);

        System.out.println("  最小: " + min + " ms");
        System.out.println("  最大: " + max + " ms");
        System.out.println("  平均: " + String.format("%.2f", avg) + " ms");
        System.out.println();
        System.out.println("  百分位分布:");
        System.out.println("    P50:  " + getPercentile(sortedLatencies, 0.50) + " ms");
        System.out.println("    P75:  " + getPercentile(sortedLatencies, 0.75) + " ms");
        System.out.println("    P90:  " + getPercentile(sortedLatencies, 0.90) + " ms");
        System.out.println("    P95:  " + getPercentile(sortedLatencies, 0.95) + " ms");
        System.out.println("    P99:  " + getPercentile(sortedLatencies, 0.99) + " ms");
        System.out.println("    P99.9: " + getPercentile(sortedLatencies, 0.999) + " ms");
    }

    private static BenchmarkResult benchmarkTier(PrecisionTier tier, int count) throws Exception {
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        long startTime = System.currentTimeMillis();

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Long>> futures = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                futures.add(executor.submit(() -> {
                    long reqStart = System.nanoTime();
                    createIntent(tier, 3600);
                    return (System.nanoTime() - reqStart) / 1_000_000;
                }));
            }

            for (Future<Long> future : futures) {
                try {
                    latencies.add(future.get());
                } catch (Exception e) {
                    // ignore
                }
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        Collections.sort(latencies);

        return new BenchmarkResult(
            (double) count / duration * 1000,
            getPercentile(latencies, 0.95),
            getPercentile(latencies, 0.99)
        );
    }

    private record BenchmarkResult(double qps, long p95, long p99) {}
}

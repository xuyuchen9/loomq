package com.loomq.benchmark;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 激进压测 - 模拟 wrk/ab 级别的并发
 */
public class AggressiveBenchmark {

    private static final String BASE_URL = "http://localhost:8080";
    private static final int WARMUP = 1000;

    public static void main(String[] args) throws Exception {
        // 测试配置
        int[] threadConfigs = {100, 200, 500, 1000};
        int durationSec = 30;
        String[] tiers = {"ULTRA", "FAST", "HIGH", "STANDARD", "ECONOMY"};

        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║         LoomQ v0.7.0 激进压测 (模拟 wrk)                     ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println();

        // 创建多个 HttpClient 实例模拟真实场景
        HttpClient[] clients = new HttpClient[10];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
        }

        // 预热
        System.out.println("🔥 预热中...");
        warmup(clients[0]);
        System.out.println("✅ 预热完成");
        System.out.println();

        // 找最佳线程数
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println("测试 1: 最佳线程数探索 (STANDARD 档位)");
        System.out.println("══════════════════════════════════════════════════════════════");

        double bestQps = 0;
        int bestThreads = 0;

        for (int threads : threadConfigs) {
            Result r = runTest(clients, "STANDARD", threads, durationSec);
            System.out.printf("  %d 线程: QPS=%,.0f, P99=%dms%n", threads, r.qps, r.p99);
            if (r.qps > bestQps) {
                bestQps = r.qps;
                bestThreads = threads;
            }
        }
        System.out.println();

        // 用最佳线程数测试所有档位
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println("测试 2: 全档位性能对比 (" + bestThreads + " 线程)");
        System.out.println("══════════════════════════════════════════════════════════════");

        Map<String, Result> results = new LinkedHashMap<>();
        for (String tier : tiers) {
            Result r = runTest(clients, tier, bestThreads, durationSec);
            results.put(tier, r);
        }

        // 打印结果
        System.out.println();
        System.out.printf("%-12s %12s %10s %8s %8s %8s%n",
            "Tier", "QPS", "Avg(ms)", "P50", "P99", "P99.9");
        System.out.println("-".repeat(60));
        for (Map.Entry<String, Result> e : results.entrySet()) {
            Result r = e.getValue();
            System.out.printf("%-12s %,12.0f %10.2f %8d %8d %8d%n",
                e.getKey(), r.qps, r.avgMs, r.p50, r.p99, r.p999);
        }

        // 汇总
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║                      压测结果汇总                             ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.printf("峰值吞吐量: %,.0f QPS%n", bestQps);
        System.out.printf("最佳线程数: %d%n", bestThreads);
        System.out.println();

        // 内存信息
        Runtime rt = Runtime.getRuntime();
        long usedMem = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
        System.out.printf("JVM 内存使用: %d MB%n", usedMem);
    }

    private static void warmup(HttpClient client) throws Exception {
        for (int i = 0; i < WARMUP; i++) {
            createIntent(client, "STANDARD");
        }
    }

    private static Result runTest(HttpClient[] clients, String tier, int threads, int durationSec) throws Exception {
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger fail = new AtomicInteger(0);
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>(1_000_000));

        CountDownLatch latch = new CountDownLatch(threads);
        AtomicLong totalRequests = new AtomicLong(0);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + durationSec * 1000L;

        // 使用多个 HttpClient 分散连接压力
        for (int t = 0; t < threads; t++) {
            final HttpClient client = clients[t % clients.length];
            Thread.ofVirtual().start(() -> {
                try {
                    while (System.currentTimeMillis() < endTime) {
                        long reqStart = System.nanoTime();
                        try {
                            createIntent(client, tier);
                            long latency = (System.nanoTime() - reqStart) / 1_000_000;
                            latencies.add(latency);
                            success.incrementAndGet();
                        } catch (Exception e) {
                            fail.incrementAndGet();
                        }
                        totalRequests.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        // 实时输出进度
        for (int i = 0; i < durationSec; i += 5) {
            Thread.sleep(5000);
            int currentSuccess = success.get();
            long elapsed = System.currentTimeMillis() - startTime;
            double currentQps = (double) currentSuccess / elapsed * 1000;
            System.out.printf("  [%ds] QPS: %,.0f, Success: %d%n", i + 5, currentQps, currentSuccess);
        }

        latch.await();
        long duration = System.currentTimeMillis() - startTime;

        Collections.sort(latencies);

        double qps = (double) success.get() / duration * 1000;
        double avgMs = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
        long p50 = getPercentile(latencies, 0.50);
        long p90 = getPercentile(latencies, 0.90);
        long p99 = getPercentile(latencies, 0.99);
        long p999 = getPercentile(latencies, 0.999);

        return new Result(qps, avgMs, p50, p90, p99, p999, success.get(), fail.get());
    }

    private static void createIntent(HttpClient client, String tier) throws Exception {
        Instant executeAt = Instant.now().plus(1, ChronoUnit.HOURS);
        Instant deadline = executeAt.plus(1, ChronoUnit.HOURS);

        String body = String.format(
            "{\"executeAt\":\"%s\",\"deadline\":\"%s\",\"precisionTier\":\"%s\",\"shardKey\":\"bench-%d\",\"callback\":{\"url\":\"http://localhost/webhook\"}}",
            executeAt.toString(), deadline.toString(), tier, System.nanoTime()
        );

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + "/v1/intents"))
            .header("Content-Type", "application/json")
            .timeout(Duration.ofSeconds(10))
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

        client.send(request, HttpResponse.BodyHandlers.discarding());
    }

    private static long getPercentile(List<Long> sorted, double p) {
        if (sorted.isEmpty()) return 0;
        int idx = (int) (sorted.size() * p);
        idx = Math.min(idx, sorted.size() - 1);
        return sorted.get(idx);
    }

    record Result(double qps, double avgMs, long p50, long p90, long p99, long p999, int success, int fail) {}
}

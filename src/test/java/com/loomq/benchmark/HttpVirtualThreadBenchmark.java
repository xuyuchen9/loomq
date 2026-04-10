package com.loomq.benchmark;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HTTP 层性能测试 (v0.5.2)
 *
 * 验证虚拟线程对 HTTP 吞吐量的提升效果。
 * 目标: >= 150K QPS
 *
 * 运行方式:
 * 1. 启动 LoomQ 服务 (java --enable-preview -jar target/loomq-0.5.2-SNAPSHOT-shaded.jar)
 * 2. 运行此测试 (java --enable-preview -cp target/test-classes:target/classes com.loomq.benchmark.HttpVirtualThreadBenchmark)
 */
public class HttpVirtualThreadBenchmark {

    private static final String BASE_URL = "http://localhost:8080";
    private static final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .version(HttpClient.Version.HTTP_1_1)
        .build();

    public static void main(String[] args) throws Exception {
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║     LoomQ v0.5.2 HTTP Virtual Thread 性能测试                ║");
        System.out.println("║     目标: >= 150,000 QPS                                     ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println();

        // 预热
        System.out.println("[预热] 发送 100 个请求预热服务...");
        warmUp(100);
        Thread.sleep(1000);
        System.out.println("[预热] 完成");
        System.out.println();

        // 测试配置: 递增线程数
        int[] threadCounts = {50, 100, 200, 500, 1000};
        int durationSec = 20;

        List<BenchmarkResult> results = new ArrayList<>();

        for (int threads : threadCounts) {
            System.out.println("══════════════════════════════════════════════════════════════");
            System.out.printf("测试配置: %d 并发线程, %d 秒持续时间%n", threads, durationSec);
            System.out.println("══════════════════════════════════════════════════════════════");

            BenchmarkResult result = runTest(threads, durationSec);
            results.add(result);

            System.out.printf("  QPS:         %,.0f%n", result.qps);
            System.out.printf("  平均延迟:    %.2f ms%n", result.avgLatency);
            System.out.printf("  P50:         %d ms%n", result.p50);
            System.out.printf("  P90:         %d ms%n", result.p90);
            System.out.printf("  P99:         %d ms%n", result.p99);
            System.out.printf("  成功/失败:   %,d / %,d%n", result.success, result.fail);
            System.out.println();

            Thread.sleep(2000); // 冷却间隔
        }

        // 打印汇总
        printSummary(results);

        // 判断是否达标
        checkTarget(results);
    }

    private static void warmUp(int count) throws Exception {
        CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            Thread.ofVirtual().start(() -> {
                try {
                    createIntent();
                } catch (Exception ignored) {
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await(30, TimeUnit.SECONDS);
    }

    private static BenchmarkResult runTest(int threads, int durationSec) throws Exception {
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger fail = new AtomicInteger(0);
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());

        CountDownLatch latch = new CountDownLatch(threads);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + durationSec * 1000L;

        for (int t = 0; t < threads; t++) {
            Thread.ofVirtual().start(() -> {
                try {
                    while (System.currentTimeMillis() < endTime) {
                        long reqStart = System.nanoTime();
                        try {
                            createIntent();
                            latencies.add((System.nanoTime() - reqStart) / 1_000_000);
                            success.incrementAndGet();
                        } catch (Exception e) {
                            fail.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long actualDuration = System.currentTimeMillis() - startTime;

        Collections.sort(latencies);

        double qps = (double) success.get() / actualDuration * 1000;
        double avgLatency = latencies.isEmpty() ? 0 : latencies.stream().mapToLong(Long::longValue).average().orElse(0);
        long p50 = latencies.isEmpty() ? 0 : getPercentile(latencies, 0.50);
        long p90 = latencies.isEmpty() ? 0 : getPercentile(latencies, 0.90);
        long p99 = latencies.isEmpty() ? 0 : getPercentile(latencies, 0.99);

        return new BenchmarkResult(qps, avgLatency, p50, p90, p99, success.get(), fail.get(), threads);
    }

    private static long getPercentile(List<Long> sortedLatencies, double percentile) {
        return sortedLatencies.get((int) (sortedLatencies.size() * percentile));
    }

    private static void createIntent() throws Exception {
        Instant executeAt = Instant.now().plus(3600, ChronoUnit.SECONDS);
        Instant deadline = executeAt.plus(5, ChronoUnit.MINUTES);
        String id = UUID.randomUUID().toString();

        String body = String.format(
            "{\"intentId\":\"%s\",\"executeAt\":\"%s\",\"deadline\":\"%s\",\"precisionTier\":\"STANDARD\",\"shardKey\":\"bench\",\"callback\":{\"url\":\"http://localhost:9999/webhook\"}}",
            id, executeAt.toString(), deadline.toString()
        );

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + "/v1/intents"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 400) {
            throw new RuntimeException("HTTP error: " + response.statusCode());
        }
    }

    private static void printSummary(List<BenchmarkResult> results) {
        System.out.println();
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println("                    性能汇总表格                               ");
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println();
        System.out.printf("%-10s %12s %10s %10s %10s%n", "Threads", "QPS", "Avg(ms)", "P99(ms)", "Success");
        System.out.println("-".repeat(56));

        for (BenchmarkResult r : results) {
            System.out.printf("%-10d %,12.0f %10.2f %10d %,10d%n",
                r.threads, r.qps, r.avgLatency, r.p99, r.success);
        }
    }

    private static void checkTarget(List<BenchmarkResult> results) {
        System.out.println();
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println("                    目标达成检查                               ");
        System.out.println("══════════════════════════════════════════════════════════════");

        double maxQps = results.stream().mapToDouble(r -> r.qps).max().orElse(0);
        double minP99 = results.stream().mapToLong(r -> r.p99).min().orElse(0);

        System.out.println();
        System.out.printf("峰值 QPS:   %,.0f (目标 >= 150,000)%n", maxQps);
        System.out.printf("最低 P99:   %d ms (目标 <= 20ms)%n", minP99);
        System.out.println();

        if (maxQps >= 150_000) {
            System.out.println("✅ QPS 目标达成!");
        } else {
            System.out.printf("❌ QPS 未达标 (差距: %.0f QPS)%n", 150_000 - maxQps);
        }

        if (minP99 <= 20) {
            System.out.println("✅ P99 延迟目标达成!");
        } else {
            System.out.printf("❌ P99 延迟未达标 (差距: %d ms)%n", minP99 - 20);
        }

        System.out.println();
        System.out.println("══════════════════════════════════════════════════════════════");
    }

    record BenchmarkResult(
        double qps,
        double avgLatency,
        long p50,
        long p90,
        long p99,
        int success,
        int fail,
        int threads
    ) {}
}

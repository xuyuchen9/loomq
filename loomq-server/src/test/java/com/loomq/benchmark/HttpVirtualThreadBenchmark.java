package com.loomq.benchmark;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadLocalRandom;

/**
 * HTTP 层性能测试。
 *
 * 只报告真实观察值，不给硬编码目标。
 */
public class HttpVirtualThreadBenchmark {

    private static final String BASE_URL = System.getProperty("loomq.benchmark.baseUrl", "http://localhost:8080");
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);
    private static final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .version(HttpClient.Version.HTTP_1_1)
        .build();

    public static void main(String[] args) throws Exception {
        boolean quick = Boolean.getBoolean("loomq.benchmark.quick");

        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║     LoomQ HTTP Virtual Thread 性能测试                       ║");
        System.out.println("║     关注 HTTP / JSON / 路由 的真实观察值                    ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("模式: " + (quick ? "QUICK" : "FULL"));
        System.out.println();

        // 预热
        int warmupCount = quick ? 20 : 100;
        System.out.println("[预热] 发送 " + warmupCount + " 个请求预热服务...");
        warmUp(warmupCount);
        Thread.sleep(1000);
        System.out.println("[预热] 完成");
        System.out.println();

        // 测试配置: 递增线程数
        int[] threadCounts = quick ? new int[] {16, 32, 64} : new int[] {50, 100, 200, 500, 1000};
        int durationSec = quick ? 4 : 20;
        int cooldownMs = quick ? 1000 : 2000;

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
            System.out.printf(Locale.ROOT,
                "RESULT_ROW|threads=%d|qps=%.0f|avg_ms=%.2f|p50_ms=%d|p90_ms=%d|p99_ms=%d|success=%d|fail=%d%n",
                result.threads, result.qps, result.avgLatency, result.p50, result.p90, result.p99, result.success, result.fail);
            System.out.println();

            Thread.sleep(cooldownMs); // 冷却间隔
        }

        // 打印汇总
        printSummary(results);
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

        // Bound wait time so a single stuck request cannot hang the whole scenario forever.
        latch.await(durationSec + 20L, TimeUnit.SECONDS);
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
        String shardKey = "bench-" + ThreadLocalRandom.current().nextInt(64);

        String body = String.format(
            "{\"intentId\":\"%s\",\"executeAt\":\"%s\",\"deadline\":\"%s\",\"precisionTier\":\"STANDARD\",\"shardKey\":\"%s\",\"callback\":{\"url\":\"http://localhost:9999/webhook\"}}",
            id, executeAt.toString(), deadline.toString(), shardKey
        );

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + "/v1/intents"))
            .header("Content-Type", "application/json")
            .timeout(REQUEST_TIMEOUT)
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

        BenchmarkResult peak = results.stream()
            .max(Comparator.comparingDouble(r -> r.qps))
            .orElse(null);
        if (peak != null) {
            BenchmarkResult bestP99 = results.stream()
                .min(Comparator.comparingLong(r -> r.p99))
                .orElse(peak);
            BenchmarkResult worstP99 = results.stream()
                .max(Comparator.comparingLong(r -> r.p99))
                .orElse(peak);
            int totalSuccess = results.stream().mapToInt(r -> r.success).sum();
            int totalFail = results.stream().mapToInt(r -> r.fail).sum();
            double failRate = (totalSuccess + totalFail) == 0 ? 0 : (double) totalFail / (totalSuccess + totalFail) * 100.0;
            System.out.println();
            System.out.printf("峰值 QPS:   %,.0f @ %d 线程%n", peak.qps, peak.threads);
            System.out.printf("最佳 P99:   %d ms @ %d 线程%n", bestP99.p99, bestP99.threads);
            System.out.printf("最差 P99:   %d ms @ %d 线程%n", worstP99.p99, worstP99.threads);
            System.out.printf("失败率:     %.2f%%%n", failRate);
            System.out.println("解释: 这组结果反映的是 HTTP + JSON + 路由 + 线程调度的真实开销，不是存储上限。");
            System.out.printf(Locale.ROOT,
                "RESULT|peak_qps=%.0f|best_threads=%d|best_p99_ms=%d|best_avg_ms=%.2f|worst_p99_ms=%d|worst_threads=%d|fail_rate=%.2f%n",
                peak.qps, peak.threads, bestP99.p99, bestP99.avgLatency, worstP99.p99, worstP99.threads, failRate);
        }
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

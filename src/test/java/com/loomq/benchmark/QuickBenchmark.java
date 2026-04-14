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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 极限性能测试 - 简化版
 */
public class QuickBenchmark {

    private static final String BASE_URL = "http://localhost:8080";
    private static final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .build();

    public static void main(String[] args) throws Exception {
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║         LoomQ v0.5.1 极限性能测试 (5 档位)                   ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println();

        // 测试参数
        int[] threadCounts = {50, 100, 200};
        int durationSec = 15;
        String[] tiers = {"ULTRA", "FAST", "HIGH", "STANDARD", "ECONOMY"};

        // 结果存储
        Map<String, Map<Integer, BenchmarkResult>> allResults = new LinkedHashMap<>();

        for (String tier : tiers) {
            System.out.println("══════════════════════════════════════════════════════════════");
            System.out.println("测试精度档位: " + tier);
            System.out.println("══════════════════════════════════════════════════════════════");

            Map<Integer, BenchmarkResult> tierResults = new LinkedHashMap<>();

            for (int threads : threadCounts) {
                BenchmarkResult result = runTest(tier, threads, durationSec);
                tierResults.put(threads, result);
                System.out.printf("  %d 线程: QPS=%,.0f, P99=%dms%n", threads, result.qps, result.p99);
            }

            allResults.put(tier, tierResults);
            System.out.println();
        }

        // 打印汇总表格
        printSummaryTable(allResults);

        // 打印最佳结果
        printBestResults(allResults);
    }

    private static BenchmarkResult runTest(String tier, int threads, int durationSec) throws Exception {
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
                            createIntent(tier);
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
        double avgLatency = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
        long p50 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.50));
        long p90 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.90));
        long p99 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.99));

        return new BenchmarkResult(qps, avgLatency, p50, p90, p99, success.get(), fail.get());
    }

    private static void createIntent(String tier) throws Exception {
        Instant executeAt = Instant.now().plus(3600, ChronoUnit.SECONDS);
        Instant deadline = executeAt.plus(5, ChronoUnit.MINUTES);

        String body = String.format(
            "{\"executeAt\":\"%s\",\"deadline\":\"%s\",\"precisionTier\":\"%s\",\"shardKey\":\"bench\",\"callback\":{\"url\":\"http://localhost:9999/webhook\"}}",
            executeAt.toString(), deadline.toString(), tier
        );

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + "/v1/intents"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

        client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static void printSummaryTable(Map<String, Map<Integer, BenchmarkResult>> allResults) {
        System.out.println();
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println("                    汇总表格 (QPS)                             ");
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println();
        System.out.printf("%-12s", "Tier\\Threads");
        for (int threads : List.of(50, 100, 200)) {
            System.out.printf("%12d", threads);
        }
        System.out.println();
        System.out.println("-".repeat(48));

        for (Map.Entry<String, Map<Integer, BenchmarkResult>> entry : allResults.entrySet()) {
            System.out.printf("%-12s", entry.getKey());
            for (int threads : List.of(50, 100, 200)) {
                BenchmarkResult r = entry.getValue().get(threads);
                if (r != null) {
                    System.out.printf("%,12.0f", r.qps);
                } else {
                    System.out.printf("%12s", "-");
                }
            }
            System.out.println();
        }
    }

    private static void printBestResults(Map<String, Map<Integer, BenchmarkResult>> allResults) {
        System.out.println();
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println("                    最佳性能指标                               ");
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println();

        double maxQps = 0;
        String bestTier = "";
        int bestThreads = 0;

        for (Map.Entry<String, Map<Integer, BenchmarkResult>> entry : allResults.entrySet()) {
            for (Map.Entry<Integer, BenchmarkResult> r : entry.getValue().entrySet()) {
                if (r.getValue().qps > maxQps) {
                    maxQps = r.getValue().qps;
                    bestTier = entry.getKey();
                    bestThreads = r.getKey();
                }
            }
        }

        System.out.printf("峰值写入吞吐量: %,.0f QPS (%s, %d 线程)%n", maxQps, bestTier, bestThreads);
        System.out.println();

        // 各档位最佳 QPS
        System.out.println("各精度档位最佳 QPS:");
        for (Map.Entry<String, Map<Integer, BenchmarkResult>> entry : allResults.entrySet()) {
            double tierMax = entry.getValue().values().stream()
                .mapToDouble(r -> r.qps)
                .max()
                .orElse(0);
            System.out.printf("  %-10s: %,.0f QPS%n", entry.getKey(), tierMax);
        }
    }

    record BenchmarkResult(double qps, double avgLatency, long p50, long p90, long p99, int success, int fail) {}
}

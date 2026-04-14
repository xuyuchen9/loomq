package com.loomq.benchmark;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 简单的 HTTP 吞吐量测试
 * 测试 /health 端点，验证纯 HTTP 层性能（无 JSON 序列化开销）
 */
public class SimpleHttpBenchmark {

    private static final String BASE_URL = "http://localhost:8080";
    private static final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .version(HttpClient.Version.HTTP_1_1)
        .build();

    public static void main(String[] args) throws Exception {
        System.out.println("=== Simple HTTP Throughput Test ===");
        System.out.println("Testing /health endpoint (no JSON serialization)");
        System.out.println();

        // Warm up
        System.out.println("[Warm up] Sending 100 requests...");
        for (int i = 0; i < 100; i++) {
            try {
                getHealth();
            } catch (Exception ignored) {}
        }
        Thread.sleep(1000);
        System.out.println("[Warm up] Done");
        System.out.println();

        // Test with different concurrency levels
        int[] threads = {50, 100, 200, 500};
        int durationSec = 10;

        for (int t : threads) {
            BenchmarkResult result = runTest(t, durationSec);
            System.out.printf("Threads=%3d: QPS=%,8.0f, Avg=%.2fms, P99=%dms%n",
                t, result.qps, result.avgLatency, result.p99);
            Thread.sleep(2000);
        }

        System.out.println();
        System.out.println("=== Testing JSON endpoint for comparison ===");
        System.out.println();

        // Test JSON endpoint for comparison
        for (int t : threads) {
            BenchmarkResult result = runJsonTest(t, durationSec);
            System.out.printf("Threads=%3d: QPS=%,8.0f, Avg=%.2fms, P99=%dms%n",
                t, result.qps, result.avgLatency, result.p99);
            Thread.sleep(2000);
        }
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
                            getHealth();
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
        long p99 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.99));

        return new BenchmarkResult(qps, avgLatency, p99);
    }

    private static BenchmarkResult runJsonTest(int threads, int durationSec) throws Exception {
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
        long p99 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.99));

        return new BenchmarkResult(qps, avgLatency, p99);
    }

    private static void getHealth() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + "/health"))
            .GET()
            .build();
        client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static void createIntent() throws Exception {
        java.time.Instant executeAt = java.time.Instant.now().plus(3600, java.time.temporal.ChronoUnit.SECONDS);
        java.time.Instant deadline = executeAt.plus(5, java.time.temporal.ChronoUnit.MINUTES);
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
        client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    record BenchmarkResult(double qps, double avgLatency, long p99) {}
}

package com.loomq.benchmark;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Loomq 压测工具
 *
 * 验证目标:
 * 1. 创建接口 P95 ≤ 10ms
 * 2. 恢复 RTO ≤ 60s
 * 3. 单机 100 万挂起 Intent
 */
public class BenchmarkTest {

    private static final String BASE_URL = "http://localhost:8080";
    private static final String TOKEN = "dev-token-12345";
    private static final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }

        String command = args[0];
        switch (command) {
            case "create" -> runCreateBenchmark(args);
            case "million" -> runMillionIntentsBenchmark(args);
            case "recovery" -> runRecoveryBenchmark(args);
            default -> printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("Loomq 压测工具");
        System.out.println();
        System.out.println("用法:");
        System.out.println("  java -cp target/test-classes:target/classes com.loomq.benchmark.BenchmarkTest <command> [options]");
        System.out.println();
        System.out.println("命令:");
        System.out.println("  create <threads> <requests>  创建接口压测");
        System.out.println("                               threads: 并发线程数 (默认 10)");
        System.out.println("                               requests: 总请求数 (默认 1000)");
        System.out.println("  million <count>              百万 Intent 压测");
        System.out.println("                               count: Intent 数量 (默认 100000)");
        System.out.println("  recovery                     恢复时间测试 (需先停止服务，再启动)");
        System.out.println();
        System.out.println("示例:");
        System.out.println("  java -cp target/test-classes:target/classes com.loomq.benchmark.BenchmarkTest create 20 5000");
        System.out.println("  java -cp target/test-classes:target/classes com.loomq.benchmark.BenchmarkTest million 100000");
    }

    /**
     * 创建接口压测
     */
    private static void runCreateBenchmark(String[] args) throws Exception {
        int threads = args.length > 1 ? Integer.parseInt(args[1]) : 10;
        int totalRequests = args.length > 2 ? Integer.parseInt(args[2]) : 1000;
        int requestsPerThread = totalRequests / threads;

        System.out.println("=== 创建接口压测 ===");
        System.out.println("并发线程: " + threads);
        System.out.println("总请求数: " + totalRequests);
        System.out.println();

        // 健康检查
        if (!healthCheck()) {
            System.err.println("服务不可用，请先启动 Loomq");
            return;
        }

        // 预热
        System.out.println("预热中...");
        for (int i = 0; i < 10; i++) {
            createIntent(3600000); // 1 小时后执行
        }
        Thread.sleep(1000);

        // 开始压测
        System.out.println("开始压测...");
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threads);
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        long startTime = System.currentTimeMillis();

        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < requestsPerThread; i++) {
                        long reqStart = System.nanoTime();
                        try {
                            createIntent(3600000);
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

        long duration = System.currentTimeMillis() - startTime;

        // 统计结果
        Collections.sort(latencies);
        double avgLatency = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
        long p50 = latencies.get((int) (latencies.size() * 0.50));
        long p90 = latencies.get((int) (latencies.size() * 0.90));
        long p95 = latencies.get((int) (latencies.size() * 0.95));
        long p99 = latencies.get((int) (latencies.size() * 0.99));
        double qps = (double) successCount.get() / duration * 1000;

        System.out.println();
        System.out.println("=== 压测结果 ===");
        System.out.println("总耗时: " + duration + " ms");
        System.out.println("成功请求: " + successCount.get());
        System.out.println("失败请求: " + failCount.get());
        System.out.println("QPS: " + String.format("%.2f", qps));
        System.out.println();
        System.out.println("延迟统计:");
        System.out.println("  平均: " + String.format("%.2f", avgLatency) + " ms");
        System.out.println("  P50:  " + p50 + " ms");
        System.out.println("  P90:  " + p90 + " ms");
        System.out.println("  P95:  " + p95 + " ms");
        System.out.println("  P99:  " + p99 + " ms");
        System.out.println();

        // 验收结果
        boolean passed = p95 <= 10;
        System.out.println("验收结果: " + (passed ? "✅ 通过 (P95 ≤ 10ms)" : "❌ 未通过 (P95 > 10ms)"));
    }

    /**
     * 百万任务压测
     */
    private static void runMillionIntentsBenchmark(String[] args) throws Exception {
        int count = args.length > 1 ? Integer.parseInt(args[1]) : 100000;

        System.out.println("=== 百万 Intent 压测 ===");
        System.out.println("Intent 数量: " + count);
        System.out.println();

        // 健康检查
        if (!healthCheck()) {
            System.err.println("服务不可用，请先启动 Loomq");
            return;
        }

        // 开始创建 Intent
        System.out.println("开始创建 Intent...");
        int batchSize = 100;
        int threads = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(count / batchSize);
        AtomicInteger created = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < count; i += batchSize) {
            final int batchStart = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < batchSize && batchStart + j < count; j++) {
                        createIntent(3600000); // 1 小时后执行
                        int current = created.incrementAndGet();
                        if (current % 10000 == 0) {
                            System.out.println("已创建: " + current + " Intent");
                        }
                    }
                } catch (Exception e) {
                    System.err.println("创建 Intent 失败: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        long duration = System.currentTimeMillis() - startTime;
        double rate = (double) count / duration * 1000;

        System.out.println();
        System.out.println("=== 结果 ===");
        System.out.println("创建 Intent: " + created.get());
        System.out.println("总耗时: " + duration + " ms");
        System.out.println("创建速率: " + String.format("%.2f", rate) + " intents/s");

        // 查询调度器状态
        System.out.println();
        System.out.println("查询调度器状态...");
        String health = httpGet(BASE_URL + "/health");
        System.out.println(health);

        System.out.println();
        System.out.println("验收: 请检查内存使用情况和系统稳定性");
        System.out.println("建议: 使用 jstat -gcutil <pid> 1000 监控 GC 情况");
    }

    /**
     * 恢复时间测试
     */
    private static void runRecoveryBenchmark(String[] args) throws Exception {
        System.out.println("=== 恢复时间测试 ===");
        System.out.println();
        System.out.println("步骤:");
        System.out.println("1. 停止 Loomq 服务");
        System.out.println("2. 按回车键启动服务并计时");
        System.out.println("3. 检查 /health 端点返回 recovery_duration_ms");
        System.out.println();

        System.out.println("按回车键开始...");
        System.in.read();

        System.out.println("请启动服务，然后按回车键检查恢复时间...");
        System.in.read();

        // 检查恢复时间
        String health = httpGet(BASE_URL + "/health");
        System.out.println(health);
        System.out.println();

        // 解析 recovery_duration_ms
        if (health.contains("recovery_duration_ms")) {
            int start = health.indexOf("recovery_duration_ms") + "recovery_duration_ms".length() + 2;
            int end = health.indexOf(",", start);
            if (end == -1) end = health.indexOf("}", start);
            String value = health.substring(start, end);
            long recoveryMs = Long.parseLong(value);

            System.out.println("恢复耗时: " + recoveryMs + " ms");
            boolean passed = recoveryMs <= 60000;
            System.out.println("验收结果: " + (passed ? "✅ 通过 (RTO ≤ 60s)" : "❌ 未通过 (RTO > 60s)"));
        }
    }

    // ========== 辅助方法 ==========

    private static boolean healthCheck() {
        try {
            String response = httpGet(BASE_URL + "/health");
            return response.contains("UP");
        } catch (Exception e) {
            return false;
        }
    }

    private static String createIntent(long delayMs) throws IOException, InterruptedException {
        String body = String.format(
                "{\"bizKey\":\"bench_%d\",\"delayMs\":%d,\"webhookUrl\":\"http://localhost:9999/webhook\"}",
                System.nanoTime(), delayMs
        );

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + "/api/v1/intents"))
                .header("Content-Type", "application/json")
                .header("X-Loomq-Token", TOKEN)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    private static String httpGet(String url) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("X-Loomq-Token", TOKEN)
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }
}

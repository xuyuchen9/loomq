package com.loomq.benchmark;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 风暴测试 v2 - 使用 HttpClient 连接池
 * 测试大规模同时到期任务的系统表现
 */
public class StormTestV2 {

    private static final String BASE_URL = "http://localhost:8080/api/v1";
    private static final String WEBHOOK_URL = "http://localhost:9999/webhook";

    // 使用全局 HttpClient 连接池
    private static final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            .build();

    private static final AtomicInteger taskIdCounter = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: StormTestV2 storm <taskCount> <delaySec>");
            System.out.println("  storm 10000 5  - 1万任务5秒后同时到期");
            return;
        }

        String testType = args[0];

        if ("storm".equals(testType)) {
            int taskCount = Integer.parseInt(args[1]);
            int delaySec = args.length > 2 ? Integer.parseInt(args[2]) : 5;
            runStormTest(taskCount, delaySec);
        }
    }

    private static void runStormTest(int taskCount, int delaySec) throws Exception {
        System.out.println("========================================");
        System.out.printf("风暴测试: %d 任务, %d 秒后同时到期%n", taskCount, delaySec);
        System.out.println("========================================");

        // 预热
        System.out.println("预热服务...");
        for (int i = 0; i < 20; i++) {
            createTask(delaySec * 1000L);
        }
        Thread.sleep(2000);

        // 统计
        long createStartTime = System.currentTimeMillis();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());

        // 使用信号量限制并发
        Semaphore semaphore = new Semaphore(200);
        CountDownLatch latch = new CountDownLatch(taskCount);

        System.out.println("创建任务中...");

        for (int i = 0; i < taskCount; i++) {
            semaphore.acquire();

            final int idx = i;
            Thread.ofVirtual().start(() -> {
                try {
                    long start = System.currentTimeMillis();
                    boolean success = createTask(delaySec * 1000L);
                    long latency = System.currentTimeMillis() - start;

                    if (success) {
                        successCount.incrementAndGet();
                        latencies.add(latency);
                    } else {
                        failCount.incrementAndGet();
                    }

                    if ((idx + 1) % 5000 == 0) {
                        System.out.printf("  已完成: %d/%d%n", idx + 1, taskCount);
                    }
                } catch (Exception e) {
                    failCount.incrementAndGet();
                } finally {
                    semaphore.release();
                    latch.countDown();
                }
            });
        }

        latch.await(5, TimeUnit.MINUTES);
        long createEndTime = System.currentTimeMillis();

        // 统计创建结果
        Collections.sort(latencies);
        long createDuration = createEndTime - createStartTime;
        double qps = (double) successCount.get() / (createDuration / 1000.0);
        long p50 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.5));
        long p95 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.95));
        long p99 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.99));

        System.out.println("\n========== 创建结果 ==========");
        System.out.printf("创建成功: %d / %d%n", successCount.get(), taskCount);
        System.out.printf("创建失败: %d%n", failCount.get());
        System.out.printf("创建耗时: %.2f s%n", createDuration / 1000.0);
        System.out.printf("创建 QPS: %.0f%n", qps);
        System.out.printf("P50 延迟: %d ms%n", p50);
        System.out.printf("P95 延迟: %d ms%n", p95);
        System.out.printf("P99 延迟: %d ms%n", p99);

        // 等待触发
        System.out.printf("%n等待任务触发 (%d 秒)...%n", delaySec + 60);
        Thread.sleep(delaySec * 1000L + 60000);

        // 获取系统指标
        System.out.println("\n========== 触发后系统状态 ==========");
        printMetrics();
    }

    private static boolean createTask(long delayMs) {
        try {
            String taskId = "storm_" + System.nanoTime() + "_" + taskIdCounter.incrementAndGet();
            String body = String.format(
                    "{\"bizKey\":\"%s\",\"delayMs\":%d,\"webhookUrl\":\"%s\"}",
                    taskId, delayMs, WEBHOOK_URL
            );

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(BASE_URL + "/tasks"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .timeout(Duration.ofSeconds(30))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200 || response.statusCode() == 201;
        } catch (Exception e) {
            return false;
        }
    }

    private static void printMetrics() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:8080/metrics"))
                    .GET()
                    .timeout(Duration.ofSeconds(10))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            String[] lines = response.body().split("\n");

            for (String line : lines) {
                if (line.startsWith("loomq_tasks_total ") ||
                    line.startsWith("loomq_tasks_scheduled ") ||
                    line.startsWith("loomq_wake_latency_ms_p95 ") ||
                    line.startsWith("loomq_webhook_latency_ms_p95 ") ||
                    line.startsWith("loomq_total_latency_ms_p95 ") ||
                    line.startsWith("loomq_wal_size_bytes ")) {
                    System.out.println(line);
                }
            }
        } catch (Exception e) {
            System.out.println("获取指标失败: " + e.getMessage());
        }
    }
}

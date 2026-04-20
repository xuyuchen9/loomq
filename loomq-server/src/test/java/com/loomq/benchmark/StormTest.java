package com.loomq.benchmark;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 风暴测试
 * 测试大规模同时到期 Intent 的系统表现
 */
public class StormTest {

    private static final String BASE_URL = "http://localhost:8080/api/v1";
    private static final AtomicInteger intentIdCounter = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: StormTest <testType> <params>");
            System.out.println("  storm <intentCount> <delaySec>  - 同时到期风暴测试");
            System.out.println("  memory <targetIntents>         - 内存极限测试");
            System.out.println("  longrun <hours>              - 长时稳定性测试");
            return;
        }

        String testType = args[0];

        switch (testType) {
            case "storm":
                int intentCount = Integer.parseInt(args[1]);
                int delaySec = args.length > 2 ? Integer.parseInt(args[2]) : 5;
                runStormTest(intentCount, delaySec);
                break;
            case "memory":
                long targetIntents = Long.parseLong(args[1]);
                runMemoryTest(targetIntents);
                break;
            case "longrun":
                int hours = Integer.parseInt(args[1]);
                runLongRunTest(hours);
                break;
            default:
                System.out.println("Unknown test type: " + testType);
        }
    }

    /**
     * 风暴测试：大量 Intent 同时到期
     */
    private static void runStormTest(int intentCount, int delaySec) throws Exception {
        System.out.println("========================================");
        System.out.printf("风暴测试: %d Intent, %d 秒后同时到期%n", intentCount, delaySec);
        System.out.println("========================================");

        // 预热
        System.out.println("预热服务...");
        for (int i = 0; i < 10; i++) {
            createIntentWithTriggerTime(1000);
        }
        Thread.sleep(2000);

        // 记录开始时间
        long createStartTime = System.currentTimeMillis();

        // 创建 Intent - 使用虚拟线程池避免端口耗尽
        int threads = Math.min(intentCount / 100, 200);
        if (threads < 1) threads = 1;

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        AtomicLong totalLatency = new AtomicLong(0);
        List<Long> latencies = new CopyOnWriteArrayList<>();

        CountDownLatch latch = new CountDownLatch(intentCount);

        // 限流器：控制创建速率避免端口耗尽
        Semaphore rateLimiter = new Semaphore(100);

        for (int i = 0; i < intentCount; i++) {
            final int idx = i;
            executor.submit(() -> {
                try {
                    rateLimiter.acquire();  // 限流
                    long start = System.currentTimeMillis();
                    String result = createIntentWithTriggerTime(delaySec * 1000L);
                    long latency = System.currentTimeMillis() - start;

                    if (result != null && !result.contains("error")) {
                        successCount.incrementAndGet();
                        totalLatency.addAndGet(latency);
                        latencies.add(latency);
                    } else {
                        failCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    failCount.incrementAndGet();
                } finally {
                    rateLimiter.release();  // 释放
                    latch.countDown();
                }
            });
        }

        // 等待创建完成
        latch.await();
        long createEndTime = System.currentTimeMillis();
        executor.shutdown();

        // 统计创建性能
        long createDuration = createEndTime - createStartTime;
        double createQps = (double) successCount.get() / (createDuration / 1000.0);
        double avgLatency = successCount.get() > 0 ? (double) totalLatency.get() / successCount.get() : 0;

        // 计算 P95
        latencies.sort(Long::compare);
        long p50 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.5));
        long p95 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.95));
        long p99 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.99));

        System.out.println("\n========== 创建结果 ==========");
        System.out.printf("创建成功: %d / %d%n", successCount.get(), intentCount);
        System.out.printf("创建失败: %d%n", failCount.get());
        System.out.printf("创建耗时: %.2f s%n", createDuration / 1000.0);
        System.out.printf("创建 QPS: %.0f%n", createQps);
        System.out.printf("平均延迟: %.1f ms%n", avgLatency);
        System.out.printf("P50 延迟: %d ms%n", p50);
        System.out.printf("P95 延迟: %d ms%n", p95);
        System.out.printf("P99 延迟: %d ms%n", p99);

        // 等待 Intent 触发
        System.out.printf("%n等待 Intent 触发 (%d 秒)...%n", delaySec + 30);
        Thread.sleep(delaySec * 1000L + 30000);

        // 获取系统指标
        System.out.println("\n========== 触发后系统状态 ==========");
        printMetrics();
    }

    /**
     * 内存极限测试：持续创建 Intent 直到系统崩溃
     */
    private static void runMemoryTest(long targetIntents) throws Exception {
        System.out.println("========================================");
        System.out.printf("内存极限测试: 目标 %d Intent%n", targetIntents);
        System.out.println("========================================");

        int batchSize = 10000;
        int threads = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        AtomicInteger totalCreated = new AtomicInteger(0);
        AtomicInteger totalFailed = new AtomicInteger(0);
        AtomicLong totalLatency = new AtomicLong(0);
        List<Long> latencies = new CopyOnWriteArrayList<>();

        long startTime = System.currentTimeMillis();
        long lastReportTime = startTime;

        while (totalCreated.get() < targetIntents) {
            int remaining = (int) (targetIntents - totalCreated.get());
            int currentBatch = Math.min(batchSize, remaining);

            CountDownLatch latch = new CountDownLatch(currentBatch);

            for (int i = 0; i < currentBatch; i++) {
                executor.submit(() -> {
                    try {
                        long start = System.currentTimeMillis();
                        String result = createIntent(3600000); // 1小时后触发
                        long latency = System.currentTimeMillis() - start;

                        if (result != null && !result.contains("error")) {
                            totalCreated.incrementAndGet();
                            totalLatency.addAndGet(latency);
                            latencies.add(latency);
                        } else {
                            totalFailed.incrementAndGet();
                        }
                    } catch (Exception e) {
                        totalFailed.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();

            // 每 10 万报告一次
            if (System.currentTimeMillis() - lastReportTime > 10000) {
                long elapsed = System.currentTimeMillis() - startTime;
                double qps = (double) totalCreated.get() / (elapsed / 1000.0);

                // 计算 P95
                List<Long> recentLatencies = new ArrayList<>(latencies);
                recentLatencies.sort(Long::compare);
                long p95 = recentLatencies.isEmpty() ? 0 :
                    recentLatencies.get((int) (recentLatencies.size() * 0.95));

                System.out.printf("已创建: %,d | QPS: %.0f | P95: %d ms | 失败: %d%n",
                    totalCreated.get(), qps, p95, totalFailed.get());

                // 打印系统指标
                printMetrics();

                lastReportTime = System.currentTimeMillis();

                // 检查是否需要停止
                if (totalFailed.get() > 1000) {
                    System.out.println("失败过多，停止测试");
                    break;
                }
            }
        }

        executor.shutdown();

        long endTime = System.currentTimeMillis();
        double totalQps = (double) totalCreated.get() / ((endTime - startTime) / 1000.0);

        System.out.println("\n========== 最终结果 ==========");
        System.out.printf("总创建: %,d%n", totalCreated.get());
        System.out.printf("总失败: %,d%n", totalFailed.get());
        System.out.printf("总耗时: %.1f 秒%n", (endTime - startTime) / 1000.0);
        System.out.printf("平均 QPS: %.0f%n", totalQps);

        printMetrics();
    }

    /**
     * 长时稳定性测试
     */
    private static void runLongRunTest(int hours) throws Exception {
        System.out.println("========================================");
        System.out.printf("长时稳定性测试: %d 小时%n", hours);
        System.out.println("========================================");

        long endTime = System.currentTimeMillis() + hours * 3600L * 1000;
        AtomicInteger totalCreated = new AtomicInteger(0);
        AtomicInteger totalSuccess = new AtomicInteger(0);

        int batchInterval = 60000; // 每分钟创建一批
        int batchSize = 1000;

        while (System.currentTimeMillis() < endTime) {
            long batchStart = System.currentTimeMillis();

            // 创建一批 Intent
            for (int i = 0; i < batchSize; i++) {
                try {
                    // 随机延时 1-10 分钟
                    int delayMs = 60000 + (int) (Math.random() * 540000);
                    String result = createIntent(delayMs);
                    if (result != null && !result.contains("error")) {
                        totalCreated.incrementAndGet();
                    }
                } catch (Exception e) {
                    // 忽略
                }
            }

            // 每 10 分钟报告一次
            if (totalCreated.get() % 10000 == 0) {
                System.out.printf("[%s] 已创建: %,d | 已完成: %,d%n",
                    new java.text.SimpleDateFormat("HH:mm:ss").format(new java.util.Date()),
                    totalCreated.get(), totalSuccess.get());
                printMetrics();
            }

            // 等待下一批
            long elapsed = System.currentTimeMillis() - batchStart;
            if (elapsed < batchInterval) {
                Thread.sleep(batchInterval - elapsed);
            }
        }

        System.out.println("\n========== 长时测试完成 ==========");
        System.out.printf("总创建: %,d%n", totalCreated.get());
        printMetrics();
    }

    private static String createIntent(long delayMs) throws Exception {
        return createIntentWithTriggerTime(delayMs);
    }

    private static String createIntentWithTriggerTime(long delayMs) throws Exception {
        String intentId = "storm_" + System.nanoTime() + "_" + intentIdCounter.incrementAndGet();
        String body = String.format(
            "{\"bizKey\":\"%s\",\"delayMs\":%d,\"webhookUrl\":\"http://localhost:9999/webhook\"}",
            intentId, delayMs
        );

        URL url = new URL(BASE_URL + "/tasks");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(10000);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes());
        }

        int code = conn.getResponseCode();
        conn.disconnect();

        return (code == 200 || code == 201) ? "success" : "error:" + code;
    }

    private static void printMetrics() {
        try {
            URL url = new URL(BASE_URL.replace("/api/v1", "") + "/metrics");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);

            if (conn.getResponseCode() == 200) {
                java.io.BufferedReader reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(conn.getInputStream())
                );
                String line;
                StringBuilder sb = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("loomq_tasks_total") ||
                        line.startsWith("loomq_tasks_pending") ||
                        line.startsWith("loomq_tasks_scheduled") ||
                        line.startsWith("loomq_wake_latency") ||
                        line.startsWith("loomq_webhook_latency") ||
                        line.startsWith("loomq_total_latency") ||
                        line.startsWith("loomq_wal_size")) {
                        sb.append(line).append("\n");
                    }
                }
                reader.close();
                System.out.println("指标:\n" + sb);
            }
            conn.disconnect();
        } catch (Exception e) {
            System.out.println("获取指标失败: " + e.getMessage());
        }
    }
}

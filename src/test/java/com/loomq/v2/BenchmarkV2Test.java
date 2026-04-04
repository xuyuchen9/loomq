package com.loomq.v2;

import com.loomq.entity.Task;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Loomq V0.3 第一性原理性能测试
 *
 * 设计原则：
 * 1. 最小化测量开销 - 只用原子计数器，不用同步集合
 * 2. JVM预热 - 消除JIT编译影响
 * 3. 多轮测试 - 取稳定值
 * 4. 渐进加压 - 找到崩溃点
 * 5. 分离关注点 - 吞吐量、延迟、容量分开测
 */
class BenchmarkV2Test {

    @TempDir
    Path tempDir;

    private LoomqEngineV2 engine;

    @BeforeEach
    void setUp() throws Exception {
        LoomqConfigV2 config = new LoomqConfigV2(
                100, 500_000, 0, 50_000, 1_000_000, 10, 5000, 100_000, 600_000,
                tempDir.resolve("wal").toString(),
                "async",
                LoomqConfigV2.AckLevel.ASYNC
        );
        engine = new LoomqEngineV2(config);
        engine.start();
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    @Test
    void runFirstPrinciplesBenchmark() throws Exception {
        printHeader();

        // 1. 预热 - 让JIT编译完成
        warmup();

        // 2. 吞吐量测试 - 找峰值
        testThroughput();

        // 3. 容量测试 - 找极限
        testCapacity();

        // 4. 延迟测试 - 找P99爆炸点
        testLatency();

        printFooter();
    }

    // ==================== 预热 ====================

    private void warmup() throws Exception {
        System.out.println("\n[预热] 50,000 任务...");
        long start = System.nanoTime();

        int tasks = 50_000;
        int threads = 50;
        CountDownLatch latch = new CountDownLatch(threads);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        AtomicInteger count = new AtomicInteger(0);

        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < tasks / threads; i++) {
                        Task task = Task.builder()
                                .taskId("warmup-" + count.incrementAndGet())
                                .bizKey("warmup")
                                .webhookUrl("http://localhost:9999/webhook")
                                .triggerTime(System.currentTimeMillis() + 3600000)
                                .build();
                        engine.createTask(task);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(2, TimeUnit.MINUTES);
        executor.shutdown();

        long ms = (System.nanoTime() - start) / 1_000_000;
        System.out.printf("[预热] 完成: %d ms, %d QPS\n", ms, tasks * 1000L / ms);
        Thread.sleep(1000); // 等待WAL刷盘
    }

    // ==================== 吞吐量测试 ====================

    private void testThroughput() throws Exception {
        System.out.println("\n════════════════════════════════════════════════════════════════════════════════════");
        System.out.println("测试1: 吞吐量极限 (找到峰值QPS)");
        System.out.println("════════════════════════════════════════════════════════════════════════════════════");

        // 测试不同规模，找到峰值
        int[] scales = {100_000, 200_000, 500_000, 1_000_000, 2_000_000};

        System.out.println("┌──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐");
        System.out.println("│    任务数    │   耗时(ms)   │     QPS      │   成功率     │     评价     │");
        System.out.println("├──────────────┼──────────────┼──────────────┼──────────────┼──────────────┤");

        long peakQps = 0;
        int peakScale = 0;

        for (int scale : scales) {
            Result r = runThroughputTest(scale);
            String eval = r.successRate >= 0.9999 ? "✅ 优秀" :
                          r.successRate >= 0.999 ? "✅ 正常" :
                          r.successRate >= 0.99 ? "⚠️ 轻微" : "❌ 异常";

            System.out.printf("│ %,12d │ %,12d │ %,12d │ %,11.4f%% │ %12s │\n",
                    scale, r.durationMs, r.qps, r.successRate * 100, eval);

            if (r.qps > peakQps && r.successRate >= 0.999) {
                peakQps = r.qps;
                peakScale = scale;
            }

            // 如果成功率下降明显，停止
            if (r.successRate < 0.99) break;

            // 重启引擎清理状态
            restartEngine("tp-" + scale);
        }

        System.out.println("└──────────────┴──────────────┴──────────────┴──────────────┴──────────────┘");
        System.out.printf("\n📊 吞吐量峰值: %,d QPS (%,d 任务)\n", peakQps, peakScale);
    }

    private Result runThroughputTest(int taskCount) {
        int threads = Math.min(500, Math.max(100, taskCount / 5000));
        int tasksPerThread = taskCount / threads;

        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger fail = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threads);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        AtomicLong counter = new AtomicLong(0);

        long start = System.nanoTime();

        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < tasksPerThread; i++) {
                        try {
                            Task task = Task.builder()
                                    .taskId("tp-" + counter.incrementAndGet())
                                    .bizKey("throughput")
                                    .webhookUrl("http://localhost:9999/webhook")
                                    .triggerTime(System.currentTimeMillis() + 3600000)
                                    .build();
                            if (engine.createTask(task).ok()) {
                                success.incrementAndGet();
                            } else {
                                fail.incrementAndGet();
                            }
                        } catch (Exception e) {
                            fail.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executor.shutdown();

        long durationMs = (System.nanoTime() - start) / 1_000_000;
        int total = threads * tasksPerThread;
        long qps = durationMs > 0 ? success.get() * 1000L / durationMs : 0;
        double successRate = total > 0 ? (double) success.get() / total : 0;

        return new Result(success.get(), fail.get(), durationMs, qps, successRate);
    }

    // ==================== 容量测试 ====================

    private void testCapacity() throws Exception {
        System.out.println("\n════════════════════════════════════════════════════════════════════════════════════");
        System.out.println("测试2: 容量极限 (找到OOM点)");
        System.out.println("════════════════════════════════════════════════════════════════════════════════════");

        int[] scales = {500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000};

        System.out.println("┌──────────────┬──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐");
        System.out.println("│    任务数    │   耗时(ms)   │     QPS      │   内存(MB)   │ 每任务(bytes)│     状态     │");
        System.out.println("├──────────────┼──────────────┼──────────────┼──────────────┼──────────────┼──────────────┤");

        long maxStable = 0;

        for (int scale : scales) {
            long beforeMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

            Result r = runThroughputTest(scale);
            boolean oom = false;

            try {
                long afterMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
                long usedMem = afterMem - beforeMem;
                long bytesPerTask = r.success > 0 ? usedMem / r.success : 0;
                long memMB = usedMem / 1024 / 1024;

                String status = r.successRate >= 0.999 ? "✅ 正常" :
                                r.successRate >= 0.99 ? "⚠️ 退化" :
                                r.successRate >= 0.90 ? "⚠️ 严重" : "❌ 极限";

                System.out.printf("│ %,12d │ %,12d │ %,12d │ %,12d │ %,12d │ %12s │\n",
                        scale, r.durationMs, r.qps, memMB, bytesPerTask, status);

                if (r.successRate >= 0.99) {
                    maxStable = scale;
                }
            } catch (OutOfMemoryError e) {
                oom = true;
                System.out.printf("│ %,12d │         OOM │            │            │            │     ❌ OOM    │\n", scale);
            }

            if (oom || r.successRate < 0.90) break;

            restartEngine("cap-" + scale);
        }

        System.out.println("└──────────────┴──────────────┴──────────────┴──────────────┴──────────────┴──────────────┘");
        System.out.printf("\n📊 稳定容量: %,d 任务\n", maxStable);
    }

    // ==================== 延迟测试 ====================

    private void testLatency() throws Exception {
        System.out.println("\n════════════════════════════════════════════════════════════════════════════════════");
        System.out.println("测试3: 延迟分布 (找到P99爆炸点)");
        System.out.println("════════════════════════════════════════════════════════════════════════════════════");

        // 使用采样方式测量延迟，避免同步开销影响主路径
        int[] concurrencies = {100, 200, 500, 1000, 2000, 5000};
        int samples = 10_000; // 采样数量

        System.out.println("┌────────────┬──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐");
        System.out.println("│  并发数    │     QPS      │   P50(ms)    │   P95(ms)    │   P99(ms)    │     状态     │");
        System.out.println("├────────────┼──────────────┼──────────────┼──────────────┼──────────────┼──────────────┤");

        for (int concurrency : concurrencies) {
            LatencyResult r = runLatencyTest(concurrency, samples);

            String status = r.p99 <= 10 ? "✅ 优秀" :
                            r.p99 <= 50 ? "✅ 正常" :
                            r.p99 <= 100 ? "⚠️ 上升" :
                            r.p99 <= 500 ? "⚠️ 明显" : "❌ 爆炸";

            System.out.printf("│ %,10d │ %,12d │ %,12.1f │ %,12.1f │ %,12.1f │ %12s │\n",
                    concurrency, r.qps, r.p50, r.p95, r.p99, status);

            if (r.p99 > 500) break;

            restartEngine("lat-" + concurrency);
        }

        System.out.println("└────────────┴──────────────┴──────────────┴──────────────┴──────────────┴──────────────┘");
    }

    private LatencyResult runLatencyTest(int concurrency, int samples) {
        // 使用 LongAdder 统计，无锁
        int tasksPerThread = samples / concurrency;

        // 延迟采样：只采样部分请求，减少开销
        int sampleInterval = Math.max(1, tasksPerThread / 100); // 每100个采样1个
        long[] latencies = new long[samples / sampleInterval];
        int[] latencyIndex = {0};

        AtomicInteger success = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(concurrency);
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        AtomicLong counter = new AtomicLong(0);

        long start = System.nanoTime();

        for (int t = 0; t < concurrency; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < tasksPerThread; i++) {
                        long reqStart = System.nanoTime();

                        Task task = Task.builder()
                                .taskId("lat-" + counter.incrementAndGet())
                                .bizKey("latency")
                                .webhookUrl("http://localhost:9999/webhook")
                                .triggerTime(System.currentTimeMillis() + 3600000)
                                .build();

                        if (engine.createTask(task).ok()) {
                            success.incrementAndGet();

                            // 采样延迟
                            if (i % sampleInterval == 0) {
                                long latency = System.nanoTime() - reqStart;
                                int idx;
                                synchronized (latencies) {
                                    idx = latencyIndex[0]++;
                                    if (idx < latencies.length) {
                                        latencies[idx] = latency / 1_000_000; // ms
                                    }
                                }
                            }
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executor.shutdown();

        long durationMs = (System.nanoTime() - start) / 1_000_000;
        long qps = durationMs > 0 ? success.get() * 1000L / durationMs : 0;

        // 计算延迟百分位
        int validSamples = Math.min(latencyIndex[0], latencies.length);
        if (validSamples == 0) {
            return new LatencyResult(qps, 0, 0, 0);
        }

        java.util.Arrays.sort(latencies, 0, validSamples);
        double p50 = latencies[(int)(validSamples * 0.50)];
        double p95 = latencies[(int)(validSamples * 0.95)];
        double p99 = latencies[(int)(validSamples * 0.99)];

        return new LatencyResult(qps, p50, p95, p99);
    }

    // ==================== 辅助方法 ====================

    private void restartEngine(String suffix) throws Exception {
        engine.stop();
        Thread.sleep(1000);

        LoomqConfigV2 config = new LoomqConfigV2(
                100, 500_000, 0, 50_000, 1_000_000, 10, 5000, 100_000, 600_000,
                tempDir.resolve("wal-" + suffix).toString(),
                "async",
                LoomqConfigV2.AckLevel.ASYNC
        );
        engine = new LoomqEngineV2(config);
        engine.start();
    }

    private void printHeader() {
        System.out.println();
        System.out.println("╔════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                     Loomq V0.3 第一性原理性能测试                                                        ║");
        System.out.println("╚════════════════════════════════════════════════════════════════════════════════════════════════════════════╝");
        System.out.println();

        Runtime r = Runtime.getRuntime();
        System.out.println("测试环境:");
        System.out.printf("  CPU 核心: %d%n", r.availableProcessors());
        System.out.printf("  最大内存: %d MB%n", r.maxMemory() / 1024 / 1024);
        System.out.printf("  Java 版本: %s%n", System.getProperty("java.version"));
        System.out.println();
        System.out.println("测试原则:");
        System.out.println("  1. 最小化测量开销 - 只用原子计数器");
        System.out.println("  2. JVM预热 - 消除JIT编译影响");
        System.out.println("  3. 渐进加压 - 找到崩溃点");
        System.out.println("  4. 采样延迟 - 减少同步开销");
    }

    private void printFooter() {
        System.out.println();
        System.out.println("════════════════════════════════════════════════════════════════════════════════════");
        System.out.println("测试完成");
        System.out.println("════════════════════════════════════════════════════════════════════════════════════");
    }

    record Result(long success, long fail, long durationMs, long qps, double successRate) {}
    record LatencyResult(long qps, double p50, double p95, double p99) {}
}

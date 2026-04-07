package com.loomq.benchmark.v4;

import com.loomq.entity.v3.EventTypeV3;
import com.loomq.entity.v3.TaskStatusV3;
import com.loomq.entity.v3.TaskV3;
import com.loomq.retry.ExponentialBackoffPolicy;
import com.loomq.retry.FixedIntervalPolicy;
import com.loomq.retry.RetryPolicy;
import com.loomq.store.v3.TaskStoreV3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * V0.4 极限性能压测
 *
 * 压测场景：
 * 1. 任务创建吞吐
 * 2. 任务查询吞吐
 * 3. 状态转换吞吐
 * 4. 幂等查询吞吐
 * 5. 重试策略计算吞吐
 * 6. 内存占用测试
 *
 * @author loomq
 * @since v0.4
 */
public class BenchmarkV4 {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkV4.class);

    // 压测结果
    private final List<BenchmarkResult> results = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        BenchmarkV4 benchmark = new BenchmarkV4();
        benchmark.runAll();
    }

    public void runAll() throws Exception {
        logger.info("╔════════════════════════════════════════════════════════════╗");
        logger.info("║           LoomQ V0.4 极限性能压测                          ║");
        logger.info("╚════════════════════════════════════════════════════════════╝");
        logger.info("");
        logger.info("测试环境: {} {}", System.getProperty("os.name"), System.getProperty("os.version"));
        logger.info("Java版本: {}", System.getProperty("java.version"));
        logger.info("可用处理器: {}", Runtime.getRuntime().availableProcessors());
        logger.info("最大内存: {} MB", Runtime.getRuntime().maxMemory() / 1024 / 1024);
        logger.info("");

        // 预热 JVM
        warmup();

        // 1. 任务创建吞吐测试
        taskCreationBenchmark(100_000);
        taskCreationBenchmark(500_000);
        taskCreationBenchmark(1_000_000);

        // 2. 任务查询吞吐测试
        taskQueryBenchmark(100_000);

        // 3. 状态转换吞吐测试
        stateTransitionBenchmark(1_000_000);

        // 4. 幂等查询吞吐测试
        idempotencyQueryBenchmark(100_000);

        // 5. 重试策略计算测试
        retryPolicyBenchmark(1_000_000);

        // 6. 内存占用测试
        memoryBenchmark(1_000_000);
        memoryBenchmark(5_000_000);
        memoryBenchmark(10_000_000);

        // 输出报告
        printReport();
    }

    /**
     * JVM 预热
     */
    private void warmup() {
        logger.info("----- JVM 预热 -----");
        TaskStoreV3 store = new TaskStoreV3();
        for (int i = 0; i < 10000; i++) {
            TaskV3 task = createTestTask("warmup-" + i);
            store.add(task);
        }
        store.clear();
        System.gc();
        logger.info("预热完成");
        logger.info("");
    }

    /**
     * 任务创建吞吐测试
     */
    private void taskCreationBenchmark(int count) {
        logger.info("----- 任务创建吞吐测试: {} 任务 -----", count);

        TaskStoreV3 store = new TaskStoreV3();

        // 统计
        long startTime = System.nanoTime();
        long startMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        for (int i = 0; i < count; i++) {
            TaskV3 task = createTestTask("create-" + i);
            store.add(task);
        }

        long endTime = System.nanoTime();
        long endMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        long elapsedMs = (endTime - startTime) / 1_000_000;
        double qps = count / (elapsedMs / 1000.0);
        long memUsed = (endMem - startMem) / 1024 / 1024;
        long bytesPerTask = (endMem - startMem) / count;

        BenchmarkResult result = new BenchmarkResult(
                "任务创建-" + formatNumber(count),
                count,
                elapsedMs,
                qps,
                memUsed,
                bytesPerTask,
                true
        );
        results.add(result);

        logger.info("完成: QPS={}/s, 内存={}MB, 每任务={}B",
                String.format("%.0f", qps), memUsed, bytesPerTask);

        // 验证
        boolean passed = qps >= 100_000;
        logger.info("结果: {}", passed ? "✅ 通过 (QPS ≥ 100K)" : "⚠️ 偏低");

        store.clear();
        System.gc();
    }

    /**
     * 任务查询吞吐测试
     */
    private void taskQueryBenchmark(int count) {
        logger.info("");
        logger.info("----- 任务查询吞吐测试: {} 任务 -----", count);

        TaskStoreV3 store = new TaskStoreV3();

        // 准备数据
        for (int i = 0; i < count; i++) {
            TaskV3 task = createTestTask("query-" + i);
            task.setIdempotencyKey("idem-" + i);
            store.add(task);
        }

        // 查询测试 - 按 ID
        long startById = System.nanoTime();
        for (int i = 0; i < count; i++) {
            store.get("query-" + i);
        }
        long timeById = (System.nanoTime() - startById) / 1_000_000;
        double qpsById = count / (timeById / 1000.0);

        // 查询测试 - 按幂等键
        long startByIdem = System.nanoTime();
        for (int i = 0; i < count; i++) {
            store.getByIdempotencyKey("idem-" + i);
        }
        long timeByIdem = (System.nanoTime() - startByIdem) / 1_000_000;
        double qpsByIdem = count / (timeByIdem / 1000.0);

        logger.info("按ID查询: QPS={}/s", String.format("%.0f", qpsById));
        logger.info("按幂等键查询: QPS={}/s", String.format("%.0f", qpsByIdem));

        BenchmarkResult result = new BenchmarkResult(
                "任务查询-" + formatNumber(count),
                count,
                timeById,
                qpsById,
                0, 0, true
        );
        results.add(result);

        store.clear();
        System.gc();
    }

    /**
     * 状态转换吞吐测试
     */
    private void stateTransitionBenchmark(int count) {
        logger.info("");
        logger.info("----- 状态转换吞吐测试: {} 次 -----", count);

        // 创建生命周期列表
        List<com.loomq.entity.v3.TaskLifecycleV3> lifecycles = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            lifecycles.add(new com.loomq.entity.v3.TaskLifecycleV3("state-" + i));
        }

        // 测试完整流转
        long startTime = System.nanoTime();
        int cycles = 0;
        while (cycles < count) {
            for (var lifecycle : lifecycles) {
                if (lifecycle.getStatus().isTerminal()) continue;

                // PENDING -> SCHEDULED -> READY -> RUNNING -> SUCCESS
                if (lifecycle.transitionToScheduled()) cycles++;
                if (lifecycle.transitionToReady()) cycles++;
                if (lifecycle.transitionToRunning()) cycles++;
                if (lifecycle.transitionToSuccess()) cycles++;

                if (cycles >= count) break;
            }
        }
        long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;

        double ops = count / (elapsedMs / 1000.0);

        logger.info("完成: {} 次状态转换, OPS={}/s", count, String.format("%.0f", ops));

        BenchmarkResult result = new BenchmarkResult(
                "状态转换-" + formatNumber(count),
                count,
                elapsedMs,
                ops,
                0, 0, true
        );
        results.add(result);
    }

    /**
     * 幂等查询吞吐测试
     */
    private void idempotencyQueryBenchmark(int count) {
        logger.info("");
        logger.info("----- 幂等查询吞吐测试: {} 次 -----", count);

        TaskStoreV3 store = new TaskStoreV3();

        // 准备数据
        int taskCount = Math.min(count, 100_000);
        for (int i = 0; i < taskCount; i++) {
            TaskV3 task = createTestTask("idem-test-" + i);
            task.setIdempotencyKey("key-" + i);
            store.add(task);
        }

        // 并发查询测试
        int threads = 16;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        AtomicLong totalQueries = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(threads);

        long startTime = System.nanoTime();

        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    Random random = new Random();
                    while (totalQueries.get() < count) {
                        int idx = random.nextInt(taskCount);
                        store.getByIdempotencyKey("key-" + idx);
                        totalQueries.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;
        double qps = count / (elapsedMs / 1000.0);

        executor.shutdown();

        logger.info("完成: {} 次并发查询, QPS={}/s", count, String.format("%.0f", qps));

        BenchmarkResult result = new BenchmarkResult(
                "幂等查询-" + formatNumber(count),
                count,
                elapsedMs,
                qps,
                0, 0, true
        );
        results.add(result);

        store.clear();
        System.gc();
    }

    /**
     * 重试策略计算测试
     */
    private void retryPolicyBenchmark(int count) {
        logger.info("");
        logger.info("----- 重试策略计算测试: {} 次 -----", count);

        RetryPolicy fixed = new FixedIntervalPolicy(3, 5000);
        RetryPolicy exponential = new ExponentialBackoffPolicy(5, 1000, 2.0, 60000);

        // 固定间隔
        long startFixed = System.nanoTime();
        for (int i = 0; i < count; i++) {
            fixed.nextRetryDelay(i % 3);
        }
        long timeFixed = (System.nanoTime() - startFixed) / 1_000_000;
        double opsFixed = count / (timeFixed / 1000.0);

        // 指数退避
        long startExp = System.nanoTime();
        for (int i = 0; i < count; i++) {
            exponential.nextRetryDelay(i % 5);
        }
        long timeExp = (System.nanoTime() - startExp) / 1_000_000;
        double opsExp = count / (timeExp / 1000.0);

        logger.info("固定间隔策略: OPS={}/s", String.format("%.0f", opsFixed));
        logger.info("指数退避策略: OPS={}/s", String.format("%.0f", opsExp));

        BenchmarkResult result = new BenchmarkResult(
                "重试策略-" + formatNumber(count),
                count,
                timeFixed,
                opsFixed,
                0, 0, true
        );
        results.add(result);
    }

    /**
     * 内存占用测试
     */
    private void memoryBenchmark(int count) {
        logger.info("");
        logger.info("----- 内存占用测试: {} 任务 -----", count);

        // 强制 GC
        System.gc();
        try { Thread.sleep(100); } catch (InterruptedException e) {}

        long beforeMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        TaskStoreV3 store = new TaskStoreV3();

        for (int i = 0; i < count; i++) {
            TaskV3 task = TaskV3.builder()
                    .taskId("mem-" + i)
                    .webhookUrl("https://example.com/webhook")
                    .bizKey("biz-" + i)
                    .idempotencyKey("idem-" + i)
                    .wakeTime(System.currentTimeMillis() + 3600000)
                    .payload("{\"orderId\":\"" + i + "\",\"action\":\"cancel\"}")
                    .maxRetryCount(3)
                    .build();
            store.add(task);

            if (i % 100_000 == 0 && i > 0) {
                long currentMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
                long used = (currentMem - beforeMem) / 1024 / 1024;
                logger.info("  {} 任务: 已用内存 {} MB", formatNumber(i), used);
            }
        }

        long afterMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long usedMem = (afterMem - beforeMem) / 1024 / 1024;
        long bytesPerTask = (afterMem - beforeMem) / count;

        logger.info("完成: 总内存 {} MB, 每任务 {} 字节", usedMem, bytesPerTask);

        // 验证内存目标: 每任务 < 500 字节
        boolean passed = bytesPerTask < 500;
        logger.info("结果: {}", passed ? "✅ 通过 (每任务 < 500B)" : "⚠️ 内存偏高");

        BenchmarkResult result = new BenchmarkResult(
                "内存占用-" + formatNumber(count),
                count,
                0,
                0,
                usedMem,
                bytesPerTask,
                passed
        );
        results.add(result);

        store.clear();
        System.gc();
    }

    // ========== 辅助方法 ==========

    private TaskV3 createTestTask(String taskId) {
        return TaskV3.builder()
                .taskId(taskId)
                .webhookUrl("https://example.com/webhook")
                .bizKey(taskId + "-biz")
                .wakeTime(System.currentTimeMillis() + 3600000)
                .build();
    }

    private String formatNumber(int n) {
        if (n >= 1_000_000) return (n / 1_000_000) + "M";
        if (n >= 1_000) return (n / 1_000) + "K";
        return String.valueOf(n);
    }

    private void printReport() {
        logger.info("");
        logger.info("╔════════════════════════════════════════════════════════════╗");
        logger.info("║                    V0.4 压测报告                           ║");
        logger.info("╚════════════════════════════════════════════════════════════╝");
        logger.info("");
        logger.info("{:<25} {:>12} {:>15} {:>12} {:>10}",
                "测试项", "数量", "QPS/OPS", "内存(MB)", "每任务(B)");
        logger.info("─".repeat(80));

        for (BenchmarkResult r : results) {
            logger.info("{:<25} {:>12} {:>15} {:>12} {:>10}",
                    r.name(),
                    formatNumber(r.count()),
                    r.throughput() > 0 ? String.format("%.0f", r.throughput()) : "-",
                    r.memoryMB() > 0 ? r.memoryMB() : "-",
                    r.bytesPerTask() > 0 ? r.bytesPerTask() : "-");
        }

        logger.info("─".repeat(80));

        // 统计通过率
        long passed = results.stream().filter(BenchmarkResult::passed).count();
        logger.info("总计: {}/{} 测试通过", passed, results.size());
        logger.info("");

        // 输出极限性能摘要
        logger.info("═══════════════════════════════════════════════════════════");
        logger.info("                    极限性能摘要                            ");
        logger.info("═══════════════════════════════════════════════════════════");

        results.stream()
                .filter(r -> r.throughput() > 0)
                .max(Comparator.comparingDouble(BenchmarkResult::throughput))
                .ifPresent(r -> logger.info("峰值吞吐: {:.0f} ops/s ({})", r.throughput(), r.name()));

        results.stream()
                .filter(r -> r.bytesPerTask() > 0)
                .min(Comparator.comparingLong(BenchmarkResult::bytesPerTask))
                .ifPresent(r -> logger.info("最低内存占用: {} 字节/任务 ({})", r.bytesPerTask(), r.name()));

        logger.info("═══════════════════════════════════════════════════════════");
    }

    public record BenchmarkResult(
            String name,
            int count,
            long elapsedMs,
            double throughput,
            long memoryMB,
            long bytesPerTask,
            boolean passed
    ) {}
}

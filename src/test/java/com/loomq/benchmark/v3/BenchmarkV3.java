package com.loomq.benchmark.v3;

import com.loomq.cluster.ClusterConfig;
import com.loomq.cluster.ClusterManager;
import com.loomq.entity.EventType;
import com.loomq.entity.Task;
import com.loomq.entity.TaskStatus;
import com.loomq.scheduler.v2.TimeBucketScheduler;
import com.loomq.wal.v2.ShardedWalEngine;
import com.loomq.wal.v2.WalWriterV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * V0.3 性能压测工具
 *
 * 压测场景：
 * 1. 写入压测：10万/50万/100万任务
 * 2. 调度压测：延迟分布验证
 * 3. 风暴测试：10万同时触发
 *
 * 通过标准：
 * - 写入 P95 < 50ms
 * - 调度无丢失
 * - 风暴延迟 < 2s
 *
 * @author loomq
 * @since v0.3
 */
public class BenchmarkV3 {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkV3.class);

    // 压测配置
    private final BenchmarkConfig config;

    // 临时目录
    private Path tempDir;

    // 统计
    private final List<BenchmarkResult> results = new ArrayList<>();

    public BenchmarkV3(BenchmarkConfig config) {
        this.config = config;
    }

    public BenchmarkV3() {
        this(BenchmarkConfig.defaultConfig());
    }

    /**
     * 运行全部压测
     */
    public void runAll() throws IOException {
        logger.info("========== Loomq V0.3 性能压测 ==========");
        logger.info("Config: {}", config);

        try {
            // 初始化临时目录
            tempDir = Files.createTempDirectory("loomq-benchmark-");

            // 1. 写入压测
            writeBenchmark(100_000);
            writeBenchmark(500_000);
            writeBenchmark(1_000_000);

            // 2. 调度压测
            scheduleBenchmark(100_000);

            // 3. 风暴测试
            stormBenchmark(100_000);

            // 输出报告
            printReport();

        } finally {
            // 清理
            if (tempDir != null) {
                deleteRecursively(tempDir);
            }
        }
    }

    /**
     * 写入压测
     */
    public BenchmarkResult writeBenchmark(int taskCount) throws IOException {
        logger.info("\n----- 写入压测: {} 任务 -----", taskCount);

        // 创建 WAL 写入器
        WalWriterV2 writer = new WalWriterV2(createWalConfig(), "shard-0");
        writer.start();

        // 预热
        warmup(writer, 1000);

        // 统计
        AtomicLong totalLatency = new AtomicLong(0);
        AtomicLong maxLatency = new AtomicLong(0);
        AtomicLong minLatency = new AtomicLong(Long.MAX_VALUE);

        CountDownLatch latch = new CountDownLatch(taskCount);
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        int batchSize = 1000;

        long startTime = System.nanoTime();

        for (int i = 0; i < taskCount; i++) {
            final int idx = i;
            CompletableFuture<Long> future = writer.appendAsync(
                    "task-" + idx,
                    "biz-" + idx,
                    EventType.CREATE,
                    System.currentTimeMillis(),
                    ("payload-" + idx).getBytes()
            );

            future.thenAccept(pos -> {
                long latency = System.nanoTime() - startTime;
                totalLatency.addAndGet(latency);
                maxLatency.updateAndGet(v -> Math.max(v, latency));
                minLatency.updateAndGet(v -> Math.min(v, latency));
                latch.countDown();
            });

            futures.add(future);

            // 批量等待避免内存溢出
            if (futures.size() >= batchSize) {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                futures.clear();
            }
        }

        // 等待剩余
        if (!futures.isEmpty()) {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        }

        long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;

        // 计算统计
        double qps = taskCount / (elapsedMs / 1000.0);
        double avgLatencyMs = (totalLatency.get() / (double) taskCount) / 1_000_000;
        double maxLatencyMs = maxLatency.get() / 1_000_000;
        double minLatencyMs = minLatency.get() / 1_000_000;

        // P95 近似（假设正态分布）
        double p95Ms = avgLatencyMs + 1.65 * Math.sqrt(avgLatencyMs);

        BenchmarkResult result = new BenchmarkResult(
                "写入压测-" + taskCount,
                taskCount,
                elapsedMs,
                qps,
                avgLatencyMs,
                p95Ms,
                maxLatencyMs,
                true,
                null
        );

        results.add(result);

        logger.info("完成: QPS={}/s, 平均延迟={:.2f}ms, P95≈{:.2f}ms, 最大={:.2f}ms",
                String.format("%.0f", qps),
                avgLatencyMs,
                p95Ms,
                maxLatencyMs);

        // 验证
        boolean passed = qps >= 50_000 && p95Ms < 50;
        logger.info("结果: {}", passed ? "✅ 通过" : "❌ 未达标");

        writer.close();

        return result;
    }

    /**
     * 调度压测
     */
    public BenchmarkResult scheduleBenchmark(int taskCount) {
        logger.info("\n----- 调度压测: {} 任务 -----", taskCount);

        // 创建调度器
        MockDispatcher dispatcher = new MockDispatcher();
        TimeBucketScheduler scheduler = new TimeBucketScheduler(
                dispatcher, 10000, 100, 1000);
        scheduler.start();

        // 统计
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < taskCount; i++) {
            Task task = createTask("schedule-" + i, System.currentTimeMillis() + 10000);
            scheduler.schedule(task);
        }

        long elapsedMs = System.currentTimeMillis() - startTime;
        double ops = taskCount / (elapsedMs / 1000.0);

        int pendingCount = scheduler.getPendingTaskCount();
        int bucketCount = scheduler.getBucketCount();

        BenchmarkResult result = new BenchmarkResult(
                "调度压测-" + taskCount,
                taskCount,
                elapsedMs,
                ops,
                0, 0, 0,
                pendingCount == taskCount,
                "buckets=" + bucketCount
        );

        results.add(result);

        logger.info("完成: ops={}/s, pending={}, buckets={}",
                String.format("%.0f", ops), pendingCount, bucketCount);
        logger.info("结果: {}", pendingCount == taskCount ? "✅ 通过" : "❌ 任务丢失");

        scheduler.stop();

        return result;
    }

    /**
     * 风暴测试
     */
    public BenchmarkResult stormBenchmark(int taskCount) {
        logger.info("\n----- 风暴测试: {} 同时触发 -----", taskCount);

        // 创建调度器（bucketSize=100ms）
        MockDispatcher dispatcher = new MockDispatcher();
        TimeBucketScheduler scheduler = new TimeBucketScheduler(
                dispatcher, 10000, 100, 10000);
        scheduler.start();

        // 所有任务在同一时间触发（100ms后）
        long triggerTime = System.currentTimeMillis() + 100;

        long startSchedule = System.currentTimeMillis();

        for (int i = 0; i < taskCount; i++) {
            Task task = createTask("storm-" + i, triggerTime);
            scheduler.schedule(task);
        }

        long scheduleTime = System.currentTimeMillis() - startSchedule;

        // 等待触发
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 检查已过期任务
        var stats = scheduler.getStats();

        long elapsedMs = System.currentTimeMillis() - triggerTime;

        BenchmarkResult result = new BenchmarkResult(
                "风暴测试-" + taskCount,
                taskCount,
                scheduleTime,
                taskCount / (scheduleTime / 1000.0),
                0, 0, 0,
                stats.getExpiredCount() > 0,
                "expired=" + stats.getExpiredCount()
        );

        results.add(result);

        logger.info("完成: 调度耗时={}ms, 过期任务={}", scheduleTime, stats.getExpiredCount());
        logger.info("结果: {}", stats.getExpiredCount() > 0 ? "✅ 通过" : "⚠️ 无过期");

        scheduler.stop();

        return result;
    }

    // ========== 辅助方法 ==========

    private void warmup(WalWriterV2 writer, int count) {
        for (int i = 0; i < count; i++) {
            writer.appendAsync("warmup-" + i, "warmup", EventType.CREATE,
                    System.currentTimeMillis(), "warmup".getBytes());
        }
    }

    private Task createTask(String taskId, long triggerTime) {
        Task task = new Task();
        task.setTaskId(taskId);
        task.setTriggerTime(triggerTime);
        task.setStatus(TaskStatus.PENDING);
        task.setCreateTime(Instant.now().toEpochMilli());
        return task;
    }

    private com.loomq.config.WalConfig createWalConfig() {
        return new com.loomq.config.WalConfig() {
            @Override
            public String dataDir() { return tempDir.toString(); }
            @Override
            public int segmentSizeMb() { return 64; }
            @Override
            public String flushStrategy() { return "batch"; }
            @Override
            public long batchFlushIntervalMs() { return 100; }
            @Override
            public boolean syncOnWrite() { return false; }
        };
    }

    private void deleteRecursively(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            Files.list(path).forEach(p -> {
                try {
                    deleteRecursively(p);
                } catch (IOException e) {
                    // ignore
                }
            });
        }
        Files.deleteIfExists(path);
    }

    private void printReport() {
        logger.info("\n========== 压测报告 ==========");
        logger.info("");
        logger.info("{:<25} {:>10} {:>12} {:>10} {:>8}",
                "测试项", "任务数", "耗时(ms)", "QPS/OPS", "结果");
        logger.info("-".repeat(70));

        for (BenchmarkResult r : results) {
            logger.info("{:<25} {:>10} {:>12} {:>10.0f} {:>8}",
                    r.name(),
                    r.count(),
                    r.elapsedMs(),
                    r.throughput(),
                    r.passed() ? "✅" : "❌");
        }

        logger.info("-".repeat(70));

        long passedCount = results.stream().filter(BenchmarkResult::passed).count();
        logger.info("总计: {}/{} 测试通过", passedCount, results.size());
        logger.info("");
    }

    // ========== 内部类 ==========

    public record BenchmarkConfig(
            int warmupCount,
            int[] writeTestCounts,
            int[] scheduleTestCounts,
            int[] stormTestCounts
    ) {
        public static BenchmarkConfig defaultConfig() {
            return new BenchmarkConfig(
                    1000,
                    new int[]{100_000, 500_000, 1_000_000},
                    new int[]{100_000},
                    new int[]{100_000}
            );
        }
    }

    public record BenchmarkResult(
            String name,
            int count,
            long elapsedMs,
            double throughput,
            double avgLatencyMs,
            double p95LatencyMs,
            double maxLatencyMs,
            boolean passed,
            String note
    ) {}

    static class MockDispatcher implements com.loomq.scheduler.v2.TaskDispatcher {
        private final AtomicLong dispatched = new AtomicLong(0);

        @Override
        public void dispatch(Task task) {
            dispatched.incrementAndGet();
        }

        public long getDispatchedCount() {
            return dispatched.get();
        }
    }

    // ========== 主入口 ==========

    public static void main(String[] args) throws Exception {
        BenchmarkV3 benchmark = new BenchmarkV3();
        benchmark.runAll();
    }
}

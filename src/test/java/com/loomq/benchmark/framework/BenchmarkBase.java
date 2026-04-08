package com.loomq.benchmark.framework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 标准化压测框架基类 (v0.4.3)
 *
 * 设计目标：让性能数字变成可信证据
 * - 统一预热阶段
 * - 多次重复取平均
 * - 统计指标标准化
 * - 报告自动生成
 *
 * @author loomq
 * @since v0.4.3
 */
public abstract class BenchmarkBase {

    protected static final Logger logger = LoggerFactory.getLogger(BenchmarkBase.class);

    // 默认配置
    protected static final Duration DEFAULT_WARMUP_DURATION = Duration.ofSeconds(10);
    protected static final int DEFAULT_REPEAT_COUNT = 5;
    protected static final double OUTLIER_THRESHOLD = 3.0; // 异常值阈值（标准差倍数）

    // 压测配置
    protected final BenchmarkConfig config;

    // GC 监控
    private final List<GarbageCollectorMXBean> gcBeans;
    private final Map<String, Long> gcStartCounts;
    private final Map<String, Long> gcStartTimes;

    // 运行时信息
    protected final BenchmarkEnvironment environment;

    public BenchmarkBase() {
        this(BenchmarkConfig.defaultConfig());
    }

    public BenchmarkBase(BenchmarkConfig config) {
        this.config = config;
        this.gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        this.gcStartCounts = new HashMap<>();
        this.gcStartTimes = new HashMap<>();
        this.environment = collectEnvironment();
    }

    // ========== 抽象方法：子类实现具体压测 ==========

    /**
     * 压测名称
     */
    protected abstract String getBenchmarkName();

    /**
     * 预热逻辑
     */
    protected abstract void warmup();

    /**
     * 单次压测执行
     *
     * @return 压测结果
     */
    protected abstract BenchmarkRunResult runSingle();

    /**
     * 清理资源
     */
    protected abstract void cleanup();

    // ========== 核心执行流程 ==========

    /**
     * 执行完整压测流程
     */
    public final BenchmarkReport execute() {
        logger.info("╔════════════════════════════════════════════════════════════╗");
        logger.info("║           LoomQ 标准化压测 v0.4.3                         ║");
        logger.info("║           {}                           ║",
                String.format("%-40s", getBenchmarkName()).substring(0, Math.min(40, getBenchmarkName().length())));
        logger.info("╚════════════════════════════════════════════════════════════╝");
        logger.info("");

        // 打印环境信息
        printEnvironment();

        // 预热
        logger.info("----- [1/4] 预热阶段 ({}) -----", config.warmupDuration());
        performWarmup();

        // 执行多次压测
        logger.info("----- [2/4] 压测阶段 ({} 次重复) -----", config.repeatCount());
        List<BenchmarkRunResult> runs = performBenchmarks();

        // 统计分析
        logger.info("----- [3/4] 统计分析 -----");
        BenchmarkStatistics statistics = analyzeResults(runs);

        // 生成报告
        logger.info("----- [4/4] 生成报告 -----");
        BenchmarkReport report = generateReport(runs, statistics);

        // 清理
        cleanup();

        // 打印摘要
        printSummary(report);

        return report;
    }

    /**
     * 执行预热
     */
    private void performWarmup() {
        long startTime = System.currentTimeMillis();
        warmup();
        long elapsedMs = System.currentTimeMillis() - startTime;

        // 如果预热时间不足，等待
        long remainingMs = config.warmupDuration().toMillis() - elapsedMs;
        if (remainingMs > 0) {
            logger.info("预热完成，等待 {}ms 达到预热时长", remainingMs);
            try {
                Thread.sleep(remainingMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // 强制 GC
        System.gc();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        logger.info("预热完成");
    }

    /**
     * 执行多次压测
     */
    private List<BenchmarkRunResult> performBenchmarks() {
        List<BenchmarkRunResult> runs = new ArrayList<>();

        for (int i = 0; i < config.repeatCount(); i++) {
            logger.info("第 {}/{} 次运行...", i + 1, config.repeatCount());

            // 记录 GC 起始状态
            recordGcStart();

            // 执行压测
            BenchmarkRunResult result = runSingle();

            // 计算 GC 时间
            Map<String, GcStats> gcStats = collectGcStats();
            result = result.withGcStats(gcStats);

            runs.add(result);

            // 中间休息
            if (i < config.repeatCount() - 1) {
                try {
                    Thread.sleep(config.intervalBetweenRunsMs());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        return runs;
    }

    /**
     * 记录 GC 起始状态
     */
    private void recordGcStart() {
        gcStartCounts.clear();
        gcStartTimes.clear();

        for (GarbageCollectorMXBean bean : gcBeans) {
            gcStartCounts.put(bean.getName(), bean.getCollectionCount());
            gcStartTimes.put(bean.getName(), bean.getCollectionTime());
        }
    }

    /**
     * 收集 GC 统计
     */
    private Map<String, GcStats> collectGcStats() {
        Map<String, GcStats> stats = new HashMap<>();

        for (GarbageCollectorMXBean bean : gcBeans) {
            String name = bean.getName();
            long startCount = gcStartCounts.getOrDefault(name, 0L);
            long startTime = gcStartTimes.getOrDefault(name, 0L);

            long count = bean.getCollectionCount() - startCount;
            long time = bean.getCollectionTime() - startTime;

            stats.put(name, new GcStats(name, count, time));
        }

        return stats;
    }

    /**
     * 统计分析结果
     */
    private BenchmarkStatistics analyzeResults(List<BenchmarkRunResult> runs) {
        // 提取吞吐量数据
        double[] throughputs = runs.stream()
                .mapToDouble(BenchmarkRunResult::throughput)
                .toArray();

        // 提取延迟数据
        List<Long> allLatencies = runs.stream()
                .flatMap(r -> r.latencies().stream())
                .sorted()
                .toList();

        // 基础统计
        double meanThroughput = mean(throughputs);
        double stdThroughput = stdDev(throughputs, meanThroughput);
        double cvThroughput = stdThroughput / meanThroughput * 100; // 变异系数

        // 延迟分位数
        long p50 = percentile(allLatencies, 50);
        long p95 = percentile(allLatencies, 95);
        long p99 = percentile(allLatencies, 99);
        long p999 = percentile(allLatencies, 99.9);
        long maxLatency = allLatencies.isEmpty() ? 0 : allLatencies.get(allLatencies.size() - 1);

        // 找出异常值
        List<BenchmarkRunResult> outliers = runs.stream()
                .filter(r -> Math.abs(r.throughput() - meanThroughput) > OUTLIER_THRESHOLD * stdThroughput)
                .toList();

        return new BenchmarkStatistics(
                meanThroughput,
                stdThroughput,
                cvThroughput,
                min(throughputs),
                max(throughputs),
                p50, p95, p99, p999, maxLatency,
                outliers.size(),
                config.repeatCount()
        );
    }

    /**
     * 生成报告
     */
    private BenchmarkReport generateReport(List<BenchmarkRunResult> runs, BenchmarkStatistics stats) {
        return new BenchmarkReport(
                getBenchmarkName(),
                Instant.now(),
                environment,
                config,
                runs,
                stats
        );
    }

    // ========== 统计工具方法 ==========

    protected static double mean(double[] values) {
        return Arrays.stream(values).average().orElse(0.0);
    }

    protected static double stdDev(double[] values, double mean) {
        double variance = Arrays.stream(values)
                .map(v -> Math.pow(v - mean, 2))
                .average()
                .orElse(0.0);
        return Math.sqrt(variance);
    }

    protected static double min(double[] values) {
        return Arrays.stream(values).min().orElse(0.0);
    }

    protected static double max(double[] values) {
        return Arrays.stream(values).max().orElse(0.0);
    }

    protected static long percentile(List<Long> sorted, double percentile) {
        if (sorted.isEmpty()) return 0;
        int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    // ========== 输出方法 ==========

    private void printEnvironment() {
        logger.info("环境信息:");
        logger.info("  操作系统: {} {}", environment.osName(), environment.osVersion());
        logger.info("  JDK: {}", environment.javaVersion());
        logger.info("  JVM参数: {}", environment.jvmArgs());
        logger.info("  处理器: {} 核", environment.availableProcessors());
        logger.info("  内存: {} MB", environment.maxMemoryMB());
        logger.info("  GC器: {}", environment.gcCollectors());
        logger.info("");
    }

    private void printSummary(BenchmarkReport report) {
        BenchmarkStatistics stats = report.statistics();

        logger.info("");
        logger.info("╔════════════════════════════════════════════════════════════╗");
        logger.info("║                    压测结果摘要                            ║");
        logger.info("╚════════════════════════════════════════════════════════════╝");
        logger.info("");
        logger.info("吞吐量统计:");
        logger.info("  均值: {:.2f} ops/s", stats.meanThroughput());
        logger.info("  标准差: {:.2f} ops/s", stats.stdThroughput());
        logger.info("  变异系数: {:.2f}%", stats.cvThroughput());
        logger.info("  范围: [{:.2f}, {:.2f}] ops/s", stats.minThroughput(), stats.maxThroughput());
        logger.info("");
        logger.info("延迟分位数:");
        logger.info("  P50: {} ms", stats.p50LatencyMs());
        logger.info("  P95: {} ms", stats.p95LatencyMs());
        logger.info("  P99: {} ms", stats.p99LatencyMs());
        logger.info("  P99.9: {} ms", stats.p999LatencyMs());
        logger.info("  Max: {} ms", stats.maxLatencyMs());
        logger.info("");

        // 可信度判断
        boolean isCredible = stats.cvThroughput() < 10.0 && stats.outlierCount() == 0;
        logger.info("可信度: {} (CV={:.2f}%, 异常值={})",
                isCredible ? "✅ 可信" : "⚠️ 存疑",
                stats.cvThroughput(),
                stats.outlierCount());

        if (!isCredible) {
            logger.info("建议: 变异系数过高，建议增加重复次数或检查系统稳定性");
        }
    }

    /**
     * 收集环境信息
     */
    private BenchmarkEnvironment collectEnvironment() {
        Runtime runtime = Runtime.getRuntime();

        String jvmArgs = ManagementFactory.getRuntimeMXBean().getInputArguments()
                .stream()
                .filter(arg -> arg.startsWith("-X") || arg.startsWith("-XX"))
                .collect(Collectors.joining(" "));

        String gcCollectors = gcBeans.stream()
                .map(GarbageCollectorMXBean::getName)
                .collect(Collectors.joining(", "));

        return new BenchmarkEnvironment(
                System.getProperty("os.name"),
                System.getProperty("os.version"),
                System.getProperty("java.version"),
                System.getProperty("java.vm.name"),
                jvmArgs.isEmpty() ? "(默认)" : jvmArgs,
                runtime.availableProcessors(),
                (int) (runtime.maxMemory() / 1024 / 1024),
                gcCollectors
        );
    }

    // ========== 数据记录器 ==========

    /**
     * 延迟记录器
     */
    protected static class LatencyRecorder {
        private final List<Long> latencies = new ArrayList<>();

        public void record(long latencyMs) {
            latencies.add(latencyMs);
        }

        public void recordNano(long latencyNs) {
            latencies.add(latencyNs / 1_000_000);
        }

        public List<Long> getLatencies() {
            return new ArrayList<>(latencies);
        }

        public int size() {
            return latencies.size();
        }
    }

    /**
     * 吞吐量计时器
     */
    protected static class ThroughputTimer {
        private final long startTime;
        private final AtomicLong operationCount;

        public ThroughputTimer() {
            this.startTime = System.nanoTime();
            this.operationCount = new AtomicLong(0);
        }

        public void increment() {
            operationCount.incrementAndGet();
        }

        public void add(long count) {
            operationCount.addAndGet(count);
        }

        public double getThroughput() {
            long elapsedNs = System.nanoTime() - startTime;
            return operationCount.get() / (elapsedNs / 1_000_000_000.0);
        }

        public long getElapsedMs() {
            return (System.nanoTime() - startTime) / 1_000_000;
        }

        public long getCount() {
            return operationCount.get();
        }
    }

    // ========== 记录定义 ==========

    /**
     * 压测配置
     */
    public record BenchmarkConfig(
            Duration warmupDuration,
            int repeatCount,
            long intervalBetweenRunsMs,
            boolean recordLatencies,
            int latencySampleRate // 1/N 采样率
    ) {
        public static BenchmarkConfig defaultConfig() {
            return new BenchmarkConfig(
                    DEFAULT_WARMUP_DURATION,
                    DEFAULT_REPEAT_COUNT,
                    1000, // 1秒间隔
                    true,
                    1 // 全量采样
            );
        }

        public BenchmarkConfig withRepeatCount(int count) {
            return new BenchmarkConfig(warmupDuration, count, intervalBetweenRunsMs, recordLatencies, latencySampleRate);
        }

        public BenchmarkConfig withWarmupDuration(Duration duration) {
            return new BenchmarkConfig(duration, repeatCount, intervalBetweenRunsMs, recordLatencies, latencySampleRate);
        }
    }

    /**
     * 压测运行结果
     */
    public record BenchmarkRunResult(
            int runNumber,
            long operationCount,
            long elapsedMs,
            double throughput,
            long memoryUsedMB,
            List<Long> latencies,
            Map<String, GcStats> gcStats,
            Map<String, Object> metadata
    ) {
        public static BenchmarkRunResult of(int runNumber, long operationCount, long elapsedMs) {
            double throughput = elapsedMs > 0 ? operationCount / (elapsedMs / 1000.0) : 0;
            return new BenchmarkRunResult(runNumber, operationCount, elapsedMs, throughput,
                    0, new ArrayList<>(), new HashMap<>(), new HashMap<>());
        }

        public BenchmarkRunResult withLatencies(List<Long> latencies) {
            return new BenchmarkRunResult(runNumber, operationCount, elapsedMs, throughput,
                    memoryUsedMB, latencies, gcStats, metadata);
        }

        public BenchmarkRunResult withGcStats(Map<String, GcStats> gcStats) {
            return new BenchmarkRunResult(runNumber, operationCount, elapsedMs, throughput,
                    memoryUsedMB, latencies, gcStats, metadata);
        }

        public BenchmarkRunResult withMemoryUsed(long memoryUsedMB) {
            return new BenchmarkRunResult(runNumber, operationCount, elapsedMs, throughput,
                    memoryUsedMB, latencies, gcStats, metadata);
        }
    }

    /**
     * GC 统计
     */
    public record GcStats(
            String name,
            long collectionCount,
            long collectionTimeMs
    ) {
        public double avgPauseTimeMs() {
            return collectionCount > 0 ? (double) collectionTimeMs / collectionCount : 0;
        }
    }

    /**
     * 压测环境信息
     */
    public record BenchmarkEnvironment(
            String osName,
            String osVersion,
            String javaVersion,
            String jvmName,
            String jvmArgs,
            int availableProcessors,
            int maxMemoryMB,
            String gcCollectors
    ) {}

    /**
     * 统计分析结果
     */
    public record BenchmarkStatistics(
            double meanThroughput,
            double stdThroughput,
            double cvThroughput,
            double minThroughput,
            double maxThroughput,
            long p50LatencyMs,
            long p95LatencyMs,
            long p99LatencyMs,
            long p999LatencyMs,
            long maxLatencyMs,
            int outlierCount,
            int totalRuns
    ) {}

    /**
     * 压测报告
     */
    public record BenchmarkReport(
            String benchmarkName,
            Instant timestamp,
            BenchmarkEnvironment environment,
            BenchmarkConfig config,
            List<BenchmarkRunResult> runs,
            BenchmarkStatistics statistics
    ) {}
}

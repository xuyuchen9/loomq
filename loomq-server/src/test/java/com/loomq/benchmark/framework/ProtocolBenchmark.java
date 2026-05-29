package com.loomq.benchmark.framework;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for protocol-level create-path benchmarks (HTTP, gRPC, etc.).
 *
 * <p>Subclasses only need to implement {@link #createIntent()} and provide
 * the protocol name via the constructor. All warmup, execution, metrics
 * collection, and summary logic is shared.
 */
public abstract class ProtocolBenchmark {

    private final String protocolName;
    private final String targetAddress;

    protected ProtocolBenchmark(String protocolName, String targetAddress) {
        this.protocolName = protocolName;
        this.targetAddress = targetAddress;
    }

    /** Send a single create-intent request using the protocol-specific transport. */
    protected abstract void createIntent() throws Exception;

    /** Called once after all tests complete. Override to close channels, etc. */
    protected void shutdown() {}

    // ── Main entry point ──

    protected final void run(String[] args) throws Exception {
        boolean quick = Boolean.getBoolean("loomq.benchmark.quick");
        boolean stress = Boolean.getBoolean("loomq.benchmark.stress");

        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.printf("║     LoomQ %s Virtual Thread 性能测试              %n", protocolName);
        System.out.printf("║     关注 %s 的真实观察值                              %n", protocolName);
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("模式: " + (quick ? "QUICK" : stress ? "STRESS" : "FULL"));
        System.out.println("目标: " + targetAddress);
        System.out.printf("CPU cores: %d%n", Runtime.getRuntime().availableProcessors());
        System.out.printf("Max memory: %d MB%n", Runtime.getRuntime().maxMemory() / 1024 / 1024);
        System.out.println();

        int[] threadCounts;
        String customThreads = System.getProperty("loomq.benchmark.threads");
        if (customThreads != null && !customThreads.isBlank()) {
            threadCounts = Arrays.stream(customThreads.split(","))
                .map(String::trim)
                .mapToInt(Integer::parseInt)
                .toArray();
        } else if (stress) {
            threadCounts = new int[] {500, 1000, 2000, 3000};
        } else {
            threadCounts = quick ? new int[] {16, 32, 64} : new int[] {50, 100, 200, 500, 1000};
        }

        int durationSec;
        String customDuration = System.getProperty("loomq.benchmark.duration_sec");
        if (customDuration != null && !customDuration.isBlank()) {
            durationSec = Integer.parseInt(customDuration);
        } else if (stress) {
            durationSec = 30;
        } else {
            durationSec = quick ? 4 : 20;
        }

        int cooldownMs;
        String customCooldown = System.getProperty("loomq.benchmark.cooldown_ms");
        if (customCooldown != null && !customCooldown.isBlank()) {
            cooldownMs = Integer.parseInt(customCooldown);
        } else if (stress) {
            cooldownMs = 5000;
        } else {
            cooldownMs = quick ? 1000 : 2000;
        }

        System.out.printf("Thread counts: %s%n", Arrays.toString(threadCounts));
        System.out.printf("Duration per tier: %ds, Cooldown: %dms%n", durationSec, cooldownMs);
        System.out.println();

        int warmupCount = quick ? 20 : 100;
        System.out.println("[预热] 发送 " + warmupCount + " 个请求预热服务...");
        warmUp(warmupCount);
        Thread.sleep(1000);
        System.out.println("[预热] 完成");
        System.out.println();

        List<BenchmarkResult> results = new ArrayList<>();

        for (int threads : threadCounts) {
            System.out.println("══════════════════════════════════════════════════════════════");
            System.out.printf("测试配置: %d 并发线程, %d 秒持续时间%n", threads, durationSec);
            System.out.println("══════════════════════════════════════════════════════════════");

            BenchmarkResult result = runTest(threads, durationSec);
            results.add(result);

            System.out.printf("  QPS:         %,.0f%n", result.qps());
            System.out.printf("  平均延迟:    %.2f ms%n", result.avgLatency());
            System.out.printf("  P50:         %d ms%n", result.p50());
            System.out.printf("  P90:         %d ms%n", result.p90());
            System.out.printf("  P99:         %d ms%n", result.p99());
            System.out.printf("  成功/失败:   %,d / %,d%n", result.success(), result.fail());
            System.out.printf(Locale.ROOT,
                "RESULT_ROW|threads=%d|qps=%.0f|avg_ms=%.2f|p50_ms=%d|p90_ms=%d|p99_ms=%d|success=%d|fail=%d%n",
                result.threads(), result.qps(), result.avgLatency(),
                result.p50(), result.p90(), result.p99(), result.success(), result.fail());
            System.out.println();

            Thread.sleep(cooldownMs);
        }

        printSummary(results);
        shutdown();
    }

    // ── Shared implementation ──

    private void warmUp(int count) throws Exception {
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

    private BenchmarkResult runTest(int threads, int durationSec) throws Exception {
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

        latch.await(durationSec + 20L, TimeUnit.SECONDS);
        long actualDuration = System.currentTimeMillis() - startTime;

        Collections.sort(latencies);

        double qps = (double) success.get() / actualDuration * 1000;
        double avgLatency = latencies.isEmpty() ? 0
            : latencies.stream().mapToLong(Long::longValue).average().orElse(0);
        long p50 = getPercentile(latencies, 0.50);
        long p90 = getPercentile(latencies, 0.90);
        long p99 = getPercentile(latencies, 0.99);

        return new BenchmarkResult(qps, avgLatency, p50, p90, p99,
            success.get(), fail.get(), threads);
    }

    static long getPercentile(List<Long> sorted, double percentile) {
        if (sorted.isEmpty()) return 0;
        int index = Math.min((int) (sorted.size() * percentile), sorted.size() - 1);
        return sorted.get(index);
    }

    private void printSummary(List<BenchmarkResult> results) {
        System.out.println();
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.printf("                    性能汇总表格 (%s)                       %n", protocolName);
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println();
        System.out.printf("%-10s %12s %10s %10s %10s %10s%n", "Threads", "QPS", "Avg(ms)", "P99(ms)", "Success", "Growth%");
        System.out.println("-".repeat(66));

        double prevQps = 0;
        int consecutiveLowGrowth = 0;
        int inflectionIdx = -1;

        for (int i = 0; i < results.size(); i++) {
            BenchmarkResult r = results.get(i);
            double growthPct = prevQps > 0 ? (r.qps() - prevQps) / prevQps * 100 : 0;
            System.out.printf("%-10d %,12.0f %10.2f %10d %,10d %9.1f%%%n",
                r.threads(), r.qps(), r.avgLatency(), r.p99(), r.success(), growthPct);

            if (i > 0 && growthPct < 5) {
                consecutiveLowGrowth++;
                if (consecutiveLowGrowth >= 2 && inflectionIdx < 0) {
                    inflectionIdx = i - 2;
                }
            } else {
                consecutiveLowGrowth = 0;
            }
            prevQps = r.qps();
        }

        BenchmarkResult peak = results.stream()
            .max(Comparator.comparingDouble(BenchmarkResult::qps))
            .orElse(null);
        if (peak != null) {
            BenchmarkResult bestP99 = results.stream()
                .min(Comparator.comparingLong(BenchmarkResult::p99))
                .orElse(peak);
            BenchmarkResult worstP99 = results.stream()
                .max(Comparator.comparingLong(BenchmarkResult::p99))
                .orElse(peak);
            int totalSuccess = results.stream().mapToInt(BenchmarkResult::success).sum();
            int totalFail = results.stream().mapToInt(BenchmarkResult::fail).sum();
            double failRate = (totalSuccess + totalFail) == 0 ? 0
                : (double) totalFail / (totalSuccess + totalFail) * 100.0;
            System.out.println();
            System.out.printf("峰值 QPS:   %,.0f @ %d 线程%n", peak.qps(), peak.threads());
            System.out.printf("最佳 P99:   %d ms @ %d 线程%n", bestP99.p99(), bestP99.threads());
            System.out.printf("最差 P99:   %d ms @ %d 线程%n", worstP99.p99(), worstP99.threads());
            System.out.printf("失败率:     %.2f%%%n", failRate);
            System.out.printf("解释: 这组结果反映的是 %s 的真实开销。%n", protocolName);

            if (inflectionIdx >= 0) {
                BenchmarkResult inflection = results.get(inflectionIdx);
                System.out.printf("拐点:       %d 线程 (%,.0f QPS, P99=%dms)%n",
                    inflection.threads(), inflection.qps(), inflection.p99());
            } else if (worstP99.p99() > 500) {
                System.out.printf("⚠ 系统进入雪崩区域 (P99 > 500ms @ %d 线程)%n", worstP99.threads());
            }

            System.out.printf(Locale.ROOT,
                "RESULT|peak_qps=%.0f|best_threads=%d|best_p99_ms=%d|best_avg_ms=%.2f|worst_p99_ms=%d|worst_threads=%d|fail_rate=%.2f|cores=%d|inflection_threads=%d|inflection_qps=%.0f%n",
                peak.qps(), peak.threads(), bestP99.p99(), bestP99.avgLatency(),
                worstP99.p99(), worstP99.threads(), failRate, Runtime.getRuntime().availableProcessors(),
                inflectionIdx >= 0 ? results.get(inflectionIdx).threads() : -1,
                inflectionIdx >= 0 ? results.get(inflectionIdx).qps() : 0);
        }
    }

    public record BenchmarkResult(
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

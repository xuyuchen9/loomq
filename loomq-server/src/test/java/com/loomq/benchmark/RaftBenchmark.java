package com.loomq.benchmark;

import com.loomq.config.WalConfig;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.raft.RaftConfig;
import com.loomq.raft.RaftNode;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Raft 共识性能基准测试。
 *
 * 覆盖：
 * 1. Propose 吞吐量（固定 payload, 单线程）
 * 2. 可变 payload 大小对吞吐量的影响
 * 3. 并发 propose 伸缩性（1-8 线程）
 * 4. Commit + Apply 管线吞吐量
 * 5. 选举延迟分布
 */
@Tag("benchmark")
public class RaftBenchmark {

    private static final int WARMUP = 500;
    private static final int DEFAULT_COUNT = 10_000;

    public static void main(String[] args) throws Exception {
        RaftBenchmark bench = new RaftBenchmark();
        bench.fixedPayloadThroughput();
        bench.varyingPayloadSizes();
        bench.concurrentProposeScaling();
        bench.commitApplyPipeline();
        bench.electionLatency();
    }

    // ================================================================
    // 1. 固定 payload propose 吞吐量（基准）
    // ================================================================

    private static WalConfig defaultWalConfig(Path dir) {
        return new WalConfig(dir.toString(), 128, "batch", 100, false, "memory_segment",
            128, 512, 64, 10, 4, 1, false);
    }

    // ================================================================
    // 2. 可变 payload 大小
    // ================================================================

    private static byte[] encodeBaselineIntent() {
        Intent intent = new Intent("bench-intent");
        intent.setPrecisionTier(com.loomq.domain.intent.PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);
        return IntentBinaryCodec.encode(intent);
    }

    // ================================================================
    // 3. 并发 propose 伸缩性
    // ================================================================

    private static long percentile(List<Long> sorted, int p) {
        if (sorted.isEmpty()) return 0;
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    // ================================================================
    // 4. Commit + Apply 管线吞吐量
    // ================================================================

    private static String formatSize(int bytes) {
        if (bytes < 1024) return bytes + "B";
        if (bytes < 1024 * 1024) return (bytes / 1024) + "KB";
        return (bytes / (1024 * 1024)) + "MB";
    }

    // ================================================================
    // 5. 选举延迟
    // ================================================================

    @Test
    void fixedPayloadThroughput() throws Exception {
        int count = Integer.getInteger("benchmark.count", DEFAULT_COUNT);
        Path dir = Files.createTempDirectory("raft-bench-fixed-");

        System.out.println("=== 1. Raft Propose Throughput (fixed 144B payload) ===");
        System.out.println("Entries: " + count);

        WalConfig cfg = defaultWalConfig(dir);
        var ctx = startSingleNode(cfg, dir);
        if (ctx == null) return;

        byte[] payload = encodeBaselineIntent();
        LatencyStats stats = measureProposeLatency(ctx.node, payload, count, true);

        printLatencyStats(count, stats);
        System.out.printf(Locale.ROOT,
            "RESULT|raft_propose_fixed|count=%d|throughput=%.0f|avg_us=%.0f|p50_us=%d|p95_us=%d|p99_us=%d%n",
            count, stats.throughput(), stats.avgUs(), stats.p50Us(), stats.p95Us(), stats.p99Us());

        ctx.node.close();
        ctx.store.shutdown();
    }

    // ========== helpers ==========

    @Test
    void varyingPayloadSizes() throws Exception {
        int count = Integer.getInteger("benchmark.count", 2000);
        int[] sizes = {128, 512, 1024, 4096, 16384};

        System.out.println("=== 2. Varying Payload Size Throughput ===");
        System.out.printf("%-12s %12s %12s %12s %12s%n",
            "Payload", "Throughput", "Avg(µs)", "P50(µs)", "P99(µs)");

        for (int size : sizes) {
            byte[] payload = new byte[size];
            ThreadLocalRandom.current().nextBytes(payload);

            Path dir = Files.createTempDirectory("raft-bench-var-");
            WalConfig cfg = defaultWalConfig(dir);
            var ctx = startSingleNode(cfg, dir);
            if (ctx == null) continue;

            LatencyStats stats = measureProposeLatency(ctx.node, payload, count, false);

            System.out.printf(Locale.ROOT, "%-12s %12.0f %12.0f %12d %12d%n",
                formatSize(size), stats.throughput(), stats.avgUs(), stats.p50Us(), stats.p99Us());
            System.out.printf(Locale.ROOT,
                "RESULT|raft_propose_vary|payload=%d|throughput=%.0f|avg_us=%.0f|p50_us=%d|p99_us=%d%n",
                size, stats.throughput(), stats.avgUs(), stats.p50Us(), stats.p99Us());

            ctx.node.close();
            ctx.store.shutdown();
        }
    }

    @Test
    void concurrentProposeScaling() throws Exception {
        int count = Integer.getInteger("benchmark.count", 5000);
        int[] threadCounts = {1, 2, 4, 8};

        System.out.println("=== 3. Concurrent Propose Scaling ===");
        System.out.printf("%-10s %12s %12s %12s %12s%n",
            "Threads", "Throughput", "Avg(µs)", "P50(µs)", "P99(µs)");

        for (int threads : threadCounts) {
            Path dir = Files.createTempDirectory("raft-bench-conc-");
            WalConfig cfg = defaultWalConfig(dir);
            var ctx = startSingleNode(cfg, dir);
            if (ctx == null) continue;

            byte[] payload = encodeBaselineIntent();
            int perThread = count / threads;

            ExecutorService exec = Executors.newFixedThreadPool(threads);
            CountDownLatch latch = new CountDownLatch(threads);
            ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();

            // Warmup
            for (int i = 0; i < WARMUP; i++) {
                ctx.node.propose(payload);
            }

            AtomicLong startNs = new AtomicLong();
            for (int t = 0; t < threads; t++) {
                exec.submit(() -> {
                    if (startNs.get() == 0) startNs.set(System.nanoTime());
                    for (int i = 0; i < perThread; i++) {
                        long t0 = System.nanoTime();
                        ctx.node.propose(payload);
                        long t1 = System.nanoTime();
                        latencies.add((t1 - t0) / 1000); // µs
                    }
                    latch.countDown();
                });
            }

            latch.await(30, TimeUnit.SECONDS);
            exec.shutdown();

            long durNs = System.nanoTime() - startNs.get();
            double durMs = durNs / 1_000_000.0;
            double qps = threads * perThread / (durMs / 1000.0);

            List<Long> sorted = latencies.stream().sorted().toList();
            long p50 = percentile(sorted, 50);
            long p95 = percentile(sorted, 95);
            long p99 = percentile(sorted, 99);
            double avg = sorted.stream().mapToLong(Long::longValue).average().orElse(0);

            System.out.printf(Locale.ROOT, "%-10s %12.0f %12.0f %12d %12d%n",
                threads + "T", qps, avg, p50, p99);
            System.out.printf(Locale.ROOT,
                "RESULT|raft_concurrent|threads=%d|throughput=%.0f|avg_us=%.0f|p50_us=%d|p99_us=%d%n",
                threads, qps, avg, p50, p99);

            ctx.node.close();
            ctx.store.shutdown();
        }
    }

    @Test
    void commitApplyPipeline() throws Exception {
        int count = Integer.getInteger("benchmark.count", 5000);

        System.out.println("=== 4. Commit + Apply Pipeline ===");
        System.out.println("Entries: " + count);

        Path dir = Files.createTempDirectory("raft-bench-commit-");
        WalConfig cfg = defaultWalConfig(dir);
        var ctx = startSingleNode(cfg, dir);
        if (ctx == null) return;

        byte[] payload = encodeBaselineIntent();

        // Warmup
        for (int i = 0; i < WARMUP; i++) {
            ctx.node.propose(payload);
        }

        // Phase 1: Propose all entries
        long proposeStart = System.nanoTime();
        for (int i = 0; i < count; i++) {
            ctx.node.propose(payload);
        }
        long proposeEnd = System.nanoTime();
        double proposeMs = (proposeEnd - proposeStart) / 1_000_000.0;
        double proposeQps = count / (proposeMs / 1000.0);

        // Phase 2: Commit (use RaftLog index, not raw WAL byte offset)
        long commitStart = System.nanoTime();
        long lastIndex = ctx.node.getRaftLog().lastIndex();
        ctx.node.getReplication().advanceCommitIndex(
            new long[]{lastIndex},
            ctx.node.getElection().currentTerm());
        long commitEnd = System.nanoTime();
        double commitMs = (commitEnd - commitStart) / 1_000_000.0;

        // Phase 3: Apply
        long applyStart = System.nanoTime();
        long appliedBefore = ctx.node.getReplication().lastApplied();
        ctx.node.applyCommitted();
        long applyEnd = System.nanoTime();
        double applyMs = (applyEnd - applyStart) / 1_000_000.0;
        long appliedDelta = ctx.node.getReplication().lastApplied() - appliedBefore;

        double totalMs = proposeMs + commitMs + applyMs;
        double pipelineQps = count / (totalMs / 1000.0);

        System.out.printf("  Propose:     %,.0f ops/s (%.1f ms)%n", proposeQps, proposeMs);
        System.out.printf("  Last Index:  %d%n", lastIndex);
        System.out.printf("  Commit:      %.1f ms%n", commitMs);
        System.out.printf("  Apply:       %.1f ms (%d entries)%n", applyMs, appliedDelta);
        System.out.printf("  Pipeline:    %,.0f ops/s (%.1f ms total)%n", pipelineQps, totalMs);
        System.out.printf(Locale.ROOT,
            "RESULT|raft_pipeline|count=%d|last_index=%d|propose_qps=%.0f|commit_ms=%.1f|apply_ms=%.1f|applied=%d|total_ms=%.1f|pipeline_qps=%.0f%n",
            count, lastIndex, proposeQps, commitMs, applyMs, appliedDelta, totalMs, pipelineQps);

        ctx.node.close();
        ctx.store.shutdown();
    }

    @Test
    void electionLatency() throws Exception {
        int rounds = Integer.getInteger("benchmark.electionRounds", 5);

        System.out.println("=== 5. Election Latency ===");
        System.out.println("Rounds: " + rounds);

        List<Long> latencies = new ArrayList<>();

        for (int i = 0; i < rounds; i++) {
            Path dir = Files.createTempDirectory("raft-bench-elect-");
            WalConfig cfg = defaultWalConfig(dir);
            SimpleWalWriter wal = new SimpleWalWriter(cfg, "raft-elect-bench");
            IntentStore store = new ConcurrentIntentStore();

            RaftConfig raftCfg = RaftConfig.singleNode("elect-" + i);
            RaftNode node = new RaftNode(raftCfg, wal, store, null);

            long t0 = System.nanoTime();
            node.start();

            long deadline = System.currentTimeMillis() + 3000;
            while (System.currentTimeMillis() < deadline && !node.isLeader()) {
                Thread.sleep(10);
            }
            long t1 = System.nanoTime();

            if (node.isLeader()) {
                long electMs = (t1 - t0) / 1_000_000;
                latencies.add(electMs);
                System.out.printf("  Round %d: %d ms%n", i + 1, electMs);
            } else {
                System.out.printf("  Round %d: FAILED (no leader within 3s)%n", i + 1);
            }

            node.close();
            store.shutdown();
        }

        if (latencies.isEmpty()) {
            System.out.println("  All rounds failed!");
            return;
        }

        List<Long> sorted = latencies.stream().sorted().toList();
        long p50 = percentile(sorted, 50);
        long p95 = percentile(sorted, 95);
        double avg = sorted.stream().mapToLong(Long::longValue).average().orElse(0);

        System.out.printf("  Avg: %.0f ms, P50: %d ms, P95: %d ms%n", avg, p50, p95);
        System.out.printf(Locale.ROOT,
            "RESULT|raft_election|rounds=%d|avg_ms=%.0f|p50_ms=%d|p95_ms=%d|succeeded=%d%n",
            rounds, avg, p50, p95, latencies.size());
    }

    /** Start a single-node Raft cluster and wait for leadership */
    private NodeContext startSingleNode(WalConfig wcfg, Path dir) throws Exception {
        SimpleWalWriter wal = new SimpleWalWriter(wcfg, "raft-bench");
        IntentStore store = new ConcurrentIntentStore();

        RaftConfig raftCfg = RaftConfig.singleNode("bench-node");
        RaftNode node = new RaftNode(raftCfg, wal, store, null);
        node.start();

        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline && !node.isLeader()) {
            Thread.sleep(20);
        }
        if (!node.isLeader()) {
            System.out.println("WARN: Node did not become leader within 3s");
            node.close();
            store.shutdown();
            return null;
        }
        return new NodeContext(node, wal, store);
    }

    /** Measure propose latency via RaftNode.propose() */
    private LatencyStats measureProposeLatency(RaftNode node, byte[] payload, int count,
            boolean warmup) {
        if (warmup) {
            for (int i = 0; i < WARMUP; i++) {
                node.propose(payload);
            }
        }

        List<Long> latencies = new ArrayList<>(count);
        long startNs = System.nanoTime();
        for (int i = 0; i < count; i++) {
            long t0 = System.nanoTime();
            node.propose(payload);
            long t1 = System.nanoTime();
            latencies.add((t1 - t0) / 1000); // µs
        }
        long durNs = System.nanoTime() - startNs;
        double durMs = durNs / 1_000_000.0;
        double qps = count / (durMs / 1000.0);

        List<Long> sorted = latencies.stream().sorted().toList();
        return new LatencyStats(
            qps,
            latencies.stream().mapToLong(Long::longValue).average().orElse(0),
            percentile(sorted, 50),
            percentile(sorted, 95),
            percentile(sorted, 99),
            (long) durMs
        );
    }

    private void printLatencyStats(int count, LatencyStats s) {
        System.out.printf("  Throughput:  %,.0f ops/s%n", s.throughput());
        System.out.printf("  Duration:    %,d ms%n", s.durMs());
        System.out.printf("  Avg latency: %,.0f µs%n", s.avgUs());
        System.out.printf("  P50 latency: %,d µs%n", s.p50Us());
        System.out.printf("  P95 latency: %,d µs%n", s.p95Us());
        System.out.printf("  P99 latency: %,d µs%n", s.p99Us());
        System.out.println();
    }

    private record NodeContext(RaftNode node, SimpleWalWriter wal, IntentStore store) {}

    private record LatencyStats(double throughput, double avgUs, long p50Us, long p95Us,
            long p99Us, long durMs) {}
}

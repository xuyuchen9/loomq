package com.loomq.benchmark;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.application.swap.ColdIntentSwapper;
import com.loomq.config.WalConfig;
import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.IntentStore;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 内部组件性能测试，测的是进程内真实上限。
 */
public class InternalBenchmark {

    public static void main(String[] args) throws Exception {
        boolean quick = Boolean.getBoolean("loomq.benchmark.quick");
        int threads = quick ? 32 : 100;
        int durationSec = quick ? 5 : 10;
        int retainedCount = quick ? 2_000 : 10_000;

        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║         LoomQ 内部组件性能测试 (进程内上限)                 ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("模式: " + (quick ? "QUICK" : "FULL"));
        System.out.println();

        // 1) 直接写入吞吐
        System.out.println("=== IntentStore 直接写入 ===");
        System.out.println("线程数: " + threads + ", 时长: " + durationSec + " 秒");
        System.out.println();

        IntentStore throughputStore = new IntentStore();
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threads);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + durationSec * 1000L;

        for (int workerId = 0; workerId < threads; workerId++) {
            final int worker = workerId;
            Thread.ofVirtual().start(() -> {
                try {
                    int local = 0;
                    while (System.currentTimeMillis() < endTime) {
                        Intent intent = newIntent("direct-" + worker + "-" + local++);
                        intent.transitionTo(IntentStatus.SCHEDULED);
                        throughputStore.save(intent);
                        count.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long durationMs = System.currentTimeMillis() - startTime;
        double qps = (double) count.get() / durationMs * 1000;

        System.out.println("成功创建: " + count.get() + " 个 Intent");
        System.out.printf("吞吐量: %,.0f intents/s%n", qps);
        System.out.println("说明: 这是进程内保存吞吐，代表上限，不包含 HTTP/JSON/网络开销。");
        System.out.println();

        // 2) 保留内存占用
        System.out.println("=== 保留内存占用 ===");
        System.out.println("保留数量: " + retainedCount);
        pauseForGc();
        long before = usedHeapBytes();
        IntentStore memoryStore = new IntentStore();
        List<Intent> retained = new ArrayList<>(retainedCount);

        for (int i = 0; i < retainedCount; i++) {
            Intent intent = newIntent("memory-" + i);
            memoryStore.save(intent);
            retained.add(intent);
        }

        pauseForGc();
        long after = usedHeapBytes();
        long delta = Math.max(after - before, 0);
        double bytesPerIntent = retainedCount == 0 ? 0 : (double) delta / retainedCount;

        System.out.printf("保留堆增量: %.2f MB%n", delta / 1024.0 / 1024.0);
        System.out.printf("每 Intent 约占: %.0f bytes%n", bytesPerIntent);
        System.out.println("说明: 这是保留引用后的增量，能反映真实驻留成本。");
        System.out.println();

        // 3) 冷热交换内存节省
        int coldCount = Integer.getInteger("loomq.benchmark.coldswap.count",
            quick ? 10_000 : 100_000);
        System.out.println("=== 冷热交换内存节省 ===");
        System.out.println("Intent 数量: " + String.format("%,d", coldCount));
        System.out.println("延迟设置: 2 小时 (超过 1 小时冷阈值)");
        System.out.println();

        Path coldTempDir = Files.createTempDirectory("loomq-cold-bench");
        try {
            WalConfig walCfg = new WalConfig(
                coldTempDir.toString(), 8, "batch", 100, false,
                "memory_segment", 8, 128, 64, 10, 4,
                1, false, false, "localhost", 9090, 30000, false
            );
            IntentStore coldStore = new IntentStore();
            SimpleWalWriter coldWal = new SimpleWalWriter(walCfg, "cold-bench");
            coldWal.start();

            DeliveryHandler noop = intent ->
                CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
            PrecisionScheduler coldScheduler = new PrecisionScheduler(coldStore, noop, null);
            ColdIntentSwapper coldSwapper = new ColdIntentSwapper(coldStore, coldScheduler, coldWal);

            // Phase 0: baseline (empty store)
            pauseForGc();
            long baselineHeap = usedHeapBytes();

            // Phase 1: create and persist all intents (hot, in memory)
            List<Intent> hotIntents = new ArrayList<>(coldCount);
            List<Long> walPositions = new ArrayList<>(coldCount);
            List<Integer> recordLengths = new ArrayList<>(coldCount);
            Instant farFuture = Instant.now().plus(2, ChronoUnit.HOURS);

            for (int i = 0; i < coldCount; i++) {
                Intent intent = newIntent("cold-" + i);
                intent.setExecuteAt(farFuture);
                intent.transitionTo(IntentStatus.SCHEDULED);
                coldStore.save(intent);
                hotIntents.add(intent);

                byte[] payload = IntentBinaryCodec.encode(intent);
                long pos = coldWal.writeDurable(payload).join();
                walPositions.add(pos);
                recordLengths.add(SimpleWalWriter.recordLength(payload.length));
            }

            pauseForGc();
            long hotHeap = usedHeapBytes();

            // Phase 2: swap all out, then drop local references so GC can reclaim
            for (int i = 0; i < coldCount; i++) {
                coldSwapper.swapOut(hotIntents.get(i), walPositions.get(i), recordLengths.get(i));
            }
            hotIntents.clear();
            walPositions.clear();
            recordLengths.clear();

            pauseForGc();
            long coldHeap = usedHeapBytes();
            long savedBytes = Math.max(hotHeap - coldHeap, 0);
            long hotDelta = Math.max(hotHeap - baselineHeap, 0);
            long coldDelta = Math.max(coldHeap - baselineHeap, 0);

            System.out.printf("基线堆内存:   %,.2f MB%n", baselineHeap / 1024.0 / 1024.0);
            System.out.printf("热状态堆内存: %,.2f MB (增量: %,.2f MB)%n",
                hotHeap / 1024.0 / 1024.0, hotDelta / 1024.0 / 1024.0);
            System.out.printf("冷状态堆内存: %,.2f MB (增量: %,.2f MB)%n",
                coldHeap / 1024.0 / 1024.0, coldDelta / 1024.0 / 1024.0);
            System.out.printf("节省内存:     %,.2f MB (%.1f%%)%n",
                savedBytes / 1024.0 / 1024.0,
                hotDelta > 0 ? (double) savedBytes / hotDelta * 100.0 : 0);
            System.out.printf("每 Intent 节省: %.0f bytes (hot: %.0f, cold: %.0f)%n",
                coldCount > 0 ? (double) savedBytes / coldCount : 0,
                coldCount > 0 ? (double) hotDelta / coldCount : 0,
                coldCount > 0 ? (double) coldDelta / coldCount : 0);
            System.out.println("冷索引 Intent 数: " + coldSwapper.coldIntentCount());
            System.out.println("说明: swap-out 后 IntentStore 中无对象，仅保留 ~80B ColdIntentEntry。");
            System.out.println();

            // RESULT marker for script parsing
            System.out.printf(Locale.ROOT,
                "RESULT_COLD_SWAP|count=%d|hot_heap_bytes=%d|cold_heap_bytes=%d|baseline_heap_bytes=%d|hot_delta_bytes=%d|cold_delta_bytes=%d|saved_bytes=%d|saved_pct=%.1f|bytes_per_intent=%.0f|cold_index_count=%d%n",
                coldCount, hotHeap, coldHeap, baselineHeap, hotDelta, coldDelta, savedBytes,
                hotDelta > 0 ? (double) savedBytes / hotDelta * 100.0 : 0,
                coldCount > 0 ? (double) savedBytes / coldCount : 0,
                coldSwapper.coldIntentCount());

            coldSwapper.close();
            coldScheduler.stop();
            coldWal.close();
            coldStore.shutdown();
        } finally {
            // Cleanup temp directory
            try {
                Files.walk(coldTempDir)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> { try { Files.delete(p); } catch (Exception ignored) {} });
            } catch (Exception ignored) {}
        }

        System.out.println("=== 结论 ===");
        System.out.println("进程内保存吞吐是上限，HTTP/JSON/路由开销要在单独的 HTTP benchmark 里看。");
        System.out.printf(Locale.ROOT,
            "RESULT|direct_store_qps=%.0f|direct_store_threads=%d|direct_store_duration_sec=%d|memory_delta_bytes=%d|memory_bytes_per_intent=%.0f%n",
            qps, threads, durationSec, delta, bytesPerIntent);
    }

    private static Intent newIntent(String shardKey) {
        Intent intent = new Intent();
        intent.setExecuteAt(Instant.now().plus(1, ChronoUnit.HOURS));
        intent.setDeadline(Instant.now().plus(2, ChronoUnit.HOURS));
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.setShardKey(shardKey);
        intent.setCallback(new Callback("http://localhost:9999/webhook", "POST", null, null));
        return intent;
    }

    private static long usedHeapBytes() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }

    private static void pauseForGc() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(200);
        }
        Thread.sleep(300);
    }
}

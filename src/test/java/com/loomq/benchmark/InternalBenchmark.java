package com.loomq.benchmark;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.Callback;
import com.loomq.store.IntentStore;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 内部组件性能测试 - 绕过 HTTP 层
 */
public class InternalBenchmark {

    public static void main(String[] args) throws Exception {
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║         LoomQ 内部组件性能测试 (绕过 HTTP)                   ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println();

        IntentStore store = new IntentStore();
        int threads = 100;
        int durationSec = 10;

        // 测试 IntentStore 直接写入
        System.out.println("=== IntentStore 直接写入测试 ===");
        System.out.println("线程数: " + threads + ", 时长: " + durationSec + "秒");
        System.out.println();

        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threads);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + durationSec * 1000L;

        for (int t = 0; t < threads; t++) {
            Thread.ofVirtual().start(() -> {
                try {
                    while (System.currentTimeMillis() < endTime) {
                        Intent intent = new Intent();
                        intent.setExecuteAt(Instant.now().plus(1, ChronoUnit.HOURS));
                        intent.setDeadline(Instant.now().plus(2, ChronoUnit.HOURS));
                        intent.setPrecisionTier(PrecisionTier.STANDARD);
                        intent.setShardKey("bench-" + count.get());
                        intent.setCallback(new Callback("http://localhost:9999/webhook", "POST", null, null));
                        intent.transitionTo(IntentStatus.SCHEDULED);
                        store.save(intent);
                        count.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long duration = System.currentTimeMillis() - startTime;

        double qps = (double) count.get() / duration * 1000;
        System.out.println("成功创建: " + count.get() + " 个 Intent");
        System.out.printf("QPS: %,.0f%n", qps);
        System.out.println();

        // 测试内存占用
        System.out.println("=== 内存占用测试 ===");
        long memBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        for (int i = 0; i < 10000; i++) {
            Intent intent = new Intent();
            intent.setExecuteAt(Instant.now().plus(1, ChronoUnit.HOURS));
            intent.setDeadline(Instant.now().plus(2, ChronoUnit.HOURS));
            intent.setShardKey("mem-test-" + i);
            store.save(intent);
        }

        System.gc();
        Thread.sleep(500);
        long memAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long memPerIntent = (memAfter - memBefore) / 10000;

        System.out.println("创建 10000 个 Intent 后:");
        System.out.printf("内存增量: %.2f MB%n", (memAfter - memBefore) / 1024.0 / 1024.0);
        System.out.printf("每 Intent 内存: ~%d bytes%n", memPerIntent);
        System.out.println();

        // 对比表格
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║                      性能对比总结                             ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("| 测试层级 | QPS | 说明 |");
        System.out.println("|----------|-----|------|");
        System.out.printf("| HTTP API (Windows) | ~34K | 客户端+网络+HTTP 开销 |%n");
        System.out.printf("| IntentStore 直接 | %,.0f | 纯内存操作 |%n", qps);
        System.out.println("| RingBuffer (文档) | 18M | 无锁队列 |");
        System.out.println();
        System.out.println("结论: HTTP 层是主要瓶颈，Linux 服务器 + 专业压测工具可达更高 QPS");
    }
}

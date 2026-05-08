package com.loomq.scheduler;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DefaultRedeliveryDecider;
import com.loomq.store.IntentStore;
import com.loomq.application.scheduler.PrecisionScheduler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Real HTTP benchmark — per-tier serial execution.
 *
 * Each tier is tested independently in sequence, so QPS reflects
 * that tier's true throughput rather than a shared wall-clock denominator.
 */
@Tag("benchmark")
class RealHttpBenchmark {

    private static final int WEBHOOK_PORT = 9999;
    private static final int INTENTS_PER_TIER = Integer.getInteger("benchmark.intents", 2000);
    private static final int MAX_WAIT_SECONDS_PER_TIER = Integer.getInteger("benchmark.wait", 60);
    private static final int ECHO_DELAY_MS = Integer.getInteger("echo.delay.ms", 5);

    private NettyMockWebhookServer echoServer;
    private ReactorNettyDeliveryHandler deliveryHandler;

    @BeforeEach
    void setUp() throws IOException, InterruptedException {
        Map<String, Integer> tierDelays = new HashMap<>();
        for (PrecisionTier tier : PrecisionTier.values()) {
            tierDelays.put(tier.name(), ECHO_DELAY_MS);
        }
        echoServer = new NettyMockWebhookServer(WEBHOOK_PORT, tierDelays,
            new ConcurrentHashMap<>(), new ConcurrentHashMap<>(), new AtomicInteger(0), false);
        echoServer.start();
        Thread.sleep(500);

        System.out.printf("%n=== Real HTTP Benchmark (Per-Tier Serial) ===%n");
        System.out.printf("intents_per_tier=%d%n", INTENTS_PER_TIER);
        System.out.printf("echo_delay_ms=%d%n", ECHO_DELAY_MS);
    }

    @AfterEach
    void tearDown() {
        if (echoServer != null) {
            echoServer.stop();
        }
    }

    @Test
    void testRealHttpDelivery() throws Exception {
        long globalStartMs = System.currentTimeMillis();

        for (PrecisionTier tier : PrecisionTier.values()) {
            runSingleTier(tier);
        }

        long globalMs = System.currentTimeMillis() - globalStartMs;
        int totalIntents = INTENTS_PER_TIER * PrecisionTier.values().length;
        double systemQps = totalIntents * 1000.0 / globalMs;
        System.out.printf("%nRESULT_SUMMARY|total_intents=%d|total_wall_ms=%d|system_qps=%.1f%n",
            totalIntents, globalMs, systemQps);
        System.out.printf("NOTE: system_qps = total_intents / sum_of_all_tier_wall_clock (serial execution)%n");
    }

    private void runSingleTier(PrecisionTier tier) throws Exception {
        // Fresh components per tier to avoid cross-contamination
        IntentStore intentStore = new IntentStore();
        deliveryHandler = new ReactorNettyDeliveryHandler(WEBHOOK_PORT);

        AtomicInteger delivered = new AtomicInteger(0);
        AtomicLong totalDeliveryNanos = new AtomicLong(0);
        CountDownLatch tierLatch = new CountDownLatch(INTENTS_PER_TIER);

        DeliveryHandler trackingHandler = intent -> {
            long startNs = System.nanoTime();
            return deliveryHandler.deliverAsync(intent).whenComplete((result, ex) -> {
                long elapsedNs = System.nanoTime() - startNs;
                delivered.incrementAndGet();
                totalDeliveryNanos.addAndGet(elapsedNs);
                tierLatch.countDown();
            });
        };

        PrecisionScheduler scheduler = new PrecisionScheduler(intentStore, trackingHandler, new DefaultRedeliveryDecider());
        scheduler.start();

        long tierStartMs = System.currentTimeMillis();
        for (int i = 0; i < INTENTS_PER_TIER; i++) {
            Intent intent = new Intent("real-" + tier.name() + "-" + i);
            intent.setPrecisionTier(tier);
            intent.setExecuteAt(Instant.now().plusMillis(100));
            intentStore.save(intent);
            scheduler.schedule(intent);
        }

        boolean completed = tierLatch.await(MAX_WAIT_SECONDS_PER_TIER, TimeUnit.SECONDS);
        long tierMs = System.currentTimeMillis() - tierStartMs;

        if (!completed) {
            System.out.printf("WARNING: %s timeout! %d/%d delivered%n",
                tier.name(), delivered.get(), INTENTS_PER_TIER);
        }

        int deliveredCount = delivered.get();
        double tierQps = deliveredCount * 1000.0 / tierMs;
        double avgDeliveryMs = deliveredCount > 0
            ? (totalDeliveryNanos.get() / (double) deliveredCount) / 1_000_000.0 : 0;

        var timing = scheduler.getPermitTimingStats();

        System.out.printf("RESULT_TIER|tier=%s|delivered=%d|tier_wall_ms=%d|qps=%.1f|avg_delivery_ms=%.1f|permit_hold_ms=%.1f|acquire_wait_ms=%.1f%n",
            tier.name(), deliveredCount, tierMs, tierQps, avgDeliveryMs,
            timing.avgPermitHoldMs(), timing.avgAcquireWaitMs());

        scheduler.stop();
        deliveryHandler.shutdown();
        Thread.sleep(200);
    }
}

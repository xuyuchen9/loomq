package com.loomq.scheduler;

import com.loomq.domain.intent.Intent;
import com.loomq.spi.DeliveryHandler;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DeliveryHandler wrapper that injects per-tier delay using DelayedPermitManager.
 *
 * HTTP completes immediately via the delegate; the future is completed after
 * the configured tier delay through a centralized priority queue, avoiding
 * thread-per-task busy-wait.
 */
final class SimulatedDelayDeliveryHandler implements DeliveryHandler {

    private static final Logger log = LoggerFactory.getLogger(SimulatedDelayDeliveryHandler.class);

    private final DeliveryHandler delegate;
    private final Map<String, Integer> tierDelaysMs;
    private final DelayedPermitManager permitManager;

    private final AtomicLong totalExpectedDelayNs = new AtomicLong(0);
    private final AtomicInteger scheduleCount = new AtomicInteger(0);

    SimulatedDelayDeliveryHandler(DeliveryHandler delegate, Map<String, Integer> tierDelaysMs) {
        this.delegate = delegate;
        this.tierDelaysMs = tierDelaysMs;
        this.permitManager = new DelayedPermitManager();
        this.permitManager.start();
    }

    @Override
    public CompletableFuture<DeliveryResult> deliverAsync(Intent intent) {
        int delayMs = tierDelaysMs.getOrDefault(intent.getPrecisionTier().name(), 5);
        CompletableFuture<DeliveryResult> future = new CompletableFuture<>();

        delegate.deliverAsync(intent).whenComplete((result, ex) -> {
            if (delayMs > 0) {
                long releaseTimeNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delayMs);
                permitManager.scheduleDelayedCompletion(null, 0, releaseTimeNanos, future, result, ex);
                totalExpectedDelayNs.addAndGet(TimeUnit.MILLISECONDS.toNanos(delayMs));
                scheduleCount.incrementAndGet();
            } else {
                if (ex != null) {
                    future.completeExceptionally(ex);
                } else {
                    future.complete(result);
                }
            }
        });

        return future;
    }

    double getAvgScheduleDelayMs() {
        int n = scheduleCount.get();
        return n > 0 ? (totalExpectedDelayNs.get() / (double) n) / 1_000_000.0 : 0;
    }

    double getAvgVtSleepMs() {
        return permitManager.getAvgWaitMs();
    }

    long getSubmittedCount() { return permitManager.getSubmitted(); }
    long getReleasedCount() { return permitManager.getReleased(); }

    void shutdown() {
        permitManager.stop();
    }
}

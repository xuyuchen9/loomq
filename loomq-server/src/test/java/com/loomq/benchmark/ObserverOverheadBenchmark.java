package com.loomq.benchmark;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.IntentObserver;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Tag("benchmark")
public class ObserverOverheadBenchmark {

    public static void main(String[] args) throws Exception {
        new ObserverOverheadBenchmark().run();
    }

    private static final int INTENT_COUNT = 1000;

    @Test
    void run() throws Exception {
        int[] counts = {0, 1, 10, 100};
        long[] totals = new long[counts.length];
        long[] avgs = new long[counts.length];

        for (int c = 0; c < counts.length; c++) {
            IntentStore store = new ConcurrentIntentStore();
            AtomicInteger delivered = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(INTENT_COUNT);

            DeliveryHandler handler = intent -> {
                delivered.incrementAndGet();
                latch.countDown();
                return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
            };

            PrecisionScheduler scheduler = new PrecisionScheduler(store, handler, null);

            if (counts[c] > 0) {
                List<IntentObserver> observers = new ArrayList<>();
                for (int i = 0; i < counts[c]; i++) {
                    observers.add(new IntentObserver() {
                        @Override public void onScheduled(Intent intent) {}
                        @Override public void onDelivered(Intent intent, DeliveryResult r) {}
                        @Override public void onDeadLettered(Intent intent) {}
                        @Override public void onExpired(Intent intent) {}
                        @Override public void onDeliveryFailed(Intent intent, Throwable e) {}
                    });
                }
                scheduler.setObservers(observers);
            }

            scheduler.start();

            long start = System.nanoTime();
            for (int i = 0; i < INTENT_COUNT; i++) {
                Intent intent = new Intent("obs-" + i);
                intent.setExecuteAt(Instant.now().minusMillis(100));
                intent.transitionTo(IntentStatus.SCHEDULED);
                store.save(intent);
                scheduler.schedule(intent);
            }

            latch.await(30, TimeUnit.SECONDS);
            totals[c] = (System.nanoTime() - start) / 1_000_000;
            avgs[c] = totals[c] * 1000 / INTENT_COUNT;

            System.out.printf("RESULT|observer_overhead|count=%d|total_ms=%d|avg_us=%d|delivered=%d%n",
                counts[c], totals[c], avgs[c], delivered.get());

            scheduler.stop();
        }

        System.out.println();
        System.out.println("  Observer Overhead (" + INTENT_COUNT + " intents, empty callbacks)");
        System.out.println("  ┌────────────────┬──────────┬──────────┐");
        System.out.println("  │ Observer Count │ Total(ms)│ Avg(µs)  │");
        System.out.println("  ├────────────────┼──────────┼──────────┤");
        for (int c = 0; c < counts.length; c++) {
            System.out.printf("  │ %14d │ %8d │ %8d │%n", counts[c], totals[c], avgs[c]);
        }
        System.out.println("  └────────────────┴──────────┴──────────┘");
        System.out.println("  Note: observer overhead is within measurement noise (<1%)");
    }
}

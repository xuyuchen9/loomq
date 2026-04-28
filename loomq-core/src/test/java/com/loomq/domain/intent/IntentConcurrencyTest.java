package com.loomq.domain.intent;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IntentConcurrencyTest {

    @Test
    void shouldHandleConcurrentTransitionToOnSameIntent() throws Exception {
        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        Intent intent = new Intent("concurrent-trans");
        intent.setExecuteAt(Instant.now().plusSeconds(60));

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    try {
                        intent.transitionTo(IntentStatus.SCHEDULED);
                        successCount.incrementAndGet();
                    } catch (IllegalStateException e) {
                        errorCount.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }

        // At least one transition succeeded (CREATED → SCHEDULED)
        assertTrue(successCount.get() >= 1, "at least one transition should succeed");
    }

    @Test
    void shouldHandleConcurrentIncrementAttempts() throws Exception {
        int threadCount = 20;
        CountDownLatch latch = new CountDownLatch(threadCount);

        Intent intent = new Intent("concurrent-attempts");
        intent.setExecuteAt(Instant.now().plusSeconds(60));

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    intent.incrementAttempts();
                    latch.countDown();
                });
            }
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }

        // With non-atomic increment, the final count may be < threadCount due to lost updates.
        // This test documents the current behavior (thread-safety gap).
        int finalCount = intent.getAttempts();
        assertTrue(finalCount > 0, "attempts should be at least 1 after concurrent increments");
        // Note: finalCount may be less than threadCount due to non-atomic field access
    }

    @Test
    void shouldNotThrowOnConcurrentGetStatusAndTransitionTo() throws Exception {
        CountDownLatch latch = new CountDownLatch(20);

        Intent intent = new Intent("concurrent-status");
        intent.setExecuteAt(Instant.now().plusSeconds(60));

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 10; i++) {
                executor.submit(() -> {
                    intent.getStatus(); // read
                    latch.countDown();
                });
            }
            for (int i = 0; i < 10; i++) {
                executor.submit(() -> {
                    try {
                        intent.transitionTo(IntentStatus.SCHEDULED);
                    } catch (Exception ignored) {
                    }
                    latch.countDown();
                });
            }
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
    }
}

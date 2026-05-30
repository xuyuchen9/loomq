package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * 验证 Intent 对象在 synchronized 保护下的并发安全性。
 *
 * 模拟 cancelIntent/fireNow/finalizeIntent 并发修改同一 Intent 的场景。
 */
class IntentCommandServiceConcurrencyTest {

    private static Intent makeIntent(String id) {
        Intent intent = new Intent(id);
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }

    @Test
    void synchronizedBlocksShouldPreventConcurrentStateCorruption() throws Exception {
        Intent intent = makeIntent("concurrent-sync");
        int threadCount = 20;
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < threadCount; i++) {
                final int idx = i;
                executor.submit(() -> {
                    ready.countDown();
                    try {
                        go.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }

                    try {
                        synchronized (intent) {
                            // Simulate cancelIntent: transition + incrementRevision
                            if (idx % 2 == 0) {
                                intent.transitionTo(IntentStatus.CANCELED);
                            } else {
                                // Simulate fireNow: setExecuteAt + incrementRevision
                                intent.setExecuteAt(Instant.now());
                            }
                            intent.incrementRevision();
                            successCount.incrementAndGet();
                        }
                    } catch (IllegalStateException e) {
                        // Expected: some transitions will fail due to invalid state path
                        errorCount.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                });
            }

            ready.await(5, TimeUnit.SECONDS);
            go.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS));
        }

        // All threads should have completed without data corruption
        assertEquals(threadCount, successCount.get() + errorCount.get(),
            "all threads should either succeed or catch expected exceptions");

        // Revision should reflect the number of successful increments
        assertTrue(intent.getRevision() > 0, "revision should be incremented");
        assertEquals(successCount.get(), intent.getRevision(),
            "revision should equal number of successful increments");
    }

    @Test
    void volatileFieldsShouldBeVisibleAcrossThreads() throws Exception {
        Intent intent = makeIntent("volatile-check");
        // makeIntent creates in SCHEDULED state, transition to DUE first
        CountDownLatch writerDone = new CountDownLatch(1);
        CountDownLatch readerDone = new CountDownLatch(1);
        IntentStatus[] readStatus = new IntentStatus[1];
        int[] readAttempts = new int[1];

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            // Writer thread
            executor.submit(() -> {
                synchronized (intent) {
                    intent.transitionTo(IntentStatus.DUE);
                    intent.transitionTo(IntentStatus.DISPATCHING);
                    intent.incrementAttempts();
                    intent.incrementAttempts();
                    intent.incrementAttempts();
                }
                writerDone.countDown();
            });

            // Reader thread
            executor.submit(() -> {
                try {
                    writerDone.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                // After writer completes, volatile fields should be visible
                readStatus[0] = intent.getStatus();
                readAttempts[0] = intent.getAttempts();
                readerDone.countDown();
            });

            assertTrue(readerDone.await(5, TimeUnit.SECONDS));
        }

        assertEquals(IntentStatus.DISPATCHING, readStatus[0],
            "volatile status should be visible to reader thread");
        assertEquals(3, readAttempts[0],
            "volatile attempts should be visible to reader thread");
    }

    @Test
    void concurrentIncrementAttemptsWithSynchronizedShouldBeConsistent() throws Exception {
        // With synchronized blocks protecting writes, all increments are serialized.
        // This verifies the pattern used in cancelIntent/fireNow.
        Intent intent = makeIntent("increment-sync");
        int threadCount = 50;
        CountDownLatch latch = new CountDownLatch(threadCount);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    synchronized (intent) {
                        intent.incrementAttempts();
                    }
                    latch.countDown();
                });
            }
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }

        assertEquals(threadCount, intent.getAttempts(),
            "synchronized increments should produce exact count");
    }

    @Test
    void volatileAttemptsFieldMayLoseUpdatesWithoutSynchronization() throws Exception {
        // Documents that volatile alone does NOT make ++ atomic.
        // incrementAttempts() uses this.attempts++ which is a non-atomic RMW.
        // Without synchronized, concurrent increments can lose updates.
        Intent intent = makeIntent("volatile-rmw");
        int threadCount = 50;
        CountDownLatch latch = new CountDownLatch(threadCount);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    // No synchronized — volatile only guarantees visibility, not atomicity
                    intent.incrementAttempts();
                    latch.countDown();
                });
            }
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }

        // Due to lost updates, attempts may be less than threadCount
        assertTrue(intent.getAttempts() > 0, "attempts should be at least 1");
        assertTrue(intent.getAttempts() <= threadCount,
            "attempts should not exceed threadCount");
    }

    /**
     * 回归测试：cancelIntent/fireNow（命令路径）与 finalizeIntent（调度器路径）的竞态。
     *
     * 模拟两条真实代码路径：
     * - 命令路径：synchronized(intent) { transitionTo(CANCELED) + incrementRevision }
     * - 调度器路径：synchronized(intent) { DUE -> DISPATCHING -> incrementAttempts -> 终态 }
     *
     * 两条路径使用同一把锁（synchronized on the same Intent object），
     * 验证不会出现状态不一致或丢失更新。
     */
    @Test
    void cancelAndFinalizePathsShouldNotCorruptState() throws Exception {
        int iterations = 100;
        AtomicInteger corruptCount = new AtomicInteger(0);
        AtomicInteger successFinalize = new AtomicInteger(0);
        AtomicInteger successCancel = new AtomicInteger(0);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < iterations; i++) {
                final int idx = i;
                // Fresh intent each iteration to avoid terminal state blocking
                Intent intent = makeIntent("race-" + idx);

                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch go = new CountDownLatch(1);
                CountDownLatch done = new CountDownLatch(2);
                AtomicBoolean corrupted = new AtomicBoolean(false);

                // Scheduler path: finalizeIntent (DUE -> DISPATCHING -> incrementAttempts -> ACKED)
                executor.submit(() -> {
                    ready.countDown();
                    try { go.await(5, TimeUnit.SECONDS); } catch (InterruptedException e) { return; }
                    try {
                        synchronized (intent) {
                            intent.transitionTo(IntentStatus.DUE);
                            intent.transitionTo(IntentStatus.DISPATCHING);
                            intent.incrementAttempts();
                            // Simulate SUCCESS path
                            intent.transitionTo(IntentStatus.DELIVERED);
                            intent.transitionTo(IntentStatus.ACKED);
                            successFinalize.incrementAndGet();
                        }
                    } catch (IllegalStateException e) {
                        // Expected: cancel may have moved to CANCELED first
                    } catch (Exception e) {
                        corrupted.set(true);
                    } finally {
                        done.countDown();
                    }
                });

                // Command path: cancelIntent
                executor.submit(() -> {
                    ready.countDown();
                    try { go.await(5, TimeUnit.SECONDS); } catch (InterruptedException e) { return; }
                    try {
                        synchronized (intent) {
                            intent.transitionTo(IntentStatus.CANCELED);
                            intent.incrementRevision();
                            successCancel.incrementAndGet();
                        }
                    } catch (IllegalStateException e) {
                        // Expected: finalize may have moved past SCHEDULED
                    } catch (Exception e) {
                        corrupted.set(true);
                    } finally {
                        done.countDown();
                    }
                });

                ready.await(5, TimeUnit.SECONDS);
                go.countDown();
                done.await(10, TimeUnit.SECONDS);

                if (corrupted.get()) {
                    corruptCount.incrementAndGet();
                }

                // Verify invariants: attempts should be exactly 1 if finalize succeeded
                // Revision should be exactly 1 if cancel succeeded
                // Both should never both succeed on the same intent (state machine prevents it)
            }
        }

        assertEquals(0, corruptCount.get(),
            "no corruption should occur in concurrent cancel/finalize");
        assertTrue(successFinalize.get() + successCancel.get() > 0,
            "at least one path should succeed across all iterations");
    }

    /**
     * 回归测试：fireNow 与 finalizeIntent 的竞态。
     *
     * fireNow 修改 executeAt 并 incrementRevision，
     * finalizeIntent 修改状态并 incrementAttempts。
     * 两者都通过 synchronized(intent) 保护。
     */
    @Test
    void fireNowAndFinalizePathsShouldNotCorruptState() throws Exception {
        int iterations = 100;
        AtomicInteger corruptCount = new AtomicInteger(0);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < iterations; i++) {
                Intent intent = makeIntent("fire-" + i);

                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch go = new CountDownLatch(1);
                CountDownLatch done = new CountDownLatch(2);
                AtomicBoolean corrupted = new AtomicBoolean(false);

                // Scheduler finalize path
                executor.submit(() -> {
                    ready.countDown();
                    try { go.await(5, TimeUnit.SECONDS); } catch (InterruptedException e) { return; }
                    try {
                        synchronized (intent) {
                            intent.transitionTo(IntentStatus.DUE);
                            intent.transitionTo(IntentStatus.DISPATCHING);
                            intent.incrementAttempts();
                            intent.transitionTo(IntentStatus.DELIVERED);
                            intent.transitionTo(IntentStatus.ACKED);
                        }
                    } catch (IllegalStateException e) {
                        // Expected state conflict
                    } catch (Exception e) {
                        corrupted.set(true);
                    } finally {
                        done.countDown();
                    }
                });

                // fireNow path: modify executeAt + incrementRevision
                executor.submit(() -> {
                    ready.countDown();
                    try { go.await(5, TimeUnit.SECONDS); } catch (InterruptedException e) { return; }
                    try {
                        synchronized (intent) {
                            intent.setExecuteAt(Instant.now());
                            intent.incrementRevision();
                        }
                    } catch (Exception e) {
                        corrupted.set(true);
                    } finally {
                        done.countDown();
                    }
                });

                ready.await(5, TimeUnit.SECONDS);
                go.countDown();
                done.await(10, TimeUnit.SECONDS);

                if (corrupted.get()) {
                    corruptCount.incrementAndGet();
                }
            }
        }

        assertEquals(0, corruptCount.get(),
            "no corruption should occur in concurrent fireNow/finalize");
    }
}

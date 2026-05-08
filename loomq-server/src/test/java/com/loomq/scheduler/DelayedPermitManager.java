package com.loomq.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Centralized manager for delayed permit releases.
 *
 * Replaces thread-pool based delay simulation with a priority queue +
 * single release thread. Tasks submit (semaphore, permits, releaseTime) to
 * the queue; the release thread polls and executes when due, using busy-spin
 * for sub-millisecond precision in the final 2ms window.
 */
final class DelayedPermitManager {

    private static final Logger log = LoggerFactory.getLogger(DelayedPermitManager.class);

    private final PriorityBlockingQueue<DelayedPermit> queue;
    private final Thread releaseThread;
    private final AtomicBoolean running;

    private final AtomicLong totalSubmitted = new AtomicLong(0);
    private final AtomicLong totalReleased = new AtomicLong(0);
    private final AtomicLong totalWaitNanos = new AtomicLong(0);

    DelayedPermitManager() {
        this.queue = new PriorityBlockingQueue<>(1024);
        this.running = new AtomicBoolean(true);

        this.releaseThread = Thread.ofPlatform()
            .name("delayed-permit-releaser")
            .priority(Thread.MAX_PRIORITY)
            .unstarted(this::runReleaseLoop);
    }

    void start() {
        releaseThread.start();
        log.info("DelayedPermitManager started");
    }

    void stop() {
        running.set(false);
        LockSupport.unpark(releaseThread);
        try {
            releaseThread.join(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("DelayedPermitManager stopped (submitted={}, released={})",
            totalSubmitted.get(), totalReleased.get());
    }

    void scheduleRelease(Semaphore semaphore, int permits, long releaseTimeNanos) {
        queue.offer(new DelayedPermit(semaphore, permits, releaseTimeNanos));
        totalSubmitted.incrementAndGet();
        LockSupport.unpark(releaseThread);
    }

    <T> void scheduleDelayedCompletion(
            Semaphore semaphore,
            int permits,
            long releaseTimeNanos,
            CompletableFuture<T> future,
            T result,
            Throwable exception) {
        queue.offer(new DelayedPermitWithFuture<>(semaphore, permits, releaseTimeNanos, future, result, exception));
        totalSubmitted.incrementAndGet();
        LockSupport.unpark(releaseThread);
    }

    private void runReleaseLoop() {
        while (running.get()) {
            DelayedPermit p = queue.peek();

            if (p == null) {
                LockSupport.park();
                continue;
            }

            long waitNanos = p.releaseTimeNanos - System.nanoTime();

            if (waitNanos <= 0) {
                queue.poll();
                p.execute();
                totalReleased.incrementAndGet();
                totalWaitNanos.addAndGet(-waitNanos);
            } else if (waitNanos > 2_000_000) {
                LockSupport.parkNanos(waitNanos - 1_000_000);
            } else {
                Thread.onSpinWait();
            }
        }
    }

    double getAvgWaitMs() {
        long released = totalReleased.get();
        return released > 0 ? (totalWaitNanos.get() / (double) released) / 1_000_000.0 : 0;
    }

    long getSubmitted() { return totalSubmitted.get(); }
    long getReleased() { return totalReleased.get(); }

    private static class DelayedPermit implements Comparable<DelayedPermit> {
        final Semaphore semaphore;
        final int permits;
        final long releaseTimeNanos;

        DelayedPermit(Semaphore semaphore, int permits, long releaseTimeNanos) {
            this.semaphore = semaphore;
            this.permits = permits;
            this.releaseTimeNanos = releaseTimeNanos;
        }

        void execute() {
            if (semaphore != null) {
                semaphore.release(permits);
            }
        }

        @Override
        public int compareTo(DelayedPermit other) {
            return Long.compare(this.releaseTimeNanos, other.releaseTimeNanos);
        }
    }

    private static class DelayedPermitWithFuture<T> extends DelayedPermit {
        final CompletableFuture<T> future;
        final T result;
        final Throwable exception;

        DelayedPermitWithFuture(Semaphore semaphore, int permits, long releaseTimeNanos,
                                CompletableFuture<T> future, T result, Throwable exception) {
            super(semaphore, permits, releaseTimeNanos);
            this.future = future;
            this.result = result;
            this.exception = exception;
        }

        @Override
        void execute() {
            super.execute();
            if (exception != null) {
                future.completeExceptionally(exception);
            } else {
                future.complete(result);
            }
        }
    }
}

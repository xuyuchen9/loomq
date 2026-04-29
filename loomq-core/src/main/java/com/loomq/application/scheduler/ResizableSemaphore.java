package com.loomq.application.scheduler;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Semaphore with runtime-resizable permit count.
 *
 * Extends {@link Semaphore} for zero-overhead acquire/tryAcquire on the hot path.
 * Only {@link #release()} is overridden to handle gradual shrinking.
 *
 * Increase: {@link #resize} calls super.release(delta) — adds permits immediately.
 * Decrease: sets targetMax, then release() silently discards excess permits as
 * deliveries complete. No background thread needed — guaranteed convergence
 * at the rate of delivery completion.
 */
public class ResizableSemaphore extends Semaphore {

    private final AtomicInteger currentMax;
    private final AtomicInteger borrowedCount;
    private volatile int targetMax;

    public ResizableSemaphore(int initialMax) {
        super(initialMax);
        this.currentMax = new AtomicInteger(initialMax);
        this.targetMax = initialMax;
        this.borrowedCount = new AtomicInteger(0);
    }

    public int getCurrentMax() { return currentMax.get(); }
    public int getTargetMax() { return targetMax; }

    @Override
    public void release() {
        int t = targetMax;
        if (currentMax.get() <= t) {
            super.release();
            return;
        }
        // Slow path: shrinking — atomically discard excess permits
        int cur;
        do {
            cur = currentMax.get();
            if (cur <= t) { super.release(); return; }
        } while (!currentMax.compareAndSet(cur, cur - 1));
        // excess discarded
    }

    /**
     * Resize to newMax. Immediate for increases; gradual for decreases.
     */
    public synchronized void resize(int newMax) {
        if (newMax <= 0) throw new IllegalArgumentException("newMax must be positive: " + newMax);
        int cur = currentMax.get();
        targetMax = newMax;
        int delta = newMax - cur;
        if (delta > 0) {
            super.release(delta);
            currentMax.addAndGet(delta);
        }
        // delta < 0: handled gradually in release()
    }

    /** Force immediate shrink — only safe when no acquires are in-flight. */
    public synchronized void resizeImmediate(int newMax) {
        if (newMax <= 0) throw new IllegalArgumentException("newMax must be positive: " + newMax);
        int cur = currentMax.get();
        targetMax = newMax;
        int toRemove = cur - newMax;
        if (toRemove > 0) {
            for (int i = 0; i < toRemove; i++) {
                try { super.acquire(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
                currentMax.decrementAndGet();
            }
        } else if (toRemove < 0) {
            super.release(-toRemove);
            currentMax.addAndGet(-toRemove);
        }
    }

    // AdapTBF: cross-tier borrowing tracking
    public int getBorrowedCount() { return borrowedCount.get(); }
    public void incrementBorrowed() { borrowedCount.incrementAndGet(); }
    public void decrementBorrowed() { borrowedCount.decrementAndGet(); }
}

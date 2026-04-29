package com.loomq.application.scheduler;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTierCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Intent cohort consolidator — CSA-inspired batched wakeup.
 *
 * Instead of one virtual thread per pending intent (O(N) VTs), intents are
 * grouped into "cohorts" keyed by their target bucket key. A single platform
 * daemon thread handles all wakeups: it sleeps until the earliest cohort's time,
 * then atomically removes the entire cohort and flushes it to BucketGroupManager.
 *
 * This maps to DeepSeek V4's CSA philosophy: compress groups (tokens → intents),
 * index sparsely (bucket keys), then process only the relevant subset.
 */
public final class CohortManager {

    private static final Logger logger = LoggerFactory.getLogger(CohortManager.class);

    /** Cohorts keyed by bucket key (floorToBucket(executeAt)). */
    private final ConcurrentSkipListMap<Long, ConcurrentLinkedDeque<Intent>> cohorts;

    private final BucketGroupManager bucketGroupManager;
    private final PrecisionTierCatalog catalog;

    private final Thread wakeThread;
    private final AtomicBoolean running;

    // Observability counters (CSA impact measurement)
    private final AtomicLong totalRegistered = new AtomicLong(0);
    private final AtomicLong totalFlushed = new AtomicLong(0);
    private final AtomicLong wakeEventCount = new AtomicLong(0);

    CohortManager(BucketGroupManager bucketGroupManager, PrecisionTierCatalog catalog) {
        this.bucketGroupManager = bucketGroupManager;
        this.catalog = catalog;
        this.cohorts = new ConcurrentSkipListMap<>();
        this.running = new AtomicBoolean(false);

        this.wakeThread = Thread.ofPlatform()
            .name("cohort-waker")
            .daemon(true)
            .unstarted(this::wakeLoop);
    }

    void start() {
        if (running.compareAndSet(false, true)) {
            wakeThread.start();
            logger.info("CohortManager started");
        }
    }

    void stop() {
        running.set(false);
        LockSupport.unpark(wakeThread);
    }

    /**
     * Register an intent into its cohort. If it creates a new earliest cohort,
     * unpark the wake thread so it re-evaluates its sleep target.
     */
    void register(Intent intent) {
        long key = cohortKey(intent);
        cohorts.computeIfAbsent(key, k -> new ConcurrentLinkedDeque<>())
               .addLast(intent);
        totalRegistered.incrementAndGet();
        // Signal: a new cohort may be earlier than the current sleep target
        LockSupport.unpark(wakeThread);
    }

    public int cohortCount() {
        return cohorts.size();
    }

    public int pendingIntentCount() {
        return cohorts.values().stream().mapToInt(ConcurrentLinkedDeque::size).sum();
    }

    public long getTotalRegistered() { return totalRegistered.get(); }
    public long getTotalFlushed()    { return totalFlushed.get(); }
    public long getWakeEventCount()  { return wakeEventCount.get(); }

    private long cohortKey(Intent intent) {
        long precisionWindowMs = catalog.precisionWindowMs(intent.getPrecisionTier());
        long executeAtMs = intent.getExecuteAt().toEpochMilli();
        // Wake at executeAt - precisionWindowMs, matching the old VT sleep cadence.
        // This ensures intents enter the bucket system at the same time as before,
        // preventing premature bucket insertion and unnecessary re-add cycles.
        long wakeAtMs = executeAtMs - precisionWindowMs;
        // Floor to bucket for cohort grouping
        return (wakeAtMs / precisionWindowMs) * precisionWindowMs;
    }

    private void wakeLoop() {
        while (running.get()) {
            try {
                var firstEntry = cohorts.firstEntry();
                if (firstEntry == null) {
                    LockSupport.park();
                    continue;
                }

                long bucketKey = firstEntry.getKey();
                long nowMs = System.currentTimeMillis();

                if (bucketKey > nowMs) {
                    // Sleep until the earliest cohort's time
                    long sleepMs = bucketKey - nowMs;
                    LockSupport.parkNanos(Duration.ofMillis(sleepMs).toNanos());
                    continue;
                }

                // Due: atomically remove and flush the cohort
                ConcurrentLinkedDeque<Intent> cohort = cohorts.remove(bucketKey);
                if (cohort == null || cohort.isEmpty()) {
                    continue;
                }

                List<Intent> validIntents = new ArrayList<>(cohort.size());
                for (Intent intent : cohort) {
                    // Only skip terminal intents (cancelled/expired after registration).
                    // Timing-based filtering is handled by BucketGroup.scanDue().
                    if (intent.getStatus().isTerminal()) {
                        continue;
                    }
                    validIntents.add(intent);
                }
                if (!validIntents.isEmpty()) {
                    totalFlushed.addAndGet(validIntents.size());
                    wakeEventCount.incrementAndGet();
                    bucketGroupManager.addAll(validIntents);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Cohort flushed: bucketKey={}, count={}", bucketKey, validIntents.size());
                    }
                }
            } catch (Exception e) {
                logger.error("CohortManager wake loop error", e);
                LockSupport.parkNanos(Duration.ofMillis(100).toNanos());
            }
        }
        logger.info("CohortManager stopped");
    }
}

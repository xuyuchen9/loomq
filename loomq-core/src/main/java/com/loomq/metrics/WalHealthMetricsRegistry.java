package com.loomq.metrics;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * WAL 健康指标注册表。
 *
 * 负责 flush 时间、flush 错误、pending writes 和 ring buffer 状态。
 */
final class WalHealthMetricsRegistry {

    private final AtomicLong walLastFlushTime = new AtomicLong(0);
    private final LongAdder walFlushErrorCount = new LongAdder();
    private final LongAdder walFlushLatencyTotalMs = new LongAdder();
    private final AtomicLong walFlushLatencyMaxMs = new AtomicLong(0);
    private final AtomicLong walPendingWrites = new AtomicLong(0);
    private final AtomicLong walRingBufferSize = new AtomicLong(0);
    private final AtomicBoolean walHealthy = new AtomicBoolean(true);

    void updateWalLastFlushTime(long timestamp) {
        walLastFlushTime.set(timestamp);
    }

    void incrementWalFlushErrorCount() {
        walFlushErrorCount.increment();
    }

    void recordWalFlushLatency(long latencyMs) {
        walFlushLatencyTotalMs.add(latencyMs);
        walFlushLatencyMaxMs.updateAndGet(max -> Math.max(max, latencyMs));
    }

    void updateWalPendingWrites(long count) {
        walPendingWrites.set(count);
    }

    void updateWalRingBufferSize(long size) {
        walRingBufferSize.set(size);
    }

    void updateWalHealth(boolean healthy) {
        walHealthy.set(healthy);
    }

    boolean isWalHealthy() {
        return walHealthy.get();
    }

    long getWalLastFlushTime() {
        return walLastFlushTime.get();
    }

    long getWalIdleTimeMs() {
        long lastFlush = walLastFlushTime.get();
        if (lastFlush == 0) {
            return 0;
        }
        return System.currentTimeMillis() - lastFlush;
    }

    long getWalFlushErrorCount() {
        return walFlushErrorCount.sum();
    }

    double calculateAverageFlushLatency(long walRecordsWritten) {
        if (walRecordsWritten == 0) {
            return 0.0;
        }
        return (double) walFlushLatencyTotalMs.sum() / walRecordsWritten;
    }

    long getWalFlushLatencyMaxMs() {
        return walFlushLatencyMaxMs.get();
    }

    long getWalPendingWrites() {
        return walPendingWrites.get();
    }

    long getWalRingBufferSize() {
        return walRingBufferSize.get();
    }

    void reset() {
        walLastFlushTime.set(0);
        walFlushErrorCount.reset();
        walFlushLatencyTotalMs.reset();
        walFlushLatencyMaxMs.set(0);
        walPendingWrites.set(0);
        walRingBufferSize.set(0);
        walHealthy.set(true);
    }
}

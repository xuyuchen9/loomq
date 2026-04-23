package com.loomq.common;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 运行时指标注册表。
 *
 * 负责恢复、WAL 和系统状态这类跨模块运行时指标，避免 MetricsCollector 继续膨胀。
 */
final class RuntimeMetricsRegistry {

    private final AtomicLong recoveryDurationMs = new AtomicLong(0);
    private final AtomicLong recoveryIntentsTotal = new AtomicLong(0);

    private volatile long walSizeBytes = 0;
    private volatile int walSegmentCount = 0;
    private volatile long walRecordCount = 0;
    private volatile String walDataDir;

    private final AtomicLong pendingIntents = new AtomicLong(0);
    private final ConcurrentHashMap<String, AtomicLong> intentStatus = new ConcurrentHashMap<>();

    void setWalDataDir(String walDataDir) {
        this.walDataDir = walDataDir;
    }

    void recordRecovery(long durationMs, long intentsRecovered) {
        recoveryDurationMs.set(durationMs);
        recoveryIntentsTotal.set(intentsRecovered);
    }

    void updateWalMetrics(long sizeBytes, int segmentCount, long recordCount) {
        this.walSizeBytes = sizeBytes;
        this.walSegmentCount = segmentCount;
        this.walRecordCount = recordCount;
    }

    void refreshWalSize() {
        if (walDataDir == null) {
            return;
        }

        File dir = new File(walDataDir);
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }

        long totalSize = 0;
        File[] files = dir.listFiles((d, name) -> name.endsWith(".wal"));
        if (files == null) {
            return;
        }

        for (File file : files) {
            totalSize += file.length();
        }

        walSizeBytes = totalSize;
        walSegmentCount = files.length;
    }

    long getWalSizeBytes() {
        refreshWalSize();
        return walSizeBytes;
    }

    long getRecoveryDurationMs() {
        return recoveryDurationMs.get();
    }

    void updatePendingIntents(long count) {
        pendingIntents.set(count);
    }

    void updateIntentStatus(String status, long count) {
        intentStatus.computeIfAbsent(status, k -> new AtomicLong(0)).set(count);
    }

    long getPendingIntents() {
        return pendingIntents.get();
    }

    Map<String, Long> getIntentStatusCounts() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        intentStatus.forEach((status, count) -> result.put(status, count.get()));
        return result;
    }

    void resetIntentState() {
        pendingIntents.set(0);
        intentStatus.clear();
    }

    void appendPrometheusMetrics(StringBuilder sb) {
        refreshWalSize();

        sb.append("# HELP loomq_recovery_duration_ms Recovery duration in milliseconds\n");
        sb.append("# TYPE loomq_recovery_duration_ms gauge\n");
        sb.append(formatMetric("loomq_recovery_duration_ms", recoveryDurationMs.get()));
        sb.append("\n");

        sb.append("# HELP loomq_recovery_intents_total Total intents recovered\n");
        sb.append("# TYPE loomq_recovery_intents_total counter\n");
        sb.append(formatMetric("loomq_recovery_intents_total", recoveryIntentsTotal.get()));
        sb.append("\n");

        sb.append("# HELP loomq_wal_size_bytes WAL total size in bytes\n");
        sb.append("# TYPE loomq_wal_size_bytes gauge\n");
        sb.append(formatMetric("loomq_wal_size_bytes", walSizeBytes));
        sb.append("\n");

        sb.append("# HELP loomq_wal_segment_count Number of WAL segments\n");
        sb.append("# TYPE loomq_wal_segment_count gauge\n");
        sb.append(formatMetric("loomq_wal_segment_count", walSegmentCount));
        sb.append("\n");

        sb.append("# HELP loomq_wal_record_count Total WAL records written\n");
        sb.append("# TYPE loomq_wal_record_count counter\n");
        sb.append(formatMetric("loomq_wal_record_count", walRecordCount));
        sb.append("\n");
    }

    private String formatMetric(String name, long value) {
        return name + " " + value + "\n";
    }
}

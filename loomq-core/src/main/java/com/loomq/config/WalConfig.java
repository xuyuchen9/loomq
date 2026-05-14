package com.loomq.config;

import java.util.Objects;
import java.util.Properties;

/**
 * WAL 配置.
 */
public record WalConfig(
        String dataDir,
        int segmentSizeMb,
        String flushStrategy,
        long batchFlushIntervalMs,
        boolean syncOnWrite,
        String engine,
        int memorySegmentInitialSizeMb,
        int memorySegmentMaxSizeMb,
        int memorySegmentFlushThresholdKb,
        long memorySegmentFlushIntervalMs,
        int memorySegmentStripeCount,
        int memorySegmentMinBatchSize,
        boolean memorySegmentAdaptiveFlushEnabled
) {

    public WalConfig {
        dataDir = requireText(dataDir, "dataDir");
        flushStrategy = requireText(flushStrategy, "flushStrategy");
        engine = requireText(engine, "engine");

        requirePositive(segmentSizeMb, "segmentSizeMb");
        requirePositive(batchFlushIntervalMs, "batchFlushIntervalMs");
        requirePositive(memorySegmentInitialSizeMb, "memorySegmentInitialSizeMb");
        requirePositive(memorySegmentMaxSizeMb, "memorySegmentMaxSizeMb");
        requirePositive(memorySegmentFlushThresholdKb, "memorySegmentFlushThresholdKb");
        requirePositive(memorySegmentFlushIntervalMs, "memorySegmentFlushIntervalMs");
        requirePositive(memorySegmentStripeCount, "memorySegmentStripeCount");
        requirePositive(memorySegmentMinBatchSize, "memorySegmentMinBatchSize");
    }

    public static WalConfig defaultConfig() {
        return new WalConfig(
                "./data/wal",
                64,
                "batch",
                100,
                false,
                "memory_segment",
                64,
                1024,
                64,
                10,
                16,
                100,
                true
        );
    }

    public static WalConfig fromProperties(Properties props) {
        Properties source = props == null ? new Properties() : props;
        return new WalConfig(
                ConfigSupport.string(source, "./data/wal", "wal.data_dir", "wal.dataDir", "dataDir"),
                ConfigSupport.intValue(source, 64, "wal.segment_size_mb", "wal.segmentSizeMb", "segmentSizeMb"),
                ConfigSupport.string(source, "batch", "wal.flush_strategy", "wal.flushStrategy", "flushStrategy"),
                ConfigSupport.longValue(source, 100, "wal.batch_flush_interval_ms", "wal.batchFlushIntervalMs", "batchFlushIntervalMs"),
                ConfigSupport.booleanValue(source, false, "wal.sync_on_write", "wal.syncOnWrite", "syncOnWrite"),
                ConfigSupport.string(source, "memory_segment", "wal.engine", "engine"),
                ConfigSupport.intValue(source, 64, "wal.memory_segment.initial_size_mb", "wal.memory_segment.initialSizeMb", "memorySegmentInitialSizeMb"),
                ConfigSupport.intValue(source, 1024, "wal.memory_segment.max_size_mb", "wal.memory_segment.maxSizeMb", "memorySegmentMaxSizeMb"),
                ConfigSupport.intValue(source, 64, "wal.memory_segment.flush_threshold_kb", "wal.memory_segment.flushThresholdKb", "memorySegmentFlushThresholdKb"),
                ConfigSupport.longValue(source, 10, "wal.memory_segment.flush_interval_ms", "wal.memory_segment.flushIntervalMs", "memorySegmentFlushIntervalMs"),
                ConfigSupport.intValue(source, 16, "wal.memory_segment.stripe_count", "wal.memory_segment.stripeCount", "memorySegmentStripeCount"),
                ConfigSupport.intValue(source, 100, "wal.memory_segment.min_batch_size", "wal.memory_segment.minBatchSize", "memorySegmentMinBatchSize"),
                ConfigSupport.booleanValue(source, true, "wal.memory_segment.adaptive_flush_enabled", "wal.memory_segment.adaptiveFlushEnabled", "memorySegmentAdaptiveFlushEnabled")
        );
    }

    public WalConfig withDataDir(String newDataDir) {
        return new WalConfig(
                newDataDir,
                segmentSizeMb,
                flushStrategy,
                batchFlushIntervalMs,
                syncOnWrite,
                engine,
                memorySegmentInitialSizeMb,
                memorySegmentMaxSizeMb,
                memorySegmentFlushThresholdKb,
                memorySegmentFlushIntervalMs,
                memorySegmentStripeCount,
                memorySegmentMinBatchSize,
                memorySegmentAdaptiveFlushEnabled
        );
    }

    public WalConfig withBatchFlushIntervalMs(long newBatchFlushIntervalMs) {
        return new WalConfig(
                dataDir,
                segmentSizeMb,
                flushStrategy,
                newBatchFlushIntervalMs,
                syncOnWrite,
                engine,
                memorySegmentInitialSizeMb,
                memorySegmentMaxSizeMb,
                memorySegmentFlushThresholdKb,
                memorySegmentFlushIntervalMs,
                memorySegmentStripeCount,
                memorySegmentMinBatchSize,
                memorySegmentAdaptiveFlushEnabled
        );
    }

    public WalConfig withMemorySegmentMaxSizeMb(int newMaxSizeMb) {
        return new WalConfig(
                dataDir,
                segmentSizeMb,
                flushStrategy,
                batchFlushIntervalMs,
                syncOnWrite,
                engine,
                memorySegmentInitialSizeMb,
                newMaxSizeMb,
                memorySegmentFlushThresholdKb,
                memorySegmentFlushIntervalMs,
                memorySegmentStripeCount,
                memorySegmentMinBatchSize,
                memorySegmentAdaptiveFlushEnabled
        );
    }

    private static String requireText(String value, String fieldName) {
        String checked = Objects.requireNonNull(value, fieldName + " cannot be null");
        if (checked.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return checked;
    }

    private static void requirePositive(int value, String fieldName) {
        if (value <= 0) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
    }

    private static void requirePositive(long value, String fieldName) {
        if (value <= 0) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
    }
}

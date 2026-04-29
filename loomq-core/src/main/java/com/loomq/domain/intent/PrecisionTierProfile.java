package com.loomq.domain.intent;

/**
 * 精度档位配置。
 *
 * 这是可复用的纯数据模型，负责承载调度器所需的参数，
 * 由 PrecisionTierCatalog 统一提供默认 preset。
 */
public record PrecisionTierProfile(
    long precisionWindowMs,
    int maxConcurrency,
    int batchSize,
    int batchWindowMs,
    int consumerCount,
    int dispatchQueueCapacity,
    WalTierMode walTierMode
) {
    /** Per-tier WAL durability strategy — "FP4 quantization" applied to persistence. */
    public enum WalTierMode {
        /** Memory-mapped write only, no fsync wait. Lowest latency, crash-lose window = flush interval. */
        ASYNC,
        /** Async write with periodic batch fsync. Middle ground for medium-precision tiers. */
        BATCH_DEFERRED,
        /** Full fsync before returning. Strongest durability, highest latency. */
        DURABLE
    }

    public PrecisionTierProfile {
        if (precisionWindowMs <= 0) {
            throw new IllegalArgumentException("precisionWindowMs must be positive");
        }
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency must be positive");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        if (batchWindowMs < 0) {
            throw new IllegalArgumentException("batchWindowMs must be non-negative");
        }
        if (consumerCount <= 0) {
            throw new IllegalArgumentException("consumerCount must be positive");
        }
        if (dispatchQueueCapacity <= 0) {
            throw new IllegalArgumentException("dispatchQueueCapacity must be positive");
        }
        if (walTierMode == null) {
            throw new IllegalArgumentException("walTierMode must not be null");
        }
    }

    /**
     * 5-argument convenience constructor using default dispatchQueueCapacity = maxConcurrency * 16
     * and default WalTierMode = DURABLE.
     */
    public PrecisionTierProfile(long precisionWindowMs, int maxConcurrency, int batchSize,
                                int batchWindowMs, int consumerCount) {
        this(precisionWindowMs, maxConcurrency, batchSize, batchWindowMs, consumerCount,
             maxConcurrency * 16, WalTierMode.DURABLE);
    }

    /**
     * 6-argument convenience constructor using default WalTierMode = DURABLE.
     */
    public PrecisionTierProfile(long precisionWindowMs, int maxConcurrency, int batchSize,
                                int batchWindowMs, int consumerCount, int dispatchQueueCapacity) {
        this(precisionWindowMs, maxConcurrency, batchSize, batchWindowMs, consumerCount,
             dispatchQueueCapacity, WalTierMode.DURABLE);
    }

    public boolean isBatchEnabled() {
        return batchSize > 1;
    }
}

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
    WalMode walMode
) {

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
        if (walMode == null) {
            throw new IllegalArgumentException("walMode must not be null");
        }
    }

    /**
     * 5-argument convenience constructor using default dispatchQueueCapacity = maxConcurrency * 16
     * and default WalMode = DURABLE.
     */
    public PrecisionTierProfile(long precisionWindowMs, int maxConcurrency, int batchSize,
                                int batchWindowMs, int consumerCount) {
        this(precisionWindowMs, maxConcurrency, batchSize, batchWindowMs, consumerCount,
             maxConcurrency * 16, WalMode.DURABLE);
    }

    /**
     * 6-argument convenience constructor using default WalMode = DURABLE.
     */
    public PrecisionTierProfile(long precisionWindowMs, int maxConcurrency, int batchSize,
                                int batchWindowMs, int consumerCount, int dispatchQueueCapacity) {
        this(precisionWindowMs, maxConcurrency, batchSize, batchWindowMs, consumerCount,
             dispatchQueueCapacity, WalMode.DURABLE);
    }

    public boolean isBatchEnabled() {
        return batchSize > 1;
    }
}

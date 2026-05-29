package com.loomq.channel.http.batch;

/**
 * 批量投递配置。
 *
 * @param maxBatchSize     单次批量请求的最大 Intent 数量
 * @param flushIntervalMs  批量队列的 flush 间隔（毫秒）
 * @param maxConnections   HTTP 连接池最大连接数
 * @param tcpNoDelay       是否启用 TCP_NODELAY
 * @param batchTimeoutMs   单个 batch 从入队到 flush 的最大等待时间
 * @author loomq
 * @since v0.9.2
 */
public record BatchDeliveryConfig(
    int maxBatchSize,
    long flushIntervalMs,
    int maxConnections,
    boolean tcpNoDelay,
    long batchTimeoutMs
) {
    /**
     * 默认配置：200 intents/batch, 10ms flush, 500 连接, TCP_NODELAY。
     */
    public static final BatchDeliveryConfig DEFAULT = new BatchDeliveryConfig(
        200,   // maxBatchSize
        10,    // flushIntervalMs
        500,   // maxConnections
        true,  // tcpNoDelay
        30000  // batchTimeoutMs (30s delivery timeout)
    );

    /**
     * 快速配置：低延迟场景，小 batch + 短 flush。
     */
    public static final BatchDeliveryConfig LOW_LATENCY = new BatchDeliveryConfig(
        50,    // maxBatchSize
        2,     // flushIntervalMs
        500,   // maxConnections
        true,  // tcpNoDelay
        10000  // batchTimeoutMs
    );

    public BatchDeliveryConfig {
        if (maxBatchSize <= 0) throw new IllegalArgumentException("maxBatchSize must be positive");
        if (flushIntervalMs <= 0) throw new IllegalArgumentException("flushIntervalMs must be positive");
        if (maxConnections <= 0) throw new IllegalArgumentException("maxConnections must be positive");
        if (batchTimeoutMs <= 0) throw new IllegalArgumentException("batchTimeoutMs must be positive");
    }
}

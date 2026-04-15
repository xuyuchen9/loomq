package com.loomq.retry;

/**
 * 固定间隔重试策略
 *
 * 每次重试之间等待固定的时间间隔。
 *
 * @author loomq
 * @since v0.4
 */
public class FixedIntervalPolicy implements RetryPolicy {

    private final int maxRetries;
    private final long intervalMs;

    /**
     * 创建固定间隔重试策略
     *
     * @param maxRetries 最大重试次数
     * @param intervalMs 重试间隔（毫秒）
     */
    public FixedIntervalPolicy(int maxRetries, long intervalMs) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be non-negative");
        }
        if (intervalMs < 0) {
            throw new IllegalArgumentException("intervalMs must be non-negative");
        }
        this.maxRetries = maxRetries;
        this.intervalMs = intervalMs;
    }

    /**
     * 默认策略：3次重试，5秒间隔
     */
    public static FixedIntervalPolicy defaultPolicy() {
        return new FixedIntervalPolicy(3, 5000);
    }

    @Override
    public long nextRetryDelay(int retryCount) {
        if (!canRetry(retryCount)) {
            return -1;
        }
        return intervalMs;
    }

    @Override
    public int maxRetries() {
        return maxRetries;
    }

    @Override
    public String name() {
        return "fixed";
    }

    public long getIntervalMs() {
        return intervalMs;
    }

    @Override
    public String toString() {
        return "FixedIntervalPolicy{maxRetries=" + maxRetries + ", intervalMs=" + intervalMs + "}";
    }
}

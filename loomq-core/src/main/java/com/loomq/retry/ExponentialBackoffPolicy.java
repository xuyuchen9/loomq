package com.loomq.retry;

/**
 * 指数退避重试策略
 *
 * 每次重试的延迟时间按指数增长，直到达到最大值。
 * 延迟计算公式：min(initialDelayMs * base^(retryCount-1), maxDelayMs)
 *
 * @author loomq
 * @since v0.4
 */
public class ExponentialBackoffPolicy implements RetryPolicy {

    private final int maxRetries;
    private final long initialDelayMs;
    private final double base;
    private final long maxDelayMs;

    /**
     * 创建指数退避重试策略
     *
     * @param maxRetries     最大重试次数
     * @param initialDelayMs 初始延迟（毫秒）
     * @param base           指数基数
     * @param maxDelayMs     最大延迟（毫秒）
     */
    public ExponentialBackoffPolicy(int maxRetries, long initialDelayMs, double base, long maxDelayMs) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be non-negative");
        }
        if (initialDelayMs <= 0) {
            throw new IllegalArgumentException("initialDelayMs must be positive");
        }
        if (base <= 1) {
            throw new IllegalArgumentException("base must be greater than 1");
        }
        if (maxDelayMs < initialDelayMs) {
            throw new IllegalArgumentException("maxDelayMs must be >= initialDelayMs");
        }

        this.maxRetries = maxRetries;
        this.initialDelayMs = initialDelayMs;
        this.base = base;
        this.maxDelayMs = maxDelayMs;
    }

    /**
     * 默认策略：5次重试，初始1秒，基数2，最大1分钟
     */
    public static ExponentialBackoffPolicy defaultPolicy() {
        return new ExponentialBackoffPolicy(5, 1000, 2.0, 60000);
    }

    @Override
    public long nextRetryDelay(int retryCount) {
        if (!canRetry(retryCount)) {
            return -1;
        }

        // 计算指数延迟
        // delay = initialDelayMs * base^retryCount
        // retryCount=0: initialDelayMs
        // retryCount=1: initialDelayMs * base
        // retryCount=2: initialDelayMs * base^2
        double delay = initialDelayMs * Math.pow(base, retryCount);

        // 限制在最大延迟内
        return Math.min((long) delay, maxDelayMs);
    }

    @Override
    public int maxRetries() {
        return maxRetries;
    }

    @Override
    public String name() {
        return "exponential";
    }

    public long getInitialDelayMs() {
        return initialDelayMs;
    }

    public double getBase() {
        return base;
    }

    public long getMaxDelayMs() {
        return maxDelayMs;
    }

    @Override
    public String toString() {
        return "ExponentialBackoffPolicy{" +
                "maxRetries=" + maxRetries +
                ", initialDelayMs=" + initialDelayMs +
                ", base=" + base +
                ", maxDelayMs=" + maxDelayMs +
                '}';
    }
}

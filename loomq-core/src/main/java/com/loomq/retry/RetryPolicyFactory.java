package com.loomq.retry;

/**
 * 重试策略工厂
 *
 * 根据配置创建重试策略实例。
 *
 * @author loomq
 * @since v0.4
 */
public class RetryPolicyFactory {

    /**
     * 策略类型
     */
    public enum PolicyType {
        FIXED,
        EXPONENTIAL
    }

    /**
     * 创建重试策略
     *
     * @param type           策略类型
     * @param maxRetries     最大重试次数
     * @param fixedInterval  固定间隔（仅 FIXED 类型使用）
     * @param initialDelay   初始延迟（仅 EXPONENTIAL 类型使用）
     * @param base           指数基数（仅 EXPONENTIAL 类型使用）
     * @param maxDelay       最大延迟（仅 EXPONENTIAL 类型使用）
     * @return 重试策略实例
     */
    public static RetryPolicy create(
            PolicyType type,
            int maxRetries,
            long fixedInterval,
            long initialDelay,
            double base,
            long maxDelay) {

        return switch (type) {
            case FIXED -> new FixedIntervalPolicy(maxRetries, fixedInterval);
            case EXPONENTIAL -> new ExponentialBackoffPolicy(maxRetries, initialDelay, base, maxDelay);
        };
    }

    /**
     * 从配置创建重试策略
     */
    public static RetryPolicy fromConfig(RetryConfig config) {
        return create(
                config.type(),
                config.maxRetries(),
                config.fixedIntervalMs(),
                config.initialDelayMs(),
                config.base(),
                config.maxDelayMs()
        );
    }

    /**
     * 重试配置
     */
    public record RetryConfig(
            PolicyType type,
            int maxRetries,
            long fixedIntervalMs,
            long initialDelayMs,
            double base,
            long maxDelayMs
    ) {
        /**
         * 默认配置
         */
        public static RetryConfig defaultConfig() {
            return new RetryConfig(
                    PolicyType.FIXED,
                    3,
                    5000,
                    1000,
                    2.0,
                    60000
            );
        }

        /**
         * 固定间隔配置
         */
        public static RetryConfig fixed(int maxRetries, long intervalMs) {
            return new RetryConfig(PolicyType.FIXED, maxRetries, intervalMs, intervalMs, 2.0, intervalMs);
        }

        /**
         * 指数退避配置
         */
        public static RetryConfig exponential(int maxRetries, long initialDelayMs, double base, long maxDelayMs) {
            return new RetryConfig(PolicyType.EXPONENTIAL, maxRetries, initialDelayMs, initialDelayMs, base, maxDelayMs);
        }

        /**
         * 转换为重试策略
         */
        public RetryPolicy toRetryPolicy() {
            return RetryPolicyFactory.fromConfig(this);
        }
    }
}

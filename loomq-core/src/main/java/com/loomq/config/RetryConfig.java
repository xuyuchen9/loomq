package com.loomq.config;

import java.util.Properties;

/**
 * 重试配置.
 */
public record RetryConfig(
        long initialDelayMs,
        long maxDelayMs,
        double multiplier,
        int defaultMaxRetry
) {

    public RetryConfig {
        if (initialDelayMs <= 0) {
            throw new IllegalArgumentException("initialDelayMs must be positive");
        }
        if (maxDelayMs <= 0) {
            throw new IllegalArgumentException("maxDelayMs must be positive");
        }
        if (multiplier <= 0.0d) {
            throw new IllegalArgumentException("multiplier must be positive");
        }
        if (defaultMaxRetry < 0) {
            throw new IllegalArgumentException("defaultMaxRetry must be non-negative");
        }
    }

    public static RetryConfig defaultConfig() {
        return new RetryConfig(1000, 60000, 2.0d, 5);
    }

    public static RetryConfig fromProperties(Properties props) {
        Properties source = props == null ? new Properties() : props;
        return new RetryConfig(
                ConfigSupport.longValue(source, 1000, "retry.initial_delay_ms", "retry.initialDelayMs", "initialDelayMs"),
                ConfigSupport.longValue(source, 60000, "retry.max_delay_ms", "retry.maxDelayMs", "maxDelayMs"),
                ConfigSupport.doubleValue(source, 2.0d, "retry.multiplier", "multiplier"),
                ConfigSupport.intValue(source, 5, "retry.default_max_retry", "retry.defaultMaxRetry", "defaultMaxRetry")
        );
    }
}

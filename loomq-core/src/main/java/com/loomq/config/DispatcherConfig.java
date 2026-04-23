package com.loomq.config;

import java.util.Properties;

/**
 * 分发器配置.
 */
public record DispatcherConfig(
        long httpTimeoutMs,
        int maxConcurrentDispatches,
        long connectTimeoutMs,
        long readTimeoutMs
) {

    public DispatcherConfig {
        if (httpTimeoutMs <= 0) {
            throw new IllegalArgumentException("httpTimeoutMs must be positive");
        }
        if (maxConcurrentDispatches <= 0) {
            throw new IllegalArgumentException("maxConcurrentDispatches must be positive");
        }
        if (connectTimeoutMs <= 0) {
            throw new IllegalArgumentException("connectTimeoutMs must be positive");
        }
        if (readTimeoutMs <= 0) {
            throw new IllegalArgumentException("readTimeoutMs must be positive");
        }
    }

    public static DispatcherConfig defaultConfig() {
        return new DispatcherConfig(3000, 1000, 5000, 3000);
    }

    public static DispatcherConfig fromProperties(Properties props) {
        Properties source = props == null ? new Properties() : props;
        return new DispatcherConfig(
                ConfigSupport.longValue(source, 3000, "dispatcher.http_timeout_ms", "dispatcher.httpTimeoutMs", "httpTimeoutMs"),
                ConfigSupport.intValue(source, 1000, "dispatcher.max_concurrent_dispatches", "dispatcher.maxConcurrentDispatches", "maxConcurrentDispatches"),
                ConfigSupport.longValue(source, 5000, "dispatcher.connect_timeout_ms", "dispatcher.connectTimeoutMs", "connectTimeoutMs"),
                ConfigSupport.longValue(source, 3000, "dispatcher.read_timeout_ms", "dispatcher.readTimeoutMs", "readTimeoutMs")
        );
    }
}

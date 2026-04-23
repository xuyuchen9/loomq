package com.loomq.config;

import java.util.Properties;

/**
 * 恢复配置.
 */
public record RecoveryConfig(
        int batchSize,
        long sleepMs,
        int concurrencyLimit,
        boolean safeMode
) {

    public RecoveryConfig {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        if (sleepMs <= 0) {
            throw new IllegalArgumentException("sleepMs must be positive");
        }
        if (concurrencyLimit <= 0) {
            throw new IllegalArgumentException("concurrencyLimit must be positive");
        }
    }

    public static RecoveryConfig defaultConfig() {
        return new RecoveryConfig(1000, 10, 100, false);
    }

    public static RecoveryConfig fromProperties(Properties props) {
        Properties source = props == null ? new Properties() : props;
        return new RecoveryConfig(
                ConfigSupport.intValue(source, 1000, "recovery.batch_size", "recovery.batchSize", "batchSize"),
                ConfigSupport.longValue(source, 10, "recovery.sleep_ms", "recovery.sleepMs", "sleepMs"),
                ConfigSupport.intValue(source, 100, "recovery.concurrency_limit", "recovery.concurrencyLimit", "concurrencyLimit"),
                ConfigSupport.booleanValue(source, false, "recovery.safe_mode", "recovery.safeMode", "safeMode")
        );
    }
}

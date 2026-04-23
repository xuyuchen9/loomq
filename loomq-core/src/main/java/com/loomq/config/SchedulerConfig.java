package com.loomq.config;

import java.util.Properties;

/**
 * 调度器配置.
 */
public record SchedulerConfig(int maxPendingIntents) {

    public SchedulerConfig {
        if (maxPendingIntents <= 0) {
            throw new IllegalArgumentException("maxPendingIntents must be positive");
        }
    }

    public static SchedulerConfig defaultConfig() {
        return new SchedulerConfig(1_000_000);
    }

    public static SchedulerConfig fromProperties(Properties props) {
        Properties source = props == null ? new Properties() : props;
        return new SchedulerConfig(ConfigSupport.intValue(
                source,
                1_000_000,
                "scheduler.max_pending_intents",
                "scheduler.maxPendingIntents",
                "maxPendingIntents"));
    }
}

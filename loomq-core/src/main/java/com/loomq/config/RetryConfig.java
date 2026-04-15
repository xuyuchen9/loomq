package com.loomq.config;

import org.aeonbits.owner.Config;

/**
 * 重试配置
 */
@Config.Sources({"classpath:application.yml", "file:./config/application.yml"})
public interface RetryConfig extends Config {
    @Key("retry.initial_delay_ms")
    @DefaultValue("1000")
    long initialDelayMs();

    @Key("retry.max_delay_ms")
    @DefaultValue("60000")
    long maxDelayMs();

    @Key("retry.multiplier")
    @DefaultValue("2.0")
    double multiplier();

    @Key("retry.default_max_retry")
    @DefaultValue("5")
    int defaultMaxRetry();
}

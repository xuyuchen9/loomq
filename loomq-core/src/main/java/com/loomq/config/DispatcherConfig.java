package com.loomq.config;

import org.aeonbits.owner.Config;

/**
 * 分发器配置
 */
@Config.Sources({"classpath:application.yml", "file:./config/application.yml"})
public interface DispatcherConfig extends Config {
    @Key("dispatcher.http_timeout_ms")
    @DefaultValue("3000")
    long httpTimeoutMs();

    @Key("dispatcher.max_concurrent_dispatches")
    @DefaultValue("1000")
    int maxConcurrentDispatches();

    @Key("dispatcher.connect_timeout_ms")
    @DefaultValue("5000")
    long connectTimeoutMs();

    @Key("dispatcher.read_timeout_ms")
    @DefaultValue("3000")
    long readTimeoutMs();
}

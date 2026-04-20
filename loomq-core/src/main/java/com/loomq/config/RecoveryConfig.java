package com.loomq.config;

import org.aeonbits.owner.Config;

/**
 * 恢复配置
 */
@Config.Sources({"classpath:application.yml", "file:./config/application.yml"})
public interface RecoveryConfig extends Config {
    @Key("recovery.batch_size")
    @DefaultValue("1000")
    int batchSize();

    @Key("recovery.sleep_ms")
    @DefaultValue("10")
    long sleepMs();

    @Key("recovery.concurrency_limit")
    @DefaultValue("100")
    int concurrencyLimit();

    @Key("recovery.safe_mode")
    @DefaultValue("false")
    boolean safeMode();
}

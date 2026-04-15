package com.loomq.config;

import org.aeonbits.owner.Config;

/**
 * 调度器配置
 */
@Config.Sources({"classpath:application.yml", "file:./config/application.yml"})
public interface SchedulerConfig extends Config {
    @Key("scheduler.max_pending_tasks")
    @DefaultValue("1000000")
    int maxPendingTasks();
}

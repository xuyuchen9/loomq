package com.loomq.config;

import org.aeonbits.owner.Config;

/**
 * HTTP 服务器配置 (v0.5.2)
 *
 * 支持虚拟线程执行模型，消除 HTTP 层线程池瓶颈。
 */
@Config.Sources({"classpath:application.yml", "file:./config/application.yml"})
public interface ServerConfig extends Config {
    @DefaultValue("0.0.0.0")
    String host();

    @DefaultValue("8080")
    int port();

    /**
     * 操作系统连接队列大小，0 表示使用系统默认值
     */
    @DefaultValue("0")
    int backlog();

    /**
     * 是否启用虚拟线程处理 HTTP 请求
     * 默认 true，利用 Java 21 虚拟线程消除线程池瓶颈
     */
    @DefaultValue("true")
    boolean virtualThreads();

    /**
     * 最大请求体大小（字节）
     * 默认 10MB
     */
    @DefaultValue("10485760")
    long maxRequestSize();

    /**
     * 线程池大小（仅当 virtualThreads=false 时生效）
     */
    @DefaultValue("200")
    int threadPoolSize();
}

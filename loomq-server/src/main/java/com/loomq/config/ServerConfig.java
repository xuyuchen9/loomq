package com.loomq.config;

import org.aeonbits.owner.Config;

/**
 * Standalone server runtime configuration.
 *
 * This interface is the single entry point for the standalone service. It keeps
 * the bind address plus the Netty tuning knobs in one place so the bootstrap
 * path does not need multiple config loaders.
 */
@Config.Sources({"classpath:application.yml", "file:./config/application.yml"})
public interface ServerConfig extends Config {
    @Key("server.host")
    @DefaultValue("0.0.0.0")
    String host();

    @Key("server.port")
    @DefaultValue("8080")
    int port();

    /**
     * 操作系统连接队列大小，0 表示使用系统默认值
     */
    @Key("server.backlog")
    @DefaultValue("0")
    int backlog();

    /**
     * 是否启用虚拟线程处理 HTTP 请求
     * 默认 true，利用 Java 21 虚拟线程消除线程池瓶颈
     */
    @Key("server.virtual_threads")
    @DefaultValue("true")
    boolean virtualThreads();

    /**
     * 最大请求体大小（字节）
     * 默认 10MB
     */
    @Key("server.max_request_size")
    @DefaultValue("10485760")
    long maxRequestSize();

    /**
     * 线程池大小（仅当 virtualThreads=false 时生效）
     */
    @Key("server.thread_pool_size")
    @DefaultValue("200")
    int threadPoolSize();

    @Key("netty.host")
    @DefaultValue("0.0.0.0")
    String nettyHost();

    @Key("netty.port")
    @DefaultValue("8080")
    int nettyPort();

    @Key("netty.bossThreads")
    @DefaultValue("1")
    int bossThreads();

    @Key("netty.workerThreads")
    @DefaultValue("0")
    int workerThreads();

    @Key("netty.maxContentLength")
    @DefaultValue("10485760")
    int maxContentLength();

    @Key("netty.useEpoll")
    @DefaultValue("true")
    boolean useEpoll();

    @Key("netty.pooledAllocator")
    @DefaultValue("true")
    boolean pooledAllocator();

    @Key("netty.soBacklog")
    @DefaultValue("1024")
    int soBacklog();

    @Key("netty.tcpNoDelay")
    @DefaultValue("true")
    boolean tcpNoDelay();

    @Key("netty.connectionTimeoutMs")
    @DefaultValue("30000")
    int connectionTimeoutMs();

    @Key("netty.idleTimeoutSeconds")
    @DefaultValue("60")
    int idleTimeoutSeconds();

    @Key("netty.maxConnections")
    @DefaultValue("10000")
    int maxConnections();

    @Key("netty.writeBufferHighWaterMark")
    @DefaultValue("1048576")
    int writeBufferHighWaterMark();

    @Key("netty.writeBufferLowWaterMark")
    @DefaultValue("524288")
    int writeBufferLowWaterMark();

    @Key("netty.maxConcurrentBusinessRequests")
    @DefaultValue("50000")
    int maxConcurrentBusinessRequests();

    @Key("netty.gracefulShutdownTimeoutMs")
    @DefaultValue("30000")
    long gracefulShutdownTimeoutMs();
}

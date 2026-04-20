package com.loomq.http.netty;

import org.aeonbits.owner.Config;

/**
 * Netty HTTP 服务器配置
 *
 * 所有默认值经过压测调优
 *
 * 注意：此配置使用默认值，实际端口由 LoomqEngine 传入
 */
public interface NettyServerConfig extends Config {

    @DefaultValue("8080")
    int port();

    @DefaultValue("0.0.0.0")
    String host();

    /**
     * Boss 线程数（处理连接）
     * 通常 1 个足够
     */
    @DefaultValue("1")
    int bossThreads();

    /**
     * Worker 线程数（处理 I/O）
     * 0 = CPU 核数
     */
    @DefaultValue("0")
    int workerThreads();

    /**
     * 最大请求体长度（字节）
     * 默认 10MB
     */
    @DefaultValue("10485760")
    int maxContentLength();

    /**
     * 是否使用 Epoll（Linux 原生传输）
     * Linux 环境强制启用
     */
    @DefaultValue("true")
    boolean useEpoll();

    /**
     * 是否使用池化内存分配器
     */
    @DefaultValue("true")
    boolean pooledAllocator();

    /**
     * TCP 连接队列大小
     */
    @DefaultValue("1024")
    int soBacklog();

    /**
     * TCP_NODELAY
     */
    @DefaultValue("true")
    boolean tcpNoDelay();

    /**
     * 连接超时（毫秒）
     */
    @DefaultValue("30000")
    int connectionTimeoutMs();

    /**
     * 空闲超时（秒）
     */
    @DefaultValue("60")
    int idleTimeoutSeconds();

    /**
     * 最大连接数
     */
    @DefaultValue("10000")
    int maxConnections();

    /**
     * 写缓冲区高水位
     */
    @DefaultValue("1048576")
    int writeBufferHighWaterMark();

    /**
     * 写缓冲区低水位
     */
    @DefaultValue("524288")
    int writeBufferLowWaterMark();

    /**
     * 最大并发业务请求数（信号量上限）
     */
    @DefaultValue("50000")
    int maxConcurrentBusinessRequests();

    /**
     * 优雅停机超时（毫秒）
     */
    @DefaultValue("30000")
    long gracefulShutdownTimeoutMs();
}

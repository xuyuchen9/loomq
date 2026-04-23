package com.loomq.config;

import java.util.Properties;

/**
 * Standalone server runtime configuration.
 */
public record ServerConfig(
        String host,
        int port,
        int backlog,
        boolean virtualThreads,
        long maxRequestSize,
        int threadPoolSize,
        String nettyHost,
        int nettyPort,
        int bossThreads,
        int workerThreads,
        int maxContentLength,
        boolean useEpoll,
        boolean pooledAllocator,
        int soBacklog,
        boolean tcpNoDelay,
        int connectionTimeoutMs,
        int idleTimeoutSeconds,
        int maxConnections,
        int writeBufferHighWaterMark,
        int writeBufferLowWaterMark,
        int maxConcurrentBusinessRequests,
        long gracefulShutdownTimeoutMs
) {

    public ServerConfig {
        host = requireText(host, "host");
        nettyHost = requireText(nettyHost, "nettyHost");
        requirePort(port, "port");
        requirePort(nettyPort, "nettyPort");
        requirePositive(backlog, "backlog");
        requirePositive(threadPoolSize, "threadPoolSize");
        requirePositive(bossThreads, "bossThreads");
        requireNonNegative(workerThreads, "workerThreads");
        requirePositive(maxRequestSize, "maxRequestSize");
        requirePositive(maxContentLength, "maxContentLength");
        requirePositive(soBacklog, "soBacklog");
        requirePositive(connectionTimeoutMs, "connectionTimeoutMs");
        requirePositive(idleTimeoutSeconds, "idleTimeoutSeconds");
        requirePositive(maxConnections, "maxConnections");
        requirePositive(writeBufferHighWaterMark, "writeBufferHighWaterMark");
        requirePositive(writeBufferLowWaterMark, "writeBufferLowWaterMark");
        requirePositive(maxConcurrentBusinessRequests, "maxConcurrentBusinessRequests");
        requirePositive(gracefulShutdownTimeoutMs, "gracefulShutdownTimeoutMs");
    }

    public static ServerConfig defaultConfig() {
        return fromProperties(new Properties());
    }

    public static ServerConfig fromProperties(Properties props) {
        Properties source = props == null ? new Properties() : props;
        return new ServerConfig(
                ConfigSupport.string(source, "0.0.0.0", "server.host", "host"),
                ConfigSupport.intValue(source, 8080, "server.port", "port"),
                ConfigSupport.intValue(source, 1024, "server.backlog", "backlog"),
                ConfigSupport.booleanValue(source, true, "server.virtual_threads", "server.virtualThreads", "virtualThreads"),
                ConfigSupport.longValue(source, 10_485_760L, "server.max_request_size", "server.maxRequestSize", "maxRequestSize"),
                ConfigSupport.intValue(source, 200, "server.thread_pool_size", "server.threadPoolSize", "threadPoolSize"),
                ConfigSupport.string(source, "0.0.0.0", "netty.host", "nettyHost"),
                ConfigSupport.intValue(source, 8080, "netty.port", "nettyPort"),
                ConfigSupport.intValue(source, 1, "netty.bossThreads", "netty.boss_threads", "bossThreads"),
                ConfigSupport.intValue(source, 0, "netty.workerThreads", "netty.worker_threads", "workerThreads"),
                ConfigSupport.intValue(source, 10_485_760, "netty.maxContentLength", "netty.max_content_length", "maxContentLength"),
                ConfigSupport.booleanValue(source, true, "netty.useEpoll", "netty.use_epoll", "useEpoll"),
                ConfigSupport.booleanValue(source, true, "netty.pooledAllocator", "netty.pooled_allocator", "pooledAllocator"),
                ConfigSupport.intValue(source, 1024, "netty.soBacklog", "netty.so_backlog", "soBacklog"),
                ConfigSupport.booleanValue(source, true, "netty.tcpNoDelay", "netty.tcp_no_delay", "tcpNoDelay"),
                ConfigSupport.intValue(source, 30000, "netty.connectionTimeoutMs", "netty.connection_timeout_ms", "connectionTimeoutMs"),
                ConfigSupport.intValue(source, 60, "netty.idleTimeoutSeconds", "netty.idle_timeout_seconds", "idleTimeoutSeconds"),
                ConfigSupport.intValue(source, 10000, "netty.maxConnections", "netty.max_connections", "maxConnections"),
                ConfigSupport.intValue(source, 1048576, "netty.writeBufferHighWaterMark", "netty.write_buffer_high_water_mark", "writeBufferHighWaterMark"),
                ConfigSupport.intValue(source, 524288, "netty.writeBufferLowWaterMark", "netty.write_buffer_low_water_mark", "writeBufferLowWaterMark"),
                ConfigSupport.intValue(source, 50000, "netty.maxConcurrentBusinessRequests", "netty.max_concurrent_business_requests", "maxConcurrentBusinessRequests"),
                ConfigSupport.longValue(source, 30000, "netty.gracefulShutdownTimeoutMs", "netty.graceful_shutdown_timeout_ms", "gracefulShutdownTimeoutMs")
        );
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return value;
    }

    private static void requirePort(int value, String fieldName) {
        if (value < 0 || value > 65535) {
            throw new IllegalArgumentException(fieldName + " must be in range [0, 65535]");
        }
    }

    private static void requirePositive(int value, String fieldName) {
        if (value <= 0) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
    }

    private static void requirePositive(long value, String fieldName) {
        if (value <= 0) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
    }

    private static void requireNonNegative(int value, String fieldName) {
        if (value < 0) {
            throw new IllegalArgumentException(fieldName + " must be non-negative");
        }
    }
}

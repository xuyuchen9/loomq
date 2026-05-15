package com.loomq.http.netty;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.loomq.config.ServerConfig;
import java.util.Properties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class NettyHttpServerLifecycleTest {

    @Test
    @Timeout(10)
    @DisplayName("Stop should be safe before start")
    void stopShouldBeSafeBeforeStart() {
        NettyHttpServer server = new NettyHttpServer(testConfig("127.0.0.1"), new RadixRouter());

        assertDoesNotThrow(server::stop);
    }

    @Test
    @Timeout(10)
    @DisplayName("Configured bind host should be honored")
    void configuredBindHostShouldBeHonored() {
        NettyHttpServer server = new NettyHttpServer(testConfig("203.0.113.10"), new RadixRouter());

        assertThrows(Exception.class, server::start);
        assertDoesNotThrow(server::stop);
    }

    private static ServerConfig testConfig(String host) {
        Properties props = new Properties();
        props.setProperty("server.host", host);
        props.setProperty("server.port", "0");
        props.setProperty("server.backlog", "1024");
        props.setProperty("server.virtual_threads", "true");
        props.setProperty("server.max_request_size", String.valueOf(10 * 1024 * 1024));
        props.setProperty("server.thread_pool_size", "200");
        props.setProperty("netty.host", host);
        props.setProperty("netty.port", "0");
        props.setProperty("netty.bossThreads", "1");
        props.setProperty("netty.workerThreads", "0");
        props.setProperty("netty.maxContentLength", String.valueOf(10 * 1024 * 1024));
        props.setProperty("netty.useEpoll", "false");
        props.setProperty("netty.pooledAllocator", "true");
        props.setProperty("netty.soBacklog", "1024");
        props.setProperty("netty.tcpNoDelay", "true");
        props.setProperty("netty.connectionTimeoutMs", "30000");
        props.setProperty("netty.idleTimeoutSeconds", "60");
        props.setProperty("netty.maxConnections", "1000");
        props.setProperty("netty.writeBufferHighWaterMark", String.valueOf(1024 * 1024));
        props.setProperty("netty.writeBufferLowWaterMark", String.valueOf(512 * 1024));
        props.setProperty("netty.maxConcurrentBusinessRequests", "100");
        props.setProperty("netty.httpSemaphoreTimeoutMs", "500");
        props.setProperty("netty.gracefulShutdownTimeoutMs", "1000");
        return ServerConfig.fromProperties(props);
    }
}

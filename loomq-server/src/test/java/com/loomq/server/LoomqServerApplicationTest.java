package com.loomq.server;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.loomq.config.ServerConfig;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class LoomqServerApplicationTest {

    @Test
    void validateSupportedStartupModesShouldAllowRaftOnlyMode() {
        String previous = System.getProperty("loomq.cluster.enabled");
        try {
            System.clearProperty("loomq.cluster.enabled");

            assertDoesNotThrow(LoomqServerApplication::validateSupportedStartupModes);
        } finally {
            restoreProperty("loomq.cluster.enabled", previous);
        }
    }

    @Test
    void validateSupportedStartupModesShouldRejectLegacyClusterMode() {
        String previous = System.getProperty("loomq.cluster.enabled");
        try {
            System.setProperty("loomq.cluster.enabled", "true");

            assertThrows(IllegalStateException.class,
                LoomqServerApplication::validateSupportedStartupModes);
        } finally {
            restoreProperty("loomq.cluster.enabled", previous);
        }
    }

    @Test
    void parseRaftPeerIdsShouldRejectDuplicateIds() {
        assertThrows(IllegalStateException.class,
            () -> LoomqServerApplication.parseRaftPeerIds(
                "node-1@127.0.0.1:7931,node-1@127.0.0.1:7932", "node-1"));
    }

    @Test
    void parseRaftPeerTargetsShouldRejectMissingEndpointForRemotePeer() {
        assertThrows(IllegalStateException.class,
            () -> LoomqServerApplication.parseRaftPeerTargets("node-1,node-2", "node-1"));
    }

    @Test
    void validateRaftStartupConfigShouldRejectMissingSelfPeer() {
        ServerConfig serverConfig = serverConfig(7928, 7929);
        List<String> peers = List.of("node-2");
        List<LoomqServerApplication.RaftPeerTarget> targets = List.of(
            new LoomqServerApplication.RaftPeerTarget("node-2", "127.0.0.1", 7931));

        assertThrows(IllegalStateException.class,
            () -> LoomqServerApplication.validateRaftStartupConfig(
                "node-1", peers, targets, 7930, serverConfig));
    }

    @Test
    void validateRaftStartupConfigShouldRejectPortCollision() {
        ServerConfig serverConfig = serverConfig(7930, 7930);
        List<String> peers = List.of("node-1");

        assertThrows(IllegalStateException.class,
            () -> LoomqServerApplication.validateRaftStartupConfig(
                "node-1", peers, List.of(), 7930, serverConfig));
    }

    private static void restoreProperty(String key, String previousValue) {
        if (previousValue == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previousValue);
        }
    }

    private static ServerConfig serverConfig(int port, int nettyPort) {
        Properties props = new Properties();
        props.setProperty("host", "127.0.0.1");
        props.setProperty("port", String.valueOf(port));
        props.setProperty("backlog", "1024");
        props.setProperty("virtualThreads", "true");
        props.setProperty("maxRequestSize", String.valueOf(10 * 1024 * 1024));
        props.setProperty("threadPoolSize", "200");
        props.setProperty("nettyHost", "127.0.0.1");
        props.setProperty("nettyPort", String.valueOf(nettyPort));
        props.setProperty("bossThreads", "1");
        props.setProperty("workerThreads", "0");
        props.setProperty("maxContentLength", String.valueOf(10 * 1024 * 1024));
        props.setProperty("useEpoll", "false");
        props.setProperty("pooledAllocator", "true");
        props.setProperty("soBacklog", "1024");
        props.setProperty("tcpNoDelay", "true");
        props.setProperty("connectionTimeoutMs", "30000");
        props.setProperty("idleTimeoutSeconds", "60");
        props.setProperty("maxConnections", "1000");
        props.setProperty("writeBufferHighWaterMark", String.valueOf(1024 * 1024));
        props.setProperty("writeBufferLowWaterMark", String.valueOf(512 * 1024));
        props.setProperty("maxConcurrentBusinessRequests", "100");
        props.setProperty("httpSemaphoreTimeoutMs", "500");
        props.setProperty("gracefulShutdownTimeoutMs", "5000");
        return ServerConfig.fromProperties(props);
    }
}

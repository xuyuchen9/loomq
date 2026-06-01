package com.loomq.channel.grpc.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class GrpcConfigTest {

    @Test
    void defaultConfigHasSaneDefaults() {
        GrpcConfig config = GrpcConfig.defaultConfig();
        assertFalse(config.enabled());
        assertEquals("0.0.0.0", config.host());
        assertEquals(8928, config.port());
        assertEquals(4 * 1024 * 1024, config.maxInboundMessageSize());
        assertEquals(1, config.bossThreads());
        assertEquals(0, config.workerThreads());
    }

    @Test
    void fromPropertiesOverridesDefaults() {
        Properties props = new Properties();
        props.setProperty("grpc.enabled", "true");
        props.setProperty("grpc.host", "127.0.0.1");
        props.setProperty("grpc.port", "9999");
        props.setProperty("grpc.max_inbound_message_size", "1024");
        props.setProperty("grpc.boss_threads", "2");
        props.setProperty("grpc.worker_threads", "4");

        GrpcConfig config = GrpcConfig.fromProperties(props);
        assertTrue(config.enabled());
        assertEquals("127.0.0.1", config.host());
        assertEquals(9999, config.port());
        assertEquals(1024, config.maxInboundMessageSize());
        assertEquals(2, config.bossThreads());
        assertEquals(4, config.workerThreads());
    }

    @Test
    void fromPropertiesAcceptsAlternateKeys() {
        Properties props = new Properties();
        props.setProperty("grpcEnabled", "true");
        props.setProperty("grpcHost", "10.0.0.1");
        props.setProperty("grpcPort", "8080");

        GrpcConfig config = GrpcConfig.fromProperties(props);
        assertTrue(config.enabled());
        assertEquals("10.0.0.1", config.host());
        assertEquals(8080, config.port());
    }

    @Test
    void fromPropertiesNullReturnsDefaults() {
        GrpcConfig config = GrpcConfig.fromProperties(null);
        assertFalse(config.enabled());
        assertEquals(8928, config.port());
    }

    @Test
    void rejectsBlankHost() {
        assertThrows(IllegalArgumentException.class,
            () -> new GrpcConfig(true, "", 8080, 1024, 1, 0, true, 1024, 1024 * 1024, 60));
        assertThrows(IllegalArgumentException.class,
            () -> new GrpcConfig(true, "  ", 8080, 1024, 1, 0, true, 1024, 1024 * 1024, 60));
    }

    @Test
    void rejectsNullHost() {
        assertThrows(IllegalArgumentException.class,
            () -> new GrpcConfig(true, null, 8080, 1024, 1, 0, true, 1024, 1024 * 1024, 60));
    }

    @Test
    void rejectsInvalidPort() {
        assertThrows(IllegalArgumentException.class,
            () -> new GrpcConfig(true, "localhost", -1, 1024, 1, 0, true, 1024, 1024 * 1024, 60));
        assertThrows(IllegalArgumentException.class,
            () -> new GrpcConfig(true, "localhost", 70000, 1024, 1, 0, true, 1024, 1024 * 1024, 60));
    }

    @Test
    void rejectsNonPositiveMaxInboundMessageSize() {
        assertThrows(IllegalArgumentException.class,
            () -> new GrpcConfig(true, "localhost", 8080, 0, 1, 0, true, 1024, 1024 * 1024, 60));
        assertThrows(IllegalArgumentException.class,
            () -> new GrpcConfig(true, "localhost", 8080, -1, 1, 0, true, 1024, 1024 * 1024, 60));
    }

    @Test
    void rejectsNonPositiveBossThreads() {
        assertThrows(IllegalArgumentException.class,
            () -> new GrpcConfig(true, "localhost", 8080, 1024, 0, 0, true, 1024, 1024 * 1024, 60));
    }

    @Test
    void rejectsNegativeWorkerThreads() {
        assertThrows(IllegalArgumentException.class,
            () -> new GrpcConfig(true, "localhost", 8080, 1024, 1, -1, true, 1024, 1024 * 1024, 60));
    }

    @Test
    void allowsZeroWorkerThreads() {
        GrpcConfig config = new GrpcConfig(true, "localhost", 8080, 1024, 1, 0, true, 1024, 1024 * 1024, 60);
        assertEquals(0, config.workerThreads());
    }

    @Test
    void allowsPortZero() {
        GrpcConfig config = new GrpcConfig(true, "localhost", 0, 1024, 1, 0, true, 1024, 1024 * 1024, 60);
        assertEquals(0, config.port());
    }
}

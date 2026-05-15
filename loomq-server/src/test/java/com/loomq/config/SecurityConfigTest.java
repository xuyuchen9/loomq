package com.loomq.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SecurityConfigTest {

    @Test
    void disabledByDefault() {
        SecurityConfig config = SecurityConfig.fromProperties(new Properties());

        assertFalse(config.enabled());
        assertTrue(config.isAuthorized("/v1/intents", Map.of()));
    }

    @Test
    void enabledSecurityRequiresToken() {
        Properties props = new Properties();
        props.setProperty("security.enabled", "true");

        assertThrows(IllegalArgumentException.class, () -> SecurityConfig.fromProperties(props));
    }

    @Test
    void shouldAuthorizeRawAndBearerTokens() {
        SecurityConfig config = new SecurityConfig(true, "X-LoomQ-Token", Set.of("secret-token"));

        assertTrue(config.isAuthorized("/v1/intents", Map.of("X-LoomQ-Token", "secret-token")));
        assertTrue(config.isAuthorized("/metrics", Map.of("x-loomq-token", "Bearer secret-token")));
        assertFalse(config.isAuthorized("/metrics", Map.of("X-LoomQ-Token", "wrong-token")));
    }

    @Test
    void healthRoutesStayOpenForProbes() {
        SecurityConfig config = new SecurityConfig(true, "X-LoomQ-Token", Set.of("secret-token"));

        assertDoesNotThrow(() -> config.isAuthorized("/health/ready", Map.of()));
        assertTrue(config.isAuthorized("/health/ready", Map.of()));
        assertFalse(config.isAuthorized("/api/v1/metrics", Map.of()));
    }
}

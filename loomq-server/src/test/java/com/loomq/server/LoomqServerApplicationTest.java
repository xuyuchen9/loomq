package com.loomq.server;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    private static void restoreProperty(String key, String previousValue) {
        if (previousValue == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previousValue);
        }
    }
}

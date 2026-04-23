package com.loomq.config;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimpleYamlConfigLoaderTest {

    @Test
    void parsesNestedMapsListsAndPlaceholders() {
        String originalHost = System.getProperty("LOOMQ_TEST_HOST");
        String originalPort = System.getProperty("LOOMQ_TEST_PORT");
        String originalToken = System.getProperty("LOOMQ_TEST_TOKEN");

        try {
            System.setProperty("LOOMQ_TEST_HOST", "127.0.0.1");
            System.setProperty("LOOMQ_TEST_PORT", "9090");
            System.setProperty("LOOMQ_TEST_TOKEN", "token-123");

            Properties properties = SimpleYamlConfigLoader.load("""
                server:
                  host: "${LOOMQ_TEST_HOST:0.0.0.0}"
                  port: ${LOOMQ_TEST_PORT:8080}
                security:
                  tokens:
                    - "${LOOMQ_TEST_TOKEN:dev-token-12345}"
                metrics:
                  histogram_buckets: [0.001, 0.005, 0.01]
                logging:
                  file:
                    path: "./logs/loomq.log" # inline comment
                """);

            assertEquals("127.0.0.1", properties.getProperty("server.host"));
            assertEquals("9090", properties.getProperty("server.port"));
            assertEquals("token-123", properties.getProperty("security.tokens"));
            assertEquals("0.001,0.005,0.01", properties.getProperty("metrics.histogram_buckets"));
            assertEquals("./logs/loomq.log", properties.getProperty("logging.file.path"));
        } finally {
            restoreProperty("LOOMQ_TEST_HOST", originalHost);
            restoreProperty("LOOMQ_TEST_PORT", originalPort);
            restoreProperty("LOOMQ_TEST_TOKEN", originalToken);
        }
    }

    @Test
    void parsesBundledApplicationYaml() throws Exception {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("application.yml")) {
            assertNotNull(input, "application.yml should be available on the test classpath");

            Properties properties = SimpleYamlConfigLoader.load(input);

            assertTrue(properties.containsKey("server.host"));
            assertTrue(properties.containsKey("server.port"));
            assertTrue(properties.containsKey("logging.file.path"));
            assertEquals("0.001,0.005,0.01,0.025,0.05,0.1,0.25,0.5,1,2.5,5,10",
                properties.getProperty("metrics.histogram_buckets"));
        }
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }
}

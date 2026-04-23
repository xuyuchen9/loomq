package com.loomq.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * 主配置类.
 */
public final class LoomqConfig {
    private static final Logger logger = LoggerFactory.getLogger(LoomqConfig.class);
    private static final String DEFAULT_RESOURCE = "application.yml";
    private static final Path EXTERNAL_CONFIG_PATH = Paths.get("./config/application.yml");

    private static volatile LoomqConfig instance;

    private final ServerConfig serverConfig;
    private final WalConfig walConfig;
    private final SchedulerConfig schedulerConfig;
    private final DispatcherConfig dispatcherConfig;
    private final RetryConfig retryConfig;
    private final RecoveryConfig recoveryConfig;

    private LoomqConfig(Properties properties) {
        Properties source = properties == null ? new Properties() : properties;
        this.serverConfig = ServerConfig.fromProperties(source);
        this.walConfig = WalConfig.fromProperties(source);
        this.schedulerConfig = SchedulerConfig.fromProperties(source);
        this.dispatcherConfig = DispatcherConfig.fromProperties(source);
        this.retryConfig = RetryConfig.fromProperties(source);
        this.recoveryConfig = RecoveryConfig.fromProperties(source);
    }

    public static LoomqConfig getInstance() {
        if (instance == null) {
            synchronized (LoomqConfig.class) {
                if (instance == null) {
                    instance = new LoomqConfig(loadEffectiveProperties(null));
                }
            }
        }
        return instance;
    }

    public static void reload(Properties properties) {
        synchronized (LoomqConfig.class) {
            instance = new LoomqConfig(loadEffectiveProperties(properties));
        }
    }

    private static Properties loadEffectiveProperties(Properties overrides) {
        Properties merged = ConfigSupport.merge(loadDefaultSources(), System.getProperties());
        if (overrides != null && !overrides.isEmpty()) {
            merged.putAll(overrides);
        }
        return merged;
    }

    private static Properties loadDefaultSources() {
        Properties merged = new Properties();

        loadFromClasspath(merged, DEFAULT_RESOURCE);
        loadFromFile(merged, EXTERNAL_CONFIG_PATH);

        return merged;
    }

    private static void loadFromClasspath(Properties target, String resourceName) {
        try (InputStream input = LoomqConfig.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (input == null) {
                logger.debug("Classpath config not found: {}", resourceName);
                return;
            }
            target.putAll(SimpleYamlConfigLoader.load(input));
        } catch (IOException e) {
            logger.warn("Failed to load classpath config {}", resourceName, e);
        }
    }

    private static void loadFromFile(Properties target, Path path) {
        if (!Files.exists(path)) {
            logger.debug("External config not found: {}", path);
            return;
        }

        try (InputStream input = Files.newInputStream(path)) {
            target.putAll(SimpleYamlConfigLoader.load(input));
        } catch (IOException e) {
            logger.warn("Failed to load external config {}", path, e);
        }
    }

    public ServerConfig getServerConfig() {
        return serverConfig;
    }

    public WalConfig getWalConfig() {
        return walConfig;
    }

    public SchedulerConfig getSchedulerConfig() {
        return schedulerConfig;
    }

    public DispatcherConfig getDispatcherConfig() {
        return dispatcherConfig;
    }

    public RetryConfig getRetryConfig() {
        return retryConfig;
    }

    public RecoveryConfig getRecoveryConfig() {
        return recoveryConfig;
    }
}

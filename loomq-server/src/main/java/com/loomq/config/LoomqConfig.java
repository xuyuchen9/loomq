package com.loomq.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 主配置类.
 */
public final class LoomqConfig {
    private static final Logger logger = LoggerFactory.getLogger(LoomqConfig.class);
    private static final String DEFAULT_RESOURCE = "application.yml";
    private static final Path EXTERNAL_CONFIG_PATH = Paths.get("./config/application.yml");
    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("^\\$\\{([^:}]+)(?::([^}]*))?}$");

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
            target.putAll(loadYaml(input));
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
            target.putAll(loadYaml(input));
        } catch (IOException e) {
            logger.warn("Failed to load external config {}", path, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Properties loadYaml(InputStream input) {
        Object loaded = new Yaml().load(input);
        Properties properties = new Properties();
        flattenValue(properties, null, loaded);
        return properties;
    }

    @SuppressWarnings("unchecked")
    private static void flattenValue(Properties target, String prefix, Object value) {
        if (value == null) {
            return;
        }

        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String childKey = prefix == null || prefix.isBlank()
                        ? String.valueOf(entry.getKey())
                        : prefix + "." + entry.getKey();
                flattenValue(target, childKey, entry.getValue());
            }
            return;
        }

        if (value instanceof List<?> list) {
            flattenList(target, prefix, list);
            return;
        }

        if (prefix != null && !prefix.isBlank()) {
            target.setProperty(prefix, resolveString(String.valueOf(value)));
        }
    }

    private static void flattenList(Properties target, String prefix, List<?> list) {
        if (prefix == null || prefix.isBlank() || list.isEmpty()) {
            return;
        }

        boolean simpleList = list.stream().allMatch(item ->
                item == null || item instanceof String || item instanceof Number || item instanceof Boolean);

        if (simpleList) {
            List<String> values = new ArrayList<>(list.size());
            for (Object item : list) {
                if (item != null) {
                    values.add(resolveString(String.valueOf(item)));
                }
            }
            target.setProperty(prefix, String.join(",", values));
            return;
        }

        for (int i = 0; i < list.size(); i++) {
            flattenValue(target, prefix + "[" + i + "]", list.get(i));
        }
    }

    private static String resolveString(String value) {
        if (value == null) {
            return null;
        }

        Matcher matcher = PLACEHOLDER_PATTERN.matcher(value.trim());
        if (!matcher.matches()) {
            return value;
        }

        String key = matcher.group(1).trim();
        String defaultValue = matcher.group(2);

        String resolved = System.getProperty(key);
        if (resolved == null || resolved.isBlank()) {
            resolved = System.getenv(key);
        }
        if (resolved == null || resolved.isBlank()) {
            resolved = defaultValue;
        }
        return resolved != null ? resolved : "";
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

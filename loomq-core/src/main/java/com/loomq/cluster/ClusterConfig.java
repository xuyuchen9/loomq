package com.loomq.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 集群配置管理
 *
 * 管理分片集群的配置信息，支持：
 * 1. 从配置文件加载
 * 2. 从环境变量加载
 * 3. 配置热更新
 * 4. 配置变更监听
 *
 * 配置文件格式（cluster.yml，使用本项目支持的简化 YAML 结构）：
 * <pre>
 * cluster:
 *   name: "loomq-cluster"
 *   total_shards: 4
 *   local_shard_index: 0
 *   nodes:
 *     - shard_id: "shard-0"
 *       host: "localhost"
 *       port: 8080
 *       weight: 100
 *     - shard_id: "shard-1"
 *       host: "localhost"
 *       port: 8081
 *       weight: 100
 * </pre>
 *
 * @author loomq
 * @since v0.3
 */
public class ClusterConfig {

    private static final Logger logger = LoggerFactory.getLogger(ClusterConfig.class);

    // 默认配置文件路径
    public static final String DEFAULT_CONFIG_PATH = "cluster.yml";

    // 环境变量前缀
    public static final String ENV_PREFIX = "LOOMQ_";

    // 集群名称
    private final String clusterName;

    // 总分片数
    private final int totalShards;

    // 本地分片索引
    private final int localShardIndex;

    // 节点配置列表
    private final List<NodeConfig> nodes;

    // 当前配置版本（用于热更新检测）
    private final long version;

    // 配置变更监听器
    private final CopyOnWriteArrayList<ConfigChangeListener> listeners;

    // 热更新配置
    private final boolean hotReloadEnabled;
    private final long hotReloadIntervalMs;

    /**
     * 创建集群配置
     */
    public ClusterConfig(String clusterName, int totalShards, int localShardIndex,
                         List<NodeConfig> nodes, boolean hotReloadEnabled, long hotReloadIntervalMs) {
        this.clusterName = Objects.requireNonNull(clusterName, "clusterName cannot be null");
        this.totalShards = validatePositive(totalShards, "totalShards");
        this.localShardIndex = validateRange(localShardIndex, 0, totalShards - 1, "localShardIndex");
        this.nodes = List.copyOf(Objects.requireNonNull(nodes, "nodes cannot be null"));
        this.version = System.currentTimeMillis();
        this.hotReloadEnabled = hotReloadEnabled;
        this.hotReloadIntervalMs = hotReloadIntervalMs;
        this.listeners = new CopyOnWriteArrayList<>();

        validateNodes();
        logger.info("ClusterConfig created: name={}, shards={}/{}, nodes={}",
                clusterName, localShardIndex, totalShards, nodes.size());
    }

    /**
     * 从配置文件加载
     *
     * @param path 配置文件路径
     * @return 集群配置
     */
    public static ClusterConfig load(String path) throws IOException {
        logger.info("Loading cluster config from: {}", path);

        Path configPath = Paths.get(path);
        if (!Files.exists(configPath)) {
            // 尝试从 classpath 加载
            InputStream is = ClusterConfig.class.getClassLoader().getResourceAsStream(path);
            if (is == null) {
                throw new IOException("Config file not found: " + path);
            }
            try (is) {
                return parseYaml(is);
            }
        }

        try (InputStream is = Files.newInputStream(configPath)) {
            return parseYaml(is);
        }
    }

    /**
     * 从环境变量加载（覆盖配置文件的值）
     */
    public static ClusterConfig fromEnv() {
        String clusterName = getEnv("CLUSTER_NAME", "loomq-cluster");
        int totalShards = getEnvInt("TOTAL_SHARDS", 1);
        int localShardIndex = getEnvInt("SHARD_INDEX", 0);
        boolean hotReload = getEnvBool("HOT_RELOAD", false);
        long reloadInterval = getEnvLong("RELOAD_INTERVAL_MS", 60000);

        // 解析节点列表（格式：shard-0:localhost:8080:100,shard-1:localhost:8081:100）
        List<NodeConfig> nodes = parseNodesFromEnv(
                getEnv("NODES", String.format("shard-%d:localhost:8080:100", localShardIndex)));

        return new ClusterConfig(clusterName, totalShards, localShardIndex, nodes, hotReload, reloadInterval);
    }

    /**
     * 加载默认配置（先尝试环境变量，再尝试配置文件）
     */
    public static ClusterConfig loadDefault() {
        try {
            // 先检查环境变量是否设置了基本配置
            if (System.getenv(ENV_PREFIX + "SHARD_INDEX") != null) {
                logger.info("Loading config from environment variables");
                return fromEnv();
            }

            // 尝试加载配置文件
            return load(DEFAULT_CONFIG_PATH);
        } catch (IOException e) {
            logger.warn("Failed to load config file, using default single-node config");
            return defaultSingleNodeConfig();
        }
    }

    /**
     * 默认单节点配置
     */
    public static ClusterConfig defaultSingleNodeConfig() {
        List<NodeConfig> nodes = List.of(
                new NodeConfig("shard-0", "localhost", 8080, 100)
        );
        return new ClusterConfig("loomq-single", 1, 0, nodes, false, 60000);
    }

    /**
     * 创建单机多 shard 配置（用于测试）
     */
    public static ClusterConfig localMultiShard(int shardCount, int basePort) {
        List<NodeConfig> nodes = new ArrayList<>();
        for (int i = 0; i < shardCount; i++) {
            nodes.add(new NodeConfig("shard-" + i, "localhost", basePort + i, 100));
        }
        return new ClusterConfig("loomq-local", shardCount, 0, nodes, false, 60000);
    }

    // Getters
    public String getClusterName() {
        return clusterName;
    }

    public int getTotalShards() {
        return totalShards;
    }

    public int getLocalShardIndex() {
        return localShardIndex;
    }

    public String getLocalShardId() {
        return "shard-" + localShardIndex;
    }

    public List<NodeConfig> getNodes() {
        return nodes;
    }

    public long getVersion() {
        return version;
    }

    public boolean isHotReloadEnabled() {
        return hotReloadEnabled;
    }

    public long getHotReloadIntervalMs() {
        return hotReloadIntervalMs;
    }

    /**
     * 获取指定 shard 的节点配置
     */
    public Optional<NodeConfig> getNodeConfig(int shardIndex) {
        String shardId = "shard-" + shardIndex;
        return nodes.stream()
                .filter(n -> n.shardId().equals(shardId))
                .findFirst();
    }

    /**
     * 获取本地节点配置
     */
    public NodeConfig getLocalNodeConfig() {
        return getNodeConfig(localShardIndex)
                .orElseThrow(() -> new IllegalStateException(
                        "Local node config not found for shard-" + localShardIndex));
    }

    /**
     * 检查是否是分片模式
     */
    public boolean isSharded() {
        return totalShards > 1;
    }

    /**
     * 添加配置变更监听器
     */
    public void addChangeListener(ConfigChangeListener listener) {
        listeners.add(listener);
    }

    /**
     * 移除配置变更监听器
     */
    public void removeChangeListener(ConfigChangeListener listener) {
        listeners.remove(listener);
    }

    /**
     * 通知配置变更
     */
    void notifyChange(ClusterConfig oldConfig) {
        for (ConfigChangeListener listener : listeners) {
            try {
                listener.onConfigChange(oldConfig, this);
            } catch (Exception e) {
                logger.error("Config change listener error", e);
            }
        }
    }

    // 验证方法
    private void validateNodes() {
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("At least one node is required");
        }

        Set<String> shardIds = new HashSet<>();
        for (NodeConfig node : nodes) {
            if (!shardIds.add(node.shardId())) {
                throw new IllegalArgumentException("Duplicate shardId: " + node.shardId());
            }
        }

        // 检查本地 shardIndex 是否在节点列表中
        if (getNodeConfig(localShardIndex).isEmpty()) {
            logger.warn("Local shard-{} not found in node list, will use dynamic configuration", localShardIndex);
        }
    }

    private static int validatePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static int validateRange(int value, int min, int max, String name) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(
                    String.format("%s must be in range [%d, %d]", name, min, max));
        }
        return value;
    }

    // 配置解析
    private static ClusterConfig parseYaml(InputStream is) throws IOException {
        Map<String, String> clusterValues = new HashMap<>();
        List<Map<String, String>> nodeValues = new ArrayList<>();
        Map<String, String> currentNode = null;
        boolean seenClusterSection = false;
        boolean seenNodesSection = false;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String rawLine;
            int lineNumber = 0;

            while ((rawLine = reader.readLine()) != null) {
                lineNumber++;

                String line = stripComment(rawLine).stripTrailing();
                if (line.isBlank()) {
                    continue;
                }

                int indent = countLeadingSpaces(line);
                if (indent % 2 != 0) {
                    throw new IllegalArgumentException("Invalid indentation at line " + lineNumber);
                }

                int level = indent / 2;
                String trimmed = line.substring(indent).trim();

                if (level == 0) {
                    if (!"cluster:".equals(trimmed)) {
                        throw new IllegalArgumentException("Expected 'cluster:' at line " + lineNumber);
                    }
                    seenClusterSection = true;
                    currentNode = null;
                    continue;
                }

                if (!seenClusterSection) {
                    throw new IllegalArgumentException("Content found before 'cluster:' section at line " + lineNumber);
                }

                if (level == 1) {
                    currentNode = null;
                    if ("nodes:".equals(trimmed)) {
                        if (seenNodesSection) {
                            throw new IllegalArgumentException("Duplicate 'nodes' section at line " + lineNumber);
                        }
                        seenNodesSection = true;
                        continue;
                    }
                    putKeyValue(clusterValues, trimmed, lineNumber, "cluster");
                    continue;
                }

                if (!seenNodesSection) {
                    throw new IllegalArgumentException("Node entry found before nodes section at line " + lineNumber);
                }

                if (level == 2) {
                    if (!trimmed.startsWith("-")) {
                        throw new IllegalArgumentException("Expected node list item at line " + lineNumber);
                    }
                    currentNode = new HashMap<>();
                    nodeValues.add(currentNode);

                    String entry = trimmed.substring(1).trim();
                    if (!entry.isEmpty()) {
                        putKeyValue(currentNode, entry, lineNumber, "node");
                    }
                    continue;
                }

                if (level == 3) {
                    if (currentNode == null) {
                        throw new IllegalArgumentException("Node attribute found before node entry at line " + lineNumber);
                    }
                    putKeyValue(currentNode, trimmed, lineNumber, "node");
                    continue;
                }

                throw new IllegalArgumentException("Unsupported indentation at line " + lineNumber);
            }
        }

        if (!seenClusterSection) {
            throw new IllegalArgumentException("Missing 'cluster' section in config");
        }

        String name = defaultString(clusterValues.get("name"), "loomq-cluster");
        int totalShards = parseIntValue(clusterValues.get("total_shards"), 1, "total_shards");
        int localShardIndex = parseIntValue(clusterValues.get("local_shard_index"), 0, "local_shard_index");
        boolean hotReload = parseBooleanValue(clusterValues.get("hot_reload"), false, "hot_reload");
        long reloadInterval = parseLongValue(clusterValues.get("reload_interval_ms"), 60000L, "reload_interval_ms");

        List<NodeConfig> nodes = new ArrayList<>();
        for (Map<String, String> node : nodeValues) {
            nodes.add(new NodeConfig(
                    requiredString(node, "shard_id", "node"),
                    requiredString(node, "host", "node"),
                    parseIntValue(node.get("port"), 8080, "port"),
                    parseIntValue(node.get("weight"), 100, "weight")
            ));
        }

        return new ClusterConfig(name, totalShards, localShardIndex, nodes, hotReload, reloadInterval);
    }

    private static void putKeyValue(Map<String, String> target, String line, int lineNumber, String scope) {
        int colonIndex = line.indexOf(':');
        if (colonIndex < 0) {
            throw new IllegalArgumentException("Expected key:value pair in " + scope + " at line " + lineNumber);
        }

        String key = line.substring(0, colonIndex).trim();
        String value = line.substring(colonIndex + 1).trim();
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Empty key in " + scope + " at line " + lineNumber);
        }
        target.put(key, unquote(value));
    }

    private static String requiredString(Map<String, String> values, String key, String scope) {
        String value = values.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required " + scope + " field: " + key);
        }
        return value;
    }

    private static String defaultString(String value, String defaultValue) {
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static int parseIntValue(String value, int defaultValue, String fieldName) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer for " + fieldName + ": " + value, e);
        }
    }

    private static long parseLongValue(String value, long defaultValue, String fieldName) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long for " + fieldName + ": " + value, e);
        }
    }

    private static boolean parseBooleanValue(String value, boolean defaultValue, String fieldName) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }

        String normalized = value.toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "true", "1", "yes", "on" -> true;
            case "false", "0", "no", "off" -> false;
            default -> throw new IllegalArgumentException("Invalid boolean for " + fieldName + ": " + value);
        };
    }

    private static String unquote(String value) {
        if (value.length() >= 2) {
            if ((value.startsWith("\"") && value.endsWith("\""))
                    || (value.startsWith("'") && value.endsWith("'"))) {
                return value.substring(1, value.length() - 1);
            }
        }
        return value;
    }

    private static String stripComment(String line) {
        boolean singleQuoted = false;
        boolean doubleQuoted = false;
        StringBuilder result = new StringBuilder(line.length());

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);

            if (ch == '\'' && !doubleQuoted) {
                singleQuoted = !singleQuoted;
                result.append(ch);
                continue;
            }

            if (ch == '"' && !singleQuoted) {
                doubleQuoted = !doubleQuoted;
                result.append(ch);
                continue;
            }

            if (ch == '#' && !singleQuoted && !doubleQuoted) {
                break;
            }

            result.append(ch);
        }

        return result.toString();
    }

    private static int countLeadingSpaces(String line) {
        int count = 0;
        while (count < line.length() && line.charAt(count) == ' ') {
            count++;
        }
        return count;
    }

    // 环境变量工具方法
    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(ENV_PREFIX + key);
        return value != null ? value : defaultValue;
    }

    private static int getEnvInt(String key, int defaultValue) {
        String value = System.getenv(ENV_PREFIX + key);
        if (value == null) return defaultValue;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static long getEnvLong(String key, long defaultValue) {
        String value = System.getenv(ENV_PREFIX + key);
        if (value == null) return defaultValue;
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static boolean getEnvBool(String key, boolean defaultValue) {
        String value = System.getenv(ENV_PREFIX + key);
        if (value == null) return defaultValue;
        return Boolean.parseBoolean(value) || value.equals("1") || value.equalsIgnoreCase("yes");
    }

    private static List<NodeConfig> parseNodesFromEnv(String envValue) {
        List<NodeConfig> nodes = new ArrayList<>();
        if (envValue == null || envValue.isEmpty()) {
            return nodes;
        }

        // 格式：shard-0:localhost:8080:100,shard-1:localhost:8081:100
        String[] parts = envValue.split(",");
        for (String part : parts) {
            String[] fields = part.split(":");
            if (fields.length >= 3) {
                String shardId = fields[0];
                String host = fields[1];
                int port = Integer.parseInt(fields[2]);
                int weight = fields.length > 3 ? Integer.parseInt(fields[3]) : 100;
                nodes.add(new NodeConfig(shardId, host, port, weight));
            }
        }
        return nodes;
    }

    /**
     * 节点配置
     */
    public record NodeConfig(String shardId, String host, int port, int weight) {

        public NodeConfig {
            Objects.requireNonNull(shardId, "shardId cannot be null");
            Objects.requireNonNull(host, "host cannot be null");
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException("Invalid port: " + port);
            }
            if (weight <= 0) {
                throw new IllegalArgumentException("weight must be positive");
            }
        }

        @Override
        public String toString() {
            return String.format("%s@%s:%d(w=%d)", shardId, host, port, weight);
        }
    }

    // ========== v0.4.8 新增：租约配置 ==========

    /**
     * 租约配置
     */
    public record LeaseConfig(
        boolean enabled,           // 是否启用租约机制
        long durationMs,           // 租约有效期（毫秒）
        double renewalWindowRatio, // 续约窗口比例（0.0-1.0）
        boolean strictMode         // 严格模式：无有效租约拒绝写入
    ) {
        public LeaseConfig {
            if (durationMs <= 0) {
                throw new IllegalArgumentException("durationMs must be positive");
            }
            if (renewalWindowRatio < 0.0 || renewalWindowRatio > 1.0) {
                throw new IllegalArgumentException("renewalWindowRatio must be in [0.0, 1.0]");
            }
        }

        public static LeaseConfig defaultConfig() {
            return new LeaseConfig(true, 10000L, 0.3, true);
        }

        public static LeaseConfig disabled() {
            return new LeaseConfig(false, 10000L, 0.3, false);
        }
    }

    /**
     * Fencing Token 配置
     */
    public record FencingConfig(
        boolean enabled,      // 是否启用 fencing token
        long tokenTtlMs       // token 最长有效期（毫秒）
    ) {
        public FencingConfig {
            if (tokenTtlMs <= 0) {
                throw new IllegalArgumentException("tokenTtlMs must be positive");
            }
        }

        public static FencingConfig defaultConfig() {
            return new FencingConfig(true, 30000L);
        }
    }

    /**
     * 配置变更监听器
     */
    @FunctionalInterface
    public interface ConfigChangeListener {
        void onConfigChange(ClusterConfig oldConfig, ClusterConfig newConfig);
    }
}

package com.loomq.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 集群配置管理
 *
 * 管理分片集群的配置信息，支持：
 * 1. 从配置文件加载
 * 2. 从环境变量加载
 * 3. 配置热更新
 * 4. 配置变更监听
 *
 * 配置文件格式（cluster.yml）：
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
    @SuppressWarnings("unchecked")
    private static ClusterConfig parseYaml(InputStream is) {
        Yaml yaml = new Yaml();
        Map<String, Object> data = yaml.load(is);

        Map<String, Object> cluster = (Map<String, Object>) data.get("cluster");
        if (cluster == null) {
            throw new IllegalArgumentException("Missing 'cluster' section in config");
        }

        String name = (String) cluster.getOrDefault("name", "loomq-cluster");
        int totalShards = (int) cluster.getOrDefault("total_shards", 1);
        int localShardIndex = (int) cluster.getOrDefault("local_shard_index", 0);
        boolean hotReload = (boolean) cluster.getOrDefault("hot_reload", false);
        int reloadInterval = (int) cluster.getOrDefault("reload_interval_ms", 60000);

        List<NodeConfig> nodes = new ArrayList<>();
        List<Map<String, Object>> nodeList = (List<Map<String, Object>>) cluster.get("nodes");
        if (nodeList != null) {
            for (Map<String, Object> node : nodeList) {
                nodes.add(new NodeConfig(
                        (String) node.get("shard_id"),
                        (String) node.get("host"),
                        (int) node.getOrDefault("port", 8080),
                        (int) node.getOrDefault("weight", 100)
                ));
            }
        }

        return new ClusterConfig(name, totalShards, localShardIndex, nodes, hotReload, reloadInterval);
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

    /**
     * 配置变更监听器
     */
    @FunctionalInterface
    public interface ConfigChangeListener {
        void onConfigChange(ClusterConfig oldConfig, ClusterConfig newConfig);
    }
}

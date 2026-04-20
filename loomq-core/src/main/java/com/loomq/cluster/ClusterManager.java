package com.loomq.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 集群管理器
 *
 * 管理集群的生命周期，包括：
 * 1. 本地节点的启动和停止
 * 2. 路由器的初始化和管理
 * 3. 集群配置的加载和热更新
 *
 * @author loomq
 * @since v0.3
 */
public class ClusterManager {

    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);

    // 集群配置
    private final ClusterConfig config;

    // 本地分片节点
    private LocalShardNode localNode;

    // 路由器
    private final ShardRouter router;

    // 已知节点（用于扩展）
    private final Map<String, ShardNode> knownNodes;

    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * 创建集群管理器（从默认配置加载）
     */
    public ClusterManager() {
        this(ClusterConfig.loadDefault());
    }

    /**
     * 创建集群管理器（指定配置）
     *
     * @param config 集群配置
     */
    public ClusterManager(ClusterConfig config) {
        this.config = config;
        this.router = new ShardRouter();
        this.knownNodes = new ConcurrentHashMap<>();
        logger.info("ClusterManager created for cluster: {}", config.getClusterName());
    }

    /**
     * 创建单节点集群管理器（用于测试或单节点部署）
     *
     * @param port 服务端口
     * @return 集群管理器
     */
    public static ClusterManager singleNode(int port) {
        ClusterConfig config = new ClusterConfig(
                "loomq-single",
                1,
                0,
                java.util.List.of(new ClusterConfig.NodeConfig("shard-0", "localhost", port, 100)),
                false,
                60000
        );
        return new ClusterManager(config);
    }

    /**
     * 启动集群管理器
     *
     * @throws IOException 如果启动失败
     */
    public void start() throws IOException {
        if (!running.compareAndSet(false, true)) {
            logger.warn("ClusterManager is already running");
            return;
        }

        logger.info("Starting ClusterManager...");

        // 1. 初始化路由器
        initializeRouter();

        // 2. 创建本地节点
        createLocalNode();

        // 3. 启动本地节点
        if (localNode != null) {
            localNode.start();
        }

        logger.info("ClusterManager started successfully, mode: {}",
                config.isSharded() ? "sharded" : "single-node");
    }

    /**
     * 停止集群管理器
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        logger.info("Stopping ClusterManager...");

        // 停止本地节点
        if (localNode != null) {
            try {
                localNode.prepareShutdown();
                // TODO: 等待数据迁移完成
                localNode.shutdown();
            } catch (Exception e) {
                logger.error("Error stopping local node", e);
            }
        }

        // 清空路由器
        router.clear();

        logger.info("ClusterManager stopped");
    }

    /**
     * 初始化路由器
     */
    private void initializeRouter() {
        logger.info("Initializing router with {} nodes", config.getNodes().size());

        // 添加所有已知节点到路由器
        for (ClusterConfig.NodeConfig nodeConfig : config.getNodes()) {
            // 解析 shard index
            int shardIndex = parseShardIndex(nodeConfig.shardId());

            // 创建节点引用（不一定是本地节点）
            InetSocketAddress address = new InetSocketAddress(nodeConfig.host(), nodeConfig.port());
            LocalShardNode node = new LocalShardNode(
                    shardIndex,
                    config.getTotalShards(),
                    address,
                    nodeConfig.weight()
            );

            // 如果配置中已经标记为 active，直接启动
            if (shardIndex == config.getLocalShardIndex()) {
                this.localNode = node;
            }

            router.addNode(node);
            knownNodes.put(nodeConfig.shardId(), node);
        }

        logger.info("Router initialized with {} virtual nodes", router.getVirtualNodeCount());
    }

    /**
     * 创建本地节点
     */
    private void createLocalNode() throws IOException {
        if (localNode == null) {
            // 使用配置创建本地节点
            ClusterConfig.NodeConfig nodeConfig = config.getLocalNodeConfig();
            InetSocketAddress address = new InetSocketAddress(nodeConfig.host(), nodeConfig.port());

            localNode = new LocalShardNode(
                    config.getLocalShardIndex(),
                    config.getTotalShards(),
                    address,
                    nodeConfig.weight()
            );

            // 更新 knownNodes
            knownNodes.put(localNode.getShardId(), localNode);

            // 更新路由器中的节点
            router.removeNode(localNode);  // 先移除旧的
            router.addNode(localNode);      // 添加新的
        }

        logger.info("Local node created: {}", localNode.toNodeString());
    }

    /**
     * 解析 shard ID 获取索引
     */
    private int parseShardIndex(String shardId) {
        if (shardId.startsWith("shard-")) {
            try {
                return Integer.parseInt(shardId.substring(6));
            } catch (NumberFormatException e) {
                logger.warn("Invalid shardId format: {}", shardId);
            }
        }
        return -1;
    }

    /**
     * 路由 Intent 到分片节点
     *
     * @param intentId Intent ID
     * @return 负责该 Intent 的分片节点
     */
    public ShardNode route(String intentId) {
        return router.route(intentId);
    }

    /**
     * 检查 Intent 是否应由本地节点处理
     *
     * @param intentId Intent ID
     * @return true 如果是本地 Intent
     */
    public boolean isLocalTask(String intentId) {
        if (localNode == null || !localNode.isAvailable()) {
            return false;
        }

        ShardNode responsible = router.route(intentId);
        if (responsible == null) {
            return false;
        }

        return responsible.getShardId().equals(localNode.getShardId());
    }

    // Getters
    public ClusterConfig getConfig() {
        return config;
    }

    public LocalShardNode getLocalNode() {
        return localNode;
    }

    public ShardRouter getRouter() {
        return router;
    }

    public String getLocalShardId() {
        return localNode != null ? localNode.getShardId() : null;
    }

    public boolean isRunning() {
        return running.get();
    }

    public boolean isSharded() {
        return config.isSharded();
    }

    /**
     * 获取节点数量
     */
    public int getNodeCount() {
        return knownNodes.size();
    }

    /**
     * 获取集群状态摘要
     */
    public ClusterStatus getStatus() {
        return new ClusterStatus(
                config.getClusterName(),
                running.get(),
                config.isSharded(),
                config.getTotalShards(),
                localNode != null ? localNode.getShardId() : null,
                localNode != null ? localNode.getState() : null,
                knownNodes.size(),
                router.getVirtualNodeCount()
        );
    }

    /**
     * 集群状态
     */
    public record ClusterStatus(
            String clusterName,
            boolean running,
            boolean sharded,
            int totalShards,
            String localShardId,
            ShardNode.State localState,
            int knownNodes,
            int virtualNodes
    ) {
        @Override
        public String toString() {
            return String.format(
                    "ClusterStatus{name=%s, running=%s, sharded=%s, shards=%d, local=%s[%s], nodes=%d, vnodes=%d}",
                    clusterName, running, sharded, totalShards,
                    localShardId, localState, knownNodes, virtualNodes
            );
        }
    }
}

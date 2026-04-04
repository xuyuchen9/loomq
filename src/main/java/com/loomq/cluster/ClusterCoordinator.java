package com.loomq.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 集群协调器（V0.3 核心组件）
 *
 * 职责：
 * 1. 节点注册与发现
 * 2. 分片分配管理
 * 3. 健康检查协调
 * 4. 集群状态同步
 *
 * V0.3 简化设计：
 * - 基于配置文件的静态节点发现（生产环境可替换为 etcd/consul）
 * - 定时心跳检测
 * - 支持手动触发分片迁移
 *
 * @author loomq
 * @since v0.3
 */
public class ClusterCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(ClusterCoordinator.class);

    // 默认心跳间隔（毫秒）
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 5000;

    // 默认心跳超时（毫秒）
    public static final long DEFAULT_HEARTBEAT_TIMEOUT_MS = 15000;

    // 集群配置
    private final ClusterConfig config;

    // 路由器
    private final ShardRouter router;

    // 已知节点：shardId -> NodeInfo
    private final ConcurrentHashMap<String, NodeInfo> knownNodes;

    // 本地节点
    private final LocalShardNode localNode;

    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);

    // 心跳执行器
    private ScheduledExecutorService heartbeatExecutor;

    // 心跳间隔
    private final long heartbeatIntervalMs;

    // 心跳超时
    private final long heartbeatTimeoutMs;

    // 状态变更监听器
    private final List<ClusterStateListener> stateListeners;

    /**
     * 创建集群协调器
     *
     * @param config    集群配置
     * @param localNode 本地节点
     * @param router    分片路由器
     */
    public ClusterCoordinator(ClusterConfig config, LocalShardNode localNode, ShardRouter router) {
        this(config, localNode, router, DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_HEARTBEAT_TIMEOUT_MS);
    }

    public ClusterCoordinator(ClusterConfig config, LocalShardNode localNode, ShardRouter router,
                              long heartbeatIntervalMs, long heartbeatTimeoutMs) {
        this.config = config;
        this.localNode = localNode;
        this.router = router;
        this.knownNodes = new ConcurrentHashMap<>();
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.stateListeners = new CopyOnWriteArrayList<>();

        logger.info("ClusterCoordinator created for cluster: {}", config.getClusterName());
    }

    /**
     * 启动协调器
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        logger.info("Starting ClusterCoordinator...");

        // 1. 注册本地节点
        registerLocalNode();

        // 2. 发现其他节点（基于配置）
        discoverNodes();

        // 3. 启动心跳
        startHeartbeat();

        logger.info("ClusterCoordinator started, known nodes: {}", knownNodes.size());
    }

    /**
     * 停止协调器
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        logger.info("Stopping ClusterCoordinator...");

        // 停止心跳
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
            try {
                heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        logger.info("ClusterCoordinator stopped");
    }

    /**
     * 注册本地节点
     */
    private void registerLocalNode() {
        // 确保本地节点已启动
        if (localNode.getState() == ShardNode.State.JOINING) {
            localNode.start();
        }

        NodeInfo info = new NodeInfo(
                localNode.getShardId(),
                localNode.getAddress().getHostName(),
                localNode.getAddress().getPort(),
                localNode.getState(),
                Instant.now(),
                Instant.now()
        );
        knownNodes.put(localNode.getShardId(), info);

        logger.info("Registered local node: {} with state {}", localNode.getShardId(), localNode.getState());
    }

    /**
     * 发现节点（基于配置文件）
     */
    private void discoverNodes() {
        for (ClusterConfig.NodeConfig nodeConfig : config.getNodes()) {
            String shardId = nodeConfig.shardId();

            // 跳过本地节点
            if (shardId.equals(localNode.getShardId())) {
                continue;
            }

            // 创建远程节点信息
            NodeInfo info = new NodeInfo(
                    shardId,
                    nodeConfig.host(),
                    nodeConfig.port(),
                    ShardNode.State.JOINING,
                    Instant.now(),
                    Instant.now()
            );
            knownNodes.put(shardId, info);

            logger.info("Discovered remote node: {}@{}:{}", shardId, nodeConfig.host(), nodeConfig.port());
        }
    }

    /**
     * 启动心跳
     */
    private void startHeartbeat() {
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cluster-heartbeat");
            t.setDaemon(true);
            return t;
        });

        heartbeatExecutor.scheduleAtFixedRate(
                this::checkNodeHealth,
                heartbeatIntervalMs,
                heartbeatIntervalMs,
                TimeUnit.MILLISECONDS
        );

        logger.info("Heartbeat started, interval={}ms, timeout={}ms",
                heartbeatIntervalMs, heartbeatTimeoutMs);
    }

    /**
     * 检查节点健康
     */
    private void checkNodeHealth() {
        if (!running.get()) {
            return;
        }

        // 更新本地节点心跳
        localNode.heartbeat();

        Instant now = Instant.now();
        long timeoutThreshold = heartbeatTimeoutMs;

        // 检查其他节点
        for (Map.Entry<String, NodeInfo> entry : knownNodes.entrySet()) {
            String shardId = entry.getKey();
            NodeInfo info = entry.getValue();

            // 跳过本地节点
            if (shardId.equals(localNode.getShardId())) {
                continue;
            }

            // 检查心跳超时
            long lastSeenMs = java.time.Duration.between(info.lastHeartbeat(), now).toMillis();

            if (lastSeenMs > timeoutThreshold) {
                // 节点可能已宕机
                if (info.state() != ShardNode.State.OFFLINE) {
                    logger.warn("Node {} heartbeat timeout (last seen {}ms ago), marking as OFFLINE",
                            shardId, lastSeenMs);

                    // 更新状态
                    NodeInfo offlineInfo = info.withState(ShardNode.State.OFFLINE);
                    knownNodes.put(shardId, offlineInfo);

                    // 通知监听器
                    notifyNodeOffline(shardId);
                }
            }
        }
    }

    /**
     * 接收心跳（用于远程节点）
     *
     * @param shardId 分片 ID
     */
    public void receiveHeartbeat(String shardId) {
        NodeInfo info = knownNodes.get(shardId);
        if (info != null) {
            NodeInfo updated = info.withHeartbeat(Instant.now());
            knownNodes.put(shardId, updated);

            // 如果节点之前是 OFFLINE，恢复为 ACTIVE
            if (info.state() == ShardNode.State.OFFLINE) {
                logger.info("Node {} recovered, marking as ACTIVE", shardId);
                knownNodes.put(shardId, updated.withState(ShardNode.State.ACTIVE));
                notifyNodeRecovered(shardId);
            }
        }
    }

    /**
     * 获取集群状态
     */
    public ClusterState getClusterState() {
        int activeNodes = 0;
        int offlineNodes = 0;
        int joiningNodes = 0;

        for (NodeInfo info : knownNodes.values()) {
            switch (info.state()) {
                case ACTIVE -> activeNodes++;
                case OFFLINE -> offlineNodes++;
                case JOINING -> joiningNodes++;
                default -> {}
            }
        }

        return new ClusterState(
                config.getClusterName(),
                config.getTotalShards(),
                knownNodes.size(),
                activeNodes,
                offlineNodes,
                joiningNodes,
                running.get()
        );
    }

    /**
     * 获取所有节点信息
     */
    public Collection<NodeInfo> getAllNodes() {
        return Collections.unmodifiableCollection(knownNodes.values());
    }

    /**
     * 获取指定节点信息
     */
    public Optional<NodeInfo> getNodeInfo(String shardId) {
        return Optional.ofNullable(knownNodes.get(shardId));
    }

    /**
     * 添加状态监听器
     */
    public void addStateListener(ClusterStateListener listener) {
        stateListeners.add(listener);
    }

    /**
     * 移除状态监听器
     */
    public void removeStateListener(ClusterStateListener listener) {
        stateListeners.remove(listener);
    }

    private void notifyNodeOffline(String shardId) {
        for (ClusterStateListener listener : stateListeners) {
            try {
                listener.onNodeOffline(shardId);
            } catch (Exception e) {
                logger.error("State listener error", e);
            }
        }
    }

    private void notifyNodeRecovered(String shardId) {
        for (ClusterStateListener listener : stateListeners) {
            try {
                listener.onNodeRecovered(shardId);
            } catch (Exception e) {
                logger.error("State listener error", e);
            }
        }
    }

    // ========== 内部类 ==========

    /**
     * 节点信息
     */
    public record NodeInfo(
            String shardId,
            String host,
            int port,
            ShardNode.State state,
            Instant joinTime,
            Instant lastHeartbeat
    ) {
        public NodeInfo withState(ShardNode.State newState) {
            return new NodeInfo(shardId, host, port, newState, joinTime, lastHeartbeat);
        }

        public NodeInfo withHeartbeat(Instant heartbeat) {
            return new NodeInfo(shardId, host, port, state, joinTime, heartbeat);
        }

        public String address() {
            return host + ":" + port;
        }
    }

    /**
     * 集群状态
     */
    public record ClusterState(
            String clusterName,
            int totalShards,
            int knownNodes,
            int activeNodes,
            int offlineNodes,
            int joiningNodes,
            boolean coordinatorRunning
    ) {
        public boolean isHealthy() {
            return activeNodes == totalShards && offlineNodes == 0;
        }

        @Override
        public String toString() {
            return String.format("ClusterState{name=%s, shards=%d, active=%d, offline=%d, joining=%d}",
                    clusterName, totalShards, activeNodes, offlineNodes, joiningNodes);
        }
    }

    /**
     * 集群状态监听器
     */
    public interface ClusterStateListener {
        default void onNodeOffline(String shardId) {}
        default void onNodeRecovered(String shardId) {}
        default void onNodeAdded(String shardId) {}
        default void onNodeRemoved(String shardId) {}
    }
}

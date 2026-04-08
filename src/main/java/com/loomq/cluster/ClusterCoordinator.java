package com.loomq.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 集群协调器 V2 - v0.4.8 租约仲裁版本
 *
 * 硬约束 #1：Coordinator 必须是"租约 + 版本号"仲裁，不是简单投票
 *
 * 核心职责：
 * 1. 租约管理：发放、续约、撤销租约
 * 2. 路由表版本管理：单调递增版本号
 * 3. Fencing Token 生成：防止脑裂
 * 4. 故障仲裁：决定哪个节点成为新 primary
 * 5. 节点健康检测与抖动判定
 *
 * 设计原则：
 * - 租约机制：只有持有有效租约的节点才能作为 primary 写入
 * - 版本号机制：路由表版本单调递增，failover 后递增
 * - 仲裁中心：所有角色切换必须经过 coordinator 确认
 * - Fencing：旧 primary 的写入会被 fencing token 拒绝
 *
 * 注意：这不是 Raft，只是一个租约仲裁服务。
 * v0.4.8 不追求分布式共识，而是追求"可解释的高可用"。
 *
 * @author loomq
 * @since v0.4.8
 */
public class ClusterCoordinator implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ClusterCoordinator.class);

    // 默认心跳间隔（毫秒）
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 3000;

    // 默认心跳超时（毫秒）
    public static final long DEFAULT_HEARTBEAT_TIMEOUT_MS = 3000;

    // 默认抖动阈值（连续失败次数）
    public static final int DEFAULT_FLAPPING_THRESHOLD = 3;

    // 抖动窗口（毫秒）
    public static final long DEFAULT_FLAPPING_WINDOW_MS = 30000;

    // ========== v0.4.8 新增：租约配置 ==========
    // 默认租约有效期（毫秒）
    public static final long DEFAULT_LEASE_DURATION_MS = 10000;  // 10 秒

    // 默认最大 token 有效期（毫秒）
    public static final long DEFAULT_MAX_TOKEN_TTL_MS = 30000;   // 30 秒

    // 默认续约窗口比例
    public static final double DEFAULT_RENEWAL_WINDOW_RATIO = 0.3;

    // 集群配置
    private final ClusterConfig config;

    // 路由表（带版本控制）
    private final RoutingTable routingTable;

    // 本地节点
    private final LocalShardNode localNode;

    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);

    // 心跳执行器
    private ScheduledExecutorService heartbeatExecutor;

    // 心跳配置
    private final long heartbeatIntervalMs;
    private final long heartbeatTimeoutMs;

    // 节点健康状态跟踪
    private final ConcurrentHashMap<String, NodeHealth> nodeHealthMap;

    // 状态变更监听器
    private final List<ClusterStateListener> stateListeners;

    // 路由表版本号
    private final AtomicLong routingTableVersion;

    // 故障时策略配置
    private final FailureHandlingConfig failureConfig;

    // ========== v0.4.8 新增：租约仲裁 ==========

    // Fencing Token 序列号生成器
    private final AtomicLong fencingSequenceGenerator;

    // 租约存储：shardId -> CoordinatorLease
    private final ConcurrentHashMap<String, CoordinatorLease> activeLeases;

    // 路由表版本：shardId -> version（每个分片独立版本）
    private final ConcurrentHashMap<String, AtomicLong> shardRoutingVersions;

    // 租约配置
    private final long leaseDurationMs;
    private final double renewalWindowRatio;

    // 租约事件监听器
    private volatile java.util.function.Consumer<LeaseEvent> leaseEventListener;

    /**
     * 创建集群协调器
     */
    public ClusterCoordinator(ClusterConfig config, LocalShardNode localNode) {
        this(config, localNode, DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_HEARTBEAT_TIMEOUT_MS,
            DEFAULT_LEASE_DURATION_MS, DEFAULT_RENEWAL_WINDOW_RATIO);
    }

    public ClusterCoordinator(ClusterConfig config, LocalShardNode localNode,
                                 long heartbeatIntervalMs, long heartbeatTimeoutMs) {
        this(config, localNode, heartbeatIntervalMs, heartbeatTimeoutMs,
            DEFAULT_LEASE_DURATION_MS, DEFAULT_RENEWAL_WINDOW_RATIO);
    }

    /**
     * 创建集群协调器（兼容 v0.4.4 配置）
     *
     * @deprecated 使用完整配置构造函数
     */
    @Deprecated
    public ClusterCoordinator(ClusterConfig config, LocalShardNode localNode,
                              long heartbeatIntervalMs, long heartbeatTimeoutMs,
                              long leaseDurationMs) {
        this(config, localNode, heartbeatIntervalMs, heartbeatTimeoutMs,
            leaseDurationMs, DEFAULT_RENEWAL_WINDOW_RATIO);
    }

    /**
     * 创建集群协调器（完整配置）
     *
     * @param config 集群配置
     * @param localNode 本地节点
     * @param heartbeatIntervalMs 心跳间隔（毫秒）
     * @param heartbeatTimeoutMs 心跳超时（毫秒）
     * @param leaseDurationMs 租约有效期（毫秒）
     * @param renewalWindowRatio 续约窗口比例
     */
    public ClusterCoordinator(ClusterConfig config, LocalShardNode localNode,
                                 long heartbeatIntervalMs, long heartbeatTimeoutMs,
                                 long leaseDurationMs, double renewalWindowRatio) {
        this.config = config;
        this.localNode = localNode;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.leaseDurationMs = leaseDurationMs;
        this.renewalWindowRatio = renewalWindowRatio;

        // 初始化路由表（带抖动检测）
        RoutingTable.RoutingTableConfig routingConfig = new RoutingTable.RoutingTableConfig(
                DEFAULT_FLAPPING_THRESHOLD,
                DEFAULT_FLAPPING_WINDOW_MS,
                5000
        );
        this.routingTable = new RoutingTable(routingConfig);

        this.nodeHealthMap = new ConcurrentHashMap<>();
        this.stateListeners = new CopyOnWriteArrayList<>();
        this.routingTableVersion = new AtomicLong(0);
        this.failureConfig = FailureHandlingConfig.defaultConfig();

        // v0.4.8 新增：初始化租约相关
        this.fencingSequenceGenerator = new AtomicLong(0);
        this.activeLeases = new ConcurrentHashMap<>();
        this.shardRoutingVersions = new ConcurrentHashMap<>();

        // 注册路由表监听器
        this.routingTable.addListener(this::onRoutingTableChanged);

        logger.info("ClusterCoordinator created for cluster: {} (v0.4.8 lease arbitration)",
            config.getClusterName());
    }

    // ========== 生命周期管理 ==========

    /**
     * 启动协调器
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        logger.info("Starting ClusterCoordinator...");

        // 1. 初始化本地节点
        initializeLocalNode();

        // 2. 初始化路由表
        initializeRoutingTable();

        // 3. 启动心跳
        startHeartbeat();

        logger.info("ClusterCoordinator started, routing table version: {}",
                routingTable.getVersion());
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

    @Override
    public void close() {
        stop();

        // v0.4.8 新增：撤销所有租约
        for (Map.Entry<String, CoordinatorLease> entry : activeLeases.entrySet()) {
            revokeLeaseInternal(entry.getKey(), entry.getValue().getLeaseId(),
                "Coordinator shutdown");
        }
    }

    // ========== 初始化方法 ==========

    private void initializeLocalNode() {
        // 确保本地节点已启动
        if (localNode.getState() == ShardNode.State.JOINING) {
            localNode.start();
        }

        NodeHealth health = new NodeHealth(
                localNode.getShardId(),
                NodeHealth.Status.HEALTHY,
                0,
                Instant.now(),
                Instant.now()
        );
        nodeHealthMap.put(localNode.getShardId(), health);

        logger.info("Local node initialized: {} with state {}",
                localNode.getShardId(), localNode.getState());
    }

    private void initializeRoutingTable() {
        ShardRouter router = new ShardRouter();

        // 添加所有配置节点到路由器
        for (ClusterConfig.NodeConfig nodeConfig : config.getNodes()) {
            int shardIndex = parseShardIndex(nodeConfig.shardId());
            LocalShardNode node = new LocalShardNode(
                    shardIndex,
                    config.getTotalShards(),
                    nodeConfig.host(),
                    nodeConfig.port(),
                    nodeConfig.weight()
            );
            router.addNode(node);
        }

        // 构建节点状态映射
        Map<String, ShardNode.State> nodeStates = new HashMap<>();
        for (ClusterConfig.NodeConfig nodeConfig : config.getNodes()) {
            nodeStates.put(nodeConfig.shardId(), ShardNode.State.ACTIVE);
        }

        // 强制初始化路由表
        routingTable.forceUpdate(router, nodeStates);
        routingTableVersion.set(routingTable.getVersion());

        logger.info("Routing table initialized with version {}, {} nodes",
                routingTable.getVersion(), config.getNodes().size());
    }

    // ========== 心跳与健康管理 ==========

    private void startHeartbeat() {
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cluster-heartbeat-v2");
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
        updateNodeHealth(localNode.getShardId(), NodeHealth.Status.HEALTHY);

        Instant now = Instant.now();

        // 检查其他节点
        for (Map.Entry<String, NodeHealth> entry : nodeHealthMap.entrySet()) {
            String shardId = entry.getKey();
            NodeHealth health = entry.getValue();

            // 跳过本地节点
            if (shardId.equals(localNode.getShardId())) {
                continue;
            }

            // 计算心跳超时
            long lastSeenMs = java.time.Duration.between(health.lastHeartbeat(), now).toMillis();

            if (lastSeenMs > heartbeatTimeoutMs) {
                // 记录失败
                int newConsecutiveFailures = health.consecutiveFailures() + 1;
                updateNodeHealth(shardId, NodeHealth.Status.SUSPECT, newConsecutiveFailures);

                logger.warn("Node {} heartbeat timeout (last seen {}ms ago), consecutive failures: {}",
                        shardId, lastSeenMs, newConsecutiveFailures);

                // 检查是否达到抖动阈值
                if (newConsecutiveFailures >= DEFAULT_FLAPPING_THRESHOLD) {
                    markNodeOffline(shardId);
                }
            }
        }
    }

    /**
     * 标记节点离线
     */
    private void markNodeOffline(String shardId) {
        NodeHealth currentHealth = nodeHealthMap.get(shardId);
        if (currentHealth == null || currentHealth.status() == NodeHealth.Status.OFFLINE) {
            return;
        }

        // 检查节点是否正在抖动
        if (routingTable.isNodeFlapping(shardId)) {
            logger.warn("Node {} is flapping, delaying offline marking", shardId);
            return;
        }

        updateNodeHealth(shardId, NodeHealth.Status.OFFLINE, currentHealth.consecutiveFailures());

        // 更新路由表（CAS 操作）
        updateRoutingTableForOfflineNode(shardId);

        // 通知监听器
        notifyNodeOffline(shardId);

        logger.info("Node {} marked as OFFLINE, routing table version: {}",
                shardId, routingTable.getVersion());
    }

    /**
     * 更新节点健康状态
     */
    private void updateNodeHealth(String shardId, NodeHealth.Status status) {
        updateNodeHealth(shardId, status,
                status == NodeHealth.Status.HEALTHY ? 0 :
                        nodeHealthMap.getOrDefault(shardId,
                                new NodeHealth(shardId, status, 0, Instant.now(), Instant.now()))
                                .consecutiveFailures());
    }

    private void updateNodeHealth(String shardId, NodeHealth.Status status, int consecutiveFailures) {
        NodeHealth newHealth = new NodeHealth(
                shardId,
                status,
                consecutiveFailures,
                nodeHealthMap.getOrDefault(shardId,
                        new NodeHealth(shardId, status, 0, Instant.now(), Instant.now())).firstFailureTime(),
                status == NodeHealth.Status.HEALTHY ? Instant.now() :
                        nodeHealthMap.getOrDefault(shardId,
                                new NodeHealth(shardId, status, 0, Instant.now(), Instant.now())).lastHeartbeat()
        );

        nodeHealthMap.put(shardId, newHealth);
    }

    // ========== 路由表更新 ==========

    /**
     * 更新路由表以反映节点离线
     */
    private void updateRoutingTableForOfflineNode(String offlineShardId) {
        long currentVersion = routingTableVersion.get();

        // 获取当前路由表快照
        RoutingTable.TableSnapshot snapshot = routingTable.getSnapshot();
        if (snapshot.router() == null) {
            logger.warn("No router available for updating");
            return;
        }

        // 创建新的路由器（排除离线节点）
        ShardRouter newRouter = new ShardRouter();
        Map<String, ShardNode.State> newNodeStates = new HashMap<>();

        for (ClusterConfig.NodeConfig nodeConfig : config.getNodes()) {
            String shardId = nodeConfig.shardId();
            NodeHealth health = nodeHealthMap.get(shardId);

            ShardNode.State state;
            if (shardId.equals(offlineShardId)) {
                state = ShardNode.State.OFFLINE;
            } else if (health != null && health.status() == NodeHealth.Status.HEALTHY) {
                state = ShardNode.State.ACTIVE;
            } else {
                state = ShardNode.State.JOINING;
            }

            newNodeStates.put(shardId, state);

            // 只添加活跃节点到路由器
            if (state == ShardNode.State.ACTIVE) {
                int shardIndex = parseShardIndex(shardId);
                LocalShardNode node = new LocalShardNode(
                        shardIndex,
                        config.getTotalShards(),
                        nodeConfig.host(),
                        nodeConfig.port(),
                        nodeConfig.weight()
                );
                newRouter.addNode(node);
            }
        }

        // CAS 更新路由表
        boolean updated = routingTable.compareAndSwap(currentVersion, newRouter, newNodeStates);
        if (updated) {
            routingTableVersion.incrementAndGet();
            logger.info("Routing table updated for offline node: {}, new version: {}",
                    offlineShardId, routingTable.getVersion());
        } else {
            logger.warn("Failed to update routing table for offline node: {}", offlineShardId);
        }
    }

    /**
     * 路由表变更回调
     */
    private void onRoutingTableChanged(RoutingTable.TableSnapshot snapshot) {
        routingTableVersion.set(snapshot.version());
        logger.debug("Routing table changed: version={}, activeNodes={}",
                snapshot.version(), snapshot.getActiveNodeCount());
    }

    // ========== 公共 API ==========

    /**
     * 接收心跳（用于远程节点）
     */
    public void receiveHeartbeat(String shardId) {
        NodeHealth currentHealth = nodeHealthMap.get(shardId);

        if (currentHealth != null && currentHealth.status() == NodeHealth.Status.OFFLINE) {
            // 节点从离线恢复
            logger.info("Node {} recovered, marking as HEALTHY", shardId);
            updateNodeHealth(shardId, NodeHealth.Status.HEALTHY, 0);

            // 恢复节点到路由表
            recoverNodeToRoutingTable(shardId);

            notifyNodeRecovered(shardId);
        } else {
            updateNodeHealth(shardId, NodeHealth.Status.HEALTHY, 0);
        }
    }

    /**
     * 恢复节点到路由表
     */
    private void recoverNodeToRoutingTable(String recoveredShardId) {
        long currentVersion = routingTableVersion.get();

        RoutingTable.TableSnapshot snapshot = routingTable.getSnapshot();
        if (snapshot.router() == null) {
            return;
        }

        // 创建新的路由器（包含恢复的节点）
        ShardRouter newRouter = new ShardRouter();
        Map<String, ShardNode.State> newNodeStates = new HashMap<>(snapshot.nodeStates());
        newNodeStates.put(recoveredShardId, ShardNode.State.ACTIVE);

        for (ClusterConfig.NodeConfig nodeConfig : config.getNodes()) {
            String shardId = nodeConfig.shardId();
            ShardNode.State state = newNodeStates.getOrDefault(shardId, ShardNode.State.JOINING);

            if (state == ShardNode.State.ACTIVE) {
                int shardIndex = parseShardIndex(shardId);
                LocalShardNode node = new LocalShardNode(
                        shardIndex,
                        config.getTotalShards(),
                        nodeConfig.host(),
                        nodeConfig.port(),
                        nodeConfig.weight()
                );
                newRouter.addNode(node);
            }
        }

        // CAS 更新路由表
        boolean updated = routingTable.compareAndSwap(currentVersion, newRouter, newNodeStates);
        if (updated) {
            routingTableVersion.incrementAndGet();
            logger.info("Routing table updated for recovered node: {}, new version: {}",
                    recoveredShardId, routingTable.getVersion());
        }
    }

    /**
     * 路由任务到分片节点（带版本检查）
     */
    public RoutingTable.RoutingResult route(String taskId, Optional<Long> expectedVersion) {
        return routingTable.route(taskId, expectedVersion);
    }

    /**
     * 简单路由（不带版本检查）
     */
    public Optional<ShardNode> route(String taskId) {
        return routingTable.routeSimple(taskId);
    }

    /**
     * 检查任务是否应由本地节点处理
     */
    public boolean isLocalTask(String taskId) {
        Optional<ShardNode> node = route(taskId);
        return node.isPresent() &&
                node.get().getShardId().equals(localNode.getShardId());
    }

    /**
     * 获取当前路由表版本
     */
    public long getRoutingTableVersion() {
        return routingTable.getVersion();
    }

    /**
     * 获取路由表
     */
    public RoutingTable getRoutingTable() {
        return routingTable;
    }

    /**
     * 获取节点健康状态
     */
    public Optional<NodeHealth> getNodeHealth(String shardId) {
        return Optional.ofNullable(nodeHealthMap.get(shardId));
    }

    /**
     * 获取所有节点健康状态
     */
    public Map<String, NodeHealth> getAllNodeHealth() {
        return Map.copyOf(nodeHealthMap);
    }

    /**
     * 获取集群状态
     */
    public ClusterState getClusterState() {
        RoutingTable.TableSnapshot snapshot = routingTable.getSnapshot();

        return new ClusterState(
                config.getClusterName(),
                config.getTotalShards(),
                (int) snapshot.getActiveNodeCount(),
                (int) snapshot.getOfflineNodeCount(),
                routingTable.getVersion(),
                running.get()
        );
    }

    // ========== 监听器管理 ==========

    public void addStateListener(ClusterStateListener listener) {
        stateListeners.add(listener);
    }

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

    // ========== 工具方法 ==========

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

    // ========== 记录定义 ==========

    /**
     * 节点健康状态
     */
    public record NodeHealth(
            String shardId,
            Status status,
            int consecutiveFailures,
            Instant firstFailureTime,
            Instant lastHeartbeat
    ) {
        public enum Status {
            HEALTHY,    // 健康
            SUSPECT,    // 可疑（部分心跳超时）
            OFFLINE     // 离线
        }

        public boolean isHealthy() {
            return status == Status.HEALTHY;
        }

        public boolean isOffline() {
            return status == Status.OFFLINE;
        }
    }

    /**
     * 集群状态
     */
    public record ClusterState(
            String clusterName,
            int totalShards,
            int activeNodes,
            int offlineNodes,
            long routingTableVersion,
            boolean coordinatorRunning
    ) {
        public boolean isHealthy() {
            return activeNodes == totalShards && offlineNodes == 0;
        }

        @Override
        public String toString() {
            return String.format("ClusterState{name=%s, shards=%d, active=%d, offline=%d, version=%d}",
                    clusterName, totalShards, activeNodes, offlineNodes, routingTableVersion);
        }
    }

    /**
     * 故障处理配置
     */
    public record FailureHandlingConfig(
            boolean keepRunningTasks,      // 故障时是否继续执行旧节点任务
            boolean rerouteNewRequests,    // 是否将新请求路由到新节点
            long taskDrainTimeoutMs        // 任务排空超时
    ) {
        public static FailureHandlingConfig defaultConfig() {
            return new FailureHandlingConfig(
                    true,    // 保持旧节点任务运行
                    true,    // 新请求路由到新节点
                    300000   // 5 分钟排空超时
            );
        }
    }

    /**
     * 集群状态监听器
     */
    public interface ClusterStateListener {
        default void onNodeOffline(String shardId) {}
        default void onNodeRecovered(String shardId) {}
        default void onRoutingTableChanged(long newVersion) {}
    }

    // ========== v0.4.8 新增：租约管理（硬约束 #1）==========

    /**
     * 申请租约
     *
     * 关键逻辑：
     * 1. 检查是否有有效租约
     * 2. 如果有，只能由持有者续约
     * 3. 如果没有或已过期，发放新租约
     *
     * @param shardId 分片 ID
     * @param nodeId 申请节点 ID
     * @return 租约（可能为空表示申请被拒绝）
     */
    public synchronized Optional<CoordinatorLease> grantLease(String shardId, String nodeId) {
        if (!running.get()) {
            logger.warn("Coordinator not running, cannot grant lease");
            return Optional.empty();
        }

        Objects.requireNonNull(shardId, "shardId cannot be null");
        Objects.requireNonNull(nodeId, "nodeId cannot be null");

        // 获取当前租约
        CoordinatorLease currentLease = activeLeases.get(shardId);

        // 检查是否有有效租约
        if (currentLease != null && currentLease.isValid()) {
            // 只能由持有者续约
            if (!currentLease.isHeldBy(nodeId)) {
                logger.warn("Lease for shard {} is held by {}, rejecting request from {}",
                    shardId, currentLease.getHolderNodeId(), nodeId);
                return Optional.empty();
            }

            // 续约（延长过期时间）
            return renewLeaseInternal(shardId, currentLease);
        }

        // 检查是否有租约但已过期
        if (currentLease != null) {
            logger.info("Lease for shard {} expired (holder={}), revoking and issuing new",
                shardId, currentLease.getHolderNodeId());
            revokeLeaseInternal(shardId, currentLease.getLeaseId(), "Lease expired");
        }

        // 发放新租约
        return issueNewLease(shardId, nodeId);
    }

    /**
     * 续约租约
     *
     * @param shardId 分片 ID
     * @param leaseId 当前租约 ID
     * @return 新租约（如果失败返回空）
     */
    public synchronized Optional<CoordinatorLease> renewLease(String shardId, String leaseId) {
        if (!running.get()) {
            return Optional.empty();
        }

        CoordinatorLease currentLease = activeLeases.get(shardId);

        if (currentLease == null) {
            logger.warn("No active lease found for shard {} to renew", shardId);
            return Optional.empty();
        }

        if (!currentLease.getLeaseId().equals(leaseId)) {
            logger.warn("Lease ID mismatch for shard {}: expected {}, got {}",
                shardId, currentLease.getLeaseId(), leaseId);
            return Optional.empty();
        }

        return renewLeaseInternal(shardId, currentLease);
    }

    /**
     * 撤销租约（用于 failover）
     *
     * @param shardId 分片 ID
     * @param leaseId 租约 ID
     * @param reason 撤销原因
     */
    public synchronized void revokeLease(String shardId, String leaseId, String reason) {
        if (!running.get()) {
            return;
        }

        CoordinatorLease currentLease = activeLeases.get(shardId);
        if (currentLease != null && currentLease.getLeaseId().equals(leaseId)) {
            revokeLeaseInternal(shardId, leaseId, reason);
        } else {
            logger.warn("Cannot revoke lease {} for shard {}: not found or mismatch",
                leaseId, shardId);
        }
    }

    /**
     * 强制撤销租约（管理员接口）
     */
    public synchronized void forceRevokeLease(String shardId, String reason) {
        CoordinatorLease currentLease = activeLeases.get(shardId);
        if (currentLease != null) {
            revokeLeaseInternal(shardId, currentLease.getLeaseId(), reason);
        }
    }

    // ========== 租约内部操作 ==========

    private Optional<CoordinatorLease> issueNewLease(String shardId, String nodeId) {
        // 获取并递增路由表版本
        long newRoutingVersion = shardRoutingVersions
            .computeIfAbsent(shardId, k -> new AtomicLong(0))
            .incrementAndGet();

        // 生成新的 fencing sequence
        long fencingSequence = fencingSequenceGenerator.incrementAndGet();

        // 创建新租约
        CoordinatorLease lease = new CoordinatorLease(
            nodeId,
            shardId,
            leaseDurationMs,
            newRoutingVersion,
            fencingSequence
        );

        // 存储租约
        activeLeases.put(shardId, lease);

        // 通知监听器
        notifyLeaseEvent(new LeaseEvent(
            LeaseEvent.EventType.GRANTED,
            shardId,
            lease,
            null
        ));

        logger.info("New lease issued: {} for shard {} to node {} (routingVer={}, fencingSeq={})",
            lease.getLeaseId(), shardId, nodeId, newRoutingVersion, fencingSequence);

        return Optional.of(lease);
    }

    private Optional<CoordinatorLease> renewLeaseInternal(String shardId,
                                                           CoordinatorLease currentLease) {
        // 检查是否在续约窗口内
        if (!currentLease.canRenew(renewalWindowRatio)) {
            logger.debug("Lease {} not yet due for renewal", currentLease.getLeaseId());
            return Optional.of(currentLease);
        }

        // 创建新租约（保持相同的 routingVersion 和 fencingSequence）
        CoordinatorLease newLease = new CoordinatorLease(
            currentLease.getHolderNodeId(),
            shardId,
            leaseDurationMs,
            currentLease.getRoutingVersion(),
            currentLease.getFencingSequence()
        );

        // 更新存储
        activeLeases.put(shardId, newLease);

        // 通知监听器
        notifyLeaseEvent(new LeaseEvent(
            LeaseEvent.EventType.RENEWED,
            shardId,
            newLease,
            currentLease
        ));

        logger.info("Lease renewed: {} for shard {} (expires at {})",
            newLease.getLeaseId(), shardId, newLease.getExpiresAt());

        return Optional.of(newLease);
    }

    private void revokeLeaseInternal(String shardId, String leaseId, String reason) {
        CoordinatorLease lease = activeLeases.remove(shardId);

        if (lease != null) {
            // 递增路由表版本（failover 后版本号变化）
            shardRoutingVersions
                .computeIfAbsent(shardId, k -> new AtomicLong(0))
                .incrementAndGet();

            // 通知监听器
            notifyLeaseEvent(new LeaseEvent(
                LeaseEvent.EventType.REVOKED,
                shardId,
                null,
                lease
            ));

            logger.info("Lease {} for shard {} revoked: {}",
                leaseId, shardId, reason);
        }
    }

    // ========== Fencing Token 验证 ==========

    /**
     * 验证 fencing token 是否有效
     */
    public boolean validateFencingToken(String shardId, FencingToken token) {
        if (token == null) {
            return false;
        }

        CoordinatorLease currentLease = activeLeases.get(shardId);
        if (currentLease == null) {
            return false;
        }

        return token.isValidFor(currentLease);
    }

    /**
     * 获取当前有效的 fencing sequence
     */
    public long getCurrentFencingSequence(String shardId) {
        CoordinatorLease lease = activeLeases.get(shardId);
        return lease != null && lease.isValid() ? lease.getFencingSequence() : -1;
    }

    // ========== 租约查询 ==========

    /**
     * 获取当前租约
     */
    public Optional<CoordinatorLease> getCurrentLease(String shardId) {
        CoordinatorLease lease = activeLeases.get(shardId);
        if (lease != null && lease.isValid()) {
            return Optional.of(lease);
        }
        return Optional.empty();
    }

    /**
     * 检查是否有有效租约
     */
    public boolean hasValidLease(String shardId) {
        CoordinatorLease lease = activeLeases.get(shardId);
        return lease != null && lease.isValid();
    }

    /**
     * 获取当前 primary 节点
     */
    public Optional<String> getCurrentPrimary(String shardId) {
        return getCurrentLease(shardId).map(CoordinatorLease::getHolderNodeId);
    }

    /**
     * 获取分片路由表版本
     */
    public long getShardRoutingVersion(String shardId) {
        AtomicLong version = shardRoutingVersions.get(shardId);
        return version != null ? version.get() : 0;
    }

    // ========== 租约事件 ==========

    public void setLeaseEventListener(java.util.function.Consumer<LeaseEvent> listener) {
        this.leaseEventListener = listener;
    }

    private void notifyLeaseEvent(LeaseEvent event) {
        if (leaseEventListener != null) {
            try {
                leaseEventListener.accept(event);
            } catch (Exception e) {
                logger.error("Lease event listener error", e);
            }
        }
    }

    // ========== 租约相关记录 ==========

    /**
     * 租约事件
     */
    public record LeaseEvent(
        EventType type,
        String shardId,
        CoordinatorLease newLease,
        CoordinatorLease oldLease
    ) {
        public enum EventType {
            GRANTED,   // 新租约发放
            RENEWED,   // 租约续约
            REVOKED,   // 租约撤销
            EXPIRED    // 租约过期
        }
    }
}

package com.loomq.cluster;

import com.loomq.domain.cluster.FencingToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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

    // 状态变更监听器
    private final List<ClusterStateListener> stateListeners;

    // 路由表版本号
    private final AtomicLong routingTableVersion;

    // 故障时策略配置
    private final FailureHandlingConfig failureConfig;

    // ========== v0.4.8 新增：租约仲裁 ==========
    private final CoordinatorLeaseRegistry leaseRegistry;

    // ========== v0.4.8 新增：心跳监控 ==========
    private final ClusterHeartbeatMonitor heartbeatMonitor;

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

        // 初始化路由表（带抖动检测）
        RoutingTable.RoutingTableConfig routingConfig = new RoutingTable.RoutingTableConfig(
                DEFAULT_FLAPPING_THRESHOLD,
                DEFAULT_FLAPPING_WINDOW_MS,
                5000
        );
        this.routingTable = new RoutingTable(routingConfig);

        this.stateListeners = new CopyOnWriteArrayList<>();
        this.routingTableVersion = new AtomicLong(0);
        this.failureConfig = FailureHandlingConfig.defaultConfig();
        this.leaseRegistry = new CoordinatorLeaseRegistry(leaseDurationMs, renewalWindowRatio);
        this.heartbeatMonitor = new ClusterHeartbeatMonitor(
                localNode,
                heartbeatIntervalMs,
                heartbeatTimeoutMs,
                DEFAULT_FLAPPING_THRESHOLD
        );
        this.heartbeatMonitor.setOfflineAdmission(shardId -> !routingTable.isNodeFlapping(shardId));
        this.heartbeatMonitor.addOfflineListener(this::handleNodeOffline);
        this.heartbeatMonitor.addRecoveredListener(this::handleNodeRecovered);

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

        // 3. 启动心跳监控
        heartbeatMonitor.start();

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

        heartbeatMonitor.close();

        logger.info("ClusterCoordinator stopped");
    }

    @Override
    public void close() {
        stop();
        leaseRegistry.close();
    }

    // ========== 初始化方法 ==========

    private void initializeLocalNode() {
        // 确保本地节点已启动
        if (localNode.getState() == ShardNode.State.JOINING) {
            localNode.start();
        }

        logger.info("Local node initialized: {} with state {}",
                localNode.getShardId(), localNode.getState());
    }

    private void initializeRoutingTable() {
        ClusterRoutingPlanner.RoutingPlan plan = ClusterRoutingPlanner.initialPlan(config);

        // 强制初始化路由表
        routingTable.forceUpdate(plan.router(), plan.nodeStates());
        routingTableVersion.set(routingTable.getVersion());

        logger.info("Routing table initialized with version {}, {} nodes",
                routingTable.getVersion(), config.getNodes().size());
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

        ClusterRoutingPlanner.RoutingPlan plan =
                ClusterRoutingPlanner.offlinePlan(config, heartbeatMonitor.getAllNodeHealth(), offlineShardId);
        applyRoutingPlan(currentVersion, plan, "offline node", offlineShardId);
    }

    private void handleNodeOffline(String shardId) {
        updateRoutingTableForOfflineNode(shardId);
        notifyNodeOffline(shardId);
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

    public void receiveHeartbeat(String shardId) {
        heartbeatMonitor.receiveHeartbeat(shardId);
    }

    private void handleNodeRecovered(String shardId) {
        recoverNodeToRoutingTable(shardId);
        notifyNodeRecovered(shardId);
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

        ClusterRoutingPlanner.RoutingPlan plan =
                ClusterRoutingPlanner.recoveryPlan(config, snapshot, recoveredShardId);
        applyRoutingPlan(currentVersion, plan, "recovered node", recoveredShardId);
    }

    private void applyRoutingPlan(long currentVersion,
                                  ClusterRoutingPlanner.RoutingPlan plan,
                                  String action,
                                  String shardId) {
        boolean updated = routingTable.compareAndSwap(currentVersion, plan.router(), plan.nodeStates());
        if (updated) {
            routingTableVersion.set(routingTable.getVersion());
            logger.info("Routing table updated for {}: {}, new version: {}",
                    action, shardId, routingTable.getVersion());
        } else {
            logger.warn("Failed to update routing table for {}: {}", action, shardId);
        }
    }

    /**
     * 路由 Intent 到分片节点（带版本检查）
     */
    public RoutingTable.RoutingResult route(String intentId, Optional<Long> expectedVersion) {
        return routingTable.route(intentId, expectedVersion);
    }

    /**
     * 简单路由（不带版本检查）
     */
    public Optional<ShardNode> route(String intentId) {
        return routingTable.routeSimple(intentId);
    }

    /**
     * 检查 Intent 是否应由本地节点处理
     */
    public boolean isLocalIntent(String intentId) {
        Optional<ShardNode> node = route(intentId);
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

    public Optional<NodeHealth> getNodeHealth(String shardId) {
        return heartbeatMonitor.getNodeHealth(shardId);
    }

    public Map<String, NodeHealth> getAllNodeHealth() {
        return heartbeatMonitor.getAllNodeHealth();
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
            boolean keepRunningIntents,      // 故障时是否继续执行旧节点 intent
            boolean rerouteNewRequests,    // 是否将新请求路由到新节点
            long intentDrainTimeoutMs        // intent 排空超时
    ) {
        public static FailureHandlingConfig defaultConfig() {
            return new FailureHandlingConfig(
                    true,    // 保持旧节点 intent 运行
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
        return leaseRegistry.grantLease(running.get(), shardId, nodeId);
    }

    /**
     * 续约租约
     *
     * @param shardId 分片 ID
     * @param leaseId 当前租约 ID
     * @return 新租约（如果失败返回空）
     */
    public synchronized Optional<CoordinatorLease> renewLease(String shardId, String leaseId) {
        return leaseRegistry.renewLease(running.get(), shardId, leaseId);
    }

    /**
     * 撤销租约（用于 failover）
     *
     * @param shardId 分片 ID
     * @param leaseId 租约 ID
     * @param reason 撤销原因
     */
    public synchronized void revokeLease(String shardId, String leaseId, String reason) {
        leaseRegistry.revokeLease(running.get(), shardId, leaseId, reason);
    }

    /**
     * 强制撤销租约（管理员接口）
     */
    public synchronized void forceRevokeLease(String shardId, String reason) {
        leaseRegistry.forceRevokeLease(shardId, reason);
    }
    public boolean validateFencingToken(String shardId, FencingToken token) {
        return leaseRegistry.validateFencingToken(shardId, token);
    }

    /**
     * 获取当前有效的 fencing sequence
     */
    public long getCurrentFencingSequence(String shardId) {
        return leaseRegistry.getCurrentFencingSequence(shardId);
    }

    // ========== 租约查询 ==========

    /**
     * 获取当前租约
     */
    public Optional<CoordinatorLease> getCurrentLease(String shardId) {
        return leaseRegistry.getCurrentLease(shardId);
    }

    /**
     * 检查是否有有效租约
     */
    public boolean hasValidLease(String shardId) {
        return leaseRegistry.hasValidLease(shardId);
    }

    /**
     * 获取当前 primary 节点
     */
    public Optional<String> getCurrentPrimary(String shardId) {
        return leaseRegistry.getCurrentPrimary(shardId);
    }

    /**
     * 获取分片路由表版本
     */
    public long getShardRoutingVersion(String shardId) {
        return leaseRegistry.getShardRoutingVersion(shardId);
    }

    // ========== 租约事件 ==========

    public void setLeaseEventListener(java.util.function.Consumer<LeaseEvent> listener) {
        leaseRegistry.setLeaseEventListener(listener);
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

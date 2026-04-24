package com.loomq.cluster;

import com.loomq.replication.ReplicationManager;
import com.loomq.replication.WalCatchUpManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Failover 控制器。
 *
 * 负责接收故障通知、执行 promotion/demotion、协调租约生命周期、
 * 更新路由版本并切换复制管理器角色。
 *
 * @author loomq
 */
public class FailoverController implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(FailoverController.class);

    // 节点信息
    private final String nodeId;
    private final String shardId;

    // 分片状态机
    private final ShardStateMachine stateMachine;

    // 复制管理器（用于角色切换）
    private final AtomicReference<ReplicationManager> replicationManager;

    // 追赶管理器（用于 replica 状态同步）
    private final WalCatchUpManager catchUpManager;

    // Primary 的当前 offset（用于追赶）
    private final AtomicLong primaryCurrentOffset;

    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);

    // 当前正在执行的 failover 操作
    private final AtomicReference<CompletableFuture<?>> activeFailover;

    // 租约失效回调
    private volatile Runnable leaseExpiredCallback;

    // 复制端点配置
    private final ReplicationEndpoints replicationEndpoints;

    // 租约生命周期管理
    private final LeaseLifecycleManager leaseLifecycleManager;

    /**
     * 创建 Failover 控制器
     *
     * @param nodeId 节点 ID
     * @param shardId 分片 ID
     * @param initialRole 初始角色
     * @param initialRoutingVersion 初始路由表版本
     */
    public FailoverController(String nodeId, String shardId,
                              ReplicaRole initialRole, long initialRoutingVersion,
                              long leaseDurationMs, double renewalWindowRatio,
                              ReplicationEndpoints replicationEndpoints) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId cannot be null");
        this.shardId = Objects.requireNonNull(shardId, "shardId cannot be null");
        this.stateMachine = new ShardStateMachine(shardId, nodeId, initialRole);
        this.replicationManager = new AtomicReference<>(null);
        this.catchUpManager = new WalCatchUpManager(nodeId, shardId, stateMachine);
        this.primaryCurrentOffset = new AtomicLong(0);
        this.activeFailover = new AtomicReference<>(null);
        this.replicationEndpoints = Objects.requireNonNull(replicationEndpoints, "replicationEndpoints cannot be null");
        AtomicLong routingVersionGenerator = new AtomicLong(initialRoutingVersion);
        this.leaseLifecycleManager = new LeaseLifecycleManager(
            shardId,
            leaseDurationMs,
            renewalWindowRatio,
            routingVersionGenerator,
            stateMachine::getCurrentLease,
            stateMachine::setCurrentLease,
            this::handleLeaseExpired
        );

        // 设置追赶管理器的回调
        setupCatchUpCallbacks();

        logger.info("FailoverController created: node={}, shard={}, role={}, routingVer={}",
            nodeId, shardId, initialRole, initialRoutingVersion);
    }

    /**
     * 设置追赶管理器的回调
     */
    private void setupCatchUpCallbacks() {
        // 追赶完成回调：转换为 SYNCED 状态
        catchUpManager.setOnCatchUpComplete(() -> {
            logger.info("Catch up completed for shard {}, transitioning to SYNCED", shardId);
            // 状态机已经在 WalCatchUpManager 中更新为 SYNCED
            // 这里可以触发额外的逻辑
        });

        // 追赶失败回调
        catchUpManager.setOnCatchUpError((error, cause) -> {
            logger.error("Catch up failed for shard {}: {}", shardId, error, cause);
            // 如果追赶失败，保持在 INIT 状态等待重试
            // 实际实现中可能需要更复杂的重试策略
        });
    }

    // ==================== 生命周期 ====================

    /**
     * 启动控制器
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        logger.info("Starting FailoverController for shard {}...", shardId);

        // 启动追赶管理器
        catchUpManager.start();

        // 根据初始角色初始化状态
        if (stateMachine.getCurrentRole() == ReplicaRole.LEADER) {
            // 如果初始是 primary，需要先完成租约获取、续约和复制角色激活
            bootstrapPrimaryRole();
        } else {
            // 如果初始是 replica，启动追赶流程
            // 从 REPLICA_INIT -> REPLICA_CATCHING_UP -> REPLICA_SYNCED
            startCatchUpIfNeeded();
        }

        logger.info("FailoverController started, state={}", stateMachine.getCurrentState());
    }

    /**
     * 启动时激活 primary 角色。
     */
    private void bootstrapPrimaryRole() {
        leaseLifecycleManager.acquireLeaseAsync().whenComplete((lease, error) -> {
            if (error != null || lease == null) {
                logger.error("Failed to acquire lease during startup", error);
                demoteToReplica();
                return;
            }

            if (activatePrimaryAfterLease(lease)) {
                logger.info("Startup lease acquired: {} for shard {}", lease.getLeaseId(), shardId);
                return;
            }

            logger.error("Failed to activate primary during startup for shard {}", shardId);
            demoteToReplica();
        });
    }

    /**
     * 如果需要，启动追赶流程
     */
    private void startCatchUpIfNeeded() {
        // 获取本地最后应用的 offset
        long localOffset = stateMachine.getLastAppliedOffset();
        // 获取 primary 当前 offset（从复制管理器或心跳中获取）
        long targetOffset = primaryCurrentOffset.get();

        if (targetOffset <= 0 || localOffset >= targetOffset) {
            // 没有需要追赶的数据，直接切换到 SYNCED
            logger.info("No catch up needed for shard {}, localOffset={}, targetOffset={}",
                shardId, localOffset, targetOffset);
            stateMachine.toSynced();
            return;
        }

        // 启动追赶
        logger.info("Starting catch up for shard {}: {} -> {}",
            shardId, localOffset, targetOffset);
        catchUpManager.startCatchUp(localOffset, targetOffset)
            .whenComplete((success, error) -> {
                if (error != null) {
                    logger.error("Catch up failed for shard {}", shardId, error);
                } else if (success) {
                    logger.info("Catch up completed for shard {}", shardId);
                } else {
                    logger.warn("Catch up did not complete successfully for shard {}", shardId);
                }
            });
    }

    /**
     * 停止控制器
     */
    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        logger.info("Stopping FailoverController...");

        // 取消租约续约任务
        leaseLifecycleManager.close();

        // 停止追赶管理器
        catchUpManager.close();

        // 如果有正在进行的 failover，等待完成
        CompletableFuture<?> active = activeFailover.get();
        if (active != null && !active.isDone()) {
            try {
                active.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                // 关闭中的等待：超时/中断/异常均非关键，仅记录日志
                logger.warn("Active failover did not complete in time", e);
            }
        }

        // 降级为 replica（如果当前是 primary）
        if (stateMachine.isPrimary()) {
            demoteToReplica();
        }

        logger.info("FailoverController stopped");
    }

    // ==================== Failover 核心逻辑 ====================

    /**
     * 处理节点故障事件（由 HeartbeatManager 调用）
     *
     * @param event 故障事件
     * @return 是否触发 failover
     */
    public CompletableFuture<Boolean> handleNodeFailure(HeartbeatManager.NodeFailureEvent event) {
        if (!running.get()) {
            return CompletableFuture.completedFuture(false);
        }

        logger.warn("Handling node failure: node={}, shard={}, type={}",
            event.nodeId(), event.shardId(), event.failureType());

        // 检查是否是本 shard 的 primary 故障
        if (!event.shardId().equals(shardId)) {
            logger.debug("Ignoring failure from different shard: {} vs {}",
                event.shardId(), shardId);
            return CompletableFuture.completedFuture(false);
        }

        // 只有当自己是 replica 且 synced 时才尝试 promotion
        if (!stateMachine.isSynced()) {
            logger.info("Not in SYNCED state, cannot promote. Current state: {}",
                stateMachine.getCurrentState());
            return CompletableFuture.completedFuture(false);
        }

        // 检查是否已经有正在进行的 failover
        if (activeFailover.get() != null && !activeFailover.get().isDone()) {
            logger.info("Failover already in progress");
            return activeFailover.get().thenApply(v -> true);
        }

        // 执行 promotion
        return executePromotion(event);
    }

    /**
     * 执行 promotion 流程
     */
    private CompletableFuture<Boolean> executePromotion(HeartbeatManager.NodeFailureEvent triggerEvent) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        activeFailover.set(future);

        logger.info("Starting promotion sequence for shard {}...", shardId);

        // Step 1: 转换到 PROMOTING 状态
        if (!stateMachine.toPromoting().join()) {
            logger.error("Failed to enter PROMOTING state");
            future.complete(false);
            activeFailover.set(null);
            return future;
        }

        // Step 2: 向 Coordinator 申请租约
        leaseLifecycleManager.acquireLeaseAsync().whenComplete((lease, error) -> {
            if (error != null || lease == null) {
                logger.error("Failed to acquire lease for promotion", error);
                stateMachine.toSynced();
                future.complete(false);
                activeFailover.set(null);
                return;
            }

            logger.info("Lease acquired: {} for shard {}", lease.getLeaseId(), shardId);

            if (activatePrimaryAfterLease(lease)) {
                logger.info("Promotion completed: node {} is now PRIMARY for shard {}",
                    nodeId, shardId);
                future.complete(true);
            } else {
                logger.error("Failed to activate PRIMARY after acquiring lease");
                stateMachine.toSynced();
                future.complete(false);
            }

            activeFailover.set(null);
        });

        return future;
    }

    /**
     * 降级为 Replica（收到新 primary 心跳时调用）
     *
     * @return 是否成功降级
     */
    public boolean demoteToReplica() {
        if (!running.get()) {
            return false;
        }

        logger.info("Starting demotion to replica for shard {}...", shardId);

        // Step 1: 进入 DEMOTING 状态
        if (!stateMachine.toDemoting()) {
            logger.error("Failed to enter DEMOTING state");
            return false;
        }

        // Step 2: 停止租约续约
        leaseLifecycleManager.stopLeaseRenewal();

        // Step 3: 转换到 REPLICA_INIT
        if (stateMachine.toReplicaInit()) {
            // Step 4: 更新复制管理器角色
            updateReplicationManagerRole();

            // Step 5: 启动追赶流程（从 REPLICA_INIT -> CATCHING_UP -> SYNCED）
            startCatchUpIfNeeded();

            logger.info("Demotion completed: node {} is now REPLICA for shard {}",
                nodeId, shardId);
            return true;
        }

        logger.error("Failed to complete demotion");
        return false;
    }

    /**
     * 手动触发 failover（运维接口）
     *
     * @param force 是否强制切换
     * @return 切换结果
     */
    public CompletableFuture<FailoverResult> triggerManualFailover(boolean force) {
        if (!running.get()) {
            return CompletableFuture.completedFuture(
                new FailoverResult(false, "Controller not running", null));
        }

        if (!force && stateMachine.isPrimary()) {
            return CompletableFuture.completedFuture(
                new FailoverResult(false, "Already primary, use force=true to override", null));
        }

        logger.info("Manual failover triggered for shard {} (force={})", shardId, force);

        // 如果当前是 primary，先降级
        if (stateMachine.isPrimary() && force) {
            demoteToReplica();
        }

        // 构造手动触发的故障事件并走同一条失败处理路径
        HeartbeatManager.NodeFailureEvent failureEvent = new HeartbeatManager.NodeFailureEvent(
            "manual-trigger", shardId, ReplicaRole.LEADER,
            HeartbeatManager.NodeFailureEvent.FailureType.MANUAL_SHUTDOWN,
            "Manual failover triggered"
        );

        return handleNodeFailure(failureEvent).thenApply(success -> {
            if (success) {
                CoordinatorLease lease = stateMachine.getCurrentLease();
                return new FailoverResult(true, "Failover successful", lease);
            } else {
                return new FailoverResult(false, "Failover failed", null);
            }
        });
    }

    // ==================== 租约管理 ====================

    /**
     * 在获得租约后激活 primary 角色。
     */
    private boolean activatePrimaryAfterLease(CoordinatorLease lease) {
        boolean activated;
        if (stateMachine.isPrimaryActive()) {
            stateMachine.setCurrentLease(lease);
            activated = true;
        } else {
            activated = stateMachine.toPrimaryActive(lease);
        }

        if (!activated) {
            return false;
        }

        leaseLifecycleManager.startLeaseRenewal(lease);
        updateReplicationManagerRole();
        return true;
    }

    /**
     * 处理租约过期
     */
    private void handleLeaseExpired() {
        logger.error("Lease expired! This node is no longer the primary.");

        // 转换到降级状态
        stateMachine.toPrimaryDegraded("Lease expired");

        // 通知回调
        if (leaseExpiredCallback != null) {
            leaseExpiredCallback.run();
        }

        // 降级为 replica
        demoteToReplica();
    }

    // ==================== 复制管理器协调 ====================

    /**
     * 设置复制管理器
     */
    public void setReplicationManager(ReplicationManager manager) {
        this.replicationManager.set(manager);
    }

    /**
     * 更新复制管理器角色
     */
    private void updateReplicationManagerRole() {
        ReplicationManager manager = replicationManager.get();
        if (manager == null) {
            return;
        }

        if (stateMachine.isPrimary()) {
            manager.promoteToPrimary(replicationEndpoints.primaryReplicaHost(), replicationEndpoints.primaryReplicaPort())
                .whenComplete((v, e) -> {
                    if (e != null) {
                        logger.error("Failed to promote replication manager", e);
                    } else {
                        logger.info("Replication manager promoted to PRIMARY");
                    }
                });
        } else {
            manager.demoteToReplica(replicationEndpoints.replicaBindHost(), replicationEndpoints.replicaBindPort())
                .whenComplete((v, e) -> {
                    if (e != null) {
                        logger.error("Failed to demote replication manager", e);
                    } else {
                        logger.info("Replication manager demoted to REPLICA");
                    }
                });
        }
    }

    // ==================== 回调设置 ====================

    /**
     * 设置租约提供者
     */
    public void setLeaseProvider(Function<String, CompletableFuture<CoordinatorLease>> provider) {
        leaseLifecycleManager.setLeaseProvider(provider);
    }

    /**
     * 设置租约续约函数
     */
    public void setLeaseRenewer(BiFunction<String, String, CompletableFuture<CoordinatorLease>> renewer) {
        leaseLifecycleManager.setLeaseRenewer(renewer);
    }

    /**
     * 设置租约过期回调
     */
    public void setLeaseExpiredCallback(Runnable callback) {
        this.leaseExpiredCallback = callback;
    }

    // ==================== 查询方法 ====================

    public ShardStateMachine getStateMachine() {
        return stateMachine;
    }

    public ShardStateMachine.ShardState getCurrentState() {
        return stateMachine.getCurrentState();
    }

    public boolean isPrimary() {
        return stateMachine.isPrimary();
    }

    public CoordinatorLease getCurrentLease() {
        return stateMachine.getCurrentLease();
    }

    public long getRoutingVersion() {
        return leaseLifecycleManager.getRoutingVersion();
    }

    /**
     * 获取追赶管理器（用于配置和监控）
     */
    public WalCatchUpManager getCatchUpManager() {
        return catchUpManager;
    }

    /**
     * 是否正在追赶中
     */
    public boolean isCatchingUp() {
        return catchUpManager.isCatchingUp();
    }

    /**
     * 获取追赶进度（0.0 - 1.0）
     */
    public double getCatchUpProgress() {
        return catchUpManager.getProgress();
    }

    /**
     * 设置 Primary 当前 offset（用于追赶计算）
     */
    public void setPrimaryCurrentOffset(long offset) {
        this.primaryCurrentOffset.set(offset);
    }

    // ==================== 内部类 ====================

    /**
     * Failover 结果
     */
    public record FailoverResult(
        boolean success,
        String message,
        CoordinatorLease newLease
    ) {
        @Override
        public String toString() {
            return String.format("FailoverResult{success=%s, msg='%s', lease=%s}",
                success, message,
                newLease != null ? newLease.getLeaseId().substring(0, 8) : "none");
        }
    }

    @Override
    public String toString() {
        return String.format("FailoverController{node=%s, shard=%s, state=%s, role=%s}",
            nodeId, shardId, stateMachine.getCurrentState(), stateMachine.getCurrentRole());
    }
}

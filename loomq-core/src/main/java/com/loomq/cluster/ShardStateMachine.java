package com.loomq.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * 分片状态机
 *
 * 硬约束 #1 续：管理分片在 failover 过程中的状态转换
 *
 * 状态转换图：
 * ```
 * REPLICA_INIT
 *      │
 *      ▼ (启动，加载本地 WAL)
 * REPLICA_CATCHING_UP ◄──────┐
 *      │                     │
 *      ▼ (追平 offset)       │ (primary 有新数据)
 * REPLICA_SYNCED ────────────┤
 *      │                     │
 *      ▼ (primary 故障，promotion)
 * PROMOTING
 *      │
 *      ▼ (路由表更新完成)
 * PRIMARY_ACTIVE
 *      │
 *      ▼ (replica 失联)
 * PRIMARY_DEGRADED
 *      │
 *      ▼ (收到新 primary 心跳)
 * DEGRADED (转为新 replica)
 * ```
 *
 * @author loomq
 * @since v0.4.8
 */
public class ShardStateMachine {

    private static final Logger logger = LoggerFactory.getLogger(ShardStateMachine.class);

    // 分片 ID
    private final String shardId;

    // 节点 ID
    private final String nodeId;

    // 当前状态
    private final AtomicReference<ShardState> currentState;

    // 当前角色
    private final AtomicReference<ReplicaRole> currentRole;

    // 最后状态变更时间
    private final AtomicReference<Instant> lastStateChangeTime;

    // 当前租约
    private final AtomicReference<CoordinatorLease> currentLease;

    // 最后应用 offset
    private final AtomicLong lastAppliedOffset;

    // 目标 offset（追赶时使用）
    private final AtomicLong targetOffset;

    // 状态变更监听器
    private volatile BiConsumer<ShardState, ShardState> stateChangeListener;

    // 错误信息
    private final AtomicReference<String> errorMessage;

    /**
     * 分片状态枚举
     */
    public enum ShardState {
        // ========== Replica 侧状态 ==========
        REPLICA_INIT("初始化中"),
        REPLICA_CATCHING_UP("追赶中"),
        REPLICA_SYNCED("已同步，待命"),

        // ========== Primary 侧状态 ==========
        PRIMARY_ACTIVE("主节点运行中"),
        PRIMARY_DEGRADED("主节点降级（replica 失联）"),

        // ========== 切换中状态 ==========
        PROMOTING("正在提升为主节点"),
        DEMOTING("正在降级为副本"),

        // ========== 异常状态 ==========
        OFFLINE("离线"),
        ERROR("错误状态");

        private final String description;

        ShardState(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }

        /**
         * 是否可以接受写入
         */
        public boolean canAcceptWrite() {
            return this == PRIMARY_ACTIVE || this == PRIMARY_DEGRADED;
        }

        /**
         * 是否可以接受读取
         */
        public boolean canAcceptRead() {
            return this == PRIMARY_ACTIVE
                || this == PRIMARY_DEGRADED
                || this == REPLICA_SYNCED;
        }

        /**
         * 是否是 Replica 状态
         */
        public boolean isReplica() {
            return this == REPLICA_INIT
                || this == REPLICA_CATCHING_UP
                || this == REPLICA_SYNCED;
        }

        /**
         * 是否是 Primary 状态
         */
        public boolean isPrimary() {
            return this == PRIMARY_ACTIVE || this == PRIMARY_DEGRADED;
        }
    }

    /**
     * 创建分片状态机
     *
     * @param shardId 分片 ID
     * @param nodeId 节点 ID
     * @param initialRole 初始角色
     */
    public ShardStateMachine(String shardId, String nodeId, ReplicaRole initialRole) {
        this.shardId = Objects.requireNonNull(shardId, "shardId cannot be null");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId cannot be null");
        this.currentRole = new AtomicReference<>(initialRole);
        this.currentState = new AtomicReference<>(
            initialRole == ReplicaRole.LEADER ? ShardState.PRIMARY_ACTIVE : ShardState.REPLICA_INIT);
        this.lastStateChangeTime = new AtomicReference<>(Instant.now());
        this.currentLease = new AtomicReference<>(null);
        this.lastAppliedOffset = new AtomicLong(-1);
        this.targetOffset = new AtomicLong(-1);
        this.errorMessage = new AtomicReference<>(null);

        logger.info("ShardStateMachine created: shard={}, node={}, initialState={}",
            shardId, nodeId, currentState.get());
    }

    // ==================== 状态转换 ====================

    /**
     * 转换到 REPLICA_CATCHING_UP 状态
     */
    public boolean toCatchingUp(long targetOffset) {
        this.targetOffset.set(targetOffset);
        return transitionTo(ShardState.REPLICA_CATCHING_UP, from ->
            from == ShardState.REPLICA_INIT || from.isReplica());
    }

    /**
     * 转换到 REPLICA_SYNCED 状态
     */
    public boolean toSynced() {
        return transitionTo(ShardState.REPLICA_SYNCED, from ->
            from == ShardState.REPLICA_CATCHING_UP || from == ShardState.REPLICA_INIT);
    }

    /**
     * 转换到 PROMOTING 状态
     */
    public CompletableFuture<Boolean> toPromoting() {
        if (transitionTo(ShardState.PROMOTING, ShardState::isReplica)) {
            return CompletableFuture.completedFuture(true);
        }
        return CompletableFuture.completedFuture(false);
    }

    /**
     * 转换到 PRIMARY_ACTIVE 状态
     */
    public boolean toPrimaryActive(CoordinatorLease lease) {
        if (lease != null) {
            currentLease.set(lease);
        }
        boolean success = transitionTo(ShardState.PRIMARY_ACTIVE, from ->
            from == ShardState.PROMOTING || from == ShardState.PRIMARY_DEGRADED);
        if (success) {
            currentRole.set(ReplicaRole.LEADER);
        }
        return success;
    }

    /**
     * 转换到 PRIMARY_DEGRADED 状态
     */
    public boolean toPrimaryDegraded(String reason) {
        logger.warn("Shard {} entering DEGRADED mode: {}", shardId, reason);
        return transitionTo(ShardState.PRIMARY_DEGRADED, from ->
            from == ShardState.PRIMARY_ACTIVE);
    }

    /**
     * 转换到 DEMOTING 状态
     */
    public boolean toDemoting() {
        return transitionTo(ShardState.DEMOTING, ShardState::isPrimary);
    }

    /**
     * 转换到 REPLICA_INIT 状态（降级后）
     */
    public boolean toReplicaInit() {
        if (transitionTo(ShardState.REPLICA_INIT, from ->
            from == ShardState.DEMOTING || from == ShardState.PRIMARY_DEGRADED)) {
            currentRole.set(ReplicaRole.FOLLOWER);
            currentLease.set(null);
            return true;
        }
        return false;
    }

    /**
     * 转换到 ERROR 状态
     */
    public boolean toError(String error) {
        errorMessage.set(error);
        return transitionTo(ShardState.ERROR, from -> true);
    }

    /**
     * 通用状态转换方法
     */
    private boolean transitionTo(ShardState newState, java.util.function.Predicate<ShardState> guard) {
        ShardState oldState = currentState.get();

        if (!guard.test(oldState)) {
            logger.warn("Invalid state transition: {} -> {}, guard rejected",
                oldState, newState);
            return false;
        }

        if (currentState.compareAndSet(oldState, newState)) {
            lastStateChangeTime.set(Instant.now());
            // 只有在非 ERROR 状态转换时才清除错误信息
            if (newState != ShardState.ERROR) {
                errorMessage.set(null);
            }
            logger.info("Shard {} state transitioned: {} -> {}", shardId, oldState, newState);

            // 通知监听器
            if (stateChangeListener != null) {
                try {
                    stateChangeListener.accept(oldState, newState);
                } catch (Exception e) {
                    logger.error("State change listener error", e);
                }
            }
            return true;
        }
        return false;
    }

    // ==================== 状态检查 ====================

    /**
     * 检查当前状态
     */
    public boolean isInState(ShardState state) {
        return currentState.get() == state;
    }

    /**
     * 是否是 Primary（活跃或降级）
     */
    public boolean isPrimary() {
        return currentState.get().isPrimary();
    }

    /**
     * 是否是活跃 Primary
     */
    public boolean isPrimaryActive() {
        return currentState.get() == ShardState.PRIMARY_ACTIVE;
    }

    /**
     * 是否是 Replica
     */
    public boolean isReplica() {
        ShardState state = currentState.get();
        if (state == null) {
            logger.error("Current state is null for shard {}!", shardId);
            return false;
        }
        boolean result = state.isReplica();
        logger.debug("isReplica() called on shard {}: state={}, isReplica={}, hashCode={}",
            shardId, state, result, System.identityHashCode(state));
        return result;
    }

    /**
     * 是否可以接受写入
     */
    public boolean canAcceptWrite() {
        return currentState.get().canAcceptWrite();
    }

    /**
     * 是否可以接受读取
     */
    public boolean canAcceptRead() {
        return currentState.get().canAcceptRead();
    }

    /**
     * 是否处于追赶中
     */
    public boolean isCatchingUp() {
        return currentState.get() == ShardState.REPLICA_CATCHING_UP;
    }

    /**
     * 是否已经同步
     */
    public boolean isSynced() {
        return currentState.get() == ShardState.REPLICA_SYNCED;
    }

    // ==================== Getter/Setter ====================

    public ShardState getCurrentState() {
        return currentState.get();
    }

    public ReplicaRole getCurrentRole() {
        return currentRole.get();
    }

    public Instant getLastStateChangeTime() {
        return lastStateChangeTime.get();
    }

    public CoordinatorLease getCurrentLease() {
        return currentLease.get();
    }

    public void setCurrentLease(CoordinatorLease lease) {
        this.currentLease.set(lease);
    }

    public long getLastAppliedOffset() {
        return lastAppliedOffset.get();
    }

    public void setLastAppliedOffset(long offset) {
        this.lastAppliedOffset.set(offset);
    }

    public long getTargetOffset() {
        return targetOffset.get();
    }

    public void setTargetOffset(long offset) {
        this.targetOffset.set(offset);
    }

    public String getShardId() {
        return shardId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getErrorMessage() {
        return errorMessage.get();
    }

    public void setStateChangeListener(BiConsumer<ShardState, ShardState> listener) {
        this.stateChangeListener = listener;
    }

    // ==================== 状态报告 ====================

    /**
     * 获取状态报告
     */
    public StateReport getReport() {
        return new StateReport(
            shardId,
            nodeId,
            currentState.get(),
            currentRole.get(),
            lastStateChangeTime.get(),
            lastAppliedOffset.get(),
            targetOffset.get(),
            currentLease.get(),
            errorMessage.get()
        );
    }

    /**
     * 状态报告记录
     */
    public record StateReport(
        String shardId,
        String nodeId,
        ShardState state,
        ReplicaRole role,
        Instant lastStateChange,
        long lastAppliedOffset,
        long targetOffset,
        CoordinatorLease lease,
        String errorMessage
    ) {
        /**
         * 计算追赶进度（0.0 - 1.0）
         */
        public double getCatchUpProgress() {
            if (targetOffset <= 0 || lastAppliedOffset < 0) {
                return 0.0;
            }
            if (lastAppliedOffset >= targetOffset) {
                return 1.0;
            }
            return (double) lastAppliedOffset / targetOffset;
        }

        /**
         * 是否在追赶中
         */
        public boolean isCatchingUp() {
            return state == ShardState.REPLICA_CATCHING_UP;
        }

        @Override
        public String toString() {
            return String.format(
                "StateReport{shard=%s, node=%s, state=%s, role=%s, offset=%d/%d, progress=%.1f%%}",
                shardId, nodeId, state, role, lastAppliedOffset, targetOffset,
                getCatchUpProgress() * 100);
        }
    }

    @Override
    public String toString() {
        return String.format("ShardStateMachine{shard=%s, node=%s, state=%s, role=%s, offset=%d}",
            shardId, nodeId, currentState.get(), currentRole.get(), lastAppliedOffset.get());
    }
}

package com.loomq.cluster;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coordinator 租约 (v0.5)
 *
 * 硬约束 #1: Coordinator 必须是"租约 + 版本号"仲裁，不是简单投票
 *
 * 租约设计原则：
 * 1. 租约 ID：唯一标识本次 primary 任期
 * 2. 持有者：当前 primary node ID
 * 3. Epoch：单调递增的世代号，用于 fencing
 * 4. 时间边界：acquiredAt（获取时间）+ expiresAt（过期时间）
 * 5. Fencing Token：单调递增序列号，用于写保护
 * 6. 续约机制：primary 必须定期续约，否则租约过期
 *
 * @author loomq
 * @since v0.5.0
 */
public final class CoordinatorLease {

    // 租约 ID：唯一标识本次 primary 任期
    private final String leaseId;

    // 分片 ID
    private final String shardId;

    // 持有者：当前 primary node ID
    private final String nodeId;

    // Epoch：单调递增的世代号
    private final long epoch;

    // 时间边界
    private final Instant acquiredAt;   // 获取时间
    private final Instant expiresAt;    // 过期时间
    private final long ttlMs;           // 租约时长

    // Fencing Token 序列号生成器
    private final AtomicLong fencingToken;

    // 版本信息
    private final long routingVersion;

    // 有效性标志
    private volatile boolean valid;

    /**
     * 创建新租约
     *
     * @param shardId         分片 ID
     * @param nodeId          持有者节点 ID
     * @param epoch           世代号
     * @param ttlMs           租约有效期（毫秒）
     * @param routingVersion  路由表版本
     */
    public CoordinatorLease(String shardId, String nodeId, long epoch,
                            long ttlMs, long routingVersion) {
        this.leaseId = UUID.randomUUID().toString();
        this.shardId = Objects.requireNonNull(shardId, "shardId cannot be null");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId cannot be null");
        this.epoch = epoch;
        this.acquiredAt = Instant.now();
        this.ttlMs = ttlMs;
        this.expiresAt = this.acquiredAt.plusMillis(ttlMs);
        this.routingVersion = routingVersion;
        this.fencingToken = new AtomicLong(0);
        this.valid = true;
    }

    /**
     * 完整构造函数（用于反序列化）
     */
    public CoordinatorLease(String leaseId, String shardId, String nodeId, long epoch,
                            Instant acquiredAt, long ttlMs, long routingVersion, long initialFencingToken) {
        this.leaseId = Objects.requireNonNull(leaseId, "leaseId cannot be null");
        this.shardId = Objects.requireNonNull(shardId, "shardId cannot be null");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId cannot be null");
        this.epoch = epoch;
        this.acquiredAt = Objects.requireNonNull(acquiredAt, "acquiredAt cannot be null");
        this.ttlMs = ttlMs;
        this.expiresAt = acquiredAt.plusMillis(ttlMs);
        this.routingVersion = routingVersion;
        this.fencingToken = new AtomicLong(initialFencingToken);
        this.valid = true;
    }

    // ==================== Fencing Token 管理 ====================

    /**
     * 获取下一个 fencing token
     *
     * @return 递增的 fencing token
     * @throws IllegalStateException 如果租约已失效
     */
    public long nextFencingToken() {
        if (!valid) {
            throw new IllegalStateException("Lease is no longer valid");
        }
        return fencingToken.incrementAndGet();
    }

    /**
     * 获取当前 fencing token（不递增）
     *
     * @return 当前 fencing token
     */
    public long currentFencingToken() {
        return fencingToken.get();
    }

    /**
     * 创建包含当前 fencing token 的 FencingToken 对象
     */
    public com.loomq.cluster.v5.FencingToken toFencingToken() {
        return new com.loomq.cluster.v5.FencingToken(epoch, fencingToken.get());
    }

    // ==================== 租约操作 ====================

    /**
     * 续期租约
     *
     * @param newTtlMs 新的租约时长
     * @return 新租约实例
     * @throws IllegalStateException 如果租约已失效
     */
    public CoordinatorLease renew(long newTtlMs) {
        if (!valid) {
            throw new IllegalStateException("Cannot renew invalid lease");
        }
        CoordinatorLease renewed = new CoordinatorLease(
            leaseId, shardId, nodeId, epoch,
            Instant.now(), newTtlMs, routingVersion, fencingToken.get()
        );
        this.valid = false; // 旧租约失效
        return renewed;
    }

    /**
     * 使租约失效
     */
    public void invalidate() {
        this.valid = false;
    }

    // ==================== 状态检查 ====================

    /**
     * 检查租约是否有效（未过期且未被标记为失效）
     */
    public boolean isValid() {
        return valid && Instant.now().isBefore(expiresAt);
    }

    /**
     * 检查租约是否已过期
     */
    public boolean isExpired() {
        return Instant.now().isAfter(expiresAt);
    }

    /**
     * 获取剩余有效时间（毫秒）
     *
     * @return 剩余毫秒数，如果已过期返回负数
     */
    public long getRemainingMs() {
        return expiresAt.toEpochMilli() - Instant.now().toEpochMilli();
    }

    /**
     * 检查是否应该在过期前续期
     *
     * @param thresholdMs 续期阈值（提前多少毫秒续期）
     * @return true if should renew
     */
    public boolean shouldRenew(long thresholdMs) {
        return getRemainingMs() < thresholdMs;
    }

    /**
     * 检查指定节点是否是持有者
     */
    public boolean isHeldBy(String nodeId) {
        return this.nodeId.equals(nodeId);
    }

    /**
     * 检查是否可以续约（在过期前指定时间窗口内）
     *
     * @param renewalWindowRatio 续约窗口比例（0.0 - 1.0）
     */
    public boolean canRenew(double renewalWindowRatio) {
        long remaining = getRemainingMs();
        return remaining < ttlMs * renewalWindowRatio && remaining > 0;
    }

    // ==================== Getter 方法 ====================

    public String getLeaseId() {
        return leaseId;
    }

    public String getShardId() {
        return shardId;
    }

    public String getNodeId() {
        return nodeId;
    }

    /** @deprecated 使用 getNodeId() */
    public String getHolderNodeId() {
        return nodeId;
    }

    /** @deprecated 使用 currentFencingToken() */
    public long getFencingSequence() {
        return fencingToken.get();
    }

    public long getEpoch() {
        return epoch;
    }

    public Instant getAcquiredAt() {
        return acquiredAt;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    public long getTtlMs() {
        return ttlMs;
    }

    public long getRoutingVersion() {
        return routingVersion;
    }

    // ==================== 对象方法 ====================

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CoordinatorLease that = (CoordinatorLease) o;
        return Objects.equals(leaseId, that.leaseId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leaseId);
    }

    @Override
    public String toString() {
        return String.format("CoordinatorLease{lease=%s, shard=%s, node=%s, epoch=%d, fencing=%d, valid=%s, remaining=%dms}",
            leaseId.substring(0, 8), shardId, nodeId, epoch, fencingToken.get(), isValid(), getRemainingMs());
    }
}

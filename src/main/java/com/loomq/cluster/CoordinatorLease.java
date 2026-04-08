package com.loomq.cluster;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Coordinator 租约
 *
 * 硬约束 #1: Coordinator 必须是"租约 + 版本号"仲裁，不是简单投票
 *
 * 租约设计原则：
 * 1. 租约 ID：唯一标识本次 primary 任期
 * 2. 持有者：当前 primary node ID
 * 3. 时间边界：issuedAt（发放时间）+ expiresAt（过期时间）
 * 4. 版本信息：关联的路由表版本号
 * 5. 续约机制：primary 必须定期续约，否则租约过期
 *
 * @author loomq
 * @since v0.4.8
 */
public class CoordinatorLease {

    // 租约 ID：唯一标识本次 primary 任期
    private final String leaseId;

    // 持有者：当前 primary node ID
    private final String holderNodeId;

    // 分片 ID
    private final String shardId;

    // 时间边界
    private final Instant issuedAt;     // 发放时间
    private final Instant expiresAt;    // 过期时间（issuedAt + leaseDuration）

    // 版本信息
    private final long routingVersion;  // 关联的路由表版本

    // Fencing Token 序列号
    private final long fencingSequence;

    /**
     * 创建新租约
     *
     * @param holderNodeId 持有者节点 ID
     * @param shardId 分片 ID
     * @param leaseDurationMs 租约有效期（毫秒）
     * @param routingVersion 路由表版本
     * @param fencingSequence Fencing Token 序列号
     */
    public CoordinatorLease(String holderNodeId, String shardId,
                            long leaseDurationMs, long routingVersion, long fencingSequence) {
        this.leaseId = UUID.randomUUID().toString();
        this.holderNodeId = Objects.requireNonNull(holderNodeId, "holderNodeId cannot be null");
        this.shardId = Objects.requireNonNull(shardId, "shardId cannot be null");
        this.issuedAt = Instant.now();
        this.expiresAt = this.issuedAt.plusMillis(leaseDurationMs);
        this.routingVersion = routingVersion;
        this.fencingSequence = fencingSequence;
    }

    /**
     * 完整构造函数（用于反序列化）
     */
    public CoordinatorLease(String leaseId, String holderNodeId, String shardId,
                            Instant issuedAt, Instant expiresAt,
                            long routingVersion, long fencingSequence) {
        this.leaseId = Objects.requireNonNull(leaseId, "leaseId cannot be null");
        this.holderNodeId = Objects.requireNonNull(holderNodeId, "holderNodeId cannot be null");
        this.shardId = Objects.requireNonNull(shardId, "shardId cannot be null");
        this.issuedAt = Objects.requireNonNull(issuedAt, "issuedAt cannot be null");
        this.expiresAt = Objects.requireNonNull(expiresAt, "expiresAt cannot be null");
        this.routingVersion = routingVersion;
        this.fencingSequence = fencingSequence;
    }

    // ==================== 状态检查 ====================

    /**
     * 检查租约是否有效（未过期）
     */
    public boolean isValid() {
        return Instant.now().isBefore(expiresAt);
    }

    /**
     * 检查租约是否已过期
     */
    public boolean isExpired() {
        return !isValid();
    }

    /**
     * 检查是否可以续约（在过期前指定时间窗口内）
     *
     * @param renewalWindowRatio 续约窗口比例（0.0 - 1.0）
     */
    public boolean canRenew(double renewalWindowRatio) {
        long totalDuration = expiresAt.toEpochMilli() - issuedAt.toEpochMilli();
        long remaining = expiresAt.toEpochMilli() - Instant.now().toEpochMilli();
        return remaining < totalDuration * renewalWindowRatio;
    }

    /**
     * 检查指定节点是否是持有者
     */
    public boolean isHeldBy(String nodeId) {
        return holderNodeId.equals(nodeId);
    }

    /**
     * 获取剩余有效时间（毫秒）
     */
    public long getRemainingTimeMs() {
        long remaining = expiresAt.toEpochMilli() - Instant.now().toEpochMilli();
        return Math.max(0, remaining);
    }

    // ==================== Getter 方法 ====================

    public String getLeaseId() {
        return leaseId;
    }

    public String getHolderNodeId() {
        return holderNodeId;
    }

    public String getShardId() {
        return shardId;
    }

    public Instant getIssuedAt() {
        return issuedAt;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    public long getRoutingVersion() {
        return routingVersion;
    }

    public long getFencingSequence() {
        return fencingSequence;
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
        return String.format("CoordinatorLease{leaseId=%s, holder=%s, shard=%s, valid=%s, routingVer=%d, fencingSeq=%d}",
            leaseId.substring(0, 8), holderNodeId, shardId, isValid(), routingVersion, fencingSequence);
    }
}

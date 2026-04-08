package com.loomq.cluster;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fencing Token（防护令牌）
 *
 * 硬约束 #1 续：防止脑裂的 fencing token 机制
 *
 * 设计原则：
 * 1. 单调递增序列号：每次生成新 token 时递增
 * 2. 关联租约 ID：token 必须与当前有效租约匹配
 * 3. 时间戳：记录生成时间
 * 4. 有效性检查：token 必须匹配当前租约才有效
 *
 * 使用场景：
 * - Primary 在写入 WAL 前检查 fencing token
 * - Replica 验证 primary 发送的记录中的 token
 * - 防止旧 primary 在 failover 后继续写入
 *
 * @author loomq
 * @since v0.4.8
 */
public class FencingToken {

    // 全局单调递增序列号生成器
    private static final AtomicLong GLOBAL_SEQUENCE = new AtomicLong(0);

    // 序列号
    private final long sequence;

    // 关联的租约 ID
    private final String leaseId;

    // 持有者节点 ID
    private final String holderNodeId;

    // 生成时间
    private final Instant timestamp;

    /**
     * 创建新 Fencing Token
     *
     * @param leaseId 关联的租约 ID
     * @param holderNodeId 持有者节点 ID
     */
    public FencingToken(String leaseId, String holderNodeId) {
        this.sequence = GLOBAL_SEQUENCE.incrementAndGet();
        this.leaseId = Objects.requireNonNull(leaseId, "leaseId cannot be null");
        this.holderNodeId = Objects.requireNonNull(holderNodeId, "holderNodeId cannot be null");
        this.timestamp = Instant.now();
    }

    /**
     * 完整构造函数（用于反序列化或测试）
     */
    public FencingToken(long sequence, String leaseId, String holderNodeId, Instant timestamp) {
        this.sequence = sequence;
        this.leaseId = Objects.requireNonNull(leaseId, "leaseId cannot be null");
        this.holderNodeId = Objects.requireNonNull(holderNodeId, "holderNodeId cannot be null");
        this.timestamp = Objects.requireNonNull(timestamp, "timestamp cannot be null");
    }

    // ==================== 有效性检查 ====================

    /**
     * 检查 token 是否对指定租约有效
     *
     * @param lease 租约对象
     * @return 是否有效
     */
    public boolean isValidFor(CoordinatorLease lease) {
        if (lease == null) {
            return false;
        }
        // token 必须与当前有效租约匹配
        return this.leaseId.equals(lease.getLeaseId())
            && lease.isValid()
            && this.holderNodeId.equals(lease.getHolderNodeId());
    }

    /**
     * 检查 token 是否已过期（超过最大有效期）
     *
     * @param maxTtlMs 最大有效期（毫秒）
     */
    public boolean isExpired(long maxTtlMs) {
        long age = Instant.now().toEpochMilli() - timestamp.toEpochMilli();
        return age > maxTtlMs;
    }

    // ==================== Getter 方法 ====================

    public long getSequence() {
        return sequence;
    }

    public String getLeaseId() {
        return leaseId;
    }

    public String getHolderNodeId() {
        return holderNodeId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    // ==================== 对象方法 ====================

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FencingToken that = (FencingToken) o;
        return sequence == that.sequence && Objects.equals(leaseId, that.leaseId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sequence, leaseId);
    }

    @Override
    public String toString() {
        return String.format("FencingToken{seq=%d, lease=%s, holder=%s}",
            sequence, leaseId.substring(0, 8), holderNodeId);
    }
}

/**
 * Fencing Token 过期异常
 */
class FencingTokenExpiredException extends RuntimeException {

    public FencingTokenExpiredException(String message) {
        super(message);
    }

    public FencingTokenExpiredException(String message, Throwable cause) {
        super(message, cause);
    }
}

/**
 * Fencing Token 验证器
 *
 * 用于集中管理 fencing token 的验证逻辑
 */
class FencingTokenValidator {

    private final long maxTokenTtlMs;

    public FencingTokenValidator(long maxTokenTtlMs) {
        this.maxTokenTtlMs = maxTokenTtlMs;
    }

    /**
     * 验证 token 是否有效
     *
     * @param token 待验证的 token
     * @param currentLease 当前租约
     * @throws FencingTokenExpiredException 验证失败时抛出
     */
    public void validate(FencingToken token, CoordinatorLease currentLease) {
        if (token == null) {
            throw new FencingTokenExpiredException(
                "Fencing token is null. This node may not have a valid lease.");
        }

        if (currentLease == null) {
            throw new FencingTokenExpiredException(
                "No active lease found. This node is not the primary.");
        }

        if (!token.isValidFor(currentLease)) {
            throw new FencingTokenExpiredException(
                String.format("Fencing token invalid for current lease. " +
                    "Token leaseId=%s, current leaseId=%s, holder=%s",
                    token.getLeaseId(), currentLease.getLeaseId(), currentLease.getHolderNodeId()));
        }

        if (token.isExpired(maxTokenTtlMs)) {
            throw new FencingTokenExpiredException(
                String.format("Fencing token expired (age > %d ms). Failover may have occurred.", maxTokenTtlMs));
        }
    }
}

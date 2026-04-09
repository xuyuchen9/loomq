package com.loomq.entity.v5;

import com.loomq.replication.AckLevel;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Intent 实体 (v0.5)
 *
 * Intent 是对外暴露的核心资源，代表一个未来必须触发的事件。
 *
 * @author loomq
 * @since v0.5.0
 */
public class Intent {

    // ========== 系统字段 ==========

    /**
     * 系统唯一标识
     */
    private final String intentId;

    /**
     * 当前状态
     */
    private IntentStatus status;

    /**
     * 创建时间
     */
    private final Instant createdAt;

    /**
     * 最后更新时间
     */
    private Instant updatedAt;

    // ========== 调度字段 ==========

    /**
     * 计划执行时间 (RFC3339)
     */
    private Instant executeAt;

    /**
     * 最晚有效时间 (RFC3339)
     */
    private Instant deadline;

    /**
     * 过期后动作：DISCARD 或 DEAD_LETTER
     */
    private ExpiredAction expiredAction;

    // ========== 路由字段 ==========

    /**
     * 分片键，用于路由到具体 Shard
     */
    private String shardKey;

    /**
     * 所属分片 ID
     */
    private String shardId;

    // ========== 可靠性字段 ==========

    /**
     * ACK 级别：ASYNC / DURABLE / REPLICATED
     */
    private AckLevel ackLevel;

    // ========== 回调字段 ==========

    /**
     * 回调配置
     */
    private Callback callback;

    // ========== 重投策略 ==========

    /**
     * 重投策略
     */
    private RedeliveryPolicy redelivery;

    // ========== 业务字段 ==========

    /**
     * 业务唯一键，用于幂等创建
     */
    private String idempotencyKey;

    /**
     * 业务标签，用于检索与分类
     */
    private Map<String, String> tags;

    // ========== 投递统计 ==========

    /**
     * 当前尝试次数
     */
    private int attempts;

    /**
     * 最后一次投递 ID
     */
    private String lastDeliveryId;

    // ========== 构造函数 ==========

    public Intent() {
        this.intentId = generateIntentId();
        this.status = IntentStatus.CREATED;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
        this.expiredAction = ExpiredAction.DISCARD;
        this.ackLevel = AckLevel.DURABLE;
        this.attempts = 0;
    }

    public Intent(String intentId) {
        this.intentId = Objects.requireNonNullElse(intentId, generateIntentId());
        this.status = IntentStatus.CREATED;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
        this.expiredAction = ExpiredAction.DISCARD;
        this.ackLevel = AckLevel.DURABLE;
        this.attempts = 0;
    }

    // ========== 业务方法 ==========

    /**
     * 生成 Intent ID
     */
    private static String generateIntentId() {
        return "intent_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    }

    /**
     * 状态转换
     */
    public void transitionTo(IntentStatus newStatus) {
        validateTransition(this.status, newStatus);
        this.status = newStatus;
        this.updatedAt = Instant.now();
    }

    /**
     * 验证状态转换是否合法
     */
    private void validateTransition(IntentStatus from, IntentStatus to) {
        // 终态不可转换
        if (from.isTerminal()) {
            throw new IllegalStateException(
                "Cannot transition from terminal state: " + from);
        }

        // 特定转换规则
        boolean valid = switch (from) {
            case CREATED -> to == IntentStatus.SCHEDULED;
            case SCHEDULED -> to == IntentStatus.DUE || to == IntentStatus.CANCELED;
            case DUE -> to == IntentStatus.DISPATCHING;
            case DISPATCHING -> to == IntentStatus.DELIVERED || to == IntentStatus.DEAD_LETTERED;
            case DELIVERED -> to == IntentStatus.ACKED || to == IntentStatus.EXPIRED;
            default -> false;
        };

        if (!valid) {
            throw new IllegalStateException(
                String.format("Invalid state transition: %s -> %s", from, to));
        }
    }

    /**
     * 检查是否已过期
     */
    public boolean isExpired() {
        return Instant.now().isAfter(deadline);
    }

    /**
     * 检查是否可以执行
     */
    public boolean isExecutable() {
        return status == IntentStatus.DUE && !isExpired();
    }

    /**
     * 增加尝试次数
     */
    public void incrementAttempts() {
        this.attempts++;
        this.updatedAt = Instant.now();
    }

    /**
     * 更新最后投递 ID
     */
    public void setLastDeliveryId(String deliveryId) {
        this.lastDeliveryId = deliveryId;
        this.updatedAt = Instant.now();
    }

    // ========== Getter / Setter ==========

    public String getIntentId() {
        return intentId;
    }

    public IntentStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public Instant getExecuteAt() {
        return executeAt;
    }

    public void setExecuteAt(Instant executeAt) {
        this.executeAt = executeAt;
        this.updatedAt = Instant.now();
    }

    public Instant getDeadline() {
        return deadline;
    }

    public void setDeadline(Instant deadline) {
        this.deadline = deadline;
        this.updatedAt = Instant.now();
    }

    public ExpiredAction getExpiredAction() {
        return expiredAction;
    }

    public void setExpiredAction(ExpiredAction expiredAction) {
        this.expiredAction = expiredAction;
        this.updatedAt = Instant.now();
    }

    public String getShardKey() {
        return shardKey;
    }

    public void setShardKey(String shardKey) {
        this.shardKey = shardKey;
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public AckLevel getAckLevel() {
        return ackLevel;
    }

    public void setAckLevel(AckLevel ackLevel) {
        this.ackLevel = ackLevel;
    }

    public Callback getCallback() {
        return callback;
    }

    public void setCallback(Callback callback) {
        this.callback = callback;
    }

    public RedeliveryPolicy getRedelivery() {
        return redelivery;
    }

    public void setRedelivery(RedeliveryPolicy redelivery) {
        this.redelivery = redelivery;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public int getAttempts() {
        return attempts;
    }

    public String getLastDeliveryId() {
        return lastDeliveryId;
    }

    @Override
    public String toString() {
        return String.format("Intent{id=%s, status=%s, executeAt=%s, deadline=%s}",
            intentId, status, executeAt, deadline);
    }
}

package com.loomq.domain.intent;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Intent 实体。
 *
 * Intent 是对外暴露的核心资源，代表一个未来必须触发的事件。
 *
 * @author loomq
 */
public class Intent {

    // ========== 系统字段 ==========

    /**
     * 系统唯一标识
     */
    private final String intentId;

    /**
     * 全链路追踪 ID (UUID 短码，用于 per-intent trace)
     */
    private final String traceId;

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

    /**
     * 精度档位：由 PrecisionTierCatalog 提供默认 preset
     */
    private PrecisionTier precisionTier;

    /**
     * WAL 持久化级别，覆盖精度档位默认值。
     * null 表示使用精度档位的默认 walMode。
     */
    private WalMode walMode;

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
    private AckMode ackLevel;

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

    /**
     * 单调递增的写版本号，用于 Raft 写幂等和乐观并发控制。
     */
    private long revision;

    // ========== 构造函数 ==========

    public Intent() {
        this.intentId = generateIntentId();
        this.traceId = generateTraceId();
        this.status = IntentStatus.CREATED;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
        this.expiredAction = ExpiredAction.DISCARD;
        this.precisionTier = defaultPrecisionTier();
        this.ackLevel = AckMode.DURABLE;
        this.attempts = 0;
        this.revision = 0L;
    }

    public Intent(String intentId) {
        this.intentId = Objects.requireNonNullElse(intentId, generateIntentId());
        this.traceId = generateTraceId();
        this.status = IntentStatus.CREATED;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
        this.expiredAction = ExpiredAction.DISCARD;
        this.precisionTier = defaultPrecisionTier();
        this.ackLevel = AckMode.DURABLE;
        this.attempts = 0;
        this.revision = 0L;
    }

    private Intent(String traceId,
                   String intentId,
                   IntentStatus status,
                   Instant createdAt,
                   Instant updatedAt,
                   Instant executeAt,
                   Instant deadline,
                   ExpiredAction expiredAction,
                   PrecisionTier precisionTier,
                   WalMode walMode,
                   String shardKey,
                   String shardId,
                   AckMode ackLevel,
                   Callback callback,
                   RedeliveryPolicy redelivery,
                   String idempotencyKey,
                   Map<String, String> tags,
                   int attempts,
                   String lastDeliveryId,
                   long revision) {
        this.traceId = traceId != null ? traceId : generateTraceId();
        this.intentId = Objects.requireNonNullElse(intentId, generateIntentId());
        this.status = status != null ? status : IntentStatus.CREATED;
        this.createdAt = createdAt != null ? createdAt : Instant.now();
        this.updatedAt = updatedAt != null ? updatedAt : this.createdAt;
        this.executeAt = executeAt;
        this.deadline = deadline;
        this.expiredAction = expiredAction != null ? expiredAction : ExpiredAction.DISCARD;
        this.precisionTier = precisionTier != null ? precisionTier : defaultPrecisionTier();
        this.walMode = walMode;
        this.shardKey = shardKey;
        this.shardId = shardId;
        this.ackLevel = ackLevel != null ? ackLevel : AckMode.DURABLE;
        this.callback = callback;
        this.redelivery = redelivery;
        this.idempotencyKey = idempotencyKey;
        this.tags = tags != null && !tags.isEmpty() ? Map.copyOf(tags) : null;
        this.attempts = attempts;
        this.lastDeliveryId = lastDeliveryId;
        this.revision = Math.max(0L, revision);
    }

    /**
     * 从持久化状态恢复 Intent。
     */
    public static Intent restore(String traceId,
                                 String intentId,
                                 IntentStatus status,
                                 Instant createdAt,
                                 Instant updatedAt,
                                 Instant executeAt,
                                 Instant deadline,
                                 ExpiredAction expiredAction,
                                 PrecisionTier precisionTier,
                                 WalMode walMode,
                                 String shardKey,
                                 String shardId,
                                 AckMode ackLevel,
                                 Callback callback,
                                 RedeliveryPolicy redelivery,
                                 String idempotencyKey,
                                 Map<String, String> tags,
                                 int attempts,
                                 String lastDeliveryId,
                                 long revision) {
        return new Intent(
            traceId,
            intentId,
            status,
            createdAt,
            updatedAt,
            executeAt,
            deadline,
            expiredAction,
            precisionTier,
            walMode,
            shardKey,
            shardId,
            ackLevel,
            callback,
            redelivery,
            idempotencyKey,
            tags,
            attempts,
            lastDeliveryId,
            revision
        );
    }

    /**
     * 创建当前 Intent 的独立副本。
     *
     * Raft 写路径会先在内存中生成最终快照，再提交到日志，避免直接
     * 修改 store 中的当前态对象。
     */
    public Intent copy() {
        return restore(
            traceId,
            intentId,
            status,
            createdAt,
            updatedAt,
            executeAt,
            deadline,
            expiredAction,
            precisionTier,
            walMode,
            shardKey,
            shardId,
            ackLevel,
            callback,
            redelivery,
            idempotencyKey,
            tags,
            attempts,
            lastDeliveryId,
            revision
        );
    }

    /**
     * 创建当前 Intent 的独立副本。
     *
     * Raft 写路径会先在内存中生成最终快照，再提交到日志，避免直接
     * 修改 store 中的当前态对象。
     */
    public Intent copy() {
        return restore(
            traceId,
            intentId,
            status,
            createdAt,
            updatedAt,
            executeAt,
            deadline,
            expiredAction,
            precisionTier,
            walMode,
            shardKey,
            shardId,
            ackLevel,
            callback,
            redelivery,
            idempotencyKey,
            tags,
            attempts,
            lastDeliveryId
        );
    }

    // ========== 业务方法 ==========

    /**
     * 生成 Intent ID
     */
    private static String generateIntentId() {
        return "intent_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    }

    private static PrecisionTier defaultPrecisionTier() {
        return PrecisionTierCatalog.defaultCatalog().defaultTier();
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
            case SCHEDULED -> to == IntentStatus.DUE || to == IntentStatus.CANCELED
                || to == IntentStatus.EXPIRED || to == IntentStatus.DEAD_LETTERED;
            case DUE -> to == IntentStatus.DISPATCHING || to == IntentStatus.CANCELED
                || to == IntentStatus.EXPIRED || to == IntentStatus.DEAD_LETTERED;
            case DISPATCHING -> to == IntentStatus.DELIVERED || to == IntentStatus.DEAD_LETTERED || to == IntentStatus.SCHEDULED || to == IntentStatus.EXPIRED;
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
        return deadline != null && Instant.now().isAfter(deadline);
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
     * 增加写版本号。
     *
     * Raft 写路径和本地写路径都会在状态变更后调用它，避免重复提交
     * 或陈旧写请求覆盖更新后的状态。
     */
    public void incrementRevision() {
        this.revision++;
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

    public String getTraceId() {
        return traceId;
    }

    private static String generateTraceId() {
        return UUID.randomUUID().toString().substring(0, 8);
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

    public PrecisionTier getPrecisionTier() {
        return precisionTier;
    }

    public void setPrecisionTier(PrecisionTier precisionTier) {
        this.precisionTier = precisionTier;
        this.updatedAt = Instant.now();
    }

    public WalMode getWalMode() {
        return walMode;
    }

    public void setWalMode(WalMode walMode) {
        this.walMode = walMode;
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

    public AckMode getAckMode() {
        return ackLevel;
    }

    public void setAckMode(AckMode ackLevel) {
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

    public long getRevision() {
        return revision;
    }

    @Override
    public String toString() {
        return String.format("Intent{id=%s, status=%s, executeAt=%s, deadline=%s, tier=%s, walMode=%s}",
            intentId, status, executeAt, deadline, precisionTier, walMode);
    }
}

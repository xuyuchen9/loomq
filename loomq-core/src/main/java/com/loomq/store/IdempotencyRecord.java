package com.loomq.store;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * 幂等性记录
 *
 * 用于在 24h 窗口期内实现幂等创建语义
 *
 * @author loomq
 * @since v0.5.0
 */
public class IdempotencyRecord {

    /**
     * 默认幂等窗口期：24小时
     */
    public static final Duration DEFAULT_WINDOW = Duration.ofHours(24);

    /**
     * 业务幂等键
     */
    private final String idempotencyKey;

    /**
     * 对应的 Intent ID
     */
    private final String intentId;

    /**
     * 创建时间
     */
    private final Instant createdAt;

    /**
     * 窗口过期时间
     */
    private final Instant windowExpiry;

    /**
     * 当前 Intent 状态（缓存，实时查询 IntentStore 为准）
     */
    private volatile IntentStatus status;

    public IdempotencyRecord(String idempotencyKey, String intentId,
                             Instant createdAt, IntentStatus status) {
        this(idempotencyKey, intentId, createdAt, createdAt.plus(DEFAULT_WINDOW), status);
    }

    public IdempotencyRecord(String idempotencyKey, String intentId,
                             Instant createdAt, Instant windowExpiry, IntentStatus status) {
        this.idempotencyKey = Objects.requireNonNull(idempotencyKey, "idempotencyKey cannot be null");
        this.intentId = Objects.requireNonNull(intentId, "intentId cannot be null");
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt cannot be null");
        this.windowExpiry = Objects.requireNonNull(windowExpiry, "windowExpiry cannot be null");
        this.status = status;
    }

    /**
     * 从 Intent 创建幂等记录
     */
    public static IdempotencyRecord fromIntent(Intent intent) {
        return new IdempotencyRecord(
            intent.getIdempotencyKey(),
            intent.getIntentId(),
            intent.getCreatedAt(),
            intent.getStatus()
        );
    }

    /**
     * 检查是否在窗口期内
     */
    public boolean isInWindow() {
        return Instant.now().isBefore(windowExpiry);
    }

    /**
     * 检查是否已过期（窗口期外）
     */
    public boolean isExpired() {
        return !isInWindow();
    }

    /**
     * 更新状态
     */
    public void updateStatus(IntentStatus newStatus) {
        this.status = newStatus;
    }

    /**
     * 检查是否为终态
     */
    public boolean isTerminal() {
        return status != null && status.isTerminal();
    }

    /**
     * 序列化为字节数组（RocksDB 存储用）。
     * 格式：idempotencyKeyLen(2B) + key + intentIdLen(2B) + id + createdAt(8B) + windowExpiry(8B)
     */
    public byte[] toBytes() {
        byte[] keyBytes = idempotencyKey.getBytes(StandardCharsets.UTF_8);
        byte[] idBytes = intentId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + keyBytes.length + 2 + idBytes.length + 8 + 8);
        buf.putShort((short) keyBytes.length);
        buf.put(keyBytes);
        buf.putShort((short) idBytes.length);
        buf.put(idBytes);
        buf.putLong(createdAt.toEpochMilli());
        buf.putLong(windowExpiry.toEpochMilli());
        return buf.array();
    }

    /**
     * 从字节数组反序列化。
     */
    public static IdempotencyRecord fromBytes(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        short keyLen = buf.getShort();
        byte[] keyBytes = new byte[keyLen];
        buf.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);

        short idLen = buf.getShort();
        byte[] idBytes = new byte[idLen];
        buf.get(idBytes);
        String intentId = new String(idBytes, StandardCharsets.UTF_8);

        Instant createdAt = Instant.ofEpochMilli(buf.getLong());
        Instant windowExpiry = Instant.ofEpochMilli(buf.getLong());

        return new IdempotencyRecord(key, intentId, createdAt, windowExpiry, null);
    }

    // ========== Getters ==========

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public String getIntentId() {
        return intentId;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getWindowExpiry() {
        return windowExpiry;
    }

    public IntentStatus getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IdempotencyRecord that = (IdempotencyRecord) o;
        return Objects.equals(idempotencyKey, that.idempotencyKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idempotencyKey);
    }

    @Override
    public String toString() {
        return String.format("IdempotencyRecord{key=%s, intentId=%s, inWindow=%s, status=%s}",
            idempotencyKey, intentId, isInWindow(), status);
    }
}

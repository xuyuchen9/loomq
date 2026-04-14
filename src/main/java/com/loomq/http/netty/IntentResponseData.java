package com.loomq.http.netty;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.replication.AckLevel;
import io.netty.buffer.ByteBuf;

import java.time.Instant;

/**
 * Intent 响应数据（支持零拷贝序列化）
 *
 * @author loomq
 * @since v0.6.0
 */
public final class IntentResponseData implements DirectSerializedResponse {

    private final String intentId;
    private final IntentStatus status;
    private final Instant executeAt;
    private final Instant deadline;
    private final String shardKey;
    private final PrecisionTier precisionTier;
    private final AckLevel ackLevel;

    public IntentResponseData(Intent intent) {
        this.intentId = intent.getIntentId();
        this.status = intent.getStatus();
        this.executeAt = intent.getExecuteAt();
        this.deadline = intent.getDeadline();
        this.shardKey = intent.getShardKey();
        this.precisionTier = intent.getPrecisionTier();
        this.ackLevel = intent.getAckLevel();
    }

    public IntentResponseData(
            String intentId,
            IntentStatus status,
            Instant executeAt,
            Instant deadline,
            String shardKey,
            PrecisionTier precisionTier,
            AckLevel ackLevel) {
        this.intentId = intentId;
        this.status = status;
        this.executeAt = executeAt;
        this.deadline = deadline;
        this.shardKey = shardKey;
        this.precisionTier = precisionTier;
        this.ackLevel = ackLevel;
    }

    @Override
    public void writeTo(ByteBuf buf) {
        IntentResponseSerializer.writeIntentResponse(
                intentId, status, executeAt, deadline,
                shardKey, precisionTier, ackLevel, buf);
    }

    @Override
    public int estimateSize() {
        // 基础模板 ~100 字节 + 动态字段
        return 100 +
                (intentId != null ? intentId.length() : 0) +
                (shardKey != null ? shardKey.length() : 0);
    }

    // Getters

    public String getIntentId() { return intentId; }
    public IntentStatus getStatus() { return status; }
    public Instant getExecuteAt() { return executeAt; }
    public Instant getDeadline() { return deadline; }
    public String getShardKey() { return shardKey; }
    public PrecisionTier getPrecisionTier() { return precisionTier; }
    public AckLevel getAckLevel() { return ackLevel; }
}

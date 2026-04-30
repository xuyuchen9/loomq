package com.loomq.api;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.WalMode;
import com.loomq.http.netty.DirectSerializedResponse;
import com.loomq.http.netty.IntentResponseSerializer;
import com.loomq.replication.AckLevel;
import io.netty.buffer.ByteBuf;

import java.time.Instant;

/**
 * Intent 响应
 *
 * @author loomq
 * @since v0.5.0
 */
public record IntentResponse(
    String intentId,
    IntentStatus status,
    Instant executeAt,
    Instant deadline,
    com.loomq.domain.intent.ExpiredAction expiredAction,
    PrecisionTier precisionTier,
    WalMode walMode,
    AckLevel ackLevel,
    int attempts,
    String lastDeliveryId,
    Instant createdAt,
    Instant updatedAt
    ) implements DirectSerializedResponse {

    public static IntentResponse from(Intent intent) {
        return new IntentResponse(
            intent.getIntentId(),
            intent.getStatus(),
            intent.getExecuteAt(),
            intent.getDeadline(),
            intent.getExpiredAction(),
            intent.getPrecisionTier(),
            intent.getWalMode(),
            intent.getAckLevel(),
            intent.getAttempts(),
            intent.getLastDeliveryId(),
            intent.getCreatedAt(),
            intent.getUpdatedAt()
        );
    }

    @Override
    public void writeTo(ByteBuf buf) {
        IntentResponseSerializer.writeIntentResponse(this, buf);
    }

    @Override
    public int estimateSize() {
        return IntentResponseSerializer.estimateSize(this);
    }
}

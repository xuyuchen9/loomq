package com.loomq.api;

import com.loomq.entity.v5.Intent;
import com.loomq.entity.v5.IntentStatus;
import com.loomq.entity.v5.PrecisionTier;
import com.loomq.replication.AckLevel;

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
    com.loomq.entity.v5.ExpiredAction expiredAction,
    PrecisionTier precisionTier,
    AckLevel ackLevel,
    int attempts,
    String lastDeliveryId,
    Instant createdAt,
    Instant updatedAt
) {

    public static IntentResponse from(Intent intent) {
        return new IntentResponse(
            intent.getIntentId(),
            intent.getStatus(),
            intent.getExecuteAt(),
            intent.getDeadline(),
            intent.getExpiredAction(),
            intent.getPrecisionTier(),
            intent.getAckLevel(),
            intent.getAttempts(),
            intent.getLastDeliveryId(),
            intent.getCreatedAt(),
            intent.getUpdatedAt()
        );
    }
}

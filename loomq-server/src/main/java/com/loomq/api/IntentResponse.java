package com.loomq.api;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
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
    com.loomq.domain.intent.ExpiredAction expiredAction,
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

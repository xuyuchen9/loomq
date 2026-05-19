package com.loomq.api;

import com.loomq.domain.intent.Intent;
import java.util.List;

/**
 * Intent 列表查询响应。
 */
public record IntentListResponse(
    List<IntentSummary> intents,
    long total,
    int offset,
    int limit
) {

    public static IntentListResponse of(List<IntentSummary> intents, long total, int offset, int limit) {
        return new IntentListResponse(intents, total, offset, limit);
    }

    public record IntentSummary(
        String intentId,
        String status,
        String executeAt,
        String deadline,
        String tier,
        int attempts,
        String callbackUrl,
        String createdAt,
        String updatedAt,
        DeathReport deathReport
    ) {

        public static IntentSummary from(Intent intent, DeathReport deathReport) {
            return new IntentSummary(
                intent.getIntentId(),
                intent.getStatus().name(),
                intent.getExecuteAt() != null ? intent.getExecuteAt().toString() : null,
                intent.getDeadline() != null ? intent.getDeadline().toString() : null,
                intent.getPrecisionTier() != null ? intent.getPrecisionTier().name() : null,
                intent.getAttempts(),
                intent.getCallback() != null ? intent.getCallback().getUrl() : null,
                intent.getCreatedAt() != null ? intent.getCreatedAt().toString() : null,
                intent.getUpdatedAt() != null ? intent.getUpdatedAt().toString() : null,
                deathReport
            );
        }
    }
}

package com.loomq.api;

import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import java.util.List;
import java.util.Map;

/**
 * 根据错误码和上下文生成恢复建议。
 *
 * <p>每个错误码映射到一条可操作的建议，告知用户：出了什么问题、下一步怎么做、
 * 如果是暂态问题大约等多久、当前状态可能转到哪些状态。</p>
 */
public final class ErrorRecoveryAdvisor {

    private ErrorRecoveryAdvisor() {}

    public static RecoveryHint advise(String errorCode, Map<String, Object> context) {
        if (errorCode == null) {
            return null;
        }

        return switch (errorCode) {
            case "42204" -> adviseCannotCancel(context);
            case "42205" -> adviseCannotModify(context);
            case "42900" -> adviseBackpressure(context);
            case "40401" -> adviseNotFound(context);
            case "40901" -> adviseIdempotencyConflict(context);
            case "40902" -> adviseRevisionConflict();
            case "42801" -> adviseRevisionRequired();
            case "40001" -> adviseInvalidRevisionHeader();
            case "42201" -> adviseMissingExecuteAt();
            case "42202" -> adviseMissingDeadline();
            case "42203" -> adviseMissingShardKey();
            case "42206" -> adviseExecuteAtPast();
            case "42207" -> adviseCannotRevive(context);
            case "40002" -> adviseMissingStatusParam();
            case "40003" -> adviseInvalidStatusParam();
            case "40004" -> adviseMissingIntentIdParam();
            case "50301" -> adviseRaftReadRedirect(context);
            case "50302" -> adviseRaftWriteRedirect(context);
            case "50303" -> adviseRaftBackpressure(context);
            case "50002" -> adviseCancelFailed();
            case "50003" -> adviseFireNowFailed();
            default -> null;
        };
    }

    private static RecoveryHint adviseCannotCancel(Map<String, Object> context) {
        IntentStatus status = extractStatus(context);
        if (status == IntentStatus.DISPATCHING) {
            return RecoveryHint.temporal(
                "Intent is currently being delivered. Wait for delivery to complete; cancel is available in SCHEDULED or DUE states.",
                "GET /v1/intents/{id}",
                estimateWaitMs(status, context),
                status.name(),
                List.of(IntentStatus.DELIVERED.name(), IntentStatus.DEAD_LETTERED.name())
            );
        }
        if (status == IntentStatus.DELIVERED) {
            return RecoveryHint.temporal(
                "Intent has been delivered and is awaiting ACK. Cancel is not available after delivery.",
                "GET /v1/intents/{id}",
                estimateWaitMs(status, context),
                status.name(),
                List.of(IntentStatus.ACKED.name(), IntentStatus.EXPIRED.name())
            );
        }
        if (status != null && status.isTerminal()) {
            return RecoveryHint.nonTemporal(
                "Intent is in terminal state " + status + " and cannot be cancelled.",
                null
            );
        }
        return RecoveryHint.nonTemporal(
            "Cancel is only available in SCHEDULED or DUE states.",
            "GET /v1/intents/{id}"
        );
    }

    private static RecoveryHint adviseCannotModify(Map<String, Object> context) {
        IntentStatus status = extractStatus(context);
        if (status == IntentStatus.DISPATCHING) {
            return RecoveryHint.temporal(
                "Intent is currently being delivered. Wait for delivery to complete, then modify.",
                "GET /v1/intents/{id}",
                estimateWaitMs(status, context),
                status.name(),
                List.of(IntentStatus.DELIVERED.name(), IntentStatus.DEAD_LETTERED.name(), IntentStatus.SCHEDULED.name())
            );
        }
        if (status == IntentStatus.DELIVERED) {
            return RecoveryHint.temporal(
                "Intent was delivered but not yet ACKed. It will auto-transition to ACKED or be redelivered.",
                "GET /v1/intents/{id}",
                estimateWaitMs(status, context),
                status.name(),
                List.of(IntentStatus.ACKED.name(), IntentStatus.SCHEDULED.name())
            );
        }
        if (status != null && status.isTerminal()) {
            return RecoveryHint.nonTemporal(
                "Intent is in terminal state " + status + " and cannot be modified.",
                null
            );
        }
        return RecoveryHint.nonTemporal(
            "Modification is not available in the current state.",
            "GET /v1/intents/{id}"
        );
    }

    private static RecoveryHint adviseBackpressure(Map<String, Object> context) {
        Long retryAfterMs = context != null && context.get("retryAfterMs") instanceof Long l ? l : null;
        long waitMs = retryAfterMs != null ? retryAfterMs : 1000L;
        return RecoveryHint.temporal(
            "The scheduler has reached capacity. Retry after the indicated delay.",
            "POST /v1/intents (same request)",
            waitMs,
            null,
            null
        );
    }

    private static RecoveryHint adviseNotFound(Map<String, Object> context) {
        return RecoveryHint.nonTemporal(
            "The intent may have already reached a terminal state (ACKED, CANCELED, EXPIRED, DEAD_LETTERED) and been garbage collected, or the ID is incorrect.",
            "POST /v1/intents (create a new intent)"
        );
    }

    private static RecoveryHint adviseIdempotencyConflict(Map<String, Object> context) {
        String intentId = context != null && context.get("intentId") instanceof String s ? s : null;
        String endpoint = intentId != null ? "GET /v1/intents/" + intentId : "GET /v1/intents/{id}";
        return RecoveryHint.nonTemporal(
            "An intent with this idempotency key has already reached a terminal state. Use a different key or check the existing intent.",
            endpoint
        );
    }

    private static RecoveryHint adviseRevisionConflict() {
        return RecoveryHint.nonTemporal(
            "Another write modified this intent. Re-fetch the intent to get the current revision, then retry with the updated revision.",
            "GET /v1/intents/{id}"
        );
    }

    private static RecoveryHint adviseRevisionRequired() {
        return RecoveryHint.nonTemporal(
            "Raft mode requires the X-LoomQ-Expected-Revision header for write operations. Fetch the intent first to get the current revision.",
            "GET /v1/intents/{id}"
        );
    }

    private static RecoveryHint adviseInvalidRevisionHeader() {
        return RecoveryHint.nonTemporal(
            "The X-LoomQ-Expected-Revision header must be a valid numeric value.",
            "GET /v1/intents/{id}"
        );
    }

    private static RecoveryHint adviseMissingExecuteAt() {
        return RecoveryHint.nonTemporal(
            "The executeAt field is required. Provide an ISO-8601 timestamp indicating when the intent should fire.",
            "POST /v1/intents"
        );
    }

    private static RecoveryHint adviseMissingDeadline() {
        return RecoveryHint.nonTemporal(
            "The deadline field is required. Provide an ISO-8601 timestamp after which the intent expires if not delivered.",
            "POST /v1/intents"
        );
    }

    private static RecoveryHint adviseMissingShardKey() {
        return RecoveryHint.nonTemporal(
            "The shardKey field is required. Provide a key used for routing and partitioning the intent.",
            "POST /v1/intents"
        );
    }

    private static RecoveryHint adviseExecuteAtPast() {
        return RecoveryHint.nonTemporal(
            "The executeAt must be in the future. If you need to fire immediately, use the fire-now endpoint on an existing intent.",
            "POST /v1/intents/{id}/fire-now"
        );
    }

    private static RecoveryHint adviseCannotRevive(Map<String, Object> context) {
        IntentStatus status = extractStatus(context);
        if (status != null && status.isTerminal() && status != IntentStatus.DEAD_LETTERED) {
            return RecoveryHint.nonTemporal(
                "Only DEAD_LETTERED intents can be revived. This intent is in " + status + " state. Create a new intent instead.",
                "POST /v1/intents"
            );
        }
        return RecoveryHint.nonTemporal(
            "Only DEAD_LETTERED intents can be revived. Use the list endpoint to find dead-lettered intents.",
            "GET /v1/intents?status=DEAD_LETTERED"
        );
    }

    private static RecoveryHint adviseMissingIntentIdParam() {
        return RecoveryHint.nonTemporal(
            "The 'intentId' query parameter is required for WAL replay.",
            "GET /v1/intents/{id}"
        );
    }

    private static RecoveryHint adviseMissingStatusParam() {
        return RecoveryHint.nonTemporal(
            "The 'status' query parameter is required. Available values: SCHEDULED, DUE, DISPATCHING, DELIVERED, ACKED, CANCELED, EXPIRED, DEAD_LETTERED.",
            "GET /v1/intents?status=DEAD_LETTERED"
        );
    }

    private static RecoveryHint adviseInvalidStatusParam() {
        return RecoveryHint.nonTemporal(
            "The 'status' value is not recognized. Valid values: SCHEDULED, DUE, DISPATCHING, DELIVERED, ACKED, CANCELED, EXPIRED, DEAD_LETTERED.",
            "GET /v1/intents?status=DEAD_LETTERED"
        );
    }

    private static RecoveryHint adviseRaftReadRedirect(Map<String, Object> context) {
        String leaderId = extractLeaderId(context);
        String msg = leaderId != null
            ? "This node is a follower. Route your read to the leader node: " + leaderId
            : "This node is a follower. Route your read to the Raft leader node.";
        return RecoveryHint.raftRedirect(msg, leaderId);
    }

    private static RecoveryHint adviseRaftWriteRedirect(Map<String, Object> context) {
        String leaderId = extractLeaderId(context);
        String msg = leaderId != null
            ? "This node is a follower. Route your write to the leader node: " + leaderId
            : "This node is a follower. Route your write to the Raft leader node.";
        return RecoveryHint.raftRedirect(msg, leaderId);
    }

    private static RecoveryHint adviseRaftBackpressure(Map<String, Object> context) {
        Long retryAfterMs = context != null && context.get("retryAfterMs") instanceof Long l ? l : null;
        long waitMs = retryAfterMs != null ? retryAfterMs : 2000L;
        return RecoveryHint.temporal(
            "Raft write backpressure. The cluster is temporarily unable to process writes. Retry after the delay.",
            null,
            waitMs,
            null,
            null
        );
    }

    private static RecoveryHint adviseCancelFailed() {
        return RecoveryHint.nonTemporal(
            "Internal failure while cancelling the intent. The intent state may have changed. Re-fetch and retry if still in a cancellable state.",
            "GET /v1/intents/{id}"
        );
    }

    private static RecoveryHint adviseFireNowFailed() {
        return RecoveryHint.nonTemporal(
            "Internal failure while triggering the intent. The intent may not be in a fireable state. Re-fetch and check its current status.",
            "GET /v1/intents/{id}"
        );
    }

    private static IntentStatus extractStatus(Map<String, Object> context) {
        if (context == null) {
            return null;
        }
        Object status = context.get("currentState");
        if (status instanceof IntentStatus s) {
            return s;
        }
        if (status instanceof String s) {
            try {
                return IntentStatus.valueOf(s);
            } catch (IllegalArgumentException ignored) {}
        }
        return null;
    }

    private static String extractLeaderId(Map<String, Object> context) {
        if (context == null) {
            return null;
        }
        Object leaderId = context.get("leaderId");
        return leaderId instanceof String s ? s : null;
    }

    private static long estimateWaitMs(IntentStatus status, Map<String, Object> context) {
        PrecisionTier tier = null;
        if (context != null && context.get("precisionTier") instanceof PrecisionTier t) {
            tier = t;
        }
        long windowMs = tier != null ? tier.getPrecisionWindowMs() : PrecisionTier.STANDARD.getPrecisionWindowMs();
        return switch (status) {
            case DISPATCHING -> windowMs * 3;
            case DELIVERED -> windowMs;
            default -> windowMs * 2;
        };
    }
}

package com.loomq.tracing;

import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;

import java.time.Instant;

/**
 * Per-intent trace for full lifecycle observability.
 *
 * Captures 5 key timestamps:
 * - createdAt: Intent created
 * - enqueuedAt: Entered dispatch queue
 * - dequeuedAt: Left dispatch queue (about to deliver)
 * - deliveredAt: Delivery completed
 * - ackedAt: ACK confirmed
 *
 * Plus computed latency breakdown:
 * - enqueueLagMs: createdAt → enqueuedAt
 * - dispatchLagMs: enqueuedAt → dequeuedAt
 * - deliveryLagMs: dequeuedAt → deliveredAt
 * - totalLagMs: createdAt → deliveredAt
 */
public record IntentTrace(
    String intentId,
    String traceId,
    PrecisionTier tier,
    IntentStatus status,
    long createdAtMs,
    long enqueuedAtMs,
    long dequeuedAtMs,
    long deliveredAtMs,
    long ackedAtMs,
    long enqueueLagMs,
    long dispatchLagMs,
    long deliveryLagMs,
    long totalLagMs
) {
    /**
     * Create with updated enqueued timestamp.
     */
    public IntentTrace withEnqueuedAt(long enqueuedAtMs) {
        long enqueueLag = createdAtMs > 0 && enqueuedAtMs > createdAtMs ? enqueuedAtMs - createdAtMs : 0;
        return new IntentTrace(intentId, traceId, tier, status, createdAtMs, enqueuedAtMs,
            dequeuedAtMs, deliveredAtMs, ackedAtMs, enqueueLag, dispatchLagMs, deliveryLagMs, totalLagMs);
    }

    /**
     * Create with updated dequeued timestamp.
     */
    public IntentTrace withDequeuedAt(long dequeuedAtMs) {
        long dispatchLag = enqueuedAtMs > 0 && dequeuedAtMs > enqueuedAtMs ? dequeuedAtMs - enqueuedAtMs : 0;
        long totalLag = createdAtMs > 0 && dequeuedAtMs > createdAtMs ? dequeuedAtMs - createdAtMs : 0;
        return new IntentTrace(intentId, traceId, tier, status, createdAtMs, enqueuedAtMs,
            dequeuedAtMs, deliveredAtMs, ackedAtMs, enqueueLagMs, dispatchLag, deliveryLagMs, totalLag);
    }

    /**
     * Create with updated delivered timestamp.
     */
    public IntentTrace withDeliveredAt(long deliveredAtMs) {
        long deliveryLag = dequeuedAtMs > 0 && deliveredAtMs > dequeuedAtMs ? deliveredAtMs - dequeuedAtMs : 0;
        long totalLag = createdAtMs > 0 && deliveredAtMs > createdAtMs ? deliveredAtMs - createdAtMs : 0;
        return new IntentTrace(intentId, traceId, tier, status, createdAtMs, enqueuedAtMs,
            dequeuedAtMs, deliveredAtMs, ackedAtMs, enqueueLagMs, dispatchLagMs, deliveryLag, totalLag);
    }

    /**
     * Create with updated acked timestamp.
     */
    public IntentTrace withAckedAt(long ackedAtMs) {
        return new IntentTrace(intentId, traceId, tier, status, createdAtMs, enqueuedAtMs,
            dequeuedAtMs, deliveredAtMs, ackedAtMs, enqueueLagMs, dispatchLagMs, deliveryLagMs, totalLagMs);
    }

    /**
     * Create with updated status.
     */
    public IntentTrace withStatus(IntentStatus status) {
        return new IntentTrace(intentId, traceId, tier, status, createdAtMs, enqueuedAtMs,
            dequeuedAtMs, deliveredAtMs, ackedAtMs, enqueueLagMs, dispatchLagMs, deliveryLagMs, totalLagMs);
    }

    /**
     * Convert to JSON string.
     */
    public String toJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"intentId\":\"").append(intentId).append("\"");
        sb.append(",\"traceId\":\"").append(traceId).append("\"");
        sb.append(",\"tier\":\"").append(tier.name()).append("\"");
        sb.append(",\"status\":\"").append(status.name()).append("\"");
        sb.append(",\"createdAt\":\"").append(createdAtMs > 0 ? Instant.ofEpochMilli(createdAtMs).toString() : "null").append("\"");
        sb.append(",\"enqueuedAt\":\"").append(enqueuedAtMs > 0 ? Instant.ofEpochMilli(enqueuedAtMs).toString() : "null").append("\"");
        sb.append(",\"dequeuedAt\":\"").append(dequeuedAtMs > 0 ? Instant.ofEpochMilli(dequeuedAtMs).toString() : "null").append("\"");
        sb.append(",\"deliveredAt\":\"").append(deliveredAtMs > 0 ? Instant.ofEpochMilli(deliveredAtMs).toString() : "null").append("\"");
        sb.append(",\"ackedAt\":\"").append(ackedAtMs > 0 ? Instant.ofEpochMilli(ackedAtMs).toString() : "null").append("\"");
        sb.append(",\"enqueueLagMs\":").append(enqueueLagMs);
        sb.append(",\"dispatchLagMs\":").append(dispatchLagMs);
        sb.append(",\"deliveryLagMs\":").append(deliveryLagMs);
        sb.append(",\"totalLagMs\":").append(totalLagMs);
        sb.append("}");
        return sb.toString();
    }
}

package com.loomq.domain.intent;

import com.loomq.replication.AckLevel;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

class IntentStateTransitionTest {

    @Test
    void shouldAllowDueToCanceledTransition() {
        Intent intent = new Intent("intent-due-cancel");
        intent.setExecuteAt(Instant.now().plusSeconds(60));

        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);

        assertDoesNotThrow(() -> intent.transitionTo(IntentStatus.CANCELED));
        assertEquals(IntentStatus.CANCELED, intent.getStatus());
    }

    @Test
    void shouldRestoreFullStateWithoutMutatingFields() {
        Instant createdAt = Instant.parse("2026-01-01T00:00:00Z");
        Instant updatedAt = Instant.parse("2026-01-01T00:05:00Z");
        Instant executeAt = Instant.parse("2026-01-01T00:10:00Z");
        Instant deadline = Instant.parse("2026-01-01T00:20:00Z");

        Intent restored = Intent.restore(
            "intent-restore",
            IntentStatus.DELIVERED,
            createdAt,
            updatedAt,
            executeAt,
            deadline,
            ExpiredAction.DEAD_LETTER,
            PrecisionTier.FAST,
            "shard-a",
            "shard-1",
            AckLevel.REPLICATED,
            null,
            null,
            "idem-123",
            Map.of("team", "core"),
            3,
            "delivery-7"
        );

        assertEquals("intent-restore", restored.getIntentId());
        assertEquals(IntentStatus.DELIVERED, restored.getStatus());
        assertEquals(createdAt, restored.getCreatedAt());
        assertEquals(updatedAt, restored.getUpdatedAt());
        assertEquals(executeAt, restored.getExecuteAt());
        assertEquals(deadline, restored.getDeadline());
        assertEquals(ExpiredAction.DEAD_LETTER, restored.getExpiredAction());
        assertEquals(PrecisionTier.FAST, restored.getPrecisionTier());
        assertEquals("shard-a", restored.getShardKey());
        assertEquals("shard-1", restored.getShardId());
        assertEquals(AckLevel.REPLICATED, restored.getAckLevel());
        assertEquals("idem-123", restored.getIdempotencyKey());
        assertEquals(Map.of("team", "core"), restored.getTags());
        assertEquals(3, restored.getAttempts());
        assertEquals("delivery-7", restored.getLastDeliveryId());
    }
}

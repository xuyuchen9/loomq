package com.loomq.domain.intent;

import com.loomq.replication.AckLevel;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IntentStateTransitionTest {

    // ========== valid transitions ==========

    @Test
    void shouldAllowCreatedToScheduled() {
        Intent intent = new Intent("intent-1");
        assertDoesNotThrow(() -> intent.transitionTo(IntentStatus.SCHEDULED));
        assertEquals(IntentStatus.SCHEDULED, intent.getStatus());
    }

    @Test
    void shouldAllowScheduledToDue() {
        Intent intent = new Intent("intent-2");
        intent.transitionTo(IntentStatus.SCHEDULED);
        assertDoesNotThrow(() -> intent.transitionTo(IntentStatus.DUE));
        assertEquals(IntentStatus.DUE, intent.getStatus());
    }

    @Test
    void shouldAllowScheduledToCanceled() {
        Intent intent = new Intent("intent-3");
        intent.transitionTo(IntentStatus.SCHEDULED);
        assertDoesNotThrow(() -> intent.transitionTo(IntentStatus.CANCELED));
        assertEquals(IntentStatus.CANCELED, intent.getStatus());
    }

    @Test
    void shouldAllowDueToDispatching() {
        Intent intent = new Intent("intent-4");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        assertDoesNotThrow(() -> intent.transitionTo(IntentStatus.DISPATCHING));
        assertEquals(IntentStatus.DISPATCHING, intent.getStatus());
    }

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
    void shouldAllowDispatchingToDelivered() {
        Intent intent = new Intent("intent-5");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        assertDoesNotThrow(() -> intent.transitionTo(IntentStatus.DELIVERED));
        assertEquals(IntentStatus.DELIVERED, intent.getStatus());
    }

    @Test
    void shouldAllowDispatchingToDeadLettered() {
        Intent intent = new Intent("intent-6");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        assertDoesNotThrow(() -> intent.transitionTo(IntentStatus.DEAD_LETTERED));
        assertEquals(IntentStatus.DEAD_LETTERED, intent.getStatus());
    }

    @Test
    void shouldAllowDispatchingToExpired() {
        Intent intent = new Intent("intent-7a");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        assertDoesNotThrow(() -> intent.transitionTo(IntentStatus.EXPIRED));
        assertEquals(IntentStatus.EXPIRED, intent.getStatus());
    }

    @Test
    void shouldAllowDispatchingToScheduledForRetry() {
        Intent intent = new Intent("intent-7");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        assertDoesNotThrow(() -> intent.transitionTo(IntentStatus.SCHEDULED));
        assertEquals(IntentStatus.SCHEDULED, intent.getStatus());
    }

    @Test
    void shouldAllowDeliveredToAcked() {
        Intent intent = new Intent("intent-8");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        intent.transitionTo(IntentStatus.DELIVERED);
        assertDoesNotThrow(() -> intent.transitionTo(IntentStatus.ACKED));
        assertEquals(IntentStatus.ACKED, intent.getStatus());
    }

    @Test
    void shouldAllowDeliveredToExpired() {
        Intent intent = new Intent("intent-9");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        intent.transitionTo(IntentStatus.DELIVERED);
        assertDoesNotThrow(() -> intent.transitionTo(IntentStatus.EXPIRED));
        assertEquals(IntentStatus.EXPIRED, intent.getStatus());
    }

    // ========== terminal states reject all transitions ==========

    @Test
    void shouldRejectTransitionFromAcked() {
        Intent intent = ackedIntent();
        assertThrows(IllegalStateException.class, () -> intent.transitionTo(IntentStatus.DUE));
    }

    @Test
    void shouldRejectTransitionFromCanceled() {
        Intent intent = new Intent("intent-c");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.CANCELED);
        assertThrows(IllegalStateException.class, () -> intent.transitionTo(IntentStatus.SCHEDULED));
    }

    @Test
    void shouldRejectTransitionFromExpired() {
        Intent intent = new Intent("intent-e");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        intent.transitionTo(IntentStatus.DELIVERED);
        intent.transitionTo(IntentStatus.EXPIRED);
        assertThrows(IllegalStateException.class, () -> intent.transitionTo(IntentStatus.ACKED));
    }

    @Test
    void shouldRejectTransitionFromDeadLettered() {
        Intent intent = new Intent("intent-dl");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        intent.transitionTo(IntentStatus.DEAD_LETTERED);
        assertThrows(IllegalStateException.class, () -> intent.transitionTo(IntentStatus.SCHEDULED));
    }

    // ========== illegal jump transitions ==========

    @Test
    void shouldRejectCreatedToDispatching() {
        Intent intent = new Intent("intent-j1");
        assertThrows(IllegalStateException.class, () -> intent.transitionTo(IntentStatus.DISPATCHING));
    }

    @Test
    void shouldRejectCreatedToDelivered() {
        Intent intent = new Intent("intent-j2");
        assertThrows(IllegalStateException.class, () -> intent.transitionTo(IntentStatus.DELIVERED));
    }

    @Test
    void shouldRejectScheduledToDelivered() {
        Intent intent = new Intent("intent-j3");
        intent.transitionTo(IntentStatus.SCHEDULED);
        assertThrows(IllegalStateException.class, () -> intent.transitionTo(IntentStatus.DELIVERED));
    }

    @Test
    void shouldRejectScheduledToAcked() {
        Intent intent = new Intent("intent-j4");
        intent.transitionTo(IntentStatus.SCHEDULED);
        assertThrows(IllegalStateException.class, () -> intent.transitionTo(IntentStatus.ACKED));
    }

    @Test
    void shouldRejectDueToAcked() {
        Intent intent = new Intent("intent-j5");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        assertThrows(IllegalStateException.class, () -> intent.transitionTo(IntentStatus.ACKED));
    }

    @Test
    void shouldRejectDueToDeadLettered() {
        Intent intent = new Intent("intent-j6");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        assertThrows(IllegalStateException.class, () -> intent.transitionTo(IntentStatus.DEAD_LETTERED));
    }

    @Test
    void shouldRejectDispatchingToAcked() {
        Intent intent = new Intent("intent-j7");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        assertThrows(IllegalStateException.class, () -> intent.transitionTo(IntentStatus.ACKED));
    }

    @Test
    void shouldRejectDispatchingToCanceled() {
        Intent intent = new Intent("intent-j8");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        assertThrows(IllegalStateException.class, () -> intent.transitionTo(IntentStatus.CANCELED));
    }

    @Test
    void shouldRejectDefaultToInvalid() {
        Intent intent = new Intent("intent-def");
        assertThrows(IllegalStateException.class, () -> intent.transitionTo(IntentStatus.DELIVERED));
    }

    // ========== restore preserves full state ==========

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
            null,
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

    @Test
    void shouldRestoreAckedIntentAsTerminal() {
        Intent restored = Intent.restore(
            "intent-acked", IntentStatus.ACKED,
            Instant.now(), Instant.now(), Instant.now(), null,
            ExpiredAction.DISCARD, PrecisionTier.HIGH, null,
            "s", "s1", AckLevel.DURABLE,
            null, null, null, Map.of(), 1, null
        );
        assertEquals(IntentStatus.ACKED, restored.getStatus());
        assertThrows(IllegalStateException.class, () -> restored.transitionTo(IntentStatus.DUE));
    }

    // ========== helpers ==========

    private static Intent ackedIntent() {
        Intent intent = new Intent("intent-acked");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        intent.transitionTo(IntentStatus.DELIVERED);
        intent.transitionTo(IntentStatus.ACKED);
        return intent;
    }
}

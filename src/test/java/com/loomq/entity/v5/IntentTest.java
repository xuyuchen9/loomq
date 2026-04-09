package com.loomq.entity.v5;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Intent 实体测试
 *
 * 验证状态机转换、业务逻辑
 */
class IntentTest {

    @Test
    void testIntentCreation() {
        Intent intent = new Intent();

        assertNotNull(intent.getIntentId());
        assertTrue(intent.getIntentId().startsWith("intent_"));
        assertEquals(IntentStatus.CREATED, intent.getStatus());
        assertNotNull(intent.getCreatedAt());
        assertEquals(intent.getCreatedAt(), intent.getUpdatedAt());
        assertEquals(0, intent.getAttempts());
    }

    @Test
    void testIntentCreationWithId() {
        String customId = "my_custom_id";
        Intent intent = new Intent(customId);

        assertEquals(customId, intent.getIntentId());
    }

    @Test
    void testStateTransition_CreatedToScheduled() {
        Intent intent = new Intent();

        intent.transitionTo(IntentStatus.SCHEDULED);

        assertEquals(IntentStatus.SCHEDULED, intent.getStatus());
        assertTrue(intent.getUpdatedAt().isAfter(intent.getCreatedAt())
                   || intent.getUpdatedAt().equals(intent.getCreatedAt()));
    }

    @Test
    void testStateTransition_ScheduledToDue() {
        Intent intent = new Intent();
        intent.transitionTo(IntentStatus.SCHEDULED);

        intent.transitionTo(IntentStatus.DUE);

        assertEquals(IntentStatus.DUE, intent.getStatus());
    }

    @Test
    void testStateTransition_DueToDispatching() {
        Intent intent = new Intent();
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);

        intent.transitionTo(IntentStatus.DISPATCHING);

        assertEquals(IntentStatus.DISPATCHING, intent.getStatus());
    }

    @Test
    void testStateTransition_DispatchingToDelivered() {
        Intent intent = new Intent();
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);

        intent.transitionTo(IntentStatus.DELIVERED);

        assertEquals(IntentStatus.DELIVERED, intent.getStatus());
    }

    @Test
    void testStateTransition_DeliveredToAcked() {
        Intent intent = new Intent();
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        intent.transitionTo(IntentStatus.DELIVERED);

        intent.transitionTo(IntentStatus.ACKED);

        assertEquals(IntentStatus.ACKED, intent.getStatus());
    }

    @Test
    void testStateTransition_ScheduledToCanceled() {
        Intent intent = new Intent();
        intent.transitionTo(IntentStatus.SCHEDULED);

        intent.transitionTo(IntentStatus.CANCELED);

        assertEquals(IntentStatus.CANCELED, intent.getStatus());
    }

    @Test
    void testStateTransition_InvalidTransition_Throws() {
        Intent intent = new Intent();
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.CANCELED);

        // 终态不能再转换
        assertThrows(IllegalStateException.class, () ->
            intent.transitionTo(IntentStatus.DUE)
        );
    }

    @Test
    void testStateTransition_CreatedDirectlyToCanceled_Throws() {
        Intent intent = new Intent();

        // CREATED 不能直接到 CANCELED
        assertThrows(IllegalStateException.class, () ->
            intent.transitionTo(IntentStatus.CANCELED)
        );
    }

    @Test
    void testIsExecutable() {
        Intent intent = new Intent();
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.setDeadline(Instant.now().plusSeconds(300));

        // CREATED 状态不可执行
        assertFalse(intent.isExecutable());

        intent.transitionTo(IntentStatus.SCHEDULED);
        assertFalse(intent.isExecutable());

        // 设置 executeAt 为过去时间
        intent.setExecuteAt(Instant.now().minusSeconds(1));
        intent.transitionTo(IntentStatus.DUE);

        assertTrue(intent.isExecutable());
    }

    @Test
    void testIsExpired() {
        Intent intent = new Intent();
        intent.setDeadline(Instant.now().minusSeconds(1));

        assertTrue(intent.isExpired());
    }

    @Test
    void testIsNotExpired() {
        Intent intent = new Intent();
        intent.setDeadline(Instant.now().plusSeconds(60));

        assertFalse(intent.isExpired());
    }

    @Test
    void testIncrementAttempts() {
        Intent intent = new Intent();

        assertEquals(0, intent.getAttempts());

        intent.incrementAttempts();
        assertEquals(1, intent.getAttempts());

        intent.incrementAttempts();
        assertEquals(2, intent.getAttempts());
    }

    @Test
    void testSettersUpdateTimestamp() {
        Intent intent = new Intent();
        Instant initialUpdate = intent.getUpdatedAt();

        // 短暂等待确保时间变化
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        intent.setExecuteAt(Instant.now().plusSeconds(60));

        assertTrue(intent.getUpdatedAt().isAfter(initialUpdate));
    }

    @Test
    void testSetLastDeliveryId() {
        Intent intent = new Intent();
        String deliveryId = "delivery_123";

        intent.setLastDeliveryId(deliveryId);

        assertEquals(deliveryId, intent.getLastDeliveryId());
    }

    @Test
    void testIntentStatus_IsTerminal() {
        assertTrue(IntentStatus.ACKED.isTerminal());
        assertTrue(IntentStatus.CANCELED.isTerminal());
        assertTrue(IntentStatus.EXPIRED.isTerminal());
        assertTrue(IntentStatus.DEAD_LETTERED.isTerminal());

        assertFalse(IntentStatus.CREATED.isTerminal());
        assertFalse(IntentStatus.SCHEDULED.isTerminal());
        assertFalse(IntentStatus.DUE.isTerminal());
        assertFalse(IntentStatus.DISPATCHING.isTerminal());
        assertFalse(IntentStatus.DELIVERED.isTerminal());
    }

    @Test
    void testIntentStatus_IsCancellable() {
        assertTrue(IntentStatus.SCHEDULED.isCancellable());
        assertTrue(IntentStatus.DUE.isCancellable());

        assertFalse(IntentStatus.CREATED.isCancellable());
        assertFalse(IntentStatus.DISPATCHING.isCancellable());
        assertFalse(IntentStatus.DELIVERED.isCancellable());
        assertFalse(IntentStatus.ACKED.isCancellable());
    }

    @Test
    void testIntentStatus_IsModifiable() {
        assertTrue(IntentStatus.CREATED.isModifiable());
        assertTrue(IntentStatus.SCHEDULED.isModifiable());
        assertTrue(IntentStatus.DUE.isModifiable());
        assertTrue(IntentStatus.DISPATCHING.isModifiable());

        assertFalse(IntentStatus.DELIVERED.isModifiable());
        assertFalse(IntentStatus.ACKED.isModifiable());
        assertFalse(IntentStatus.CANCELED.isModifiable());
    }

    @Test
    void testIntentStatus_NeedsDispatch() {
        assertTrue(IntentStatus.DUE.needsDispatch());
        assertTrue(IntentStatus.DELIVERED.needsDispatch());

        assertFalse(IntentStatus.SCHEDULED.needsDispatch());
        assertFalse(IntentStatus.DISPATCHING.needsDispatch());
    }

    @Test
    void testIntentStatus_AllowsRedelivery() {
        assertTrue(IntentStatus.DELIVERED.allowsRedelivery());

        assertFalse(IntentStatus.DUE.allowsRedelivery());
        assertFalse(IntentStatus.ACKED.allowsRedelivery());
    }

    @Test
    void testExpiredAction_Values() {
        assertEquals(2, ExpiredAction.values().length);
        assertNotNull(ExpiredAction.DISCARD);
        assertNotNull(ExpiredAction.DEAD_LETTER);
    }

    @Test
    void testCallbackBuilder() {
        Callback callback = new Callback();
        callback.setUrl("http://example.com/webhook");
        callback.setMethod("POST");

        assertEquals("http://example.com/webhook", callback.getUrl());
        assertEquals("POST", callback.getMethod());
    }

    @Test
    void testRedeliveryPolicy() {
        RedeliveryPolicy policy = new RedeliveryPolicy();
        policy.setMaxAttempts(5);
        policy.setInitialDelayMs(1000);
        policy.setMaxDelayMs(60000);
        policy.setMultiplier(2.0);

        assertEquals(5, policy.getMaxAttempts());
        assertEquals(1000, policy.getInitialDelayMs());
        assertEquals(60000, policy.getMaxDelayMs());
        assertEquals(2.0, policy.getMultiplier());
    }

    @Test
    void testRedeliveryPolicy_CalculateDelay() {
        RedeliveryPolicy policy = new RedeliveryPolicy();
        policy.setInitialDelayMs(1000);
        policy.setMaxDelayMs(30000);
        policy.setMultiplier(2.0);
        policy.setJitter(false); // 关闭抖动以便测试

        // 第1次重试: 1000ms
        assertEquals(1000, policy.calculateDelay(1));

        // 第2次重试: 2000ms
        assertEquals(2000, policy.calculateDelay(2));

        // 第3次重试: 4000ms
        assertEquals(4000, policy.calculateDelay(3));

        // 达到上限: 30000ms
        assertEquals(30000, policy.calculateDelay(10));
    }

    @Test
    void testRedeliveryPolicy_FixedBackoff() {
        RedeliveryPolicy policy = new RedeliveryPolicy();
        policy.setBackoff("fixed");
        policy.setInitialDelayMs(5000);
        policy.setJitter(false);

        // 固定退避每次都相同
        assertEquals(5000, policy.calculateDelay(1));
        assertEquals(5000, policy.calculateDelay(2));
        assertEquals(5000, policy.calculateDelay(5));
    }

    @Test
    void testRedeliveryPolicy_WithJitter() {
        RedeliveryPolicy policy = new RedeliveryPolicy();
        policy.setInitialDelayMs(1000);
        policy.setJitter(true);

        // 有抖动时，值应该在 80% ~ 120% 之间
        long delay = policy.calculateDelay(1);
        assertTrue(delay >= 800 && delay <= 1200,
            "Jittered delay should be between 800ms and 1200ms, but was: " + delay);
    }
}

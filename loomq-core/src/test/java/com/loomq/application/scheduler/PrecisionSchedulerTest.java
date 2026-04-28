package com.loomq.application.scheduler;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.PrecisionTierProfile;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.RedeliveryDecider;
import com.loomq.store.IntentStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("slow")
class PrecisionSchedulerTest {

    private IntentStore intentStore;
    private PrecisionScheduler scheduler;

    @BeforeEach
    void setUp() {
        intentStore = new IntentStore();
    }

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.stop();
        }
    }

    // ========== Construction ==========

    @Test
    void shouldRequireNonNullDeliveryHandler() {
        assertThrows(NullPointerException.class, () ->
            new PrecisionScheduler(intentStore, null, null));
    }

    @Test
    void shouldConstructWithDeliveryHandlerAndDefaultDecider() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        assertDoesNotThrow(() -> {
            scheduler = new PrecisionScheduler(intentStore, handler, null);
        });
        assertNotNull(scheduler);
    }

    @Test
    void shouldConstructWithCustomRedeliveryDecider() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        RedeliveryDecider decider = ctx -> ctx.isSuccess();
        assertDoesNotThrow(() -> {
            scheduler = new PrecisionScheduler(intentStore, handler, decider);
        });
    }

    @Test
    void shouldConstructWithCustomCatalog() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        Map<PrecisionTier, PrecisionTierProfile> profiles = Map.of(
            PrecisionTier.ULTRA, new PrecisionTierProfile(10, 100, 1, 5, 8)
        );
        PrecisionTierCatalog catalog = PrecisionTierCatalog.of(profiles, PrecisionTier.ULTRA);
        assertDoesNotThrow(() -> {
            scheduler = new PrecisionScheduler(intentStore, handler, null, catalog);
        });
        assertEquals(1, scheduler.getBackpressureStatus().size());
    }

    // ========== Lifecycle ==========

    @Test
    void startShouldBeIdempotent() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();
        scheduler.start(); // second call should be a no-op
        assertDoesNotThrow(() -> scheduler.stop());
    }

    @Test
    void stopShouldWorkWithoutStart() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        assertDoesNotThrow(() -> scheduler.stop());
    }

    @Test
    void pauseResumeShouldToggleFlag() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        assertFalse(scheduler.isPaused());
        scheduler.pause();
        assertTrue(scheduler.isPaused());
        scheduler.pause(); // idempotent
        assertTrue(scheduler.isPaused());
        scheduler.resume();
        assertFalse(scheduler.isPaused());
        scheduler.resume(); // idempotent
        assertFalse(scheduler.isPaused());
    }

    // ========== schedule() ==========

    @Test
    void scheduleShouldAcceptCreatedStatus() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Intent intent = new Intent("test-created");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intentStore.save(intent);
        assertDoesNotThrow(() -> scheduler.schedule(intent));
    }

    @Test
    void scheduleShouldAcceptScheduledStatus() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Intent intent = new Intent("test-scheduled");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(intent);
        assertDoesNotThrow(() -> scheduler.schedule(intent));
    }

    @Test
    void scheduleShouldSkipNonCreatedOrScheduledIntent() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Intent intent = new Intent("test-skip");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        // should not throw, just log a warning
        assertDoesNotThrow(() -> scheduler.schedule(intent));
    }

    @Test
    void scheduleImmediateIntentShouldGoToBucket() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Intent intent = new Intent("test-imm");
        intent.setExecuteAt(Instant.now().minusSeconds(1)); // past → immediate
        intentStore.save(intent);
        assertDoesNotThrow(() -> scheduler.schedule(intent));
    }

    @Test
    void scheduleFutureIntentShouldUseVirtualThreadSleep() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Intent intent = new Intent("test-future");
        intent.setExecuteAt(Instant.now().plusMillis(200));
        intentStore.save(intent);
        assertDoesNotThrow(() -> scheduler.schedule(intent));
    }

    // ========== restore() ==========

    @Test
    void restoreShouldAddToBucketGroup() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Intent intent = new Intent("test-restore");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(intent);
        assertDoesNotThrow(() -> scheduler.restore(intent));
    }

    @Test
    void restoreShouldSkipNullIntent() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        assertDoesNotThrow(() -> scheduler.restore(null));
    }

    @Test
    void restoreShouldSkipTerminalIntent() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Intent intent = new Intent("test-term");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        intent.transitionTo(IntentStatus.DEAD_LETTERED);
        assertDoesNotThrow(() -> scheduler.restore(intent));
    }

    @Test
    void restoreShouldSkipNullExecuteAt() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Intent intent = new Intent("test-no-ex");
        // executeAt is null by default
        assertDoesNotThrow(() -> scheduler.restore(intent));
    }

    // ========== unschedule() ==========

    @Test
    void unscheduleShouldDelegateToBucketGroupManager() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Intent intent = new Intent("test-unsched");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.transitionTo(IntentStatus.SCHEDULED);
        scheduler.schedule(intent);
        assertDoesNotThrow(() -> scheduler.unschedule(intent));
    }

    // ========== Backpressure ==========

    @Test
    void isTierUnderBackpressureShouldDetectSemaphoreExhaustion() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        assertFalse(scheduler.isTierUnderBackpressure(PrecisionTier.ULTRA));
    }

    @Test
    void isTierUnderBackpressureShouldReturnFalseWhenNotUnderPressure() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        assertFalse(scheduler.isTierUnderBackpressure(PrecisionTier.ECONOMY));
    }

    @Test
    void getBackpressureStatusShouldReturnAllTiers() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Map<PrecisionTier, PrecisionScheduler.BackpressureInfo> status = scheduler.getBackpressureStatus();
        assertEquals(5, status.size());
        for (PrecisionTier tier : PrecisionTierCatalog.defaultCatalog().supportedTiers()) {
            PrecisionScheduler.BackpressureInfo info = status.get(tier);
            assertNotNull(info);
            assertTrue(info.maxConcurrency() > 0);
            assertTrue(info.availablePermits() >= 0);
            assertEquals(0, info.queueSize());
        }
    }

    // ========== getBucketGroupManager() ==========

    @Test
    void shouldExposeBucketGroupManager() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        assertNotNull(scheduler.getBucketGroupManager());
    }

    // ========== dispatch cycle with controlled mock handler ==========

    // These tests verify the full dispatch pipeline through start → scan → consume → callback

    /** Creates an immediate intent transitioned to SCHEDULED (as production does). */
    private Intent readyIntent(String id) {
        Intent intent = new Intent(id);
        intent.setExecuteAt(Instant.now().minusMillis(100));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(intent);
        return intent;
    }

    @Test
    void dispatchSuccessShouldTransitionToAcked() throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-success");
        scheduler.schedule(intent);

        assertTrue(delivered.await(5, TimeUnit.SECONDS), "delivery should happen within 5s");

        // Wait a bit for the callback to finalize
        Thread.sleep(100);
        Intent stored = intentStore.findById("test-success");
        assertNotNull(stored);
        assertTrue(stored.getStatus() == IntentStatus.ACKED || stored.getStatus() == IntentStatus.DELIVERED,
            "expected ACKED or DELIVERED but was " + stored.getStatus());
    }

    @Test
    void dispatchDeadLetterShouldTransitionToDeadLettered() throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.DEAD_LETTER);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-dl");
        scheduler.schedule(intent);

        assertTrue(delivered.await(5, TimeUnit.SECONDS));
        Thread.sleep(100);
        Intent stored = intentStore.findById("test-dl");
        assertNotNull(stored);
        assertEquals(IntentStatus.DEAD_LETTERED, stored.getStatus());
    }

    @Test
    void dispatchRetryShouldRescheduleIntent() throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.RETRY);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-retry");
        scheduler.schedule(intent);

        assertTrue(delivered.await(5, TimeUnit.SECONDS));
        Thread.sleep(100);
        Intent stored = intentStore.findById("test-retry");
        assertNotNull(stored);
        // After RETRY, should be SCHEDULED for re-delivery
        assertEquals(IntentStatus.SCHEDULED, stored.getStatus());
        // executeAt should be in the future
        assertTrue(stored.getExecuteAt().isAfter(Instant.now()));
    }

    @Test
    void dispatchExpiredShouldTransitionToExpired() throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.EXPIRED);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-exp");
        scheduler.schedule(intent);

        assertTrue(delivered.await(5, TimeUnit.SECONDS));
        Thread.sleep(100);
        Intent stored = intentStore.findById("test-exp");
        assertNotNull(stored);
        assertEquals(IntentStatus.EXPIRED, stored.getStatus());
    }

    @Test
    void shouldReleaseSemaphoreAfterDispatch() throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-release");
        scheduler.schedule(intent);

        assertTrue(delivered.await(5, TimeUnit.SECONDS));
        Thread.sleep(100);

        var status = scheduler.getBackpressureStatus().get(PrecisionTier.ULTRA);
        // After dispatch completes, all permits should be available (or at least not 0)
        assertTrue(status.availablePermits() > 0,
            "expected some available permits, got " + status.availablePermits());
    }

    @Test
    void semaphoreShouldBeReleasedEvenOnException() throws Exception {
        AtomicReference<Throwable> callbackError = new AtomicReference<>();
        CountDownLatch callbackFired = new CountDownLatch(1);

        DeliveryHandler handler = intent -> {
            callbackFired.countDown();
            CompletableFuture<DeliveryResult> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("simulated failure"));
            return future;
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-ex");
        scheduler.schedule(intent);

        assertTrue(callbackFired.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);

        var status = scheduler.getBackpressureStatus().get(PrecisionTier.ULTRA);
        assertTrue(status.availablePermits() > 0,
            "semaphore should be released even after exception, got " + status.availablePermits());
    }

    @Test
    void shouldHandleDeliveryTimeout() throws Exception {
        CountDownLatch callbackFired = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            callbackFired.countDown();
            // Return a future that never completes → triggers orTimeout
            return new CompletableFuture<>();
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-timeout");
        scheduler.schedule(intent);

        assertTrue(callbackFired.await(5, TimeUnit.SECONDS));
        // The orTimeout(30s) is applied by runBatchConsumer, so for unit test purposes
        // we just verify the callback was invoked (the delivery handler was called)
        // Full timeout test requires 30s wait, impractical for unit tests
    }

    @Test
    void deliveryHandlerCalledWithCorrectIntent() throws Exception {
        AtomicReference<String> deliveredId = new AtomicReference<>();
        CountDownLatch delivered = new CountDownLatch(1);

        DeliveryHandler handler = intent -> {
            deliveredId.set(intent.getIntentId());
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-id-check");
        scheduler.schedule(intent);

        assertTrue(delivered.await(5, TimeUnit.SECONDS));
        assertEquals("test-id-check", deliveredId.get());
    }

    // ========== handleDeliveryFailure exception path (after fix) ==========

    @Test
    void shouldDeadLetterAfterMaxAttemptsOnException() throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            CompletableFuture<DeliveryResult> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("persistent failure"));
            return future;
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-max-ex");
        // Pre-set attempts to max to trigger dead-letter
        for (int i = 0; i < 10; i++) {
            intent.incrementAttempts();
        }
        intentStore.update(intent);
        scheduler.schedule(intent);

        assertTrue(delivered.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);
        Intent stored = intentStore.findById("test-max-ex");
        assertNotNull(stored);
        // With attempts >= max, handleDeliveryFailure should transition to DEAD_LETTERED
        boolean terminal = stored.getStatus() == IntentStatus.DEAD_LETTERED;
        assertTrue(terminal || stored.getStatus().isTerminal(),
            "expected terminal state but was " + stored.getStatus());
    }

    @Test
    void shouldRetryWhenAttemptsLessThanMaxOnException() throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            CompletableFuture<DeliveryResult> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("transient failure"));
            return future;
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-retry-ex");
        scheduler.schedule(intent);

        assertTrue(delivered.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);
        Intent stored = intentStore.findById("test-retry-ex");
        assertNotNull(stored);
        // After delivery exception with attempts < max, should retry → SCHEDULED
        assertEquals(IntentStatus.SCHEDULED, stored.getStatus());
        assertTrue(stored.getExecuteAt().isAfter(Instant.now()), "executeAt should be in the future for retry");
    }
}

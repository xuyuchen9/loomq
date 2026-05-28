package com.loomq.grpc;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.LoomqEngine;
import com.loomq.grpc.converter.ProtoConverter;
import com.loomq.grpc.gen.CancelIntentRequest;
import com.loomq.grpc.gen.CreateIntentRequest;
import com.loomq.grpc.gen.FireNowRequest;
import com.loomq.grpc.gen.GetIntentRequest;
import com.loomq.grpc.gen.IntentActionResponse;
import com.loomq.grpc.gen.IntentEvent;
import com.loomq.grpc.gen.ListIntentsRequest;
import com.loomq.grpc.gen.PatchIntentRequest;
import com.loomq.grpc.gen.ReviveIntentRequest;
import com.loomq.grpc.gen.SloRequestMessage;
import com.loomq.grpc.gen.WatchIntentRequest;
import com.loomq.spi.DeliveryHandler;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link LoomqGrpcService}.
 *
 * <p>Tests each RPC method by invoking it directly with a fake
 * {@link StreamObserver} that captures the response or error.
 */
class LoomqGrpcServiceTest {

    private LoomqEngine engine;
    private LoomqGrpcService service;
    private GlobalIntentObserver observer;
    private java.nio.file.Path walDir;

    @BeforeEach
    void setUp() throws Exception {
        walDir = Files.createTempDirectory("loomq-grpc-test");
        observer = new GlobalIntentObserver();
        DeliveryHandler noopHandler = intent -> java.util.concurrent.CompletableFuture.completedFuture(
            DeliveryHandler.DeliveryResult.SUCCESS);
        engine = LoomqEngine.builder()
            .nodeId("grpc-test")
            .walDir(walDir)
            .deliveryHandler(noopHandler)
            .build();
        engine.registerObserver(observer);
        engine.start();
        service = new LoomqGrpcService(engine, null, null, observer);
    }

    @AfterEach
    void tearDown() {
        try { engine.close(); } catch (Exception ignored) {}
        if (walDir != null) {
            try (var paths = Files.walk(walDir)) {
                paths.sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> { try { Files.delete(p); } catch (Exception ignored) {} });
            } catch (Exception ignored) {}
        }
    }

    // ── createIntent ──

    @Test
    void shouldCreateAndGetIntent() throws Exception {
        var latch = new CountDownLatch(1);
        var response = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(latch);

        var createReq = CreateIntentRequest.newBuilder()
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("test-shard")
            .setPrecisionTier("STANDARD")
            .build();

        service.createIntent(createReq, response);
        assertTrue(latch.await(5, TimeUnit.SECONDS), "createIntent should complete");
        assertNull(response.error, "Should not error");
        assertNotNull(response.value, "Should have response");
        assertNotNull(response.value.getIntentId());
        assertEquals("SCHEDULED", response.value.getStatus());
    }

    @Test
    void shouldRejectCreateWithoutExecuteAt() throws Exception {
        var latch = new CountDownLatch(1);
        var response = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(latch);

        var createReq = CreateIntentRequest.newBuilder()
            .setShardKey("test-shard")
            .build();

        service.createIntent(createReq, response);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(response.error);
        assertTrue(response.error instanceof StatusRuntimeException);
        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT,
            ((StatusRuntimeException) response.error).getStatus().getCode());
    }

    // ── getIntent ──

    @Test
    void shouldReturn404ForUnknownIntent() throws Exception {
        var latch = new CountDownLatch(1);
        var response = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(latch);

        service.getIntent(GetIntentRequest.newBuilder().setIntentId("nonexistent").build(), response);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(response.error);
        assertEquals(io.grpc.Status.Code.NOT_FOUND,
            ((StatusRuntimeException) response.error).getStatus().getCode());
    }

    // ── listIntents ──

    @Test
    void shouldListIntentsByStatus() throws Exception {
        // Create an intent first
        var createLatch = new CountDownLatch(1);
        var createResp = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(createLatch);
        service.createIntent(CreateIntentRequest.newBuilder()
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("test")
            .build(), createResp);
        assertTrue(createLatch.await(5, TimeUnit.SECONDS));

        // List intents
        var listLatch = new CountDownLatch(1);
        var listResp = new TestStreamObserver<com.loomq.grpc.gen.ListIntentsResponse>(listLatch);
        service.listIntents(ListIntentsRequest.newBuilder()
            .setStatus("SCHEDULED")
            .setLimit(10)
            .build(), listResp);
        assertTrue(listLatch.await(5, TimeUnit.SECONDS));
        assertNull(listResp.error);
        assertTrue(listResp.value.getIntentsCount() >= 1);
    }

    @Test
    void shouldRejectListWithoutStatus() throws Exception {
        var latch = new CountDownLatch(1);
        var resp = new TestStreamObserver<com.loomq.grpc.gen.ListIntentsResponse>(latch);
        service.listIntents(ListIntentsRequest.newBuilder().build(), resp);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(resp.error);
        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT,
            ((StatusRuntimeException) resp.error).getStatus().getCode());
    }

    // ── cancelIntent ──

    @Test
    void shouldCancelIntent() throws Exception {
        // Create
        var createLatch = new CountDownLatch(1);
        var createResp = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(createLatch);
        service.createIntent(CreateIntentRequest.newBuilder()
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("test")
            .build(), createResp);
        assertTrue(createLatch.await(5, TimeUnit.SECONDS));
        String intentId = createResp.value.getIntentId();

        // Cancel
        var cancelLatch = new CountDownLatch(1);
        var cancelResp = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(cancelLatch);
        service.cancelIntent(CancelIntentRequest.newBuilder()
            .setIntentId(intentId)
            .build(), cancelResp);
        assertTrue(cancelLatch.await(5, TimeUnit.SECONDS));
        assertNull(cancelResp.error);
        assertEquals("CANCELED", cancelResp.value.getStatus());
    }

    // ── healthCheck ──

    @Test
    void shouldReturnHealthStatus() throws Exception {
        var latch = new CountDownLatch(1);
        var resp = new TestStreamObserver<com.loomq.grpc.gen.HealthCheckResponse>(latch);
        service.healthCheck(com.loomq.grpc.gen.HealthCheckRequest.newBuilder().build(), resp);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(resp.value);
        assertNotNull(resp.value.getStatus());
    }

    // ── fireNow ──

    @Test
    void shouldFireNowIntent() throws Exception {
        // Create an intent first
        var createLatch = new CountDownLatch(1);
        var createResp = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(createLatch);
        service.createIntent(CreateIntentRequest.newBuilder()
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("test")
            .build(), createResp);
        assertTrue(createLatch.await(5, TimeUnit.SECONDS));
        String intentId = createResp.value.getIntentId();

        // Fire now
        var fireLatch = new CountDownLatch(1);
        var fireResp = new TestStreamObserver<IntentActionResponse>(fireLatch);
        service.fireNow(FireNowRequest.newBuilder()
            .setIntentId(intentId)
            .build(), fireResp);
        assertTrue(fireLatch.await(5, TimeUnit.SECONDS));
        assertNull(fireResp.error);
        assertEquals(intentId, fireResp.value.getIntentId());
        assertEquals("DISPATCHING", fireResp.value.getStatus());
    }

    @Test
    void shouldRejectFireNowNonExistentIntent() throws Exception {
        var latch = new CountDownLatch(1);
        var resp = new TestStreamObserver<IntentActionResponse>(latch);
        service.fireNow(FireNowRequest.newBuilder()
            .setIntentId("nonexistent")
            .build(), resp);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(resp.error);
        assertEquals(io.grpc.Status.Code.NOT_FOUND,
            ((StatusRuntimeException) resp.error).getStatus().getCode());
    }

    // ── reviveIntent ──

    @Test
    void shouldRejectReviveNonExistentIntent() throws Exception {
        var latch = new CountDownLatch(1);
        var resp = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(latch);
        service.reviveIntent(ReviveIntentRequest.newBuilder()
            .setIntentId("nonexistent")
            .build(), resp);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(resp.error);
        assertEquals(io.grpc.Status.Code.NOT_FOUND,
            ((StatusRuntimeException) resp.error).getStatus().getCode());
    }

    @Test
    void shouldRejectReviveNonDeadLetteredIntent() throws Exception {
        // Create a SCHEDULED intent
        var createLatch = new CountDownLatch(1);
        var createResp = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(createLatch);
        service.createIntent(CreateIntentRequest.newBuilder()
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("test")
            .build(), createResp);
        assertTrue(createLatch.await(5, TimeUnit.SECONDS));
        String intentId = createResp.value.getIntentId();

        // Try to revive a non-DEAD_LETTERED intent
        var reviveLatch = new CountDownLatch(1);
        var reviveResp = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(reviveLatch);
        service.reviveIntent(ReviveIntentRequest.newBuilder()
            .setIntentId(intentId)
            .build(), reviveResp);
        assertTrue(reviveLatch.await(5, TimeUnit.SECONDS));
        assertNotNull(reviveResp.error);
        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT,
            ((StatusRuntimeException) reviveResp.error).getStatus().getCode());
    }

    // ── SLO ──

    @Test
    void shouldUseSloForTierRecommendation() throws Exception {
        var latch = new CountDownLatch(1);
        var resp = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(latch);

        // SLO: 1000ms maxTardiness, AT_LEAST_ONCE → 2× safety → 500ms window → STANDARD (500ms) is cheapest match
        var slo = SloRequestMessage.newBuilder()
            .setMaxTardinessMs(1000)
            .setReliability("AT_LEAST_ONCE")
            .build();

        service.createIntent(CreateIntentRequest.newBuilder()
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("test")
            .setSlo(slo)
            .build(), resp);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNull(resp.error);
        assertNotNull(resp.value);
        // STANDARD(500ms) satisfies 500ms safety window as the cheapest tier
        assertEquals("STANDARD", resp.value.getPrecisionTier());
    }

    // ── patchIntent ──

    @Test
    void shouldPatchIntent() throws Exception {
        // Create
        var createLatch = new CountDownLatch(1);
        var createResp = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(createLatch);
        service.createIntent(CreateIntentRequest.newBuilder()
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("test")
            .setPrecisionTier("STANDARD")
            .build(), createResp);
        assertTrue(createLatch.await(5, TimeUnit.SECONDS));
        assertNotNull(createResp.value, "createIntent should succeed: " + createResp.error);
        String intentId = createResp.value.getIntentId();

        // Patch
        var patchLatch = new CountDownLatch(1);
        var patchResp = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(patchLatch);
        Instant newExecuteAt = Instant.now().plusSeconds(300);
        service.patchIntent(PatchIntentRequest.newBuilder()
            .setIntentId(intentId)
            .setExecuteAt(ProtoConverter.toProto(newExecuteAt))
            .setExpiredAction("DEAD_LETTER")
            .putTags("env", "test")
            .build(), patchResp);
        assertTrue(patchLatch.await(5, TimeUnit.SECONDS));
        assertNull(patchResp.error);
        assertEquals(intentId, patchResp.value.getIntentId());
        assertEquals("DEAD_LETTER", patchResp.value.getExpiredAction());
        assertEquals("test", patchResp.value.getTagsMap().get("env"));
    }

    @Test
    void shouldRejectPatchNonExistentIntent() throws Exception {
        var latch = new CountDownLatch(1);
        var resp = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(latch);
        service.patchIntent(PatchIntentRequest.newBuilder()
            .setIntentId("nonexistent")
            .setExpiredAction("DEAD_LETTER")
            .build(), resp);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(resp.error);
        assertEquals(io.grpc.Status.Code.NOT_FOUND,
            ((StatusRuntimeException) resp.error).getStatus().getCode());
    }

    @Test
    void shouldRejectPatchOnNonModifiableIntent() throws Exception {
        // Create + cancel to get a CANCELED intent
        var createLatch = new CountDownLatch(1);
        var createResp = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(createLatch);
        service.createIntent(CreateIntentRequest.newBuilder()
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("test")
            .build(), createResp);
        assertTrue(createLatch.await(5, TimeUnit.SECONDS));
        assertNotNull(createResp.value, "createIntent should succeed: " + createResp.error);
        String intentId = createResp.value.getIntentId();

        var cancelLatch = new CountDownLatch(1);
        var cancelResp = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(cancelLatch);
        service.cancelIntent(CancelIntentRequest.newBuilder().setIntentId(intentId).build(), cancelResp);
        assertTrue(cancelLatch.await(5, TimeUnit.SECONDS));

        // Try to patch the canceled intent
        var patchLatch = new CountDownLatch(1);
        var patchResp = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(patchLatch);
        service.patchIntent(PatchIntentRequest.newBuilder()
            .setIntentId(intentId)
            .setExpiredAction("DEAD_LETTER")
            .build(), patchResp);
        assertTrue(patchLatch.await(5, TimeUnit.SECONDS));
        assertNotNull(patchResp.error);
        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT,
            ((StatusRuntimeException) patchResp.error).getStatus().getCode());
    }

    // ── cancelIntent error paths ──

    @Test
    void shouldRejectCancelOnNonCancellableIntent() throws Exception {
        // Create + cancel to get a CANCELED intent, then try to cancel again
        var createLatch = new CountDownLatch(1);
        var createResp = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(createLatch);
        service.createIntent(CreateIntentRequest.newBuilder()
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("test")
            .build(), createResp);
        assertTrue(createLatch.await(5, TimeUnit.SECONDS));
        assertNotNull(createResp.value, "createIntent should succeed: " + createResp.error);
        String intentId = createResp.value.getIntentId();

        // First cancel — should succeed
        var cancelLatch1 = new CountDownLatch(1);
        var cancelResp1 = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(cancelLatch1);
        service.cancelIntent(CancelIntentRequest.newBuilder().setIntentId(intentId).build(), cancelResp1);
        assertTrue(cancelLatch1.await(5, TimeUnit.SECONDS));
        assertNull(cancelResp1.error);

        // Second cancel — should fail (CANCELED is not cancellable)
        var cancelLatch2 = new CountDownLatch(1);
        var cancelResp2 = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(cancelLatch2);
        service.cancelIntent(CancelIntentRequest.newBuilder().setIntentId(intentId).build(), cancelResp2);
        assertTrue(cancelLatch2.await(5, TimeUnit.SECONDS));
        assertNotNull(cancelResp2.error);
        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT,
            ((StatusRuntimeException) cancelResp2.error).getStatus().getCode());
    }

    // ── default tier ──

    @Test
    void shouldApplyDefaultTierWhenNoneSpecified() throws Exception {
        var latch = new CountDownLatch(1);
        var resp = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(latch);
        service.createIntent(CreateIntentRequest.newBuilder()
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("test")
            .build(), resp);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNull(resp.error);
        assertNotNull(resp.value.getPrecisionTier());
        assertFalse(resp.value.getPrecisionTier().isEmpty());
    }

    // ── watchIntent ──

    @Test
    void shouldWatchIntentAndReceiveEvents() throws Exception {
        var eventLatch = new CountDownLatch(1);
        var watcher = new StreamingTestObserver(eventLatch);

        // Register wildcard watcher
        service.watchIntent(WatchIntentRequest.newBuilder().build(), watcher);

        // Create an intent — should trigger onScheduled → CREATED→SCHEDULED event
        var createLatch = new CountDownLatch(1);
        var createResp = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(createLatch);
        service.createIntent(CreateIntentRequest.newBuilder()
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("test")
            .build(), createResp);
        assertTrue(createLatch.await(5, TimeUnit.SECONDS));
        assertNull(createResp.error, "createIntent should not error: " + createResp.error);
        assertNotNull(createResp.value, "createIntent should return a value");

        // Wait for the watcher to receive the event (async dispatch)
        assertTrue(eventLatch.await(5, TimeUnit.SECONDS), "Watcher should receive event");
        assertFalse(watcher.events.isEmpty());
        IntentEvent event = watcher.events.get(0);
        assertEquals("CREATED", event.getFromStatus());
        assertEquals("SCHEDULED", event.getToStatus());
        assertEquals(createResp.value.getIntentId(), event.getIntentId());
    }

    @Test
    void shouldFilterWatchEventsByStatus() throws Exception {
        // Register watcher that only cares about DELIVERED events
        var eventLatch = new CountDownLatch(1);
        var watcher = new StreamingTestObserver(eventLatch);
        service.watchIntent(WatchIntentRequest.newBuilder()
            .addStatusFilter("DELIVERED")
            .build(), watcher);

        // Create an intent — triggers SCHEDULED event, which should be filtered out
        var createLatch = new CountDownLatch(1);
        var createResp = new TestStreamObserver<com.loomq.grpc.gen.IntentMessage>(createLatch);
        service.createIntent(CreateIntentRequest.newBuilder()
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("test")
            .build(), createResp);
        assertTrue(createLatch.await(5, TimeUnit.SECONDS));
        assertNull(createResp.error, "createIntent should not error: " + createResp.error);

        // The watcher should NOT receive the SCHEDULED event (filtered)
        // Wait a short time to confirm no event arrives
        assertFalse(eventLatch.await(200, TimeUnit.MILLISECONDS),
            "Watcher with DELIVERED filter should not receive SCHEDULED event");
        assertTrue(watcher.events.isEmpty());
    }

    // ── Helper: Test StreamObserver ──

    static class TestStreamObserver<T> implements StreamObserver<T> {
        final CountDownLatch latch;
        T value;
        Throwable error;

        TestStreamObserver(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onNext(T value) {
            this.value = value;
            latch.countDown();
        }

        @Override
        public void onError(Throwable t) {
            this.error = t;
            latch.countDown();
        }

        @Override
        public void onCompleted() {
            latch.countDown();
        }
    }

    /** ServerCallStreamObserver for watchIntent tests, captures multiple events. */
    static class StreamingTestObserver extends ServerCallStreamObserver<IntentEvent> {
        final List<IntentEvent> events = new CopyOnWriteArrayList<>();
        final CountDownLatch latch;

        StreamingTestObserver(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setOnReadyHandler(Runnable onReadyHandler) {}

        @Override
        public void disableAutoInboundFlowControl() {}

        @Override
        public void request(int count) {}

        @Override
        public void setMessageCompression(boolean enabled) {}

        @Override
        public void setCompression(String compression) {}

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void setOnCancelHandler(Runnable onCancelHandler) {}

        @Override
        public void setOnCloseHandler(Runnable onCloseHandler) {}

        @Override
        public void onNext(IntentEvent value) {
            events.add(value);
            latch.countDown();
        }

        @Override
        public void onError(Throwable t) {
            latch.countDown();
        }

        @Override
        public void onCompleted() {
            latch.countDown();
        }
    }
}

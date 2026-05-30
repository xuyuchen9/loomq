package com.loomq.channel.grpc.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.LoomqEngine;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.grpc.gen.CallbackMessage;
import com.loomq.grpc.gen.CancelIntentRequest;
import com.loomq.grpc.gen.CreateIntentRequest;
import com.loomq.grpc.gen.FireNowRequest;
import com.loomq.grpc.gen.GetIntentRequest;
import com.loomq.grpc.gen.HealthCheckRequest;
import com.loomq.grpc.gen.HealthCheckResponse;
import com.loomq.grpc.gen.IntentActionResponse;
import com.loomq.grpc.gen.IntentEvent;
import com.loomq.grpc.gen.IntentMessage;
import com.loomq.grpc.gen.ListIntentsRequest;
import com.loomq.grpc.gen.ListIntentsResponse;
import com.loomq.grpc.gen.PatchIntentRequest;
import com.loomq.grpc.gen.ReviveIntentRequest;
import com.loomq.grpc.gen.SloRequestMessage;
import com.loomq.grpc.gen.WatchIntentRequest;
import com.loomq.spi.DeliveryHandler;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LoomqGrpcServiceTest {

    @TempDir
    Path tempDir;

    private LoomqEngine engine;
    private GlobalIntentObserver globalObserver;
    private LoomqGrpcService service;

    @BeforeEach
    void setUp() {
        engine = LoomqEngine.builder()
            .nodeId("test-node")
            .walDir(tempDir.resolve("grpc-test"))
            .deliveryHandler((DeliveryHandler) intent ->
                java.util.concurrent.CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.DEAD_LETTER))
            .build();

        engine.start();

        globalObserver = new GlobalIntentObserver();
        engine.registerObserver(globalObserver);
        service = new LoomqGrpcService(engine, null, null, globalObserver);
    }

    @AfterEach
    void tearDown() {
        if (globalObserver != null) globalObserver.close();
        if (engine != null) {
            try { engine.close(); } catch (Exception ignored) {}
        }
    }

    // ── createIntent ──

    @Test
    @DisplayName("createIntent should create and return intent with SCHEDULED status")
    void shouldCreateAndGetIntent() throws Exception {
        TestStreamObserver<IntentMessage> observer = new TestStreamObserver<>();
        service.createIntent(buildRequest("create-1"), observer);

        IntentMessage result = observer.await();
        assertEquals("create-1", result.getIntentId());
        assertEquals("SCHEDULED", result.getStatus());
    }

    @Test
    @DisplayName("createIntent should reject request without executeAt")
    void shouldRejectCreateWithoutExecuteAt() throws Exception {
        TestStreamObserver<IntentMessage> observer = new TestStreamObserver<>();
        service.createIntent(CreateIntentRequest.newBuilder()
            .setIntentId("no-time")
            .setShardKey("s")
            .build(), observer);

        Throwable error = observer.awaitError();
        assertTrue(error instanceof StatusRuntimeException);
        assertEquals(Status.INVALID_ARGUMENT.getCode(), ((StatusRuntimeException) error).getStatus().getCode());
    }

    @Test
    @DisplayName("createIntent should apply SLO tier recommendation")
    void shouldUseSloForTierRecommendation() throws Exception {
        TestStreamObserver<IntentMessage> observer = new TestStreamObserver<>();
        service.createIntent(CreateIntentRequest.newBuilder()
            .setIntentId("slo-1")
            .setExecuteAt(com.loomq.channel.grpc.converter.ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(com.loomq.channel.grpc.converter.ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("s")
            .setSlo(SloRequestMessage.newBuilder()
                .setMaxTardinessMs(1000)
                .setReliability("AT_LEAST_ONCE")
                .build())
            .build(), observer);

        IntentMessage result = observer.await();
        assertEquals("slo-1", result.getIntentId());
        assertNotNull(result.getPrecisionTier());
    }

    @Test
    @DisplayName("createIntent should apply default tier when none specified")
    void shouldApplyDefaultTierWhenNoneSpecified() throws Exception {
        TestStreamObserver<IntentMessage> observer = new TestStreamObserver<>();
        service.createIntent(CreateIntentRequest.newBuilder()
            .setIntentId("default-tier")
            .setExecuteAt(com.loomq.channel.grpc.converter.ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(com.loomq.channel.grpc.converter.ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("s")
            .build(), observer);

        IntentMessage result = observer.await();
        assertNotNull(result.getPrecisionTier());
        assertTrue(result.getPrecisionTier().length() > 0);
    }

    // ── getIntent ──

    @Test
    @DisplayName("getIntent should return 404 for unknown intent")
    void shouldReturn404ForUnknownIntent() throws Exception {
        TestStreamObserver<IntentMessage> observer = new TestStreamObserver<>();
        service.getIntent(GetIntentRequest.newBuilder()
            .setIntentId("nonexistent")
            .build(), observer);

        Throwable error = observer.awaitError();
        assertTrue(error instanceof StatusRuntimeException);
        assertEquals(Status.NOT_FOUND.getCode(), ((StatusRuntimeException) error).getStatus().getCode());
    }

    // ── listIntents ──

    @Test
    @DisplayName("listIntents should list by status")
    void shouldListIntentsByStatus() throws Exception {
        // Create an intent first
        TestStreamObserver<IntentMessage> createObs = new TestStreamObserver<>();
        service.createIntent(buildRequest("list-1"), createObs);
        createObs.await();

        TestStreamObserver<ListIntentsResponse> observer = new TestStreamObserver<>();
        service.listIntents(ListIntentsRequest.newBuilder()
            .setStatus("SCHEDULED")
            .setLimit(10)
            .build(), observer);

        ListIntentsResponse result = observer.await();
        assertTrue(result.getTotal() >= 1);
    }

    @Test
    @DisplayName("listIntents should reject request without status")
    void shouldRejectListWithoutStatus() throws Exception {
        TestStreamObserver<ListIntentsResponse> observer = new TestStreamObserver<>();
        service.listIntents(ListIntentsRequest.newBuilder()
            .setLimit(10)
            .build(), observer);

        Throwable error = observer.awaitError();
        assertTrue(error instanceof StatusRuntimeException);
        assertEquals(Status.INVALID_ARGUMENT.getCode(), ((StatusRuntimeException) error).getStatus().getCode());
    }

    // ── patchIntent ──

    @Test
    @DisplayName("patchIntent should patch fields")
    void shouldPatchIntent() throws Exception {
        TestStreamObserver<IntentMessage> createObs = new TestStreamObserver<>();
        service.createIntent(buildRequest("patch-1"), createObs);
        createObs.await();

        TestStreamObserver<IntentMessage> observer = new TestStreamObserver<>();
        service.patchIntent(PatchIntentRequest.newBuilder()
            .setIntentId("patch-1")
            .setExpiredAction("DEAD_LETTER")
            .putTags("updated", "true")
            .build(), observer);

        IntentMessage result = observer.await();
        assertEquals("patch-1", result.getIntentId());
    }

    @Test
    @DisplayName("patchIntent should return 404 for non-existent intent")
    void shouldRejectPatchNonExistentIntent() throws Exception {
        TestStreamObserver<IntentMessage> observer = new TestStreamObserver<>();
        service.patchIntent(PatchIntentRequest.newBuilder()
            .setIntentId("ghost")
            .setExpiredAction("DEAD_LETTER")
            .build(), observer);

        Throwable error = observer.awaitError();
        assertTrue(error instanceof StatusRuntimeException);
        assertEquals(Status.NOT_FOUND.getCode(), ((StatusRuntimeException) error).getStatus().getCode());
    }

    // ── cancelIntent ──

    @Test
    @DisplayName("cancelIntent should cancel intent")
    void shouldCancelIntent() throws Exception {
        TestStreamObserver<IntentMessage> createObs = new TestStreamObserver<>();
        service.createIntent(buildRequest("cancel-1"), createObs);
        createObs.await();

        TestStreamObserver<IntentMessage> observer = new TestStreamObserver<>();
        service.cancelIntent(CancelIntentRequest.newBuilder()
            .setIntentId("cancel-1")
            .build(), observer);

        IntentMessage result = observer.await();
        assertEquals("cancel-1", result.getIntentId());
        assertEquals("CANCELED", result.getStatus());
    }

    // ── fireNow ──

    @Test
    @DisplayName("fireNow should trigger immediate dispatch")
    void shouldFireNowIntent() throws Exception {
        TestStreamObserver<IntentMessage> createObs = new TestStreamObserver<>();
        service.createIntent(buildRequest("fire-1"), createObs);
        createObs.await();

        TestStreamObserver<IntentActionResponse> observer = new TestStreamObserver<>();
        service.fireNow(FireNowRequest.newBuilder()
            .setIntentId("fire-1")
            .build(), observer);

        IntentActionResponse result = observer.await();
        assertEquals("fire-1", result.getIntentId());
        assertEquals("DISPATCHING", result.getStatus());
    }

    @Test
    @DisplayName("fireNow should return 404 for non-existent intent")
    void shouldRejectFireNowNonExistentIntent() throws Exception {
        TestStreamObserver<IntentActionResponse> observer = new TestStreamObserver<>();
        service.fireNow(FireNowRequest.newBuilder()
            .setIntentId("ghost")
            .build(), observer);

        Throwable error = observer.awaitError();
        assertTrue(error instanceof StatusRuntimeException);
        assertEquals(Status.NOT_FOUND.getCode(), ((StatusRuntimeException) error).getStatus().getCode());
    }

    // ── reviveIntent ──

    @Test
    @DisplayName("reviveIntent should return 404 for non-existent intent")
    void shouldRejectReviveNonExistentIntent() throws Exception {
        TestStreamObserver<IntentMessage> observer = new TestStreamObserver<>();
        service.reviveIntent(ReviveIntentRequest.newBuilder()
            .setIntentId("ghost")
            .build(), observer);

        Throwable error = observer.awaitError();
        assertTrue(error instanceof StatusRuntimeException);
        assertEquals(Status.NOT_FOUND.getCode(), ((StatusRuntimeException) error).getStatus().getCode());
    }

    // ── healthCheck ──

    @Test
    @DisplayName("healthCheck should return health status")
    void shouldReturnHealthStatus() throws Exception {
        TestStreamObserver<HealthCheckResponse> observer = new TestStreamObserver<>();
        service.healthCheck(HealthCheckRequest.newBuilder().build(), observer);

        HealthCheckResponse result = observer.await();
        assertNotNull(result.getStatus());
    }

    // ── watchIntent ──

    @Test
    @DisplayName("watchIntent should receive events for wildcard watcher")
    void shouldWatchIntentAndReceiveEvents() throws Exception {
        StreamingTestObserver eventObserver = new StreamingTestObserver();
        service.watchIntent(WatchIntentRequest.newBuilder().build(), eventObserver);

        // Directly trigger observer to test dispatch path
        Intent intent = new Intent("watch-1");
        intent.transitionTo(IntentStatus.SCHEDULED);
        globalObserver.onScheduled(intent);

        // Wait for async dispatch
        Thread.sleep(1000);

        assertTrue(eventObserver.getEvents().size() >= 1,
            "Expected at least 1 event, got " + eventObserver.getEvents().size());
    }

    @Test
    @DisplayName("watchIntent should filter events by status")
    void shouldFilterWatchEventsByStatus() throws Exception {
        StreamingTestObserver eventObserver = new StreamingTestObserver();
        service.watchIntent(WatchIntentRequest.newBuilder()
            .addStatusFilter("DELIVERED")
            .build(), eventObserver);

        // Create an intent (triggers CREATED->SCHEDULED, should be filtered out)
        TestStreamObserver<IntentMessage> createObs = new TestStreamObserver<>();
        service.createIntent(buildRequest("filter-1"), createObs);
        createObs.await();

        Thread.sleep(500);

        assertEquals(0, eventObserver.getEvents().size(),
            "SCHEDULED events should be filtered out when only DELIVERED is watched");
    }

    // ── Helper methods ──

    private CreateIntentRequest buildRequest(String intentId) {
        return CreateIntentRequest.newBuilder()
            .setIntentId(intentId)
            .setExecuteAt(com.loomq.channel.grpc.converter.ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(com.loomq.channel.grpc.converter.ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("test-shard")
            .setPrecisionTier("STANDARD")
            .setCallback(CallbackMessage.newBuilder().setUrl("http://localhost/webhook").build())
            .build();
    }

    // ── Test helpers ──

    static class TestStreamObserver<T> implements StreamObserver<T> {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final List<T> values = new ArrayList<>();
        private volatile Throwable error;

        @Override
        public void onNext(T value) {
            values.add(value);
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

        T await() throws Exception {
            assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for response");
            if (error != null) {
                if (error instanceof Exception e) throw e;
                throw new RuntimeException(error);
            }
            assertTrue(!values.isEmpty(), "No value received");
            return values.get(0);
        }

        Throwable awaitError() throws Exception {
            assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for error");
            assertNotNull(error, "Expected error but got success");
            return error;
        }
    }

    static class StreamingTestObserver extends ServerCallStreamObserver<IntentEvent> {
        private final List<IntentEvent> events = new ArrayList<>();
        private Runnable onReadyHandler;
        private Runnable onCancelHandler;
        private Runnable onCloseHandler;

        @Override
        public boolean isReady() { return true; }

        @Override
        public void setOnReadyHandler(Runnable handler) { this.onReadyHandler = handler; }

        @Override
        public void request(int count) {}

        @Override
        public void setMessageCompression(boolean enable) {}

        @Override
        public void onNext(IntentEvent event) {
            synchronized (events) {
                events.add(event);
            }
        }

        @Override
        public void onError(Throwable t) {}

        @Override
        public void onCompleted() {}

        @Override
        public void setOnCancelHandler(Runnable handler) { this.onCancelHandler = handler; }

        @Override
        public void setOnCloseHandler(Runnable handler) { this.onCloseHandler = handler; }

        @Override
        public boolean isCancelled() { return false; }

        @Override
        public void setCompression(String compression) {}

        @Override
        public void disableAutoInboundFlowControl() {}

        List<IntentEvent> getEvents() {
            synchronized (events) {
                return new ArrayList<>(events);
            }
        }
    }
}

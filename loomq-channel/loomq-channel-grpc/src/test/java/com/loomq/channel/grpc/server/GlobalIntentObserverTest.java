package com.loomq.channel.grpc.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.grpc.gen.IntentEvent;
import com.loomq.grpc.gen.WatchIntentRequest;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class GlobalIntentObserverTest {

    private GlobalIntentObserver observer;

    @BeforeEach
    void setUp() {
        observer = new GlobalIntentObserver();
    }

    @AfterEach
    void tearDown() {
        observer.close();
    }

    @Test
    @DisplayName("Wildcard watcher should receive events for any intent")
    void wildcardWatcherReceivesEvents() throws Exception {
        CapturingStreamObserver streamObs = new CapturingStreamObserver();
        observer.register(WatchIntentRequest.newBuilder().build(), streamObs);

        Intent intent = new Intent("wild-1");
        intent.transitionTo(IntentStatus.SCHEDULED);
        observer.onScheduled(intent);

        assertTrue(streamObs.awaitEvents(1, 2000),
            "Expected at least 1 event");
        assertEquals("wild-1", streamObs.getEvents().get(0).getIntentId());
        assertEquals("CREATED", streamObs.getEvents().get(0).getFromStatus());
        assertEquals("SCHEDULED", streamObs.getEvents().get(0).getToStatus());
    }

    @Test
    @DisplayName("Per-intent watcher should only receive events for that intent")
    void perIntentWatcherFiltersCorrectly() throws Exception {
        CapturingStreamObserver streamObs = new CapturingStreamObserver();
        observer.register(WatchIntentRequest.newBuilder()
            .addIntentIds("target-1")
            .build(), streamObs);

        // Event for a different intent — should not be received
        Intent other = new Intent("other-1");
        other.transitionTo(IntentStatus.SCHEDULED);
        observer.onScheduled(other);

        // Event for the target intent — should be received
        Intent target = new Intent("target-1");
        target.transitionTo(IntentStatus.SCHEDULED);
        observer.onScheduled(target);

        assertTrue(streamObs.awaitEvents(1, 2000));
        assertEquals(1, streamObs.getEvents().size());
        assertEquals("target-1", streamObs.getEvents().get(0).getIntentId());
    }

    @Test
    @DisplayName("Status filter should exclude non-matching events")
    void statusFilterExcludesNonMatching() throws Exception {
        CapturingStreamObserver streamObs = new CapturingStreamObserver();
        observer.register(WatchIntentRequest.newBuilder()
            .addStatusFilter("DELIVERED")
            .build(), streamObs);

        Intent intent = new Intent("filter-1");
        intent.transitionTo(IntentStatus.SCHEDULED);
        observer.onScheduled(intent);  // CREATED->SCHEDULED, should be filtered

        Thread.sleep(500);
        assertEquals(0, streamObs.getEvents().size(),
            "SCHEDULED event should be filtered when watching DELIVERED only");
    }

    @Test
    @DisplayName("Status filter should include matching events")
    void statusFilterIncludesMatching() throws Exception {
        CapturingStreamObserver streamObs = new CapturingStreamObserver();
        observer.register(WatchIntentRequest.newBuilder()
            .addStatusFilter("SCHEDULED")
            .build(), streamObs);

        Intent intent = new Intent("filter-2");
        intent.transitionTo(IntentStatus.SCHEDULED);
        observer.onScheduled(intent);

        assertTrue(streamObs.awaitEvents(1, 2000));
        assertEquals("SCHEDULED", streamObs.getEvents().get(0).getToStatus());
    }

    @Test
    @DisplayName("Unregister should stop receiving events")
    void unregisterStopsEvents() throws Exception {
        CapturingStreamObserver streamObs = new CapturingStreamObserver();
        var entry = observer.register(WatchIntentRequest.newBuilder().build(), streamObs);

        observer.unregister(entry);

        Intent intent = new Intent("unreg-1");
        intent.transitionTo(IntentStatus.SCHEDULED);
        observer.onScheduled(intent);

        Thread.sleep(500);
        assertEquals(0, streamObs.getEvents().size());
    }

    @Test
    @DisplayName("onDelivered with SUCCESS should dispatch DELIVERED status")
    void onDeliveredSuccessDispatchesDelivered() throws Exception {
        CapturingStreamObserver streamObs = new CapturingStreamObserver();
        observer.register(WatchIntentRequest.newBuilder().build(), streamObs);

        Intent intent = new Intent("delivered-1");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        observer.onDelivered(intent, com.loomq.spi.DeliveryHandler.DeliveryResult.SUCCESS);

        assertTrue(streamObs.awaitEvents(1, 2000));
        assertEquals("DELIVERED", streamObs.getEvents().get(0).getToStatus());
    }

    @Test
    @DisplayName("onDeadLettered should dispatch DEAD_LETTERED status")
    void onDeadLetteredDispatchesCorrectly() throws Exception {
        CapturingStreamObserver streamObs = new CapturingStreamObserver();
        observer.register(WatchIntentRequest.newBuilder().build(), streamObs);

        Intent intent = new Intent("dl-1");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        observer.onDeadLettered(intent);

        assertTrue(streamObs.awaitEvents(1, 2000));
        assertEquals("DEAD_LETTERED", streamObs.getEvents().get(0).getToStatus());
    }

    private static class CapturingStreamObserver extends ServerCallStreamObserver<IntentEvent> {
        private final List<IntentEvent> events = new ArrayList<>();
        private final CountDownLatch latch = new CountDownLatch(1);
        private int expectedCount;

        @Override
        public boolean isReady() { return true; }

        @Override
        public void setOnReadyHandler(Runnable handler) {}

        @Override
        public void request(int count) {}

        @Override
        public void setMessageCompression(boolean enable) {}

        @Override
        public void onNext(IntentEvent event) {
            synchronized (events) {
                events.add(event);
                if (events.size() >= expectedCount) latch.countDown();
            }
        }

        @Override
        public void onError(Throwable t) {}

        @Override
        public void onCompleted() {}

        @Override
        public void setOnCancelHandler(Runnable handler) {}

        @Override
        public void setOnCloseHandler(Runnable handler) {}

        @Override
        public boolean isCancelled() { return false; }

        @Override
        public void setCompression(String compression) {}

        @Override
        public void disableAutoInboundFlowControl() {}

        boolean awaitEvents(int count, long timeoutMs) throws InterruptedException {
            this.expectedCount = count;
            return latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        }

        List<IntentEvent> getEvents() {
            synchronized (events) {
                return new ArrayList<>(events);
            }
        }
    }
}

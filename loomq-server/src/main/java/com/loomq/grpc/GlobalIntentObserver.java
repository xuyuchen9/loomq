package com.loomq.grpc;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.grpc.converter.ProtoConverter;
import com.loomq.grpc.gen.IntentEvent;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.IntentObserver;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridges the core {@link IntentObserver} SPI to gRPC streaming clients.
 *
 * <p>Clients call {@code WatchIntent} and receive {@link IntentEvent} messages
 * pushed in real-time as intents transition through their lifecycle.
 */
public final class GlobalIntentObserver implements IntentObserver {

    private static final Logger logger = LoggerFactory.getLogger(GlobalIntentObserver.class);

    private final ConcurrentHashMap<String, CopyOnWriteArraySet<WatchEntry>> watchersByIntentId = new ConcurrentHashMap<>();
    private final CopyOnWriteArraySet<WatchEntry> wildcardWatchers = new CopyOnWriteArraySet<>();
    private final ExecutorService dispatchExecutor = Executors.newVirtualThreadPerTaskExecutor();

    static final class WatchEntry {
        private static final int MAX_PENDING = 256;
        final Set<String> statusFilter;
        final ServerCallStreamObserver<IntentEvent> observer;
        final ConcurrentLinkedQueue<IntentEvent> pending = new ConcurrentLinkedQueue<>();

        WatchEntry(Set<String> statusFilter, ServerCallStreamObserver<IntentEvent> observer) {
            this.statusFilter = statusFilter;
            this.observer = observer;
        }

        boolean matchesStatus(String status) {
            return statusFilter == null || statusFilter.isEmpty() || statusFilter.contains(status);
        }

        /** Try direct send if ready, otherwise enqueue. Drop oldest when queue full. */
        void deliver(IntentEvent event) {
            if (observer.isReady()) {
                observer.onNext(event);
            } else {
                if (pending.size() >= MAX_PENDING) {
                    pending.poll();
                }
                pending.offer(event);
            }
        }

        /** Called by setOnReadyHandler to drain buffered events. */
        void drainQueue() {
            while (observer.isReady()) {
                IntentEvent event = pending.poll();
                if (event == null) break;
                observer.onNext(event);
            }
        }
    }

    /**
     * Register a new watcher for the given intent id filter (empty = all intents).
     *
     * @return the WatchEntry that was registered (for use in cancel/close handlers)
     */
    public WatchEntry register(com.loomq.grpc.gen.WatchIntentRequest request,
                               ServerCallStreamObserver<IntentEvent> observer) {
        Set<String> filter = request.getStatusFilterCount() > 0
            ? Set.copyOf(request.getStatusFilterList())
            : Set.of();
        WatchEntry entry = new WatchEntry(filter, observer);

        if (request.getIntentIdsCount() > 0) {
            for (String id : request.getIntentIdsList()) {
                watchersByIntentId.computeIfAbsent(id, k -> new CopyOnWriteArraySet<>()).add(entry);
            }
        } else {
            wildcardWatchers.add(entry);
        }
        return entry;
    }

    /**
     * Remove a watcher (called when the gRPC stream is cancelled).
     */
    public void unregister(WatchEntry entry) {
        wildcardWatchers.remove(entry);
        watchersByIntentId.values().forEach(set -> set.remove(entry));
        watchersByIntentId.values().removeIf(Set::isEmpty);
    }

    private void dispatch(String intentId, String traceId, String fromStatus, String toStatus) {
        // Dispatch asynchronously to avoid blocking the scheduler thread.
        // IntentObserver contract: "callbacks execute in the scheduler thread,
        // implementations should stay lightweight, avoid synchronous blocking."
        dispatchExecutor.submit(() -> {
            var event = IntentEvent.newBuilder()
                .setIntentId(intentId)
                .setFromStatus(fromStatus)
                .setToStatus(toStatus)
                .setTimestamp(ProtoConverter.toProto(java.time.Instant.now()))
                .setTraceId(traceId != null ? traceId : "")
                .build();

            // Notify specific watchers
            var specific = watchersByIntentId.get(intentId);
            if (specific != null) {
                for (WatchEntry entry : specific) {
                    if (entry.matchesStatus(toStatus)) {
                        try {
                            entry.deliver(event);
                        } catch (Exception e) {
                            logger.debug("Removing stale watcher for intent {}", intentId);
                            unregister(entry);
                        }
                    }
                }
            }

            // Notify wildcard watchers
            for (WatchEntry entry : wildcardWatchers) {
                if (entry.matchesStatus(toStatus)) {
                    try {
                        entry.deliver(event);
                    } catch (Exception e) {
                        wildcardWatchers.remove(entry);
                    }
                }
            }
        });
    }

    // ── IntentObserver SPI overrides ──

    @Override
    public void onScheduled(Intent intent) {
        dispatch(intent.getIntentId(), intent.getTraceId(),
            IntentStatus.CREATED.name(), IntentStatus.SCHEDULED.name());
    }

    @Override
    public void onDelivered(Intent intent, DeliveryResult result) {
        dispatch(intent.getIntentId(), intent.getTraceId(), IntentStatus.DISPATCHING.name(),
            result == DeliveryResult.SUCCESS
                ? IntentStatus.DELIVERED.name() : IntentStatus.DEAD_LETTERED.name());
    }

    @Override
    public void onDeadLettered(Intent intent) {
        dispatch(intent.getIntentId(), intent.getTraceId(),
            IntentStatus.DISPATCHING.name(), IntentStatus.DEAD_LETTERED.name());
    }

    @Override
    public void onExpired(Intent intent) {
        dispatch(intent.getIntentId(), intent.getTraceId(),
            intent.getStatus().name(), IntentStatus.EXPIRED.name());
    }

    @Override
    public void onDeliveryFailed(Intent intent, Throwable error) {
        dispatch(intent.getIntentId(), intent.getTraceId(),
            IntentStatus.DISPATCHING.name(), "FAILED");
    }

    /**
     * Shut down the dispatch executor. Call during server shutdown.
     */
    public void close() {
        dispatchExecutor.shutdownNow();
    }
}

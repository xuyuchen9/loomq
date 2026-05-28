package com.loomq.channel.grpc.server;

import com.loomq.channel.grpc.converter.ProtoConverter;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.grpc.gen.IntentEvent;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.IntentObserver;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final AtomicBoolean hasWildcardWatchers = new AtomicBoolean(false);
    private final AtomicBoolean hasSpecificWatchers = new AtomicBoolean(false);
    private final ExecutorService dispatchExecutor = new ThreadPoolExecutor(
        Runtime.getRuntime().availableProcessors() * 2,
        Runtime.getRuntime().availableProcessors() * 2,
        60L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(1024),
        Thread.ofPlatform().daemon(true).name("grpc-observer-dispatch", 0).factory(),
        new ThreadPoolExecutor.DiscardOldestPolicy()
    );

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

        // Set flag BEFORE adding watcher to narrow the concurrent window:
        // a dispatch() call may miss this watcher, but won't miss later ones.
        // This is acceptable — WatchIntent follows "best-effort delivery" semantics.
        if (request.getIntentIdsCount() > 0) {
            hasSpecificWatchers.set(true);
            for (String id : request.getIntentIdsList()) {
                watchersByIntentId.computeIfAbsent(id, k -> new CopyOnWriteArraySet<>()).add(entry);
            }
        } else {
            hasWildcardWatchers.set(true);
            wildcardWatchers.add(entry);
        }
        return entry;
    }

    /**
     * Remove a watcher (called when the gRPC stream is cancelled).
     */
    public void unregister(WatchEntry entry) {
        // Remove watcher first, then clear flag if collection is empty.
        // This ordering ensures dispatch() won't iterate an empty collection
        // (it may short-circuit past a newly added watcher — acceptable).
        wildcardWatchers.remove(entry);
        if (wildcardWatchers.isEmpty()) {
            hasWildcardWatchers.set(false);
        }
        watchersByIntentId.values().forEach(set -> set.remove(entry));
        watchersByIntentId.values().removeIf(Set::isEmpty);
        if (watchersByIntentId.isEmpty()) {
            hasSpecificWatchers.set(false);
        }
    }

    private void dispatch(String intentId, String traceId, String fromStatus, String toStatus) {
        // Fast path: no watchers registered, skip entirely.
        // AtomicBoolean.get() is a volatile read — negligible cost vs.
        // creating a virtual thread + building proto for every state transition.
        if (!hasWildcardWatchers.get() && !hasSpecificWatchers.get()) {
            return;
        }

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
        dispatchExecutor.shutdown();
        try {
            if (!dispatchExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                dispatchExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            dispatchExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

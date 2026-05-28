package com.loomq.raft;

import com.loomq.domain.intent.Intent;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.metrics.LoomQMetrics;
import com.loomq.store.IntentStore;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Coordinates Raft writes from the HTTP layer.
 *
 * The coordinator serializes writes on the leader, deduplicates retried
 * requests, applies bounded backpressure, proposes the final intent snapshot
 * to the Raft log, then waits for the committed entry to be applied.
 */
public final class RaftWriteCoordinator {

    private static final long DEFAULT_WRITE_TIMEOUT_MS = 5_000L;
    private static final long DEFAULT_BACKPRESSURE_TIMEOUT_MS = 500L;
    private static final long DEFAULT_REQUEST_CACHE_TTL_MS = TimeUnit.MINUTES.toMillis(15);
    private static final int DEFAULT_MAX_PENDING_WRITES = 64;

    private final RaftNode raftNode;
    private final IntentStore store;
    private final LoomQMetrics metrics = LoomQMetrics.getInstance();
    private final ReentrantLock writeLock = new ReentrantLock();
    private final Semaphore writePermits;
    private final ConcurrentMap<String, CachedWrite> requestCache = new ConcurrentHashMap<>();
    private final long acquireTimeoutMs;
    private final long writeTimeoutMs;
    private final long requestCacheTtlMs;
    private long pendingWrites;

    public RaftWriteCoordinator(RaftNode raftNode, IntentStore store) {
        this(raftNode, store, DEFAULT_MAX_PENDING_WRITES, DEFAULT_BACKPRESSURE_TIMEOUT_MS, DEFAULT_WRITE_TIMEOUT_MS);
    }

    public RaftWriteCoordinator(RaftNode raftNode, IntentStore store, int maxPendingWrites,
                                long acquireTimeoutMs, long writeTimeoutMs) {
        this.raftNode = Objects.requireNonNull(raftNode, "raftNode");
        this.store = Objects.requireNonNull(store, "store");
        this.writePermits = new Semaphore(Math.max(1, maxPendingWrites));
        this.acquireTimeoutMs = Math.max(1L, acquireTimeoutMs);
        this.writeTimeoutMs = Math.max(1_000L, writeTimeoutMs);
        this.requestCacheTtlMs = DEFAULT_REQUEST_CACHE_TTL_MS;
    }

    /**
     * Propose a fully materialized intent snapshot and wait for it to be applied.
     */
    public Intent commitSnapshot(Intent snapshot, String operation, String requestKey) {
        if (snapshot == null) {
            throw new IllegalArgumentException("snapshot cannot be null");
        }
        String normalizedRequestKey = normalizeRequestKey(operation, snapshot.getIntentId(), requestKey);
        return commitWrite(normalizedRequestKey, operation, snapshot.getIntentId(), () -> {
            if (snapshot.getRevision() <= 0) {
                snapshot.incrementRevision();
            }
            return snapshot;
        });
    }

    /**
     * Apply a mutating operation against the current authoritative state and
     * commit the resulting snapshot through Raft.
     */
    public Intent commitMutation(String intentId, String operation, String requestKey,
                                 long expectedRevision, Function<Intent, Intent> mutator) {
        if (intentId == null || intentId.isBlank()) {
            throw new IllegalArgumentException("intentId cannot be blank");
        }
        if (mutator == null) {
            throw new IllegalArgumentException("mutator cannot be null");
        }

        String normalizedRequestKey = normalizeRequestKey(operation, intentId, requestKey);
        return commitWrite(normalizedRequestKey, operation, intentId, () -> {
            Intent current = store.findById(intentId);
            if (current == null) {
                throw new RaftWriteUnavailableException(operation, intentId, "Intent not found: " + intentId);
            }
            if (current.getRevision() != expectedRevision) {
                metrics.incrementRaftWriteRevisionConflicts();
                throw new RaftWriteConflictException(operation, intentId, expectedRevision,
                    current.getRevision(),
                    "Stale revision: expected " + expectedRevision + ", actual " + current.getRevision());
            }

            Intent snapshot = current.copy();
            Intent mutated = mutator.apply(snapshot);
            if (mutated == null) {
                mutated = snapshot;
            }
            mutated.incrementRevision();
            return mutated;
        });
    }

    private Intent commitWrite(String requestKey, String operation, String intentId, java.util.function.Supplier<Intent> snapshotSupplier) {
        cleanupExpiredCache();

        CachedWrite existing = requestCache.get(requestKey);
        long now = System.currentTimeMillis();
        if (existing != null && !existing.isExpired(now)) {
            return awaitCached(existing, operation, intentId);
        }

        CachedWrite inflight = new CachedWrite();
        CachedWrite previous = requestCache.putIfAbsent(requestKey, inflight);
        if (previous != null) {
            if (!previous.isExpired(now)) {
                return awaitCached(previous, operation, intentId);
            }
            requestCache.replace(requestKey, previous, inflight);
        }

        boolean permitAcquired = false;
        boolean locked = false;
        updatePendingWrites(1);
        long startNs = System.nanoTime();
        try {
            permitAcquired = writePermits.tryAcquire(acquireTimeoutMs, TimeUnit.MILLISECONDS);
            if (!permitAcquired) {
                metrics.incrementRaftWriteBackpressureRejects();
                throw new RaftWriteBackPressureException(operation, intentId,
                    "Raft write queue is full",
                    TimeUnit.MILLISECONDS.toMillis(acquireTimeoutMs));
            }

            writeLock.lock();
            locked = true;

            if (!raftNode.isLeader()) {
                metrics.incrementRaftWriteStepDownAborts();
                throw new RaftWriteUnavailableException(operation, intentId,
                    "Raft leader changed before the write could start");
            }

            Intent snapshot = snapshotSupplier.get();
            long index = raftNode.propose(IntentBinaryCodec.encode(snapshot));
            boolean applied = raftNode.getReplication().awaitApplied(index, writeTimeoutMs);
            if (!applied) {
                metrics.incrementRaftWriteTimeouts();
                throw new RaftWriteUnavailableException(operation, intentId,
                    "Timed out waiting for Raft commit application");
            }

            if (!raftNode.isLeader()) {
                metrics.incrementRaftWriteStepDownAborts();
                throw new RaftWriteUnavailableException(operation, intentId,
                    "Raft leader changed before the write completed");
            }

            long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
            metrics.recordRaftWriteProposalLatency(latencyMs);

            Intent committed = store.findById(snapshot.getIntentId());
            Intent result = committed != null ? committed : snapshot;
            inflight.complete(result);
            inflight.expireAfter(requestCacheTtlMs);
            return result;
        } catch (RaftWriteConflictException | RaftWriteBackPressureException | RaftWriteUnavailableException e) {
            inflight.completeExceptionally(e);
            requestCache.remove(requestKey, inflight);
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            metrics.incrementRaftWriteStepDownAborts();
            RaftWriteUnavailableException unavailable = new RaftWriteUnavailableException(operation, intentId,
                "Raft write interrupted");
            inflight.completeExceptionally(unavailable);
            requestCache.remove(requestKey, inflight);
            throw unavailable;
        } catch (RuntimeException e) {
            if (isLeadershipLoss(e)) {
                metrics.incrementRaftWriteStepDownAborts();
            }
            RaftWriteUnavailableException unavailable = new RaftWriteUnavailableException(operation, intentId,
                e.getMessage() != null ? e.getMessage() : "Raft write failed");
            inflight.completeExceptionally(unavailable);
            requestCache.remove(requestKey, inflight);
            throw unavailable;
        } finally {
            if (locked) {
                writeLock.unlock();
            }
            if (permitAcquired) {
                writePermits.release();
            }
            updatePendingWrites(-1);
        }
    }

    private Intent awaitCached(CachedWrite cachedWrite, String operation, String intentId) {
        try {
            return cachedWrite.future.get(writeTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RaftWriteUnavailableException(operation, intentId, "Interrupted while waiting for cached Raft write");
        } catch (java.util.concurrent.TimeoutException e) {
            metrics.incrementRaftWriteTimeouts();
            throw new RaftWriteUnavailableException(operation, intentId, "Timed out waiting for cached Raft write result");
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new RaftWriteUnavailableException(operation, intentId, cause.getMessage() != null ? cause.getMessage() : "Raft write failed");
        }
    }

    private void cleanupExpiredCache() {
        long now = System.currentTimeMillis();
        requestCache.entrySet().removeIf(entry -> entry.getValue().isExpired(now));
    }

    private String normalizeRequestKey(String operation, String intentId, String requestKey) {
        String normalized = requestKey == null ? "" : requestKey.trim();
        if (!normalized.isBlank()) {
            return normalized;
        }
        return operation + ":" + intentId;
    }

    private void updatePendingWrites(long delta) {
        synchronized (this) {
            pendingWrites = Math.max(0, pendingWrites + delta);
            metrics.updateRaftPendingWrites(pendingWrites);
        }
    }

    private boolean isLeadershipLoss(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null) {
                String lower = message.toLowerCase();
                if (lower.contains("leadership lost")
                    || lower.contains("leader changed")
                    || lower.contains("raft node closed")
                    || lower.contains("must be issued by the leader")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private static final class CachedWrite {
        private final CompletableFuture<Intent> future = new CompletableFuture<>();
        private volatile long expiresAtMs = Long.MAX_VALUE;

        private void complete(Intent result) {
            future.complete(result);
        }

        private void completeExceptionally(Throwable throwable) {
            future.completeExceptionally(throwable);
        }

        private void expireAfter(long ttlMs) {
            expiresAtMs = System.currentTimeMillis() + Math.max(1L, ttlMs);
        }

        private boolean isExpired(long nowMs) {
            return future.isDone() && nowMs >= expiresAtMs;
        }
    }
}

package com.loomq.raft;

import com.loomq.domain.intent.Intent;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.metrics.LoomQMetrics;
import com.loomq.store.IntentStore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Coordinates Raft writes from the HTTP layer.
 *
 * The coordinator serializes writes on the leader, proposes the final intent
 * snapshot to the Raft log, then waits for the committed entry to be applied.
 */
public final class RaftWriteCoordinator {

    private static final long DEFAULT_WRITE_TIMEOUT_MS = 5_000L;

    private final RaftNode raftNode;
    private final IntentStore store;
    private final LoomQMetrics metrics = LoomQMetrics.getInstance();
    private final ReentrantLock writeLock = new ReentrantLock();
    private final long writeTimeoutMs;
    private long pendingWrites;

    public RaftWriteCoordinator(RaftNode raftNode, IntentStore store) {
        this(raftNode, store, DEFAULT_WRITE_TIMEOUT_MS);
    }

    public RaftWriteCoordinator(RaftNode raftNode, IntentStore store, long writeTimeoutMs) {
        this.raftNode = raftNode;
        this.store = store;
        this.writeTimeoutMs = Math.max(1_000L, writeTimeoutMs);
    }

    /**
     * Propose a fully materialized intent snapshot and wait for it to be applied.
     */
    public Intent commitSnapshot(Intent snapshot, String operation) {
        if (snapshot == null) {
            throw new IllegalArgumentException("snapshot cannot be null");
        }
        if (!raftNode.isLeader()) {
            throw new RaftWriteUnavailableException(operation, snapshot.getIntentId(),
                "Write must be served by the Raft leader");
        }

        boolean locked = false;
        updatePendingWrites(1);
        long startNs = System.nanoTime();
        try {
            writeLock.lock();
            locked = true;

            if (!raftNode.isLeader()) {
                metrics.incrementRaftWriteStepDownAborts();
                throw new RaftWriteUnavailableException(operation, snapshot.getIntentId(),
                    "Raft leader changed before the write could start");
            }

            long index = raftNode.propose(IntentBinaryCodec.encode(snapshot));
            boolean applied = raftNode.getReplication().awaitApplied(index, writeTimeoutMs);
            if (!applied) {
                metrics.incrementRaftWriteTimeouts();
                throw new RaftWriteUnavailableException(operation, snapshot.getIntentId(),
                    "Timed out waiting for Raft commit application");
            }

            if (!raftNode.isLeader()) {
                metrics.incrementRaftWriteStepDownAborts();
                throw new RaftWriteUnavailableException(operation, snapshot.getIntentId(),
                    "Raft leader changed before the write completed");
            }

            long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
            metrics.recordRaftWriteProposalLatency(latencyMs);

            Intent committed = store.findById(snapshot.getIntentId());
            return committed != null ? committed : snapshot;
        } catch (RaftWriteUnavailableException e) {
            throw e;
        } catch (RuntimeException e) {
            if (isLeadershipLoss(e)) {
                metrics.incrementRaftWriteStepDownAborts();
            }
            throw new RaftWriteUnavailableException(operation, snapshot.getIntentId(),
                e.getMessage() != null ? e.getMessage() : "Raft write failed");
        } finally {
            if (locked) {
                writeLock.unlock();
            }
            updatePendingWrites(-1);
        }
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
}

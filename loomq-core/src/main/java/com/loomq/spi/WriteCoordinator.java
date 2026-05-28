package com.loomq.spi;

import com.loomq.domain.intent.Intent;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Coordinates write operations across the system.
 *
 * In single-node mode, writes go directly to the local store.
 * In Raft mode, writes are proposed to the Raft log and committed through consensus.
 *
 * Implementations should be thread-safe.
 */
public interface WriteCoordinator {

    /**
     * Whether write coordination is enabled.
     *
     * @return true if writes should go through the coordinator, false for direct writes
     */
    boolean isWriteEnabled();

    /**
     * Submit a write operation for coordination.
     *
     * @param command the write command to execute
     * @return a future that completes with the write result
     */
    CompletableFuture<WriteResult> submitWrite(WriteCommand command);

    /**
     * Check if the current node can handle writes.
     *
     * @return true if the node is ready to accept writes
     */
    boolean canHandleWrite();

    /**
     * Propose a fully materialized intent snapshot and wait for it to be applied.
     *
     * @param snapshot the intent snapshot to commit
     * @param operation the operation name (e.g., "create", "revive")
     * @param requestKey the request key for deduplication
     * @return the committed intent
     */
    Intent commitSnapshot(Intent snapshot, String operation, String requestKey);

    /**
     * Apply a mutating operation against the current authoritative state and
     * commit the resulting snapshot through the coordinator.
     *
     * @param intentId the target intent id
     * @param operation the operation name (e.g., "patch", "cancel", "fire-now")
     * @param requestKey the request key for deduplication
     * @param expectedRevision the expected revision for optimistic concurrency
     * @param mutator the mutation function
     * @return the committed intent
     */
    Intent commitMutation(String intentId, String operation, String requestKey,
                         long expectedRevision, Function<Intent, Intent> mutator);
}

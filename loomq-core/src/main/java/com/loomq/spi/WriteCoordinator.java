package com.loomq.spi;

import java.util.concurrent.CompletableFuture;

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
}

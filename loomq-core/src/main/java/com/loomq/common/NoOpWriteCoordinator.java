package com.loomq.common;

import com.loomq.spi.WriteCommand;
import com.loomq.spi.WriteCoordinator;
import com.loomq.spi.WriteResult;
import java.util.concurrent.CompletableFuture;

/**
 * No-op WriteCoordinator for single-node mode.
 *
 * This coordinator always reports that writes should go directly to the engine
 * without coordination. It is used when Raft is not enabled.
 */
public class NoOpWriteCoordinator implements WriteCoordinator {

    public static final NoOpWriteCoordinator INSTANCE = new NoOpWriteCoordinator();

    @Override
    public boolean isWriteEnabled() {
        return false;
    }

    @Override
    public CompletableFuture<WriteResult> submitWrite(WriteCommand command) {
        return CompletableFuture.failedFuture(
            new UnsupportedOperationException("NoOpWriteCoordinator does not support write coordination"));
    }

    @Override
    public boolean canHandleWrite() {
        return true;
    }
}

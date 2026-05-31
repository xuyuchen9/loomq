package com.loomq.spi;

import com.loomq.application.command.IntentCommandService;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.store.IntentStore;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-node WriteCoordinator implementation.
 *
 * Writes go directly to WAL + IntentStore via IntentCommandService,
 * bypassing Raft consensus. Used when Raft is disabled.
 */
public final class DirectWriteCoordinator implements WriteCoordinator {

    private static final Logger log = LoggerFactory.getLogger(DirectWriteCoordinator.class);

    private final IntentCommandService commandService;
    private final IntentStore store;
    private final AtomicBoolean running;

    public DirectWriteCoordinator(IntentCommandService commandService, IntentStore store, AtomicBoolean running) {
        this.commandService = Objects.requireNonNull(commandService, "commandService");
        this.store = Objects.requireNonNull(store, "store");
        this.running = Objects.requireNonNull(running, "running");
    }

    @Override
    public boolean isWriteEnabled() {
        return true;
    }

    @Override
    public CompletableFuture<WriteResult> submitWrite(WriteCommand command) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Intent result;
                if (command.isSnapshot()) {
                    result = commitSnapshot(command.snapshot(), command.operation(), command.requestKey());
                } else if (command.isMutation()) {
                    result = commitMutation(command.intentId(), command.operation(), command.requestKey(),
                        command.expectedRevision(), command.mutator());
                } else {
                    return WriteResult.failure("400", "Invalid write command");
                }
                return WriteResult.success(result);
            } catch (Exception e) {
                return WriteResult.failure("500", e.getMessage());
            }
        });
    }

    @Override
    public boolean canHandleWrite() {
        return running.get();
    }

    @Override
    public Intent commitSnapshot(Intent snapshot, String operation, String requestKey) {
        if (snapshot == null) {
            throw new IllegalArgumentException("snapshot cannot be null");
        }
        commandService.createIntent(snapshot, snapshot.getAckMode() != null ? snapshot.getAckMode() : AckMode.DURABLE);
        Intent committed = store.findById(snapshot.getIntentId());
        return committed != null ? committed : snapshot;
    }

    @Override
    public Intent commitMutation(String intentId, String operation, String requestKey,
                                 long expectedRevision, Function<Intent, Intent> mutator) {
        if (intentId == null || intentId.isBlank()) {
            throw new IllegalArgumentException("intentId cannot be blank");
        }
        if (mutator == null) {
            throw new IllegalArgumentException("mutator cannot be null");
        }

        // Verify intent exists before attempting mutation
        Intent current = store.findById(intentId);
        if (current == null) {
            throw new IllegalArgumentException("Intent not found: " + intentId);
        }

        // Apply mutation via IntentCommandService (handles WAL persist + store update)
        Optional<Intent> updated = commandService.updateIntent(intentId, intent -> {
            // The mutator receives the live intent (already synchronized by updateIntent).
            // In RaftWriteCoordinator the mutator receives a copy, but here we operate
            // in-place since updateIntent's Consumer<Intent> pattern expects it.
            mutator.apply(intent);
        });

        if (updated.isEmpty()) {
            throw new IllegalArgumentException("Intent not found during mutation: " + intentId);
        }

        return updated.get();
    }
}

package com.loomq.spi;

import com.loomq.domain.intent.Intent;
import java.util.function.Function;

/**
 * Represents a write operation to be coordinated through the WriteCoordinator.
 *
 * @param operation   the operation name (e.g., "create", "patch", "cancel")
 * @param intentId    the target intent id
 * @param requestKey  the request key for deduplication
 * @param snapshot    the intent snapshot to commit (for snapshot writes)
 * @param mutator     the mutation function (for mutation writes)
 * @param expectedRevision the expected revision for optimistic concurrency (for mutation writes)
 */
public record WriteCommand(
    String operation,
    String intentId,
    String requestKey,
    Intent snapshot,
    Function<Intent, Intent> mutator,
    long expectedRevision
) {
    /**
     * Create a snapshot write command.
     */
    public static WriteCommand snapshot(String operation, String intentId, String requestKey, Intent snapshot) {
        return new WriteCommand(operation, intentId, requestKey, snapshot, null, 0);
    }

    /**
     * Create a mutation write command.
     */
    public static WriteCommand mutation(String operation, String intentId, String requestKey,
                                        long expectedRevision, Function<Intent, Intent> mutator) {
        return new WriteCommand(operation, intentId, requestKey, null, mutator, expectedRevision);
    }

    /**
     * Whether this is a snapshot write.
     */
    public boolean isSnapshot() {
        return snapshot != null;
    }

    /**
     * Whether this is a mutation write.
     */
    public boolean isMutation() {
        return mutator != null;
    }
}

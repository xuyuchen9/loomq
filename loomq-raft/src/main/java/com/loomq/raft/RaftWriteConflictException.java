package com.loomq.raft;

/**
 * Raised when a mutating request targets a stale intent revision.
 */
public final class RaftWriteConflictException extends RuntimeException {

    private final String operation;
    private final String intentId;
    private final long expectedRevision;
    private final long actualRevision;

    public RaftWriteConflictException(String operation, String intentId,
                                      long expectedRevision, long actualRevision, String reason) {
        super(reason);
        this.operation = operation;
        this.intentId = intentId;
        this.expectedRevision = expectedRevision;
        this.actualRevision = actualRevision;
    }

    public String operation() {
        return operation;
    }

    public String intentId() {
        return intentId;
    }

    public long expectedRevision() {
        return expectedRevision;
    }

    public long actualRevision() {
        return actualRevision;
    }
}

package com.loomq.raft;

/**
 * Raised when the Raft leader cannot accept more pending writes.
 */
public final class RaftWriteBackPressureException extends RuntimeException {

    private final String operation;
    private final String intentId;
    private final long retryAfterMs;

    public RaftWriteBackPressureException(String operation, String intentId, String reason, long retryAfterMs) {
        super(reason);
        this.operation = operation;
        this.intentId = intentId;
        this.retryAfterMs = retryAfterMs;
    }

    public String operation() {
        return operation;
    }

    public String intentId() {
        return intentId;
    }

    public long retryAfterMs() {
        return retryAfterMs;
    }
}

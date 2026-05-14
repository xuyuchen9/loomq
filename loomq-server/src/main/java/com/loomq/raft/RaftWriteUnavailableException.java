package com.loomq.raft;

/**
 * Raised when a Raft write cannot be completed safely.
 */
public final class RaftWriteUnavailableException extends RuntimeException {

    private final String operation;
    private final String intentId;
    private final String reason;

    public RaftWriteUnavailableException(String operation, String intentId, String reason) {
        super(reason);
        this.operation = operation;
        this.intentId = intentId;
        this.reason = reason;
    }

    public String operation() {
        return operation;
    }

    public String intentId() {
        return intentId;
    }

    public String reason() {
        return reason;
    }
}

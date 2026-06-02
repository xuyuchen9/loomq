package com.loomq.raft;

public class TransportException extends RuntimeException {
    private final boolean retriable;

    public TransportException(String message, Throwable cause, boolean retriable) {
        super(message, cause);
        this.retriable = retriable;
    }

    public TransportException(String message, boolean retriable) {
        super(message);
        this.retriable = retriable;
    }

    public boolean isRetriable() { return retriable; }
}

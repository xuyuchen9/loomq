package com.loomq.spi;

import com.loomq.domain.intent.Intent;

/**
 * Result of a write operation coordinated through WriteCoordinator.
 *
 * @param success whether the write was successful
 * @param intent  the committed intent (if successful)
 * @param error   the error message (if failed)
 * @param errorCode the error code (if failed)
 */
public record WriteResult(
    boolean success,
    Intent intent,
    String error,
    String errorCode
) {
    /**
     * Create a successful write result.
     */
    public static WriteResult success(Intent intent) {
        return new WriteResult(true, intent, null, null);
    }

    /**
     * Create a failed write result.
     */
    public static WriteResult failure(String errorCode, String error) {
        return new WriteResult(false, null, error, errorCode);
    }

    /**
     * Whether the write was successful.
     */
    public boolean isSuccess() {
        return success;
    }
}

package com.loomq.common.exception;

import com.loomq.domain.intent.PrecisionTier;

/**
 * 背压异常
 *
 * 当档位并发限制超过阈值时抛出，由HTTP层转换为429状态码。
 *
 * @author loomq
 * @since v0.6.2
 */
public class BackPressureException extends LoomQException {

    private final PrecisionTier tier;
    private final int retryAfterMs;

    public BackPressureException(String message, PrecisionTier tier, int retryAfterMs) {
        super("42900", message);
        this.tier = tier;
        this.retryAfterMs = retryAfterMs;
    }

    public PrecisionTier getTier() {
        return tier;
    }

    public int getRetryAfterMs() {
        return retryAfterMs;
    }
}

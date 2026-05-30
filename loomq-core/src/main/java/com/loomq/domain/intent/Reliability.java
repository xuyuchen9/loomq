package com.loomq.domain.intent;

/**
 * SLO-driven reliability level.
 *
 * Maps directly to AckMode:
 * - AT_MOST_ONCE → ASYNC
 * - AT_LEAST_ONCE → DURABLE
 */
public enum Reliability {

    AT_MOST_ONCE,
    AT_LEAST_ONCE;

    public AckMode toAckMode() {
        return switch (this) {
            case AT_MOST_ONCE -> AckMode.ASYNC;
            case AT_LEAST_ONCE -> AckMode.DURABLE;
        };
    }
}

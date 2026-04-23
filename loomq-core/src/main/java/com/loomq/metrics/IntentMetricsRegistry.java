package com.loomq.metrics;

import java.util.concurrent.atomic.LongAdder;

/**
 * Intent 指标注册表。
 *
 * 负责 intent 创建、完成、取消、过期和死信计数。
 */
final class IntentMetricsRegistry {

    private final LongAdder intentsCreated = new LongAdder();
    private final LongAdder intentsCompleted = new LongAdder();
    private final LongAdder intentsCancelled = new LongAdder();
    private final LongAdder intentsExpired = new LongAdder();
    private final LongAdder intentsDeadLettered = new LongAdder();

    void incrementIntentsCreated() {
        intentsCreated.increment();
    }

    void incrementIntentsCompleted() {
        intentsCompleted.increment();
    }

    void incrementIntentsCancelled() {
        intentsCancelled.increment();
    }

    void incrementIntentsExpired() {
        intentsExpired.increment();
    }

    void incrementIntentsDeadLettered() {
        intentsDeadLettered.increment();
    }

    long getIntentsCreated() {
        return intentsCreated.sum();
    }

    long getIntentsCompleted() {
        return intentsCompleted.sum();
    }

    long getIntentsCancelled() {
        return intentsCancelled.sum();
    }

    long getIntentsExpired() {
        return intentsExpired.sum();
    }

    long getIntentsDeadLettered() {
        return intentsDeadLettered.sum();
    }

    void reset() {
        intentsCreated.reset();
        intentsCompleted.reset();
        intentsCancelled.reset();
        intentsExpired.reset();
        intentsDeadLettered.reset();
    }
}

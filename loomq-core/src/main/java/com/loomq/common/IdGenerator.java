package com.loomq.common;

import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ID生成器
 * 格式: t_{timestamp}_{sequence}_{random}
 */
public class IdGenerator {
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final AtomicLong SEQUENCE = new AtomicLong(0);

    private static final long MAX_SEQUENCE = 999999L;

    /**
     * 生成 Intent ID
     * 格式: t_{timestamp}_{sequence}_{random}
     */
    public static String generateIntentId() {
        long timestamp = System.currentTimeMillis();
        long seq = SEQUENCE.updateAndGet(current -> current >= MAX_SEQUENCE ? 0 : current + 1);
        int random = RANDOM.nextInt(1000);
        return String.format("t_%d_%06d_%03d", timestamp, seq, random);
    }

    /**
     * 兼容旧调用点的任务 ID 生成入口。
     *
     * @deprecated use {@link #generateIntentId()} instead
     */
    @Deprecated(forRemoval = false)
    public static String generateTaskId() {
        return generateIntentId();
    }

    /**
     * 生成事件ID
     * 格式: e_{timestamp}_{sequence}_{random}
     */
    public static String generateEventId() {
        long timestamp = System.currentTimeMillis();
        long seq = SEQUENCE.updateAndGet(current -> current >= MAX_SEQUENCE ? 0 : current + 1);
        int random = RANDOM.nextInt(1000);
        return String.format("e_%d_%06d_%03d", timestamp, seq, random);
    }

    /**
     * 生成幂等键
     * 格式: req_{timestamp}_{random}
     */
    public static String generateIdempotencyKey() {
        long timestamp = System.currentTimeMillis();
        int random = RANDOM.nextInt(100000);
        return String.format("req_%d_%05d", timestamp, random);
    }
}

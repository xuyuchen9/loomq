package com.loomq.wal;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 自适应刷盘策略
 *
 * 核心设计：
 * 1. 基于字节阈值触发（高吞吐场景）
 * 2. 基于时间间隔触发（延迟敏感场景）
 * 3. 自适应等待（动态调整批量大小）
 *
 * @author loomq
 * @since v0.6.0
 */
public class AdaptiveFlushStrategy {

    // 配置参数
    private final long flushThresholdBytes;
    private final long flushIntervalNs;
    private final int minBatchSize;

    // 自适应参数（动态调整）
    private volatile long adaptiveWaitNs;
    private final long minWaitNs;
    private final long maxWaitNs;

    // 运行时状态
    private volatile long lastFlushTime;
    private volatile int lastBatchSize;
    private volatile long lastFlushLatencyNs;

    // 统计（用于监控）
    private final AtomicLong thresholdTriggers = new AtomicLong(0);
    private final AtomicLong intervalTriggers = new AtomicLong(0);
    private final AtomicLong adaptiveTriggers = new AtomicLong(0);

    /**
     * 刷盘决策枚举
     */
    public enum FlushDecision {
        IMMEDIATE_THRESHOLD,    // 字节阈值触发
        IMMEDIATE_INTERVAL,     // 时间间隔触发
        IMMEDIATE_ADAPTIVE,     // 自适应触发
        WAIT                    // 继续等待
    }

    /**
     * 创建自适应刷盘策略
     *
     * @param flushThresholdBytes 刷盘字节阈值
     * @param flushIntervalNs 刷盘时间间隔（纳秒）
     * @param minBatchSize 最小批量大小
     */
    public AdaptiveFlushStrategy(long flushThresholdBytes, long flushIntervalNs, int minBatchSize) {
        this.flushThresholdBytes = flushThresholdBytes;
        this.flushIntervalNs = flushIntervalNs;
        this.minBatchSize = minBatchSize;

        // 初始等待时间为间隔的 1/10
        this.adaptiveWaitNs = flushIntervalNs / 10;
        this.minWaitNs = 100_000;          // 100µs
        this.maxWaitNs = flushIntervalNs;   // 最大等于间隔
        this.lastFlushTime = System.nanoTime();
    }

    /**
     * 刷盘决策
     *
     * @param pendingBytes 待刷盘字节数
     * @param pendingCount 待刷盘请求数（估算）
     * @param waiterCount 等待者数量
     * @return 刷盘决策结果
     */
    public FlushDecision shouldFlush(long pendingBytes, int pendingCount, int waiterCount) {
        long now = System.nanoTime();
        long elapsed = now - lastFlushTime;

        // 1. 字节阈值触发（高吞吐场景）
        if (pendingBytes >= flushThresholdBytes) {
            thresholdTriggers.incrementAndGet();
            return FlushDecision.IMMEDIATE_THRESHOLD;
        }

        // 2. 批量大小触发
        if (pendingCount >= minBatchSize) {
            adaptiveTriggers.incrementAndGet();
            return FlushDecision.IMMEDIATE_ADAPTIVE;
        }

        // 3. 有等待者时立即刷盘（避免死锁，优先保证正确性）
        if (waiterCount > 0 && pendingBytes > 0) {
            adaptiveTriggers.incrementAndGet();
            return FlushDecision.IMMEDIATE_ADAPTIVE;
        }

        // 4. 时间间隔触发（延迟敏感场景）
        if (elapsed >= flushIntervalNs) {
            intervalTriggers.incrementAndGet();
            return FlushDecision.IMMEDIATE_INTERVAL;
        }

        // 5. 自适应等待触发
        if (elapsed >= adaptiveWaitNs && pendingBytes > 0) {
            adaptiveTriggers.incrementAndGet();
            return FlushDecision.IMMEDIATE_ADAPTIVE;
        }

        // 6. 继续等待
        return FlushDecision.WAIT;
    }

    /**
     * 记录刷盘完成，更新自适应参数
     *
     * @param batchSize 批量大小
     * @param latencyNs 刷盘延迟（纳秒）
     */
    public void recordFlush(int batchSize, long latencyNs) {
        this.lastBatchSize = batchSize;
        this.lastFlushLatencyNs = latencyNs;
        this.lastFlushTime = System.nanoTime();

        // 简化的自适应逻辑
        // 目标：批量大小稳定在 200-500，延迟 < 1ms
        if (batchSize < 100) {
            // 批量太小，增加等待时间以累积更多
            adaptiveWaitNs = Math.min(maxWaitNs, adaptiveWaitNs * 3 / 2);
        } else if (batchSize > 500) {
            // 批量太大，减少等待时间以降低延迟
            adaptiveWaitNs = Math.max(minWaitNs, adaptiveWaitNs * 2 / 3);
        } else if (latencyNs > 1_000_000) {
            // 延迟高（> 1ms），减少等待时间
            adaptiveWaitNs = Math.max(minWaitNs, adaptiveWaitNs / 2);
        } else if (latencyNs < 100_000 && batchSize > 200) {
            // 延迟低且批量适中，可以增加等待时间
            adaptiveWaitNs = Math.min(maxWaitNs, adaptiveWaitNs * 5 / 4);
        }
    }

    /**
     * 获取自适应等待时间（纳秒）
     */
    public long getAdaptiveWaitNs() {
        return adaptiveWaitNs;
    }

    /**
     * 获取最后一次刷盘时间
     */
    public long getLastFlushTime() {
        return lastFlushTime;
    }

    /**
     * 获取最后一次批量大小
     */
    public int getLastBatchSize() {
        return lastBatchSize;
    }

    /**
     * 获取最后一次刷盘延迟（微秒）
     */
    public long getLastFlushLatencyUs() {
        return lastFlushLatencyNs / 1000;
    }

    /**
     * 获取阈值触发次数
     */
    public long getThresholdTriggers() {
        return thresholdTriggers.get();
    }

    /**
     * 获取间隔触发次数
     */
    public long getIntervalTriggers() {
        return intervalTriggers.get();
    }

    /**
     * 获取自适应触发次数
     */
    public long getAdaptiveTriggers() {
        return adaptiveTriggers.get();
    }
}

package com.loomq.domain.intent;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * 精度档位枚举
 *
 * 定义调度器检查到期任务的频率，直接影响唤醒延迟与 CPU 开销。
 *
 * @author loomq
 * @since v0.5.1
 */
public enum PrecisionTier {

    /**
     * 极速档位
     * 唤醒精度：≤10ms
     * Bucket 扫描间隔：10ms
     * 最大并发：100
     * 批量大小：1（禁用批量）
     * 适用场景：分布式锁续期、实时回调
     */
    ULTRA(10, 50, 1, 0, 8),

    /**
     * 快速档位
     * 唤醒精度：≤50ms
     * Bucket 扫描间隔：50ms
     * 最大并发：50
     * 批量大小：1（禁用批量）
     * 适用场景：消息重试、较短间隔指数退避
     */
    FAST(50, 50, 1, 0, 6),

    /**
     * 高精档位
     * 唤醒精度：≤100ms
     * Bucket 扫描间隔：100ms
     * 最大并发：50
     * 批量大小：5
     * 适用场景：默认档位，兼容历史行为
     */
    HIGH(100, 50, 5, 50, 4),

    /**
     * 标准档位
     * 唤醒精度：≤500ms
     * Bucket 扫描间隔：500ms
     * 最大并发：50
     * 批量大小：20
     * 适用场景：生产推荐，订单超时、数据清理
     */
    STANDARD(500, 50, 20, 100, 3),

    /**
     * 经济档位
     * 唤醒精度：≤1000ms
     * Bucket 扫描间隔：1000ms
     * 最大并发：50
     * 批量大小：25（调优后，确保 ≤ 并发数/2 避免背压）
     * 批量窗口：300ms（调优后）
     * 适用场景：海量长延时任务，追求极致吞吐
     */
    ECONOMY(1000, 50, 25, 300, 2);

    /**
     * 精度窗口（毫秒）
     * 定义 Bucket 扫描间隔和最大唤醒误差
     */
    private final long precisionWindowMs;

    /**
     * 最大并发数
     * 该档位同时处理的最大任务数
     */
    private final int maxConcurrency;

    /**
     * 批量大小
     * 内部队列攒批的最大数量（1表示禁用批量）
     */
    private final int batchSize;

    /**
     * 批量窗口（毫秒）
     * 等待凑批的最大时间
     */
    private final int batchWindowMs;

    /**
     * 批量消费者数量
     * 该档位的批量消费线程数
     */
    private final int consumerCount;

    PrecisionTier(long precisionWindowMs, int maxConcurrency, int batchSize,
                  int batchWindowMs, int consumerCount) {
        this.precisionWindowMs = precisionWindowMs;
        this.maxConcurrency = maxConcurrency;
        this.batchSize = batchSize;
        this.batchWindowMs = batchWindowMs;
        this.consumerCount = consumerCount;
    }

    /**
     * 获取精度窗口（毫秒）
     *
     * @return 精度窗口，单位毫秒
     */
    public long getPrecisionWindowMs() {
        return precisionWindowMs;
    }

    /**
     * 获取最大并发数
     *
     * @return 最大并发任务数
     */
    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    /**
     * 获取批量大小
     *
     * @return 批量大小（1表示禁用批量）
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * 获取批量窗口（毫秒）
     *
     * @return 批量等待时间
     */
    public int getBatchWindowMs() {
        return batchWindowMs;
    }

    /**
     * 获取消费者数量
     *
     * @return 批量消费者线程数
     */
    public int getConsumerCount() {
        return consumerCount;
    }

    /**
     * 是否启用批量投递
     *
     * @return true 如果 batchSize > 1
     */
    public boolean isBatchEnabled() {
        return batchSize > 1;
    }

    /**
     * JSON 序列化值
     */
    @JsonValue
    public String toJson() {
        return name();
    }

    /**
     * JSON 反序列化
     * 不区分大小写，未知值返回默认 STANDARD
     *
     * @param value 字符串值
     * @return 精度档位，默认 STANDARD
     */
    @JsonCreator
    public static PrecisionTier fromString(String value) {
        if (value == null || value.isBlank()) {
            return STANDARD;
        }
        try {
            return PrecisionTier.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            return STANDARD;
        }
    }
}

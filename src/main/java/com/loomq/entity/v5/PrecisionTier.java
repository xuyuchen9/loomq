package com.loomq.entity.v5;

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
     * 适用场景：分布式锁续期、实时回调
     */
    ULTRA(10),

    /**
     * 快速档位
     * 唤醒精度：≤50ms
     * Bucket 扫描间隔：50ms
     * 适用场景：消息重试、较短间隔指数退避
     */
    FAST(50),

    /**
     * 高精档位
     * 唤醒精度：≤100ms
     * Bucket 扫描间隔：100ms
     * 适用场景：默认档位，兼容历史行为
     */
    HIGH(100),

    /**
     * 标准档位
     * 唤醒精度：≤500ms
     * Bucket 扫描间隔：500ms
     * 适用场景：生产推荐，订单超时、数据清理
     */
    STANDARD(500),

    /**
     * 经济档位
     * 唤醒精度：≤1000ms
     * Bucket 扫描间隔：1000ms
     * 适用场景：海量长延时任务，极低 CPU 占用
     */
    ECONOMY(1000);

    /**
     * 精度窗口（毫秒）
     * 定义 Bucket 扫描间隔和最大唤醒误差
     */
    private final long precisionWindowMs;

    PrecisionTier(long precisionWindowMs) {
        this.precisionWindowMs = precisionWindowMs;
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

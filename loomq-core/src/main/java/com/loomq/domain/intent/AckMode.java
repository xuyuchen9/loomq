package com.loomq.domain.intent;

/**
 * ACK 确认级别。
 *
 * 定义 Intent 创建后的持久化和可靠性保证级别，同时用于 WAL 二进制编码。
 *
 * @author loomq
 */
public enum AckMode {

    /**
     * 异步确认：进入内存队列即返回。
     * 延迟最低（<1ms），可靠性最低（进程崩溃可能丢失）。
     * 适用场景：低价值任务，可接受少量丢失。
     */
    ASYNC((byte) 0x01),

    /**
     * 持久化确认：等待 WAL fsync 后返回。
     * 延迟中等（1-10ms），主机级不丢数据。
     * 适用场景：一般任务，平衡性能和可靠性。推荐用于生产环境。
     */
    DURABLE((byte) 0x02),

    /**
     * 副本确认：等待 primary fsync + replica 持久化确认后返回。
     * 延迟最高（5-50ms），单点故障不丢数据。
     * 适用场景：关键任务，不能丢失。需要集群模式支持。
     */
    REPLICATED((byte) 0x03);

    private final byte code;

    AckMode(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    /** 是否需要等待本地 WAL fsync */
    public boolean requiresLocalSync() {
        return this == DURABLE || this == REPLICATED;
    }

    /** 是否需要等待 replica 确认 */
    public boolean requiresReplicaAck() {
        return this == REPLICATED;
    }

    /** 获取 SLA 描述 */
    public String getSlaDescription() {
        return switch (this) {
            case ASYNC -> "Low latency, may lose data on crash";
            case DURABLE -> "Host-level durability, may lose if replica fails";
            case REPLICATED -> "Highest durability, single-point-failure safe";
        };
    }

    public static AckMode fromCode(byte code) {
        for (AckMode level : values()) {
            if (level.code == code) {
                return level;
            }
        }
        throw new IllegalArgumentException("Unknown ack mode code: " + code);
    }
}

package com.loomq.replication;

/**
 * ACK 级别枚举
 *
 * 对应三种可靠性级别：
 * - ASYNC: 进入内存队列即返回，性能最高，可能丢数据
 * - DURABLE: primary WAL fsync 后返回，主机级不丢数据
 * - REPLICATED: primary fsync + replica 确认后返回，最高可靠性
 *
 * @author loomq
 * @since v0.4.8
 */
public enum AckLevel {

    /**
     * 异步确认：进入内存队列即返回
     * - 延迟：最低（<1ms）
     * - 可靠性：最低（进程崩溃可能丢失）
     * - 适用场景：低价值任务，可接受少量丢失
     */
    ASYNC((byte) 0x01),

    /**
     * 持久化确认：primary WAL fsync 后返回
     * - 延迟：中等（取决于磁盘 IO，通常 1-10ms）
     * - 可靠性：主机级（主机崩溃不丢，但 replica 故障时可能丢）
     * - 适用场景：一般任务，平衡性能和可靠性
     */
    DURABLE((byte) 0x02),

    /**
     * 复制确认：primary fsync + replica 持久化确认后返回
     * - 延迟：最高（取决于网络 + 双写 IO，通常 5-50ms）
     * - 可靠性：最高（单点故障不丢数据）
     * - 适用场景：关键任务，不能丢失
     */
    REPLICATED((byte) 0x03);

    private final byte code;

    AckLevel(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    /**
     * 是否需要等待本地 WAL fsync
     */
    public boolean requiresLocalSync() {
        return this == DURABLE || this == REPLICATED;
    }

    /**
     * 是否需要等待 replica 确认
     */
    public boolean requiresReplicaAck() {
        return this == REPLICATED;
    }

    /**
     * 获取 SLA 描述
     */
    public String getSlaDescription() {
        return switch (this) {
            case ASYNC -> "Low latency, may lose data on crash";
            case DURABLE -> "Host-level durability, may lose if replica fails";
            case REPLICATED -> "Highest durability, single-point-failure safe";
        };
    }

    public static AckLevel fromCode(byte code) {
        for (AckLevel level : values()) {
            if (level.code == code) {
                return level;
            }
        }
        throw new IllegalArgumentException("Unknown ack level code: " + code);
    }
}

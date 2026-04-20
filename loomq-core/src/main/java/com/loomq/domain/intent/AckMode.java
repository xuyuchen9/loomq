package com.loomq.domain.intent;

/**
 * WAL 确认级别
 *
 * 定义意图创建后的持久化保证级别
 *
 * @author loomq
 * @since v0.7.0
 */
public enum AckMode {
    /**
     * 异步确认
     * 写入内存即返回，不等待刷盘
     * RPO < 100ms（最后 100ms 数据可能丢失）
     * 最高性能，适合可容忍少量丢失的场景
     */
    ASYNC,

    /**
     * 持久化确认
     * 等待 WAL fsync 后返回
     * RPO = 0（已确认数据不会丢失）
     * 性能与可靠性平衡，推荐用于生产环境
     */
    DURABLE,

    /**
     * 副本确认（预留）
     * 等待副本 ACK 后返回
     * RPO = 0 且具备高可用
     * 需要集群模式支持，当前版本预留
     */
    REPLICATED
}

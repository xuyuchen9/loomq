package com.loomq.domain.intent;

/**
 * WAL 持久化策略。
 *
 * 与精度档位解耦后，用户可在创建 Intent 时显式指定，
 * 未指定则使用精度档位的默认值。
 */
public enum WalMode {
    /** 内存映射写入，不等待 fsync。延迟最低，崩溃可能丢失最近写入。 */
    ASYNC,
    /** 异步写入 + 周期性批量 fsync。折中方案。 */
    BATCH_DEFERRED,
    /** 写入后立即 fsync 再返回。最强持久化，延迟最高。 */
    DURABLE
}

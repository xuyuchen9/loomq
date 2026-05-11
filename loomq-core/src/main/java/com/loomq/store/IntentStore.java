package com.loomq.store;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;

import java.util.Map;

/**
 * Intent 存储接口。
 *
 * 内核通过此接口访问 Intent 存储，实现可替换（内存、RocksDB 等）。
 * 默认实现为 ConcurrentIntentStore（基于 ConcurrentHashMap）。
 *
 * @author loomq
 * @since v0.9.0
 */
public interface IntentStore {

    /** 保存新 Intent */
    void save(Intent intent);

    /** 更新已存在的 Intent */
    void update(Intent intent);

    /** 按 ID 查找 Intent */
    Intent findById(String intentId);

    /** 删除 Intent */
    void delete(String intentId);

    /** 获取所有 Intent（恢复/快照用，大数据量场景慎用） */
    Map<String, Intent> getAllIntents();

    /** 按状态统计数量 */
    long countByStatus(IntentStatus status);

    /** 幂等性检查 */
    IdempotencyResult checkIdempotency(String idempotencyKey);

    /** 待处理 Intent 数量 */
    long getPendingCount();

    /** 关闭存储，释放资源 */
    void shutdown();
}

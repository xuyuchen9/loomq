package com.loomq.store;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Intent 存储
 *
 * 支持幂等性检查和状态管理
 *
 * @author loomq
 * @since v0.5.0
 */
public class IntentStore {

    private static final Logger logger = LoggerFactory.getLogger(IntentStore.class);

    private final Map<String, Intent> intents = new ConcurrentHashMap<>();
    private final Map<String, IdempotencyRecord> idempotencyRecords = new ConcurrentHashMap<>();

    private final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "idempotency-cleanup");
        t.setDaemon(true);
        return t;
    });

    public IntentStore() {
        // 每小时清理过期幂等记录
        cleanupExecutor.scheduleAtFixedRate(this::cleanupExpiredRecords, 1, 1, TimeUnit.HOURS);
    }

    /**
     * 保存 Intent
     */
    public void save(Intent intent) {
        intents.put(intent.getIntentId(), intent);

        // 保存幂等记录
        if (intent.getIdempotencyKey() != null) {
            IdempotencyRecord record = IdempotencyRecord.fromIntent(intent);
            idempotencyRecords.put(intent.getIdempotencyKey(), record);
        }

        logger.debug("Intent saved: id={}, status={}", intent.getIntentId(), intent.getStatus());
    }

    /**
     * 更新 Intent
     */
    public void update(Intent intent) {
        intents.put(intent.getIntentId(), intent);

        // 更新幂等记录状态
        if (intent.getIdempotencyKey() != null) {
            IdempotencyRecord record = idempotencyRecords.get(intent.getIdempotencyKey());
            if (record != null) {
                record.updateStatus(intent.getStatus());
            }
        }

        logger.debug("Intent updated: id={}, status={}", intent.getIntentId(), intent.getStatus());
    }

    /**
     * 根据 ID 查找 Intent
     */
    public Intent findById(String intentId) {
        return intents.get(intentId);
    }

    /**
     * 检查幂等性
     *
     * @param idempotencyKey 业务幂等键
     * @return 幂等性检查结果
     */
    public IdempotencyResult checkIdempotency(String idempotencyKey) {
        IdempotencyRecord record = idempotencyRecords.get(idempotencyKey);

        if (record == null) {
            // 幂等键不存在
            return IdempotencyResult.newRequest();
        }

        if (!record.isInWindow()) {
            // 窗口期外，视为新业务请求
            logger.debug("Idempotency window expired for key={}, treat as new request", idempotencyKey);
            return IdempotencyResult.windowExpired();
        }

        // 窗口期内，根据状态判断
        Intent intent = intents.get(record.getIntentId());
        if (intent == null) {
            // Intent 已删除，清理幂等记录
            idempotencyRecords.remove(idempotencyKey);
            return IdempotencyResult.newRequest();
        }

        if (intent.getStatus().isTerminal()) {
            // 终态，返回 409
            return IdempotencyResult.duplicateTerminal(intent);
        } else {
            // 非终态，返回已存在 Intent
            return IdempotencyResult.duplicateActive(intent);
        }
    }

    /**
     * 删除 Intent
     */
    public void delete(String intentId) {
        Intent intent = intents.remove(intentId);
        if (intent != null && intent.getIdempotencyKey() != null) {
            idempotencyRecords.remove(intent.getIdempotencyKey());
        }
    }

    /**
     * 获取所有 Intent（调试用）
     */
    public Map<String, Intent> getAllIntents() {
        return Map.copyOf(intents);
    }

    /**
     * 获取指定状态的 Intent 数量
     */
    public long countByStatus(IntentStatus status) {
        return intents.values().stream()
            .filter(i -> i.getStatus() == status)
            .count();
    }

    /**
     * 获取待处理 Intent 数量
     */
    public long getPendingCount() {
        return intents.values().stream()
            .filter(i -> !i.getStatus().isTerminal())
            .count();
    }

    /**
     * 清理过期幂等记录
     */
    private void cleanupExpiredRecords() {
        Instant now = Instant.now();
        int cleaned = 0;

        var iterator = idempotencyRecords.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (entry.getValue().isExpired()) {
                iterator.remove();
                cleaned++;
            }
        }

        if (cleaned > 0) {
            logger.info("Cleaned {} expired idempotency records", cleaned);
        }
    }

    /**
     * 关闭存储
     */
    public void shutdown() {
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

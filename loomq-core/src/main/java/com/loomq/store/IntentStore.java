package com.loomq.store;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Objects;
import java.util.EnumMap;

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

    private final Map<String, StoredIntent> intents = new ConcurrentHashMap<>();
    private final Map<String, IdempotencyRecord> idempotencyRecords = new ConcurrentHashMap<>();
    private final Map<IntentStatus, AtomicLong> statusCounts = new EnumMap<>(IntentStatus.class);
    private final AtomicLong pendingCount = new AtomicLong();

    private final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "idempotency-cleanup");
        t.setDaemon(true);
        return t;
    });

    public IntentStore() {
        for (IntentStatus status : IntentStatus.values()) {
            statusCounts.put(status, new AtomicLong());
        }

        // 每小时清理过期幂等记录
        cleanupExecutor.scheduleAtFixedRate(this::cleanupExpiredRecords, 1, 1, TimeUnit.HOURS);
    }

    /**
     * 保存 Intent
     */
    public void save(Intent intent) {
        upsert(intent);

        logger.debug("Intent saved: id={}, status={}", intent.getIntentId(), intent.getStatus());
    }

    /**
     * 更新 Intent
     */
    public void update(Intent intent) {
        upsert(intent);

        logger.debug("Intent updated: id={}, status={}", intent.getIntentId(), intent.getStatus());
    }

    /**
     * 根据 ID 查找 Intent
     */
    public Intent findById(String intentId) {
        StoredIntent stored = intents.get(intentId);
        return stored != null ? stored.intent() : null;
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
        StoredIntent stored = intents.get(record.getIntentId());
        if (stored == null) {
            // Intent 已删除，清理幂等记录
            idempotencyRecords.remove(idempotencyKey);
            return IdempotencyResult.newRequest();
        }

        Intent intent = stored.intent();
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
        StoredIntent removed = intents.remove(intentId);
        if (removed != null) {
            decrementStatus(removed.status());
            String idempotencyKey = removed.intent().getIdempotencyKey();
            if (idempotencyKey != null) {
                idempotencyRecords.remove(idempotencyKey);
            }
        }
    }

    /**
     * 获取所有 Intent（调试用）
     */
    public Map<String, Intent> getAllIntents() {
        Map<String, Intent> snapshot = new HashMap<>(intents.size());
        intents.forEach((id, stored) -> snapshot.put(id, stored.intent()));
        return Map.copyOf(snapshot);
    }

    /**
     * 获取指定状态的 Intent 数量
     */
    public long countByStatus(IntentStatus status) {
        AtomicLong counter = statusCounts.get(status);
        return counter != null ? counter.get() : 0L;
    }

    /**
     * 获取待处理 Intent 数量
     */
    public long getPendingCount() {
        return pendingCount.get();
    }

    /**
     * 清理过期幂等记录
     */
    private void cleanupExpiredRecords() {
        AtomicLong cleaned = new AtomicLong();
        idempotencyRecords.entrySet().removeIf(entry -> {
            if (entry.getValue().isExpired()) {
                cleaned.incrementAndGet();
                return true;
            }
            return false;
        });

        if (cleaned.get() > 0) {
            logger.info("Cleaned {} expired idempotency records", cleaned.get());
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

    private void upsert(Intent intent) {
        intents.compute(intent.getIntentId(), (id, existing) -> {
            IntentStatus previousStatus = existing != null ? existing.status() : null;
            String previousIdempotencyKey = existing != null ? existing.intent().getIdempotencyKey() : null;

            if (existing != null) {
                decrementStatus(previousStatus);
                if (previousIdempotencyKey != null && !Objects.equals(previousIdempotencyKey, intent.getIdempotencyKey())) {
                    idempotencyRecords.remove(previousIdempotencyKey);
                }
            }

            StoredIntent stored = new StoredIntent(intent);
            incrementStatus(stored.status());

            String idempotencyKey = intent.getIdempotencyKey();
            if (idempotencyKey != null) {
                idempotencyRecords.put(idempotencyKey, IdempotencyRecord.fromIntent(intent));
            }

            return stored;
        });
    }

    private void incrementStatus(IntentStatus status) {
        if (status == null) {
            return;
        }
        AtomicLong counter = statusCounts.get(status);
        if (counter != null) {
            counter.incrementAndGet();
        }
        if (!status.isTerminal()) {
            pendingCount.incrementAndGet();
        }
    }

    private void decrementStatus(IntentStatus status) {
        if (status == null) {
            return;
        }
        AtomicLong counter = statusCounts.get(status);
        if (counter != null) {
            counter.decrementAndGet();
        }
        if (!status.isTerminal()) {
            pendingCount.decrementAndGet();
        }
    }

    private record StoredIntent(Intent intent, IntentStatus status) {
        private StoredIntent(Intent intent) {
            this(intent, intent.getStatus());
        }
    }
}

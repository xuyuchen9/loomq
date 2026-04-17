package com.loomq.application.scheduler;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 精度桶组
 *
 * 使用 ConcurrentSkipListMap 存储待触发任务，按时间窗口分组。
 * 支持 O(log n) 插入和范围删除，无全局锁。
 *
 * @author loomq
 * @since v0.5.1
 */
public class BucketGroup {

    private static final Logger logger = LoggerFactory.getLogger(BucketGroup.class);

    /**
     * 精度档位
     */
    private final PrecisionTier tier;

    /**
     * 精度窗口（毫秒）
     */
    private final long precisionWindowMs;

    /**
     * 时间桶存储
     * Key: 执行时间戳按精度窗口向下取整
     * Value: 该时间窗口内的 Intent 列表
     */
    private final ConcurrentSkipListMap<Long, List<Intent>> buckets;

    /**
     * 桶操作锁（仅用于 List 操作，Map 本身是线程安全的）
     */
    private final ReentrantLock bucketLock;

    /**
     * 构造函数
     *
     * @param tier 精度档位
     */
    public BucketGroup(PrecisionTier tier) {
        this.tier = tier;
        this.precisionWindowMs = tier.getPrecisionWindowMs();
        this.buckets = new ConcurrentSkipListMap<>();
        this.bucketLock = new ReentrantLock();
    }

    /**
     * 添加 Intent 到对应时间桶
     *
     * @param intent    Intent 实例
     * @param executeAt 执行时间
     */
    public void add(Intent intent, Instant executeAt) {
        long bucketKey = floorToBucket(executeAt.toEpochMilli());

        buckets.compute(bucketKey, (key, list) -> {
            if (list == null) {
                list = new ArrayList<>();
            }
            // 使用 synchronized 保护 ArrayList 操作
            synchronized (list) {
                list.add(intent);
            }
            return list;
        });

        if (logger.isTraceEnabled()) {
            logger.trace("Added intent {} to bucket {} for tier {}",
                intent.getIntentId(), bucketKey, tier);
        }
    }

    /**
     * 扫描并获取所有到期任务
     *
     * @param now 当前时间
     * @return 到期的 Intent 列表
     */
    public List<Intent> scanDue(Instant now) {
        long currentBucketKey = floorToBucket(now.toEpochMilli());

        // 获取所有 <= 当前时间桶的条目
        NavigableMap<Long, List<Intent>> dueBuckets = buckets.headMap(currentBucketKey, true);

        if (dueBuckets.isEmpty()) {
            return Collections.emptyList();
        }

        List<Intent> dueIntents = new ArrayList<>();

        // 遍历到期桶，收集任务
        for (Long bucketKey : new ArrayList<>(dueBuckets.keySet())) {
            List<Intent> bucketIntents = buckets.remove(bucketKey);
            if (bucketIntents != null) {
                // 使用 synchronized 保护列表遍历
                synchronized (bucketIntents) {
                    // 过滤真正到期的任务（考虑精度窗口内的实际执行时间）
                    for (Intent intent : bucketIntents) {
                        if (!intent.getExecuteAt().isAfter(now)) {
                            dueIntents.add(intent);
                        } else {
                            // 未到期的任务重新入桶
                            add(intent, intent.getExecuteAt());
                        }
                    }
                }
            }
        }

        if (!dueIntents.isEmpty() && logger.isDebugEnabled()) {
            logger.debug("Scanned {} due intents from tier {} buckets", dueIntents.size(), tier);
        }

        return dueIntents;
    }

    /**
     * 计算休眠时长
     *
     * @param delay    延迟时间（毫秒）
     * @return 休眠时长（毫秒），0 表示直接入桶
     */
    public long calculateSleepMs(long delay) {
        if (delay <= precisionWindowMs) {
            // 短延迟场景：跳过休眠，直接进入 Bucket
            return 0;
        }

        // 长延迟场景：引入随机抖动，避免海量任务同时苏醒
        long jitter = ThreadLocalRandom.current().nextLong(precisionWindowMs);
        return delay - precisionWindowMs - jitter;
    }

    /**
     * 将时间戳按精度窗口向下取整
     *
     * @param timestampMs 时间戳（毫秒）
     * @return 桶 Key
     */
    private long floorToBucket(long timestampMs) {
        return (timestampMs / precisionWindowMs) * precisionWindowMs;
    }

    /**
     * 获取当前桶数量
     *
     * @return 桶数量
     */
    public int getBucketCount() {
        return buckets.size();
    }

    /**
     * 获取当前等待任务总数
     *
     * @return 任务总数
     */
    public int getPendingCount() {
        return buckets.values().stream()
            .mapToInt(List::size)
            .sum();
    }

    /**
     * 清空所有桶
     */
    public void clear() {
        buckets.clear();
    }

    /**
     * 获取精度档位
     *
     * @return 精度档位
     */
    public PrecisionTier getTier() {
        return tier;
    }

    /**
     * 获取精度窗口（毫秒）
     *
     * @return 精度窗口
     */
    public long getPrecisionWindowMs() {
        return precisionWindowMs;
    }
}

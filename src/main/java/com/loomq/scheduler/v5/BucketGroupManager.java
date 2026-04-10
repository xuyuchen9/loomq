package com.loomq.scheduler.v5;

import com.loomq.entity.v5.Intent;
import com.loomq.entity.v5.PrecisionTier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * 精度桶组管理器
 *
 * 管理所有精度档位的 BucketGroup，提供统一的任务添加和扫描接口。
 *
 * @author loomq
 * @since v0.5.1
 */
public class BucketGroupManager {

    private static final Logger logger = LoggerFactory.getLogger(BucketGroupManager.class);

    /**
     * 按精度档位分组的 BucketGroup
     */
    private final Map<PrecisionTier, BucketGroup> bucketGroups;

    /**
     * 构造函数
     * 初始化所有精度档位的 BucketGroup
     */
    public BucketGroupManager() {
        this.bucketGroups = new EnumMap<>(PrecisionTier.class);

        for (PrecisionTier tier : PrecisionTier.values()) {
            bucketGroups.put(tier, new BucketGroup(tier));
        }

        logger.info("Initialized {} bucket groups for precision tiers", bucketGroups.size());
    }

    /**
     * 添加 Intent 到对应精度档位的桶
     *
     * @param intent Intent 实例
     */
    public void add(Intent intent) {
        PrecisionTier tier = intent.getPrecisionTier();
        BucketGroup group = bucketGroups.get(tier);

        if (group == null) {
            logger.warn("Unknown precision tier {}, using STANDARD", tier);
            group = bucketGroups.get(PrecisionTier.STANDARD);
        }

        group.add(intent, intent.getExecuteAt());
    }

    /**
     * 扫描指定精度档位的到期任务
     *
     * @param tier 精度档位
     * @param now  当前时间
     * @return 到期的 Intent 列表
     */
    public List<Intent> scanDue(PrecisionTier tier, Instant now) {
        BucketGroup group = bucketGroups.get(tier);
        if (group == null) {
            return List.of();
        }
        return group.scanDue(now);
    }

    /**
     * 扫描所有精度档位的到期任务
     *
     * @param now 当前时间
     * @return 按精度档位分组的到期任务
     */
    public Map<PrecisionTier, List<Intent>> scanAllDue(Instant now) {
        Map<PrecisionTier, List<Intent>> result = new EnumMap<>(PrecisionTier.class);

        for (Map.Entry<PrecisionTier, BucketGroup> entry : bucketGroups.entrySet()) {
            List<Intent> due = entry.getValue().scanDue(now);
            if (!due.isEmpty()) {
                result.put(entry.getKey(), due);
            }
        }

        return result;
    }

    /**
     * 获取指定精度档位的 BucketGroup
     *
     * @param tier 精度档位
     * @return BucketGroup
     */
    public BucketGroup getBucketGroup(PrecisionTier tier) {
        return bucketGroups.get(tier);
    }

    /**
     * 获取所有精度档位的等待任务数
     *
     * @return 按精度档位分组的任务数
     */
    public Map<PrecisionTier, Integer> getPendingCounts() {
        Map<PrecisionTier, Integer> counts = new EnumMap<>(PrecisionTier.class);

        for (Map.Entry<PrecisionTier, BucketGroup> entry : bucketGroups.entrySet()) {
            counts.put(entry.getKey(), entry.getValue().getPendingCount());
        }

        return counts;
    }

    /**
     * 获取总等待任务数
     *
     * @return 总任务数
     */
    public int getTotalPendingCount() {
        return bucketGroups.values().stream()
            .mapToInt(BucketGroup::getPendingCount)
            .sum();
    }

    /**
     * 清空所有桶
     */
    public void clear() {
        bucketGroups.values().forEach(BucketGroup::clear);
        logger.info("Cleared all bucket groups");
    }
}

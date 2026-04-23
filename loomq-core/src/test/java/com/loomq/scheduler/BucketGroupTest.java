package com.loomq.scheduler;

import com.loomq.application.scheduler.BucketGroup;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * BucketGroup 精度桶组测试
 *
 * @author loomq
 * @since v0.5.1
 */
class BucketGroupTest {

    private BucketGroup standardGroup;
    private BucketGroup economyGroup;

    @BeforeEach
    void setUp() {
        standardGroup = new BucketGroup(PrecisionTier.STANDARD);
        economyGroup = new BucketGroup(PrecisionTier.ECONOMY);
    }

    @Test
    @DisplayName("BucketGroup 初始化正确")
    void testInitialization() {
        assertEquals(PrecisionTier.STANDARD, standardGroup.getTier());
        assertEquals(500, standardGroup.getPrecisionWindowMs());

        assertEquals(PrecisionTier.ECONOMY, economyGroup.getTier());
        assertEquals(1000, economyGroup.getPrecisionWindowMs());

        assertEquals(0, standardGroup.getBucketCount());
        assertEquals(0, standardGroup.getPendingCount());
    }

    @Test
    @DisplayName("添加任务到桶")
    void testAddIntent() {
        Intent intent = createTestIntent(PrecisionTier.STANDARD, Instant.now().plusMillis(5000));
        standardGroup.add(intent, intent.getExecuteAt());

        assertEquals(1, standardGroup.getPendingCount());
        assertTrue(standardGroup.getBucketCount() >= 1);
    }

    @Test
    @DisplayName("扫描到期任务")
    void testScanDueIntents() {
        Instant now = Instant.now();
        Intent intent1 = createTestIntent(PrecisionTier.STANDARD, now.minusMillis(100));
        Intent intent2 = createTestIntent(PrecisionTier.STANDARD, now.plusMillis(1000));

        standardGroup.add(intent1, intent1.getExecuteAt());
        standardGroup.add(intent2, intent2.getExecuteAt());

        List<Intent> dueIntents = standardGroup.scanDue(now);

        assertEquals(1, dueIntents.size());
        assertEquals(intent1.getIntentId(), dueIntents.get(0).getIntentId());
    }

    @Test
    @DisplayName("扫描移除到期桶")
    void testScanRemovesDueBuckets() {
        Instant now = Instant.now();
        Intent intent = createTestIntent(PrecisionTier.STANDARD, now.minusMillis(1000));
        standardGroup.add(intent, intent.getExecuteAt());

        assertEquals(1, standardGroup.getPendingCount());

        standardGroup.scanDue(now);

        assertEquals(0, standardGroup.getPendingCount());
    }

    @Test
    @DisplayName("按 intentId 删除任务")
    void testRemoveIntent() {
        Instant executeAt = Instant.now().plusMillis(1000);
        Intent intent = createTestIntent(PrecisionTier.STANDARD, executeAt);
        standardGroup.add(intent, intent.getExecuteAt());

        assertTrue(standardGroup.remove(intent));
        assertEquals(0, standardGroup.getPendingCount());
        assertEquals(0, standardGroup.getBucketCount());
    }

    @Test
    @DisplayName("计算休眠时间 - 长延迟场景")
    void testCalculateSleepMsLongDelay() {
        // 长延迟场景：delay > precisionWindow
        long delay = 10000; // 10 秒
        long sleepMs = standardGroup.calculateSleepMs(delay);

        // sleepMs = delay - precisionWindow - jitter
        // jitter 是 [0, precisionWindow) 的随机值
        // 所以 sleepMs 应该在 [delay - 2*precisionWindow, delay - precisionWindow] 范围内
        long minSleep = delay - 2 * standardGroup.getPrecisionWindowMs();
        long maxSleep = delay - standardGroup.getPrecisionWindowMs();

        assertTrue(sleepMs >= minSleep, "sleepMs should be >= " + minSleep + " but was " + sleepMs);
        assertTrue(sleepMs <= maxSleep, "sleepMs should be <= " + maxSleep + " but was " + sleepMs);
    }

    @Test
    @DisplayName("计算休眠时间 - 短延迟场景")
    void testCalculateSleepMsShortDelay() {
        // 短延迟场景：delay <= precisionWindow
        long delay = 300; // 300ms < STANDARD 的 500ms
        long sleepMs = standardGroup.calculateSleepMs(delay);

        // 短延迟场景应该返回 0
        assertEquals(0, sleepMs);
    }

    @Test
    @DisplayName("计算休眠时间 - 边界值")
    void testCalculateSleepMsBoundary() {
        // delay == precisionWindow
        long delay = standardGroup.getPrecisionWindowMs();
        long sleepMs = standardGroup.calculateSleepMs(delay);

        assertEquals(0, sleepMs);
    }

    @Test
    @DisplayName("抖动随机性验证")
    void testJitterRandomness() {
        long delay = 10000;
        long firstSleep = standardGroup.calculateSleepMs(delay);
        long secondSleep = standardGroup.calculateSleepMs(delay);

        // 由于抖动的随机性，两次计算的休眠时间可能不同
        // 但都在有效范围内
        long minSleep = delay - 2 * standardGroup.getPrecisionWindowMs();
        long maxSleep = delay - standardGroup.getPrecisionWindowMs();

        assertTrue(firstSleep >= minSleep && firstSleep <= maxSleep);
        assertTrue(secondSleep >= minSleep && secondSleep <= maxSleep);
    }

    @Test
    @DisplayName("不同精度档位有不同的休眠计算")
    void testDifferentPrecisionTiers() {
        long delay = 10000;

        long standardSleep = standardGroup.calculateSleepMs(delay);
        long economySleep = economyGroup.calculateSleepMs(delay);

        // ECONOMY 的精度窗口更大，所以休眠时间范围不同
        // 但实际休眠时间由于随机抖动可能有重叠
        // 主要验证计算逻辑正确
        assertTrue(standardSleep > 0);
        assertTrue(economySleep > 0);
    }

    @Test
    @DisplayName("清空桶")
    void testClear() {
        Intent intent = createTestIntent(PrecisionTier.STANDARD, Instant.now().plusMillis(5000));
        standardGroup.add(intent, intent.getExecuteAt());

        assertTrue(standardGroup.getPendingCount() > 0);

        standardGroup.clear();

        assertEquals(0, standardGroup.getPendingCount());
        assertEquals(0, standardGroup.getBucketCount());
    }

    @Test
    @DisplayName("多个任务入同一桶")
    void testMultipleIntentsInSameBucket() {
        Instant baseTime = Instant.parse("2026-04-09T12:30:00.000Z");
        Intent intent1 = createTestIntent(PrecisionTier.STANDARD, baseTime.plusMillis(100));
        Intent intent2 = createTestIntent(PrecisionTier.STANDARD, baseTime.plusMillis(400));

        // 两个任务的执行时间在同一个精度窗口内（500ms）
        standardGroup.add(intent1, intent1.getExecuteAt());
        standardGroup.add(intent2, intent2.getExecuteAt());

        // 应该在同一个桶内
        assertEquals(1, standardGroup.getBucketCount());
        assertEquals(2, standardGroup.getPendingCount());
    }

    @Test
    @DisplayName("不同时间桶的任务分离")
    void testDifferentBuckets() {
        Instant baseTime = Instant.parse("2026-04-09T12:30:00.000Z");
        Intent intent1 = createTestIntent(PrecisionTier.STANDARD, baseTime.plusMillis(100));
        Intent intent2 = createTestIntent(PrecisionTier.STANDARD, baseTime.plusMillis(600));

        // 两个任务的执行时间跨越精度窗口边界（500ms）
        standardGroup.add(intent1, intent1.getExecuteAt());
        standardGroup.add(intent2, intent2.getExecuteAt());

        // 应该在不同的桶内
        assertEquals(2, standardGroup.getBucketCount());
        assertEquals(2, standardGroup.getPendingCount());
    }

    // ========== 辅助方法 ==========

    private Intent createTestIntent(PrecisionTier tier, Instant executeAt) {
        Intent intent = new Intent();
        intent.setPrecisionTier(tier);
        intent.setExecuteAt(executeAt);
        return intent;
    }
}

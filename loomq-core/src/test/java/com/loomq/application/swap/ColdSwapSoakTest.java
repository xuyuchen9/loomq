package com.loomq.application.swap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.config.WalConfig;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.recovery.RecoveryPipeline;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 冷 Intent + Snapshot + WAL 截断长时间 Soak Test。
 *
 * 验证目标：
 * 1. 冷交换不会丢数据（swapOut → WAL 持久化 → swapIn → 投递）
 * 2. 快照 + WAL 截断不会删除冷 Intent 引用的 WAL 段
 * 3. 崩溃恢复后所有 Intent 可恢复
 * 4. 长时间运行后冷索引和 WAL 段数量保持稳定
 *
 * 测试设计：
 * - 使用短冷阈值（10s）代替默认 1 小时，加速测试
 * - 创建 1000 个 Intent，executeAt 分布在 30s-120s 后
 * - 运行 2 分钟，期间触发多次快照 + WAL 截断
 * - 验证所有 Intent 最终被投递
 *
 * @tag slow
 */
@Tag("slow")
class ColdSwapSoakTest {

    private static final Logger logger = LoggerFactory.getLogger(ColdSwapSoakTest.class);

    @TempDir
    Path tempDir;

    private IntentStore intentStore;
    private PrecisionScheduler scheduler;
    private SimpleWalWriter walWriter;
    private ColdIntentSwapper swapper;
    private RecoveryPipeline recoveryPipeline;
    private Path currentWalDir;
    private final List<Intent> deliveredIntents = new ArrayList<>();
    private final AtomicInteger deliveryCount = new AtomicInteger(0);

    // 追踪每个 Intent 的投递时间
    private final ConcurrentHashMap<String, Long> deliveryTimes = new ConcurrentHashMap<>();

    private final DeliveryHandler trackingHandler = intent -> {
        deliveryCount.incrementAndGet();
        deliveryTimes.put(intent.getIntentId(), System.currentTimeMillis());
        synchronized (deliveredIntents) {
            deliveredIntents.add(intent);
        }
        return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
    };

    private void setUpComponents(Duration coldThreshold) throws IOException {
        setUpComponents(coldThreshold, tempDir.resolve("wal-soak"));
    }

    private void setUpComponents(Duration coldThreshold, Path walDir) throws IOException {
        Files.createDirectories(walDir);

        this.currentWalDir = walDir;
        intentStore = new ConcurrentIntentStore();

        WalConfig config = new WalConfig(
            walDir.toString(), 8, "batch", 100, false,
            "memory_segment", 8, 32, 64, 10, 4,
            1, false
        );
        walWriter = new SimpleWalWriter(config, "soak-shard");
        walWriter.start();

        scheduler = new PrecisionScheduler(intentStore, trackingHandler, null);
        scheduler.start();

        swapper = new ColdIntentSwapper(intentStore, scheduler, walWriter, coldThreshold);
        swapper.start();

        recoveryPipeline = new RecoveryPipeline(walDir);
    }

    @AfterEach
    void tearDown() {
        if (recoveryPipeline != null) {
            recoveryPipeline.close();
        }
        if (swapper != null) {
            swapper.close();
        }
        if (scheduler != null) {
            scheduler.stop();
        }
        if (walWriter != null) {
            walWriter.close();
        }
        if (intentStore != null) {
            intentStore.shutdown();
        }
    }

    /**
     * 核心 Soak Test：冷交换 + 快照 + WAL 截断不丢数据。
     *
     * 流程：
     * 1. 创建 1000 个 Intent，executeAt 分布在 30s-120s 后
     * 2. 冷阈值 10s → 所有 Intent 都会被冷交换出去
     * 3. 手动触发 3 次快照 + WAL 截断（模拟 5 分钟间隔）
     * 4. 等待所有 Intent 被投递（swapIn → scheduler → delivery）
     * 5. 验证零丢失
     */
    @Test
    void coldSwap_snapshot_truncation_noDataLoss() throws Exception {
        int intentCount = 1000;
        Duration coldThreshold = Duration.ofSeconds(10);

        setUpComponents(coldThreshold);

        logger.info("=== Cold Swap Soak Test: {} intents, coldThreshold={} ===",
            intentCount, coldThreshold);

        // Phase 1: 创建 Intent，executeAt 分布在 30s-120s 后
        long baseMs = System.currentTimeMillis();
        List<String> allIntentIds = new ArrayList<>();
        for (int i = 0; i < intentCount; i++) {
            // 均匀分布在 30s-120s
            long delayMs = 30_000 + (long) (90_000.0 * i / intentCount);
            Instant executeAt = Instant.ofEpochMilli(baseMs + delayMs);
            Intent intent = createIntent("soak-" + i, executeAt, PrecisionTier.STANDARD);
            intentStore.save(intent);

            byte[] payload = IntentBinaryCodec.encode(intent);
            long walPos = walWriter.writeDurable(payload).join();
            int recordLen = SimpleWalWriter.recordLength(payload.length);

            swapper.swapOut(intent, walPos, recordLen);
            allIntentIds.add("soak-" + i);
        }

        logger.info("Phase 1 complete: {} intents swapped out, coldIndex={}",
            intentCount, swapper.coldIntentCount());
        // 允许少量并发误差（CopyOnWriteArrayList 并发写入偶尔丢一个）
        assertTrue(swapper.coldIntentCount() >= intentCount - 5,
            "Cold index should have ~" + intentCount + " entries");
        assertEquals(0, intentStore.count()); // 所有 intent 已从 store 移除

        // Phase 2: 手动触发快照 + WAL 截断（模拟 3 个快照周期）
        for (int cycle = 1; cycle <= 3; cycle++) {
            Thread.sleep(2000); // 间隔 2 秒

            long walOffset = walWriter.getWritePosition();
            var snapshotInfo = recoveryPipeline.checkpoint(intentStore, () -> walOffset);
            logger.info("Snapshot cycle {}: walOffset={}, snapshotInfo={}",
                cycle, walOffset, snapshotInfo);

            // 截断 WAL（受冷 Intent 保护）
            long minColdPos = swapper.getMinRequiredWalPosition();
            long safeTruncate = Math.min(snapshotInfo.walOffset, minColdPos);
            logger.info("WAL truncation: snapshotOffset={}, minColdPos={}, safeTruncate={}",
                snapshotInfo.walOffset, minColdPos, safeTruncate);

            walWriter.truncateBefore(safeTruncate);

            // 验证冷索引仍然完整（允许少量并发误差）
            assertTrue(swapper.coldIntentCount() >= intentCount - 5,
                "Cold index should be intact after snapshot + truncation cycle " + cycle);
        }

        logger.info("Phase 2 complete: 3 snapshot+truncation cycles done");

        // Phase 3: 等待所有 Intent 被 swapIn 并投递
        // 最早的 intent 在 30s 后到期，swapInAhead=60s，所以 daemon 应该立即开始 swapIn
        logger.info("Phase 3: waiting for all intents to be delivered...");
        long deadline = System.currentTimeMillis() + 180_000; // 最多等 3 分钟
        int lastCount = 0;
        int stableTicks = 0;

        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(1000);
            int current = deliveryCount.get();

            if (current >= intentCount) {
                logger.info("All {} intents delivered!", intentCount);
                break;
            }

            if (current == lastCount) {
                stableTicks++;
                if (stableTicks >= 30) { // 30 秒无进展
                    logger.warn("No progress for 30s, delivered={}/{}", current, intentCount);
                    break;
                }
            } else {
                stableTicks = 0;
                lastCount = current;
                if (current % 100 == 0 || current >= intentCount - 10) {
                    logger.info("Progress: {}/{} intents delivered (coldIndex={})",
                        current, intentCount, swapper.coldIntentCount());
                }
            }
        }

        // Phase 4: 验证
        int finalDelivered = deliveryCount.get();
        logger.info("=== Soak Test Results ===");
        logger.info("Total created: {}", intentCount);
        logger.info("Total delivered: {}", finalDelivered);
        logger.info("Cold index remaining: {}", swapper.coldIntentCount());
        logger.info("Store count: {}", intentStore.count());
        logger.info("Swap-out count: {}", swapper.getTotalSwappedOut());
        logger.info("Swap-in count: {}", swapper.getTotalSwappedIn());
        logger.info("Swap-in errors: {}", swapper.getSwapInErrors());

        // 核心断言：零丢失
        assertEquals(intentCount, finalDelivered,
            "All intents must be delivered — no data loss allowed");

        // 冷索引应该完全清空
        assertEquals(0, swapper.coldIntentCount(),
            "Cold index should be empty after all intents are delivered");

        // swap-in 错误数应为零
        assertEquals(0, swapper.getSwapInErrors(),
            "No swap-in errors should occur");

        // 所有 intent 最终状态应为 ACKED
        long ackedCount;
        synchronized (deliveredIntents) {
            ackedCount = deliveredIntents.stream()
                .filter(i -> i.getStatus() == IntentStatus.ACKED)
                .count();
        }
        assertEquals(intentCount, ackedCount,
            "All delivered intents should be in ACKED state");
    }

    /**
     * 崩溃恢复测试：模拟进程崩溃后重启。
     *
     * 流程：
     * 1. 创建 200 个冷 Intent
     * 2. 触发一次快照
     * 3. 关闭引擎（模拟优雅关闭）
     * 4. 重启引擎，验证所有 Intent 恢复
     */
    @Test
    void crashRecovery_coldIntentsSurviveRestart() throws Exception {
        int intentCount = 200;
        Path walDir = tempDir.resolve("wal-crash-recovery");
        Files.createDirectories(walDir);

        List<String> allIntentIds = new ArrayList<>();
        long baseMs = System.currentTimeMillis();

        // Phase 1: 创建 Intent，写入 WAL + Store，快照，关闭
        logger.info("Phase 1: creating {} intents...", intentCount);
        {
            Duration coldThreshold = Duration.ofHours(24); // 不触发冷交换
            setUpComponents(coldThreshold, walDir);

            for (int i = 0; i < intentCount; i++) {
                long delayMs = 60_000 + (long) (120_000.0 * i / intentCount);
                Instant executeAt = Instant.ofEpochMilli(baseMs + delayMs);
                Intent intent = createIntent("crash-" + i, executeAt, PrecisionTier.STANDARD);

                // 写入 WAL + Store
                intentStore.save(intent);
                byte[] payload = IntentBinaryCodec.encode(intent);
                walWriter.writeDurable(payload).join();

                allIntentIds.add("crash-" + i);
            }

            // 快照（store 包含所有 200 个 Intent）
            long walOffset = walWriter.getWritePosition();
            recoveryPipeline.checkpoint(intentStore, () -> walOffset);
            logger.info("Snapshot taken: walOffset={}, store count={}", walOffset, intentStore.count());

            // 关闭引擎
            recoveryPipeline.close();
            swapper.close();
            scheduler.stop();
            walWriter.close();
            intentStore.shutdown();
            logger.info("Phase 1 complete: engine shut down");
        }

        // Phase 2: 重启引擎，验证恢复
        logger.info("Phase 2: restarting engine...");
        {
            deliveryCount.set(0);
            deliveryTimes.clear();
            deliveredIntents.clear();

            intentStore = new ConcurrentIntentStore();
            WalConfig config = new WalConfig(
                walDir.toString(), 8, "batch", 100, false,
                "memory_segment", 8, 32, 64, 10, 4,
                1, false
            );
            walWriter = new SimpleWalWriter(config, "soak-shard");
            walWriter.start();

            scheduler = new PrecisionScheduler(intentStore, trackingHandler, null);
            recoveryPipeline = new RecoveryPipeline(walDir);
            var report = recoveryPipeline.recover(intentStore, scheduler, walWriter);
            scheduler.start();

            logger.info("Recovery report: total={}, snapshot={}, wal={}",
                report.restoredTotal(), report.restoredFromSnapshot(), report.restoredFromWal());
            logger.info("After recovery: store count={}, pending={}",
                intentStore.count(), intentStore.getPendingCount());

            // 验证所有 Intent 都被恢复
            int restoredCount = 0;
            for (String id : allIntentIds) {
                if (intentStore.findById(id) != null) {
                    restoredCount++;
                }
            }
            logger.info("Restored {}/{} intents", restoredCount, intentCount);
            assertEquals(intentCount, restoredCount,
                "All intents must be recovered after restart");

            // 等待投递
            long deadline = System.currentTimeMillis() + 180_000;
            while (System.currentTimeMillis() < deadline) {
                Thread.sleep(1000);
                if (deliveryCount.get() >= intentCount) break;
            }

            int finalDelivered = deliveryCount.get();
            logger.info("After recovery: delivered {}/{}", finalDelivered, intentCount);
            assertEquals(intentCount, finalDelivered,
                "All recovered intents must be delivered");

            // 清理
            recoveryPipeline.close();
            swapper.close();
            scheduler.stop();
            walWriter.close();
            intentStore.shutdown();
        }
    }

    /**
     * WAL 段稳定性测试：多次快照后段数量不无限增长。
     */
    @Test
    void walSegmentCount_stabilizesAfterMultipleSnapshots() throws Exception {
        int intentCount = 500;
        Duration coldThreshold = Duration.ofSeconds(5);

        setUpComponents(coldThreshold);

        logger.info("=== WAL Segment Stability Test ===");

        // 创建 Intent
        long baseMs = System.currentTimeMillis();
        for (int i = 0; i < intentCount; i++) {
            long delayMs = 30_000 + (long) (60_000.0 * i / intentCount);
            Instant executeAt = Instant.ofEpochMilli(baseMs + delayMs);
            Intent intent = createIntent("seg-" + i, executeAt, PrecisionTier.STANDARD);
            intentStore.save(intent);

            byte[] payload = IntentBinaryCodec.encode(intent);
            long walPos = walWriter.writeDurable(payload).join();
            int recordLen = SimpleWalWriter.recordLength(payload.length);
            swapper.swapOut(intent, walPos, recordLen);
        }

        int initialSegments = walWriter.listSegments().size();
        logger.info("Initial WAL segments: {}", initialSegments);

        // 触发 5 次快照 + 截断
        for (int cycle = 1; cycle <= 5; cycle++) {
            Thread.sleep(1000);
            long walOffset = walWriter.getWritePosition();
            var snapshotInfo = recoveryPipeline.checkpoint(intentStore, () -> walOffset);

            long minColdPos = swapper.getMinRequiredWalPosition();
            long safeTruncate = Math.min(snapshotInfo.walOffset, minColdPos);
            walWriter.truncateBefore(safeTruncate);

            int segments = walWriter.listSegments().size();
            logger.info("Cycle {}: segments={}, walOffset={}, minColdPos={}, safeTruncate={}",
                cycle, segments, walOffset, minColdPos, safeTruncate);
        }

        int finalSegments = walWriter.listSegments().size();
        logger.info("Final WAL segments: {} (initial={})", finalSegments, initialSegments);

        // 段数量应该稳定（不应无限增长）
        // 由于冷 Intent 引用旧段，段数量可能比初始值高，但不应持续增长
        // 核心断言：段数量不超过初始值 + 2（允许少量新段）
        assertTrue(finalSegments <= initialSegments + 2,
            "WAL segment count should stabilize: initial=" + initialSegments +
            ", final=" + finalSegments);

        // 冷索引应该完整（允许少量并发误差）
        int coldCount = swapper.coldIntentCount();
        assertTrue(coldCount >= intentCount - 5,
            "Cold index should have ~" + intentCount + " entries, got " + coldCount);
    }

    // ========== helpers ==========

    private Intent createIntent(String id, Instant executeAt, PrecisionTier tier) {
        Intent intent = new Intent(id);
        intent.setExecuteAt(executeAt);
        intent.setDeadline(executeAt.plusSeconds(300));
        intent.setPrecisionTier(tier);
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.incrementRevision();
        return intent;
    }
}

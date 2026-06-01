package com.loomq.recovery;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.domain.intent.Intent;
import com.loomq.snapshot.SnapshotManager;
import com.loomq.snapshot.SnapshotManager.SnapshotInfo;
import com.loomq.snapshot.SnapshotManager.SnapshotRestoreResult;
import com.loomq.spi.WalAccessor;
import com.loomq.store.IntentStore;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 恢复管线。
 *
 * 负责：
 * 1. 从快照恢复当前状态
 * 2. 回放快照之后的 WAL 增量（段文件模式）
 * 3. 定期生成快照并截断旧 WAL 段
 */
public final class RecoveryPipeline implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RecoveryPipeline.class);
    private static final long SNAPSHOT_INTERVAL_MINUTES = 5L;

    private final SnapshotManager snapshotManager;
    private final WalReplayManager walReplayManager;
    private final ScheduledExecutorService snapshotExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public RecoveryPipeline(Path walDir) {
        this.snapshotManager = new SnapshotManager(walDir.toString());
        this.walReplayManager = new WalReplayManager();
        this.snapshotExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "recovery-snapshot");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * 恢复 store 和调度器（段文件模式）。
     */
    public RecoveryReport recover(IntentStore store, PrecisionScheduler scheduler, WalAccessor walAccessor) {
        store.clear();
        scheduler.getBucketGroupManager().clear();

        SnapshotRestoreResult snapshotResult = snapshotManager.restoreFromSnapshot(intent -> restoreIntent(intent, store, scheduler));
        int restoredFromSnapshot = snapshotResult.restoredCount();

        // 使用 WalAccessor 接口从段文件回放
        int restoredFromWal = walReplayManager.replay(walAccessor, snapshotResult.walOffset(),
            intent -> restoreIntent(intent, store, scheduler));

        int totalRestored = restoredFromSnapshot + restoredFromWal;
        if (totalRestored > 0) {
            logger.info("Recovery completed: snapshot={}, walReplay={}, offset={}",
                restoredFromSnapshot, restoredFromWal, snapshotResult.walOffset());
        } else {
            logger.info("Recovery completed: no state to restore");
        }

        return new RecoveryReport(totalRestored, restoredFromSnapshot, restoredFromWal, snapshotResult.walOffset());
    }

    /**
     * 启动定期快照，并在快照完成后截断旧 WAL 段。
     *
     * @param store         Intent 存储
     * @param walOffsetSupplier 获取当前 WAL 写位置
     * @param walAccessorSupplier 获取 WalAccessor（用于截断）
     */
    public void startSnapshots(IntentStore store, LongSupplier walOffsetSupplier, Supplier<WalAccessor> walAccessorSupplier) {
        startSnapshots(store, walOffsetSupplier, walAccessorSupplier, () -> Long.MAX_VALUE);
    }

    /**
     * 启动定期快照，并在快照完成后截断旧 WAL 段，同时保护冷 Intent 的 WAL 段不被截断。
     *
     * @param store                      Intent 存储
     * @param walOffsetSupplier          获取当前 WAL 写位置
     * @param walAccessorSupplier        获取 WalAccessor（用于截断）
     * @param minTruncationOffsetSupplier 获取冷 Intent 需要的最小 WAL 位置，
     *                                   截断点取 min(snapshotOffset, 此值)，
     *                                   返回 Long.MAX_VALUE 表示不额外约束
     */
    public void startSnapshots(IntentStore store,
                               LongSupplier walOffsetSupplier,
                               Supplier<WalAccessor> walAccessorSupplier,
                               LongSupplier minTruncationOffsetSupplier) {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        snapshotExecutor.scheduleAtFixedRate(() -> {
            try {
                long walOffset = walOffsetSupplier.getAsLong();
                // Async snapshot: create snapshot in background, truncate WAL on completion
                CompletableFuture.supplyAsync(() ->
                        snapshotManager.createSnapshot(store, walOffset), snapshotExecutor)
                    .thenAcceptAsync(info -> {
                        logger.debug("Snapshot checkpoint written: {}", info);
                        WalAccessor wal = walAccessorSupplier.get();
                        if (wal != null) {
                            long safeTruncateOffset = Math.min(info.walOffset, minTruncationOffsetSupplier.getAsLong());
                            if (safeTruncateOffset < info.walOffset) {
                                logger.info("WAL truncation limited by cold intents: snapshotOffset={}, safeTruncateOffset={}",
                                    info.walOffset, safeTruncateOffset);
                            }
                            wal.truncateBefore(safeTruncateOffset);
                        }
                    }, snapshotExecutor)
                    .exceptionally(ex -> {
                        logger.error("Async snapshot checkpoint failed", ex);
                        return null;
                    });
            } catch (Exception e) {
                logger.error("Snapshot checkpoint failed", e);
            }
        }, SNAPSHOT_INTERVAL_MINUTES, SNAPSHOT_INTERVAL_MINUTES, TimeUnit.MINUTES);

        logger.info("RecoveryPipeline started: snapshot interval {} minutes", SNAPSHOT_INTERVAL_MINUTES);
    }

    /**
     * 启动定期快照（无截断，向后兼容）。
     */
    public void startSnapshots(IntentStore store, LongSupplier walOffsetSupplier) {
        startSnapshots(store, walOffsetSupplier, () -> null);
    }

    /**
     * 立即生成一个快照。
     */
    public SnapshotInfo checkpoint(IntentStore store, LongSupplier walOffsetSupplier) {
        return snapshotManager.createSnapshot(store, walOffsetSupplier.getAsLong());
    }

    private void restoreIntent(Intent intent, IntentStore store, PrecisionScheduler scheduler) {
        if (intent == null) {
            return;
        }

        store.upsert(intent);
        scheduler.restore(intent);
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            snapshotExecutor.shutdownNow();
            return;
        }

        snapshotExecutor.shutdown();
        try {
            if (!snapshotExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                snapshotExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            snapshotExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("RecoveryPipeline stopped");
    }

    /**
     * 恢复结果。
     */
    public record RecoveryReport(
        int restoredTotal,
        int restoredFromSnapshot,
        int restoredFromWal,
        long snapshotOffset
    ) {}
}

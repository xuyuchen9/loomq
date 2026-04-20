package com.loomq.recovery;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.domain.intent.Intent;
import com.loomq.snapshot.SnapshotManager;
import com.loomq.snapshot.SnapshotManager.SnapshotInfo;
import com.loomq.snapshot.SnapshotManager.SnapshotRestoreResult;
import com.loomq.store.IntentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/**
 * 恢复管线。
 *
 * 负责：
 * 1. 从快照恢复当前状态
 * 2. 回放快照之后的 WAL 增量
 * 3. 定期生成快照
 */
public final class RecoveryPipeline implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RecoveryPipeline.class);
    private static final long SNAPSHOT_INTERVAL_MINUTES = 5L;

    private final SnapshotManager snapshotManager;
    private final WalReplayManager walReplayManager;
    private final Path walPath;
    private final ScheduledExecutorService snapshotExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public RecoveryPipeline(Path walDir) {
        this.snapshotManager = new SnapshotManager(walDir.toString());
        this.walReplayManager = new WalReplayManager();
        this.walPath = walDir.resolve("shard-0").resolve("wal.bin");
        this.snapshotExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "recovery-snapshot");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * 恢复 store 和调度器。
     */
    public RecoveryReport recover(IntentStore store, PrecisionScheduler scheduler) {
        SnapshotRestoreResult snapshotResult = snapshotManager.restoreFromSnapshot(intent -> restoreIntent(intent, store, scheduler));
        int restoredFromSnapshot = snapshotResult.restoredCount();

        int restoredFromWal = walReplayManager.replay(walPath, snapshotResult.walOffset(),
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
     * 启动定期快照。
     */
    public void startSnapshots(IntentStore store, LongSupplier walOffsetSupplier) {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        snapshotExecutor.scheduleAtFixedRate(() -> {
            try {
                SnapshotInfo info = snapshotManager.createSnapshot(store, walOffsetSupplier.getAsLong());
                logger.debug("Snapshot checkpoint written: {}", info);
            } catch (Exception e) {
                logger.error("Snapshot checkpoint failed", e);
            }
        }, SNAPSHOT_INTERVAL_MINUTES, SNAPSHOT_INTERVAL_MINUTES, TimeUnit.MINUTES);

        logger.info("RecoveryPipeline started: snapshot interval {} minutes", SNAPSHOT_INTERVAL_MINUTES);
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

        store.save(intent);
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

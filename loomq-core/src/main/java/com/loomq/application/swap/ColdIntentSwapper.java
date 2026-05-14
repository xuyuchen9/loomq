package com.loomq.application.swap;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.domain.intent.Intent;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.store.IntentStore;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 长延迟 Intent 冷热交换器。
 *
 * 核心思路：执行时间 > coldThreshold 的 Intent 在 WAL 持久化后，
 * 从内存 IntentStore 中移除，仅保留 ColdIntentEntry（~80 bytes）。
 * 到期前由 daemon 线程从 WAL 读回、解码并重新调度。
 *
 * 适用场景：大量延迟 > 1 小时的 Intent（如定时通知、批量调度），
 * 可节省 90%+ 内存占用。
 *
 * 线程安全：swap-out 由调用方保证单线程（createIntent 路径），
 * swap-in 由内部 daemon 单线程执行，索引操作无需额外同步。
 */
public final class ColdIntentSwapper implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ColdIntentSwapper.class);

    /** 默认冷热阈值：1 小时 */
    static final Duration DEFAULT_COLD_THRESHOLD = Duration.ofHours(1);

    /** 提前换入窗口：执行前 60 秒从 WAL 读回 */
    private static final Duration SWAP_IN_AHEAD = Duration.ofSeconds(60);

    /** Daemon 扫描间隔 */
    private static final long SCAN_INTERVAL_MS = 30_000;

    private final IntentStore intentStore;
    private final PrecisionScheduler scheduler;
    private final SimpleWalWriter walWriter;
    private final Duration coldThreshold;

    /** 按 executeAtEpochMs 排序的冷 Intent 索引 */
    private final ConcurrentSkipListMap<Long, List<ColdIntentEntry>> coldIndex;

    private final Thread swapInThread;
    private final AtomicBoolean running;

    /** 观测指标 */
    private final AtomicLong totalSwappedOut = new AtomicLong(0);
    private final AtomicLong totalSwappedIn = new AtomicLong(0);
    private final AtomicLong swapInErrors = new AtomicLong(0);

    public ColdIntentSwapper(IntentStore intentStore,
                             PrecisionScheduler scheduler,
                             SimpleWalWriter walWriter,
                             Duration coldThreshold) {
        this.intentStore = intentStore;
        this.scheduler = scheduler;
        this.walWriter = walWriter;
        this.coldThreshold = coldThreshold != null ? coldThreshold : DEFAULT_COLD_THRESHOLD;
        this.coldIndex = new ConcurrentSkipListMap<>();
        this.running = new AtomicBoolean(false);

        this.swapInThread = Thread.ofPlatform()
            .name("cold-swap-in")
            .daemon(true)
            .unstarted(this::swapInLoop);
    }

    /** 使用默认阈值（1 小时）的便捷构造器 */
    public ColdIntentSwapper(IntentStore intentStore,
                             PrecisionScheduler scheduler,
                             SimpleWalWriter walWriter) {
        this(intentStore, scheduler, walWriter, DEFAULT_COLD_THRESHOLD);
    }

    // ========== 生命周期 ==========

    public void start() {
        if (running.compareAndSet(false, true)) {
            swapInThread.start();
            logger.info("ColdIntentSwapper started: coldThreshold={}, swapInAhead={}",
                coldThreshold, SWAP_IN_AHEAD);
        }
    }

    @Override
    public void close() {
        running.set(false);
        LockSupport.unpark(swapInThread);
        try {
            swapInThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        logger.info("ColdIntentSwapper closed: swappedOut={}, swappedIn={}, errors={}",
            totalSwappedOut.get(), totalSwappedIn.get(), swapInErrors.get());
    }

    // ========== 换出 ==========

    /**
     * 判断 Intent 是否应该被换出内存。
     *
     * @param delayMs 距离 executeAt 的延迟毫秒数
     */
    public boolean shouldSwapOut(long delayMs) {
        return delayMs > coldThreshold.toMillis();
    }

    /**
     * 将 Intent 从 IntentStore 换出到冷索引。
     *
     * 调用前确保 WAL 已持久化（DURABLE 模式），否则崩溃时丢失数据。
     *
     * @param intent       要换出的 Intent
     * @param walPosition   WAL 记录起始位置
     * @param recordLength  WAL 记录长度
     */
    public void swapOut(Intent intent, long walPosition, int recordLength) {
        ColdIntentEntry entry = new ColdIntentEntry(
            intent.getIntentId(),
            walPosition,
            recordLength,
            intent.getExecuteAt().toEpochMilli(),
            intent.getPrecisionTier()
        );

        // 从 IntentStore 移除（释放内存）
        intentStore.delete(intent.getIntentId());

        // 加入冷索引
        coldIndex.computeIfAbsent(entry.executeAtEpochMs(), k -> new ArrayList<>())
                 .add(entry);

        totalSwappedOut.incrementAndGet();
        logger.debug("Intent {} swapped out: walPos={}, executeAt={}, tier={}",
            entry.intentId(), walPosition, intent.getExecuteAt(), intent.getPrecisionTier());
    }

    // ========== 换入 (daemon) ==========

    private void swapInLoop() {
        while (running.get()) {
            try {
                long nowMs = System.currentTimeMillis();
                long thresholdMs = nowMs + SWAP_IN_AHEAD.toMillis();

                // 查找所有即将到期的冷 Intent
                var dueEntries = coldIndex.headMap(thresholdMs, true);
                if (dueEntries.isEmpty()) {
                    LockSupport.parkNanos(Duration.ofMillis(SCAN_INTERVAL_MS).toNanos());
                    continue;
                }

                List<ColdIntentEntry> toSwapIn = new ArrayList<>();
                for (var entry : dueEntries.entrySet()) {
                    toSwapIn.addAll(entry.getValue());
                }

                int successCount = 0;
                for (ColdIntentEntry entry : toSwapIn) {
                    if (swapIn(entry)) {
                        successCount++;
                    }
                }

                if (successCount > 0) {
                    logger.info("Cold swap-in batch: {} intents reloaded from WAL", successCount);
                }

                LockSupport.parkNanos(Duration.ofMillis(SCAN_INTERVAL_MS).toNanos());

            } catch (Exception e) {
                logger.error("ColdIntentSwapper daemon error", e);
                LockSupport.parkNanos(Duration.ofSeconds(5).toNanos());
            }
        }
    }

    /**
     * 从 WAL 读回单条 Intent，恢复至 IntentStore 并重新调度。
     *
     * @return true 如果换入成功
     */
    private boolean swapIn(ColdIntentEntry entry) {
        try {
            // 1. 从 WAL 读取二进制 payload
            byte[] payload = walWriter.readRecord(entry.walPosition(), entry.recordLength());

            // 2. 解码为 Intent
            Intent intent = IntentBinaryCodec.decode(payload);

            // 3. 验证 identity
            if (!entry.intentId().equals(intent.getIntentId())) {
                logger.error("Intent ID mismatch after WAL read: expected={}, actual={}",
                    entry.intentId(), intent.getIntentId());
                swapInErrors.incrementAndGet();
                return false;
            }

            // 4. 放回 IntentStore
            intentStore.upsert(intent);

            // 5. 重新调度
            scheduler.restore(intent);

            // 6. 从冷索引移除
            coldIndex.compute(entry.executeAtEpochMs(), (k, list) -> {
                if (list != null) {
                    list.remove(entry);
                    return list.isEmpty() ? null : list;
                }
                return null;
            });

            totalSwappedIn.incrementAndGet();
            logger.debug("Intent {} swapped in: walPos={}, tier={}",
                entry.intentId(), entry.walPosition(), entry.tier());
            return true;

        } catch (IOException e) {
            logger.error("Failed to read WAL record for intent {} at position {}",
                entry.intentId(), entry.walPosition(), e);
            swapInErrors.incrementAndGet();
            return false;
        } catch (Exception e) {
            logger.error("Unexpected error swapping in intent {}", entry.intentId(), e);
            swapInErrors.incrementAndGet();
            return false;
        }
    }

    // ========== 观测 ==========

    public int coldIntentCount() {
        return coldIndex.values().stream().mapToInt(List::size).sum();
    }

    public long getTotalSwappedOut() { return totalSwappedOut.get(); }
    public long getTotalSwappedIn()  { return totalSwappedIn.get(); }
    public long getSwapInErrors()    { return swapInErrors.get(); }

    public Duration getColdThreshold() { return coldThreshold; }
}

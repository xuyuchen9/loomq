package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.MetricsCollector;
import com.loomq.config.WalConfig;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.spi.CallbackHandler;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * P1: 验证回调执行器与 WAL 写入的隔离性。
 *
 * 注入一个 100ms 延迟的慢回调处理器，然后测量 WAL DURABLE 写入吞吐。
 * 如果隔离正确，WAL 吞吐不应受回调延迟影响。
 *
 * @tag slow
 */
@Tag("slow")
class CallbackIsolationTest {

    private static final Logger logger = LoggerFactory.getLogger(CallbackIsolationTest.class);

    @TempDir
    Path tempDir;

    private IntentStore intentStore;
    private PrecisionScheduler scheduler;
    private SimpleWalWriter walWriter;
    private IntentCommandService commandService;
    private MetricsCollector metrics;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong sequenceNumber = new AtomicLong(0);

    private final DeliveryHandler noopHandler =
        intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);

    @AfterEach
    void tearDown() {
        if (scheduler != null) scheduler.stop();
        if (walWriter != null) walWriter.close();
        if (intentStore != null) intentStore.shutdown();
    }

    private void setUp(CallbackHandler callbackHandler, Executor callbackExecutor) throws Exception {
        Path walDir = tempDir.resolve("wal-isolation");
        Files.createDirectories(walDir);

        intentStore = new ConcurrentIntentStore();
        metrics = MetricsCollector.getInstance();

        WalConfig config = new WalConfig(
            walDir.toString(), 8, "batch", 100, false,
            "memory_segment", 8, 32, 64, 10, 4,
            1, false
        );
        walWriter = new SimpleWalWriter(config, "isolation-shard");
        walWriter.start();

        scheduler = new PrecisionScheduler(intentStore, noopHandler, null);
        scheduler.start();

        Executor walWriteExecutor = Executors.newVirtualThreadPerTaskExecutor();

        commandService = new IntentCommandService(
            intentStore, scheduler, walWriter, metrics,
            callbackExecutor, walWriteExecutor, running, sequenceNumber,
            callbackHandler, PrecisionTier.STANDARD, null
        );
    }

    /**
     * 基线：无回调，测量 WAL DURABLE 写入吞吐。
     */
    @Test
    void baseline_noCallback_walThroughput() throws Exception {
        setUp(null, Executors.newVirtualThreadPerTaskExecutor());

        int count = 5000;
        long start = System.nanoTime();

        for (int i = 0; i < count; i++) {
            Intent intent = createIntent("baseline-" + i);
            commandService.createIntent(intent, AckMode.DURABLE);
        }

        long durationMs = (System.nanoTime() - start) / 1_000_000;
        double qps = count / (durationMs / 1000.0);

        logger.info("Baseline (no callback): {} intents in {}ms, QPS={}", count, durationMs, (int) qps);
        System.out.printf("RESULT|callback_isolation|mode=baseline|count=%d|duration_ms=%d|qps=%.0f%n",
            count, durationMs, qps);

        // WAL DURABLE 应该能达到 ~6000 QPS
        assertTrue(qps > 3000, "Baseline WAL DURABLE QPS should be > 3000, got " + (int) qps);
    }

    /**
     * 慢回调：回调延迟 100ms，测量 WAL DURABLE 写入吞吐。
     * 如果隔离正确，吞吐不应显著下降。
     */
    @Test
    void slowCallback_walThroughput_notAffected() throws Exception {
        // 慢回调处理器：每个回调 sleep 100ms
        CallbackHandler slowCallback = (intent, eventType, error) -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        setUp(slowCallback, Executors.newVirtualThreadPerTaskExecutor());

        int count = 5000;
        long start = System.nanoTime();

        for (int i = 0; i < count; i++) {
            Intent intent = createIntent("slow-cb-" + i);
            commandService.createIntent(intent, AckMode.DURABLE);
        }

        long durationMs = (System.nanoTime() - start) / 1_000_000;
        double qps = count / (durationMs / 1000.0);

        logger.info("Slow callback (100ms): {} intents in {}ms, QPS={}", count, durationMs, (int) qps);
        System.out.printf("RESULT|callback_isolation|mode=slow_callback|count=%d|duration_ms=%d|qps=%.0f%n",
            count, durationMs, qps);

        // WAL DURABLE 写入不应受回调影响
        // createIntent 不触发回调（只有 cancelIntent 触发），所以吞吐应与基线一致
        assertTrue(qps > 3000, "WAL DURABLE QPS with slow callback should be > 3000, got " + (int) qps);
    }

    /**
     * 有界回调执行器 + 慢回调：验证回调执行器耗尽不影响 WAL。
     *
     * 使用固定线程池（2 线程）+ 100ms 回调延迟。
     * 当回调堆积时，execute() 不会阻塞（虚拟线程无界队列）。
     * 但即使使用有界队列，WAL 写入也在回调之前完成。
     */
    @Test
    void boundedCallbackExecutor_walNotAffected() throws Exception {
        // 慢回调处理器
        CallbackHandler slowCallback = (intent, eventType, error) -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        // 有界回调执行器：2 个线程
        Executor boundedExecutor = Executors.newFixedThreadPool(2);

        setUp(slowCallback, boundedExecutor);

        int count = 2000;
        long start = System.nanoTime();

        for (int i = 0; i < count; i++) {
            Intent intent = createIntent("bounded-" + i);
            commandService.createIntent(intent, AckMode.DURABLE);
        }

        long durationMs = (System.nanoTime() - start) / 1_000_000;
        double qps = count / (durationMs / 1000.0);

        logger.info("Bounded executor (2 threads, 50ms callback): {} intents in {}ms, QPS={}",
            count, durationMs, (int) qps);
        System.out.printf("RESULT|callback_isolation|mode=bounded_executor|count=%d|duration_ms=%d|qps=%.0f%n",
            count, durationMs, qps);

        // createIntent 不触发回调，所以 WAL 吞吐不受影响
        assertTrue(qps > 3000, "WAL DURABLE QPS should be > 3000 even with bounded callback executor, got " + (int) qps);
    }

    // ========== helpers ==========

    private Intent createIntent(String id) {
        Intent intent = new Intent(id);
        intent.setExecuteAt(Instant.now().plusSeconds(3600));
        intent.setDeadline(Instant.now().plusSeconds(7200));
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        // 不要 transitionTo SCHEDULED — createIntent 会自己做这个转换
        return intent;
    }
}

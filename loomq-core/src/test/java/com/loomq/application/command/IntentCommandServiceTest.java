package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.MetricsCollector;
import com.loomq.config.WalConfig;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * IntentCommandService 单元测试。
 *
 * 验证 updateIntent 在 updater 直接修改 executeAt 时的重调度安全网。
 */
class IntentCommandServiceTest {

    private Path dataDir;
    private IntentStore store;
    private PrecisionScheduler scheduler;
    private SimpleWalWriter walWriter;
    private IntentCommandService commandService;

    private static final DeliveryHandler NOOP_HANDLER =
        intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);

    private static Intent makeIntent(String id) {
        Intent intent = new Intent(id);
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.setExecuteAt(Instant.now().plusSeconds(300));
        // 不预设状态——createIntent 会执行 CREATED → SCHEDULED 转换
        return intent;
    }

    @BeforeEach
    void setUp() throws Exception {
        dataDir = Files.createTempDirectory("command-service-test-");
        store = new ConcurrentIntentStore();
        scheduler = new PrecisionScheduler(store, NOOP_HANDLER, null);

        WalConfig config = new WalConfig(
            dataDir.toString(), 1, "batch", 100, false, "memory_segment",
            1, 8, 64, 10, 4, 1, false
        );
        walWriter = new SimpleWalWriter(config, "cmd-test");
        walWriter.start();

        var executor = Executors.newSingleThreadExecutor();
        commandService = new IntentCommandService(
            store, scheduler, walWriter,
            MetricsCollector.getInstance(),
            executor, executor,
            new AtomicBoolean(true), new AtomicLong(0),
            null, null, null
        );
    }

    @AfterEach
    void tearDown() {
        if (scheduler != null) scheduler.stop();
        if (walWriter != null) walWriter.close();
        if (store != null) store.shutdown();
        try {
            Files.walk(dataDir)
                .sorted(java.util.Comparator.reverseOrder())
                .forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignored) {} });
        } catch (IOException ignored) {}
    }

    /**
     * 验证：通过两参数 updateIntent 在 updater 中直接修改 executeAt 时，
     * intent 应被正确重调度到新桶，而不是停留在旧位置。
     */
    @Test
    void updateIntentWithDirectExecuteAtChangeShouldReschedule() {
        // Given: 创建一个 intent，executeAt 为 5 分钟后
        Intent intent = makeIntent("reschedule-test");
        Instant originalExecuteAt = intent.getExecuteAt();
        commandService.createIntent(intent, AckMode.DURABLE);

        // 确认 intent 已存入 store
        Intent stored = store.findById("reschedule-test");
        assertTrue(stored != null, "intent should be in store after creation");
        assertEquals(originalExecuteAt, stored.getExecuteAt(), "initial executeAt should match");

        // When: 通过 updater 直接修改 executeAt（不走 newExecuteAt 参数）
        Instant newExecuteAt = originalExecuteAt.plusSeconds(600);
        commandService.updateIntent("reschedule-test", it -> it.setExecuteAt(newExecuteAt));

        // Then: store 中的 executeAt 应已更新
        Intent updated = store.findById("reschedule-test");
        assertTrue(updated != null, "intent should still be in store");
        assertEquals(newExecuteAt, updated.getExecuteAt(),
            "executeAt should be updated to the new value set by updater");
    }
}

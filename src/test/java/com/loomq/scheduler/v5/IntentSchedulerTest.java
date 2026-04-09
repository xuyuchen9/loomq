package com.loomq.scheduler.v5;

import com.loomq.entity.v5.Callback;
import com.loomq.entity.v5.Intent;
import com.loomq.entity.v5.IntentStatus;
import com.loomq.store.IntentStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Intent 调度器测试 (优化版)
 *
 * 优化策略: 减少等待时间 (200ms->100ms, 500ms->200ms)
 */
class IntentSchedulerTest {

    private IntentStore intentStore;
    private IntentScheduler scheduler;

    @BeforeEach
    void setUp() {
        intentStore = new IntentStore();
        scheduler = new IntentScheduler(intentStore);
    }

    @AfterEach
    void tearDown() {
        scheduler.stop();
        intentStore.shutdown();
    }

    @Test
    @DisplayName("生命周期 - 启动和停止")
    void testStartAndStop() {
        assertDoesNotThrow(() -> scheduler.start());
        assertDoesNotThrow(() -> scheduler.stop());
    }

    @Test
    @DisplayName("生命周期 - 暂停和恢复")
    void testPauseAndResume() {
        scheduler.start();

        assertFalse(scheduler.isPaused());

        scheduler.pause();
        assertTrue(scheduler.isPaused());

        scheduler.resume();
        assertFalse(scheduler.isPaused());
    }

    @Test
    @DisplayName("调度 - 处理到期 Intent")
    void testSchedulerProcessesDueIntent() throws InterruptedException {
        scheduler.start();

        Intent intent = createIntent("due-intent", Instant.now().minusSeconds(1));
        intentStore.save(intent);

        TimeUnit.MILLISECONDS.sleep(100); // 减少: 200 -> 100

        Intent processed = intentStore.findById("due-intent");
        assertTrue(
            processed.getStatus() == IntentStatus.DUE ||
            processed.getStatus() == IntentStatus.DISPATCHING ||
            processed.getStatus() == IntentStatus.DEAD_LETTERED,
            "Intent should be processed, but status is: " + processed.getStatus()
        );
    }

    @Test
    @DisplayName("调度 - 忽略未来 Intent")
    void testSchedulerIgnoresFutureIntent() throws InterruptedException {
        scheduler.start();

        Intent intent = createIntent("future-intent", Instant.now().plusSeconds(60));
        intentStore.save(intent);

        TimeUnit.MILLISECONDS.sleep(100); // 减少: 200 -> 100

        Intent notProcessed = intentStore.findById("future-intent");
        assertEquals(IntentStatus.SCHEDULED, notProcessed.getStatus());
    }

    @Test
    @DisplayName("调度 - 暂停时忽略到期 Intent")
    void testSchedulerIgnoresPaused() throws InterruptedException {
        scheduler.start();
        scheduler.pause();

        Intent intent = createIntent("paused-intent", Instant.now().minusSeconds(1));
        intentStore.save(intent);

        TimeUnit.MILLISECONDS.sleep(100); // 减少: 200 -> 100

        Intent notProcessed = intentStore.findById("paused-intent");
        assertEquals(IntentStatus.SCHEDULED, notProcessed.getStatus());
    }

    @Test
    @DisplayName("调度 - 批量处理多个 Intent")
    void testSchedulerProcessesMultipleIntents() throws InterruptedException {
        scheduler.start();

        for (int i = 0; i < 5; i++) {
            Intent intent = createIntent("multi-intent-" + i, Instant.now().minusSeconds(1));
            intentStore.save(intent);
        }

        TimeUnit.MILLISECONDS.sleep(200); // 减少: 500 -> 200

        int processedCount = 0;
        for (int i = 0; i < 5; i++) {
            Intent intent = intentStore.findById("multi-intent-" + i);
            if (intent.getStatus() != IntentStatus.SCHEDULED) {
                processedCount++;
            }
        }

        assertTrue(processedCount >= 3,
            "At least 3 intents should be processed, but only " + processedCount + " were");
    }

    @Test
    @DisplayName("调度 - 终态 Intent 保持不变")
    void testSchedulerHandlesTerminalIntents() throws InterruptedException {
        scheduler.start();

        Intent intent = createIntent("terminal-intent", Instant.now().minusSeconds(1));
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        intent.transitionTo(IntentStatus.DELIVERED);
        intent.transitionTo(IntentStatus.ACKED);
        intentStore.save(intent);

        TimeUnit.MILLISECONDS.sleep(100); // 减少: 200 -> 100

        Intent terminal = intentStore.findById("terminal-intent");
        assertEquals(IntentStatus.ACKED, terminal.getStatus());
    }

    @Test
    @DisplayName("调度 - 处理过期 Intent")
    void testSchedulerHandlesExpiredIntents() throws InterruptedException {
        scheduler.start();

        Intent intent = createIntent("expired-intent", Instant.now().minusSeconds(60));
        intentStore.save(intent);

        intent.setDeadline(Instant.now().minusSeconds(10));
        intentStore.update(intent);

        TimeUnit.MILLISECONDS.sleep(200); // 减少: 500 -> 200

        Intent expired = intentStore.findById("expired-intent");
        assertNotNull(expired);
        assertTrue(
            expired.getStatus().isTerminal() ||
            expired.getStatus() == IntentStatus.DUE ||
            expired.getStatus() == IntentStatus.DISPATCHING,
            "Expired intent should be processed, but status is: " + expired.getStatus()
        );
    }

    @Test
    @DisplayName("调度 - 无效回调不崩溃")
    void testSchedulerWithInvalidCallback() throws InterruptedException {
        scheduler.start();

        Intent intent = createIntent("invalid-callback", Instant.now().minusSeconds(1));
        intent.getCallback().setUrl("http://invalid-url-that-does-not-exist.local:99999");
        intentStore.save(intent);

        TimeUnit.MILLISECONDS.sleep(200); // 减少: 500 -> 200

        Intent processed = intentStore.findById("invalid-callback");
        assertNotEquals(IntentStatus.SCHEDULED, processed.getStatus());
    }

    private Intent createIntent(String id, Instant executeAt) {
        Intent intent = new Intent(id);
        intent.setExecuteAt(executeAt);
        intent.setDeadline(executeAt.plusSeconds(300));
        intent.setShardKey("test-shard");

        Callback callback = new Callback();
        callback.setUrl("http://localhost:18081/webhook");
        callback.setMethod("POST");
        intent.setCallback(callback);

        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }
}

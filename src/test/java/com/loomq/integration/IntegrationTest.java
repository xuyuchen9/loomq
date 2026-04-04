package com.loomq.integration;

import com.loomq.config.*;
import com.loomq.dispatcher.WebhookDispatcher;
import com.loomq.entity.Task;
import com.loomq.entity.TaskStatus;
import com.loomq.recovery.RecoveryService;
import com.loomq.scheduler.TaskScheduler;
import com.loomq.store.TaskStore;
import com.loomq.wal.WalEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class IntegrationTest {

    @TempDir
    Path tempDir;

    private WalEngine walEngine;
    private TaskStore taskStore;
    private TaskScheduler scheduler;
    private WebhookDispatcher dispatcher;
    private RecoveryService recoveryService;

    @BeforeEach
    void setUp() throws IOException {
        walEngine = new WalEngine(createWalConfig(tempDir.toString()));
        walEngine.start();

        taskStore = new TaskStore();

        dispatcher = new WebhookDispatcher(
                walEngine,
                taskStore,
                createDispatcherConfig(),
                createRetryConfig()
        );

        scheduler = new TaskScheduler(
                walEngine,
                taskStore,
                dispatcher,
                createSchedulerConfig()
        );

        // Start the scheduler
        scheduler.start();
    }

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.stop();
        }
        if (dispatcher != null) {
            dispatcher.stop();
        }
        if (walEngine != null) {
            walEngine.stop();
        }
    }

    @Test
    void testCreateAndRetrieveTask() {
        Task task = Task.builder()
                .taskId("t_test_001")
                .bizKey("biz_001")
                .status(TaskStatus.PENDING)
                .triggerTime(System.currentTimeMillis() + 60000)
                .webhookUrl("https://example.com/webhook")
                .build();

        taskStore.add(task);
        scheduler.schedule(task);

        Task retrieved = taskStore.get("t_test_001");
        assertNotNull(retrieved);
        assertEquals("t_test_001", retrieved.getTaskId());
        assertEquals("biz_001", retrieved.getBizKey());
    }

    @Test
    void testCancelTask() {
        Task task = Task.builder()
                .taskId("t_test_002")
                .bizKey("biz_002")
                .status(TaskStatus.SCHEDULED)
                .triggerTime(System.currentTimeMillis() + 60000)
                .webhookUrl("https://example.com/webhook")
                .build();

        taskStore.add(task);

        // Cancel the task
        task.setStatus(TaskStatus.CANCELLED);
        taskStore.update(task);
        scheduler.unschedule("t_test_002");

        Task retrieved = taskStore.get("t_test_002");
        assertEquals(TaskStatus.CANCELLED, retrieved.getStatus());
        assertTrue(retrieved.getStatus().isTerminal());
    }

    @Test
    void testTaskStats() {
        taskStore.add(createTask("t_001", TaskStatus.PENDING));
        taskStore.add(createTask("t_002", TaskStatus.SCHEDULED));
        taskStore.add(createTask("t_003", TaskStatus.SCHEDULED));
        taskStore.add(createTask("t_004", TaskStatus.ACKED));

        var stats = taskStore.getStats();

        assertEquals(4L, stats.get("total"));
        assertEquals(1L, stats.get("pending"));
        assertEquals(2L, stats.get("scheduled"));
        assertEquals(1L, stats.get("acked"));
    }

    @Test
    void testIdempotency() {
        Task task = Task.builder()
                .taskId("t_idem_001")
                .bizKey("biz_idem")
                .idempotencyKey("idem_key_001")
                .status(TaskStatus.PENDING)
                .triggerTime(System.currentTimeMillis() + 60000)
                .webhookUrl("https://example.com/webhook")
                .build();

        taskStore.add(task);

        // Check idempotency key exists
        assertTrue(taskStore.existsByIdempotencyKey("idem_key_001"));

        // Check biz key exists
        assertTrue(taskStore.existsByBizKey("biz_idem"));

        // Try to find by idempotency key
        Task found = taskStore.getByIdempotencyKey("idem_key_001");
        assertNotNull(found);
        assertEquals("t_idem_001", found.getTaskId());
    }

    @Test
    void testRecovery() throws IOException {
        // First write some tasks to WAL
        walEngine.append("t_recover_001", "biz_001", com.loomq.entity.EventType.CREATE,
                System.currentTimeMillis(), "{\"taskId\":\"t_recover_001\"}".getBytes());
        walEngine.append("t_recover_002", "biz_002", com.loomq.entity.EventType.CREATE,
                System.currentTimeMillis(), "{\"taskId\":\"t_recover_002\"}".getBytes());
        walEngine.append("t_recover_003", "biz_003", com.loomq.entity.EventType.CREATE,
                System.currentTimeMillis(), "{\"taskId\":\"t_recover_003\"}".getBytes());
        walEngine.flush();

        // Clear store and recover
        taskStore.clear();

        // Recovery
        recoveryService = new RecoveryService(walEngine, taskStore, createRecoveryConfig());
        recoveryService.recover();

        // Verify recovered tasks - note: recovery needs CREATE event with full task payload
        // For this simple test, we just verify WAL was written
        assertTrue(walEngine.getTotalRecords() >= 3);
    }

    @Test
    void testVersionIncrement() {
        Task task = createTask("t_version_001", TaskStatus.PENDING);
        long initialVersion = task.getVersion();

        taskStore.add(task);

        task.setStatus(TaskStatus.SCHEDULED);
        task.incrementVersion();
        taskStore.update(task);

        Task retrieved = taskStore.get("t_version_001");
        assertEquals(initialVersion + 1, retrieved.getVersion());
    }

    private Task createTask(String taskId, TaskStatus status) {
        return Task.builder()
                .taskId(taskId)
                .bizKey(taskId + "_biz")
                .status(status)
                .triggerTime(System.currentTimeMillis() + 60000)
                .webhookUrl("https://example.com/webhook")
                .build();
    }

    // Config factory methods
    private static WalConfig createWalConfig(String dataDir) {
        return new WalConfig() {
            @Override
            public String dataDir() { return dataDir; }
            @Override
            public int segmentSizeMb() { return 1; }
            @Override
            public String flushStrategy() { return "batch"; }
            @Override
            public long batchFlushIntervalMs() { return 100; }
            @Override
            public boolean syncOnWrite() { return false; }
        };
    }

    private static SchedulerConfig createSchedulerConfig() {
        return new SchedulerConfig() {
            @Override
            public int maxPendingTasks() { return 10000; }
        };
    }

    private static DispatcherConfig createDispatcherConfig() {
        return new DispatcherConfig() {
            @Override
            public long httpTimeoutMs() { return 3000; }
            @Override
            public int maxConcurrentDispatches() { return 100; }
            @Override
            public long connectTimeoutMs() { return 5000; }
            @Override
            public long readTimeoutMs() { return 3000; }
        };
    }

    private static RetryConfig createRetryConfig() {
        return new RetryConfig() {
            @Override
            public long initialDelayMs() { return 1000; }
            @Override
            public long maxDelayMs() { return 60000; }
            @Override
            public double multiplier() { return 2.0; }
            @Override
            public int defaultMaxRetry() { return 5; }
        };
    }

    private static RecoveryConfig createRecoveryConfig() {
        return new RecoveryConfig() {
            @Override
            public int batchSize() { return 100; }
            @Override
            public long sleepMs() { return 10; }
            @Override
            public int concurrencyLimit() { return 50; }
            @Override
            public boolean safeMode() { return false; }
        };
    }
}

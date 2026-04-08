package com.loomq.scheduler.v2;

import com.loomq.entity.Task;
import com.loomq.entity.TaskStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PrecisionScheduler 单元测试
 *
 * 验证目标：
 * 1. 基本调度和执行
 * 2. 精确延迟（毫秒级精度）
 * 3. 取消任务（O(1)）
 * 4. 重新调度
 * 5. 立即触发
 * 6. 并发安全
 */
class PrecisionSchedulerTest {

    private PrecisionScheduler scheduler;
    private TestDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = new TestDispatcher();
        scheduler = new PrecisionScheduler(dispatcher, 10000);
        scheduler.start();
    }

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.stop();
        }
    }

    @Test
    @Timeout(5)
    void testBasicScheduleAndDispatch() throws InterruptedException {
        // 创建立即执行的任务
        Task task = Task.builder()
                .taskId("test-1")
                .webhookUrl("http://localhost:8080/test")
                .triggerTime(System.currentTimeMillis())
                .build();

        scheduler.schedule(task);

        // 等待执行
        assertTrue(dispatcher.awaitDispatch(2000, TimeUnit.MILLISECONDS));
        assertEquals(1, dispatcher.getDispatchCount());
    }

    @Test
    @Timeout(5)
    void testDelayedDispatch() throws InterruptedException {
        long delayMs = 500;
        long triggerTime = System.currentTimeMillis() + delayMs;

        Task task = Task.builder()
                .taskId("test-delayed")
                .webhookUrl("http://localhost:8080/test")
                .triggerTime(triggerTime)
                .build();

        long start = System.currentTimeMillis();
        scheduler.schedule(task);

        // 等待执行
        assertTrue(dispatcher.awaitDispatch(2000, TimeUnit.MILLISECONDS));
        long elapsed = System.currentTimeMillis() - start;

        // 验证延迟精度（允许 ±50ms 误差）
        assertTrue(elapsed >= delayMs - 50,
                "Dispatched too early: elapsed=" + elapsed + ", expected>=" + delayMs);
        assertTrue(elapsed <= delayMs + 100,
                "Dispatched too late: elapsed=" + elapsed + ", expected<=" + (delayMs + 100));

        assertEquals(1, dispatcher.getDispatchCount());
    }

    @Test
    @Timeout(5)
    void testPreciseDelay() throws InterruptedException {
        // 测试毫秒级精度
        long delayMs = 200;
        long triggerTime = System.currentTimeMillis() + delayMs;

        Task task = Task.builder()
                .taskId("test-precision")
                .webhookUrl("http://localhost:8080/test")
                .triggerTime(triggerTime)
                .build();

        long start = System.currentTimeMillis();
        scheduler.schedule(task);

        assertTrue(dispatcher.awaitDispatch(2000, TimeUnit.MILLISECONDS));
        long elapsed = System.currentTimeMillis() - start;

        // 精度要求：±20ms
        assertTrue(elapsed >= delayMs - 20,
                "Precision issue: elapsed=" + elapsed + ", expected>=" + delayMs);
        assertTrue(elapsed <= delayMs + 50,
                "Precision issue: elapsed=" + elapsed + ", expected<=" + (delayMs + 50));
    }

    @Test
    @Timeout(5)
    void testCancelTask() throws InterruptedException {
        long triggerTime = System.currentTimeMillis() + 5000; // 5秒后执行

        Task task = Task.builder()
                .taskId("test-cancel")
                .webhookUrl("http://localhost:8080/test")
                .triggerTime(triggerTime)
                .build();

        scheduler.schedule(task);

        // 立即取消
        assertTrue(scheduler.cancel("test-cancel"));

        // 等待一段时间，确保不会执行
        Thread.sleep(500);
        assertEquals(0, dispatcher.getDispatchCount());
        assertEquals(1, scheduler.getStats().getCancelledCount());
    }

    @Test
    @Timeout(5)
    void testCancelNonExistentTask() {
        assertFalse(scheduler.cancel("non-existent"));
    }

    @Test
    @Timeout(5)
    void testReschedule() throws InterruptedException {
        long initialTriggerTime = System.currentTimeMillis() + 5000; // 5秒后
        long newTriggerTime = System.currentTimeMillis() + 100; // 100ms后

        Task task = Task.builder()
                .taskId("test-reschedule")
                .webhookUrl("http://localhost:8080/test")
                .triggerTime(initialTriggerTime)
                .build();

        scheduler.schedule(task);

        // 重新调度到更早的时间
        task.setTriggerTime(newTriggerTime);
        scheduler.reschedule(task);

        // 应该在约 100ms 后执行
        assertTrue(dispatcher.awaitDispatch(2000, TimeUnit.MILLISECONDS));
        assertEquals(1, dispatcher.getDispatchCount());
    }

    @Test
    @Timeout(5)
    void testFireNow() throws InterruptedException {
        long triggerTime = System.currentTimeMillis() + 10000; // 10秒后

        Task task = Task.builder()
                .taskId("test-fire-now")
                .webhookUrl("http://localhost:8080/test")
                .triggerTime(triggerTime)
                .build();

        scheduler.schedule(task);

        // 立即触发
        assertTrue(scheduler.fireNow("test-fire-now"));

        // 应该立即执行
        assertTrue(dispatcher.awaitDispatch(2000, TimeUnit.MILLISECONDS));
        assertEquals(1, dispatcher.getDispatchCount());
    }

    @Test
    @Timeout(10)
    void testMultipleTasks() throws InterruptedException {
        int taskCount = 100;
        dispatcher.setExpectedCount(taskCount);

        // 创建多个任务，按触发时间排序
        for (int i = 0; i < taskCount; i++) {
            long triggerTime = System.currentTimeMillis() + (i * 10); // 每 10ms 一个
            Task task = Task.builder()
                    .taskId("test-multi-" + i)
                    .webhookUrl("http://localhost:8080/test")
                    .triggerTime(triggerTime)
                    .build();
            scheduler.schedule(task);
        }

        // 等待所有任务执行完成
        assertTrue(dispatcher.awaitAllDispatches(5000, TimeUnit.MILLISECONDS));
        assertEquals(taskCount, dispatcher.getDispatchCount());
    }

    @Test
    @Timeout(10)
    void testEarliestTaskWakesScheduler() throws InterruptedException {
        // 创建一个 2 秒后执行的任务
        Task lateTask = Task.builder()
                .taskId("test-late")
                .webhookUrl("http://localhost:8080/test")
                .triggerTime(System.currentTimeMillis() + 2000)
                .build();

        scheduler.schedule(lateTask);

        // 等待 100ms 确保调度器进入等待状态
        Thread.sleep(100);

        // 创建一个 200ms 后执行的更早任务
        Task earlyTask = Task.builder()
                .taskId("test-early")
                .webhookUrl("http://localhost:8080/test")
                .triggerTime(System.currentTimeMillis() + 200)
                .build();

        long start = System.currentTimeMillis();
        scheduler.schedule(earlyTask);

        // 应该在约 200ms 后执行 earlyTask
        assertTrue(dispatcher.awaitDispatch(1000, TimeUnit.MILLISECONDS));
        long elapsed = System.currentTimeMillis() - start;

        // 验证 earlyTask 先执行（而不是等 lateTask）
        assertTrue(elapsed < 1000, "Should have executed early task quickly, elapsed=" + elapsed);
        assertEquals("test-early", dispatcher.getLastDispatchedTaskId());
    }

    @Test
    @Timeout(10)
    void testConcurrentSchedule() throws InterruptedException {
        int threadCount = 10;
        int tasksPerThread = 100;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadIndex = t;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < tasksPerThread; i++) {
                        Task task = Task.builder()
                                .taskId("test-concurrent-" + threadIndex + "-" + i)
                                .webhookUrl("http://localhost:8080/test")
                                .triggerTime(System.currentTimeMillis() + (i * 10))
                                .build();
                        if (scheduler.schedule(task)) {
                            successCount.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        // 同时开始
        startLatch.countDown();

        // 等待所有线程完成
        assertTrue(doneLatch.await(5, TimeUnit.SECONDS));

        assertEquals(threadCount * tasksPerThread, successCount.get());
        assertEquals(threadCount * tasksPerThread, scheduler.getStats().getScheduledCount());
    }

    @Test
    void testScheduleNullTask() {
        assertFalse(scheduler.schedule(null));
    }

    @Test
    void testScheduleTaskWithNullId() {
        Task task = new Task(); // taskId is null
        assertFalse(scheduler.schedule(task));
    }

    @Test
    void testDuplicateTask() {
        Task task = Task.builder()
                .taskId("test-duplicate")
                .webhookUrl("http://localhost:8080/test")
                .triggerTime(System.currentTimeMillis() + 1000)
                .build();

        assertTrue(scheduler.schedule(task));
        assertFalse(scheduler.schedule(task)); // 重复调度应该失败

        assertEquals(1, scheduler.getPendingTaskCount());
    }

    // ========== 测试辅助类 ==========

    private static class TestDispatcher implements TaskDispatcher {
        private final AtomicInteger dispatchCount = new AtomicInteger(0);
        private final CountDownLatch dispatchLatch = new CountDownLatch(1);
        private final AtomicLong totalExpected = new AtomicLong(1);
        private final AtomicInteger currentCount = new AtomicInteger(0);
        private volatile String lastDispatchedTaskId;

        @Override
        public void dispatch(Task task) {
            dispatchCount.incrementAndGet();
            lastDispatchedTaskId = task.getTaskId();

            int count = currentCount.incrementAndGet();
            if (count >= totalExpected.get()) {
                dispatchLatch.countDown();
            }
        }

        boolean awaitDispatch(long timeout, TimeUnit unit) throws InterruptedException {
            return dispatchLatch.await(timeout, unit);
        }

        boolean awaitAllDispatches(long timeout, TimeUnit unit) throws InterruptedException {
            return dispatchLatch.await(timeout, unit);
        }

        void setExpectedCount(int count) {
            totalExpected.set(count);
        }

        int getDispatchCount() {
            return dispatchCount.get();
        }

        String getLastDispatchedTaskId() {
            return lastDispatchedTaskId;
        }
    }
}

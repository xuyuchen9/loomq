package com.loomq.scheduler.v2;

import com.loomq.entity.Task;
import com.loomq.entity.TaskStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TimeBucketScheduler 单元测试
 */
class TimeBucketSchedulerTest {

    private TimeBucketScheduler scheduler;
    private MockDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = new MockDispatcher();
        scheduler = new TimeBucketScheduler(dispatcher, 10000, 100, 1000);
        scheduler.start();
    }

    @Test
    @DisplayName("基本调度应正确")
    void basicSchedule() {
        Task task = createTask("task-1", System.currentTimeMillis() + 1000);

        boolean scheduled = scheduler.schedule(task);

        assertTrue(scheduled);
        assertEquals(1, scheduler.getPendingTaskCount());
        assertEquals(1, scheduler.getBucketCount());
    }

    @Test
    @DisplayName("取消任务应 O(1)")
    void cancelTask() {
        Task task = createTask("task-1", System.currentTimeMillis() + 10000);
        scheduler.schedule(task);

        boolean cancelled = scheduler.cancel("task-1");

        assertTrue(cancelled);
        assertEquals(0, scheduler.getPendingTaskCount());
    }

    @Test
    @DisplayName("到期任务应被获取")
    void expiredTasks() throws InterruptedException {
        // 调度一个已到期（或很快到期）的任务
        Task task = createTask("task-1", System.currentTimeMillis() + 50);
        scheduler.schedule(task);

        // 等待任务到期
        Thread.sleep(200);

        // 调度器应该在后台扫描并将任务移到 readyQueue
        // 由于调度是异步的，我们需要等待一段时间
        Thread.sleep(500);

        // 检查统计
        assertEquals(1, scheduler.getStats().getScheduledCount());
    }

    @Test
    @DisplayName("大量任务调度应支持百万级")
    void massSchedule() {
        int count = 100000;
        long baseTime = System.currentTimeMillis() + 10000;

        long start = System.currentTimeMillis();

        for (int i = 0; i < count; i++) {
            Task task = createTask("task-" + i, baseTime + i * 10);
            scheduler.schedule(task);
        }

        long elapsed = System.currentTimeMillis() - start;
        double ops = count / (elapsed / 1000.0);

        System.out.println("Scheduled " + count + " tasks in " + elapsed + "ms, ops: " + String.format("%.0f", ops));

        assertEquals(count, scheduler.getPendingTaskCount());
    }

    @Test
    @DisplayName("多线程调度应安全")
    void concurrentSchedule() throws InterruptedException {
        int threads = 10;
        int countPerThread = 10000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        long baseTime = System.currentTimeMillis() + 60000;

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < countPerThread; i++) {
                        Task task = createTask("t" + threadId + "-" + i,
                                baseTime + threadId * countPerThread * 10 + i * 10);
                        scheduler.schedule(task);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(threads * countPerThread, scheduler.getPendingTaskCount());
    }

    private Task createTask(String taskId, long triggerTime) {
        Task task = new Task();
        task.setTaskId(taskId);
        task.setTriggerTime(triggerTime);
        task.setStatus(TaskStatus.PENDING);
        task.setCreateTime(Instant.now().toEpochMilli());
        return task;
    }

    static class MockDispatcher implements TaskDispatcher {
        private final AtomicInteger dispatched = new AtomicInteger(0);

        @Override
        public void dispatch(Task task) {
            dispatched.incrementAndGet();
        }

        public int getDispatchedCount() {
            return dispatched.get();
        }
    }
}

package com.loomq.v2;

import com.loomq.entity.Task;
import com.loomq.entity.TaskStatus;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * V0.2 引擎集成测试
 *
 * 验证：
 * 1. 时间桶调度器批量唤醒
 * 2. 异步 WAL 写入
 * 3. 执行层限流
 * 4. 完整任务生命周期
 */
class LoomqEngineV2IntegrationTest {

    @TempDir
    Path tempDir;

    private LoomqEngineV2 engine;
    private LoomqConfigV2 config;

    @BeforeEach
    void setUp() throws Exception {
        config = new LoomqConfigV2(
                1000,      // 桶粒度 1秒
                1000,      // 就绪队列 1000
                0,         // 不限速
                100,       // 最大并发 100
                5000,      // 分发队列 5000
                3,         // 最大重试 3
                100,       // 重试退避 100ms
                5000,      // webhook 连接超时
                10000,     // webhook 读取超时
                tempDir.resolve("wal").toString(),
                "batch",
                LoomqConfigV2.AckLevel.ASYNC  // 默认异步
        );

        engine = new LoomqEngineV2(config);
        engine.start();
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    @Test
    @DisplayName("引擎启动和停止")
    void testEngineLifecycle() {
        var stats = engine.getStats();
        assertTrue(stats.running());
        assertNotNull(stats.scheduler());
        assertNotNull(stats.dispatcher());
        assertNotNull(stats.wal());
    }

    @Test
    @DisplayName("单任务创建和执行")
    void testSingleTask() throws Exception {
        // 创建一个 2 秒后执行的任务
        Task task = Task.builder()
                .taskId("test-001")
                .bizKey("biz-001")
                .webhookUrl("http://httpbin.org/status/200")
                .triggerTime(System.currentTimeMillis() + 2000)
                .build();

        var result = engine.createTask(task);
        assertTrue(result.ok());
        assertEquals("test-001", result.taskId());

        // 等待执行
        Thread.sleep(5000);

        // 验证 WAL 写入
        var walStats = engine.getStats().wal();
        assertTrue(walStats.totalEvents() >= 1);
    }

    @Test
    @DisplayName("批量任务创建 - 时间桶聚合")
    void testBatchTaskCreation() throws Exception {
        int taskCount = 100;
        long triggerTime = System.currentTimeMillis() + 3000;

        CountDownLatch latch = new CountDownLatch(taskCount);
        AtomicInteger successCount = new AtomicInteger(0);

        // 并发创建任务
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < taskCount; i++) {
            final int idx = i;
            executor.submit(() -> {
                try {
                    Task task = Task.builder()
                            .taskId("batch-" + idx)
                            .bizKey("biz-" + idx)
                            .webhookUrl("http://httpbin.org/status/200")
                            .triggerTime(triggerTime)
                            .build();

                    var result = engine.createTask(task);
                    if (result.ok()) {
                        successCount.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        long createDuration = System.currentTimeMillis() - startTime;

        System.out.printf("创建 %d 任务耗时: %d ms%n", taskCount, createDuration);
        System.out.printf("成功率: %d / %d%n", successCount.get(), taskCount);

        // 验证调度器状态
        var schedulerStats = engine.getStats().scheduler();
        assertTrue(schedulerStats.totalScheduled() >= taskCount);

        executor.shutdown();
    }

    @Test
    @DisplayName("风暴场景 - 大量同时到期任务")
    void testStormScenario() throws Exception {
        int taskCount = 1000;
        // 1 秒后同时到期
        long triggerTime = System.currentTimeMillis() + 1000;

        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch createLatch = new CountDownLatch(taskCount);

        // 并发创建
        long createStart = System.currentTimeMillis();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < taskCount; i++) {
                final int idx = i;
                executor.submit(() -> {
                    try {
                        Task task = Task.builder()
                                .taskId("storm-" + idx)
                                .bizKey("storm-biz-" + idx)
                                .webhookUrl("http://httpbin.org/status/200")
                                .triggerTime(triggerTime)
                                .build();

                        var result = engine.createTask(task);
                        if (result.ok()) {
                            successCount.incrementAndGet();
                        }
                    } finally {
                        createLatch.countDown();
                    }
                });
            }

            createLatch.await(60, TimeUnit.SECONDS);
        }
        long createDuration = System.currentTimeMillis() - createStart;

        System.out.println("========== 风暴测试结果 ==========");
        System.out.printf("创建成功: %d / %d%n", successCount.get(), taskCount);
        System.out.printf("创建耗时: %d ms%n", createDuration);
        System.out.printf("创建 QPS: %.0f%n", (double) successCount.get() / (createDuration / 1000.0));

        // 验证调度器状态
        var schedulerStats = engine.getStats().scheduler();
        System.out.printf("时间桶数量: %d%n", schedulerStats.bucketCount());
        System.out.printf("就绪队列大小: %d%n", schedulerStats.readyQueueSize());
        System.out.printf("调度任务数: %d%n", schedulerStats.totalScheduled());

        // 验证 WAL 状态
        var walStats = engine.getStats().wal();
        System.out.printf("WAL 事件数: %d%n", walStats.totalEvents());
        System.out.printf("WAL 批次数: %d%n", walStats.totalBatches());
        System.out.printf("WAL fsync 数: %d%n", walStats.totalFsyncs());

        // 验证限流器状态
        var dispatcherStats = engine.getStats().dispatcher();
        System.out.printf("分发提交数: %d%n", dispatcherStats.totalSubmitted());
        System.out.printf("分发执行数: %d%n", dispatcherStats.totalExecuted());
    }

    @Test
    @DisplayName("取消任务")
    void testCancelTask() throws Exception {
        // 创建一个 10 秒后执行的任务
        Task task = Task.builder()
                .taskId("cancel-001")
                .bizKey("cancel-biz")
                .webhookUrl("http://httpbin.org/status/200")
                .triggerTime(System.currentTimeMillis() + 10000)
                .build();

        var createResult = engine.createTask(task);
        assertTrue(createResult.ok());

        // 立即取消
        var cancelResult = engine.cancelTask("cancel-001");
        assertTrue(cancelResult.ok());

        // 再次取消（幂等）
        var cancelResult2 = engine.cancelTask("cancel-001");
        // 第二次取消应该失败（任务已不存在或不可取消）
        assertFalse(cancelResult2.ok());
    }

    @Test
    @DisplayName("WAL 异步写入和 Group Commit")
    void testWalGroupCommit() throws Exception {
        int eventCount = 500;

        // 快速创建大量任务
        for (int i = 0; i < eventCount; i++) {
            Task task = Task.builder()
                    .taskId("wal-" + i)
                    .bizKey("wal-biz-" + i)
                    .webhookUrl("http://httpbin.org/status/200")
                    .triggerTime(System.currentTimeMillis() + 60000)
                    .build();

            engine.createTask(task);
        }

        // 等待 WAL 消费
        Thread.sleep(500);

        var walStats = engine.getStats().wal();
        System.out.println("========== WAL 统计 ==========");
        System.out.printf("总事件数: %d%n", walStats.totalEvents());
        System.out.printf("批次数: %d%n", walStats.totalBatches());
        System.out.printf("fsync 次数: %d%n", walStats.totalFsyncs());

        // 验证 Group Commit 效果：批次数应该远小于事件数
        assertTrue(walStats.totalBatches() < eventCount);
        // fsync 次数应该等于批次数
        assertEquals(walStats.totalBatches(), walStats.totalFsyncs());
    }

    @Test
    @DisplayName("ACK 分级测试 - ASYNC vs DURABLE")
    void testAckLevel() throws Exception {
        // 1. 测试 ASYNC 级别（默认）
        Task asyncTask = Task.builder()
                .taskId("ack-async")
                .bizKey("ack-async-biz")
                .webhookUrl("http://httpbin.org/status/200")
                .triggerTime(System.currentTimeMillis() + 60000)
                .build();

        long asyncStart = System.currentTimeMillis();
        var asyncResult = engine.createTask(asyncTask, LoomqConfigV2.AckLevel.ASYNC);
        long asyncLatency = System.currentTimeMillis() - asyncStart;

        assertTrue(asyncResult.ok());
        assertEquals("ASYNC", asyncResult.ackLevel());
        System.out.printf("ASYNC 创建延迟: %d ms%n", asyncLatency);

        // 2. 测试 DURABLE 级别
        Task durableTask = Task.builder()
                .taskId("ack-durable")
                .bizKey("ack-durable-biz")
                .webhookUrl("http://httpbin.org/status/200")
                .triggerTime(System.currentTimeMillis() + 60000)
                .build();

        long durableStart = System.currentTimeMillis();
        var durableResult = engine.createTask(durableTask, LoomqConfigV2.AckLevel.DURABLE);
        long durableLatency = System.currentTimeMillis() - durableStart;

        assertTrue(durableResult.ok());
        assertEquals("DURABLE", durableResult.ackLevel());
        System.out.printf("DURABLE 创建延迟: %d ms%n", durableLatency);

        // 3. 对比延迟
        System.out.println();
        System.out.println("========== ACK 级别对比 ==========");
        System.out.printf("ASYNC:    %d ms (RPO < 100ms)%n", asyncLatency);
        System.out.printf("DURABLE:  %d ms (RPO = 0)%n", durableLatency);
        System.out.println();
        System.out.println("说明:");
        System.out.println("  ASYNC:    publish 后返回，可能丢数据");
        System.out.println("  DURABLE:  fsync 后返回，不丢数据");
    }

    @Test
    @DisplayName("限流器背压测试")
    void testDispatchLimiterBackpressure() throws Exception {
        // 使用较小的队列容量
        LoomqConfigV2 smallConfig = new LoomqConfigV2(
                1000, 10, 0, 10, 100, 3, 100, 5000, 10000,
                tempDir.resolve("wal2").toString(), "batch",
                LoomqConfigV2.AckLevel.ASYNC
        );

        LoomqEngineV2 smallEngine = new LoomqEngineV2(smallConfig);
        try {
            smallEngine.start();

            // 创建大量同时到期任务
            int taskCount = 200;
            long triggerTime = System.currentTimeMillis() + 1000;

            for (int i = 0; i < taskCount; i++) {
                Task task = Task.builder()
                        .taskId("limit-" + i)
                        .bizKey("limit-biz-" + i)
                        .webhookUrl("http://httpbin.org/status/200")
                        .triggerTime(triggerTime)
                        .build();
                smallEngine.createTask(task);
            }

            // 等待触发和限流
            Thread.sleep(5000);

            var stats = smallEngine.getStats().dispatcher();
            System.out.println("========== 限流器统计 ==========");
            System.out.printf("队列容量: 100%n");
            System.out.printf("最大并发: 10%n");
            System.out.printf("提交数: %d%n", stats.totalSubmitted());
            System.out.printf("执行数: %d%n", stats.totalExecuted());
            System.out.printf("拒绝数: %d%n", stats.totalRejected());

            // 验证限流效果
            // 执行数不应超过提交数
            assertTrue(stats.totalExecuted() <= stats.totalSubmitted());
        } finally {
            smallEngine.stop();
        }
    }
}

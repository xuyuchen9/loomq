package com.loomq.wal;

import com.loomq.entity.EventType;
import com.loomq.test.TestUtils;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * WalWriter 统一单元测试
 *
 * 合并自:
 * - WalWriterBasicTest (基础功能)
 * - WalWriterImprovedTest (健康检查、背压、Future分离)
 *
 * 测试分类:
 * 1. 生命周期测试 - 创建、启动、停止、关闭
 * 2. 健康监控测试 - 健康状态、刷盘时间、错误计数
 * 3. 写入语义测试 - ASYNC/DURABLE 语义、Future 分离
 * 4. 背压控制测试 - 超时写入、RingBuffer 压力
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class WalWriterTest {

    @TempDir
    Path tempDir;

    private WalWriter writer;

    // ==================== 生命周期测试 ====================

    @Test
    @Order(1)
    @DisplayName("生命周期 - 创建 Writer")
    void testCreate() throws IOException {
        writer = new WalWriter(TestUtils.createFastFlushWalConfig(tempDir), "test-create");
        assertNotNull(writer);
        assertEquals(0, writer.getRecordCount());
        // 创建后未启动，running 状态为 false
        assertFalse(writer.getHealthStatus().running());
    }

    @Test
    @Order(2)
    @DisplayName("生命周期 - 启动和停止")
    void testStartAndStop() throws IOException {
        writer = new WalWriter(TestUtils.createFastFlushWalConfig(tempDir), "test-lifecycle");

        writer.start();
        assertTrue(writer.isHealthy());
        assertTrue(writer.getHealthStatus().running());

        // 停止后状态
        writer.close();
        assertFalse(writer.getHealthStatus().running());
    }

    @Test
    @Order(3)
    @DisplayName("生命周期 - 幂等关闭")
    void testIdempotentClose() throws IOException {
        writer = new WalWriter(TestUtils.createFastFlushWalConfig(tempDir), "test-idempotent");
        writer.start();

        // 多次关闭不应抛异常
        assertDoesNotThrow(() -> writer.close());
        assertDoesNotThrow(() -> writer.close());
        assertDoesNotThrow(() -> writer.close());
    }

    @Test
    @Order(4)
    @DisplayName("生命周期 - 关闭后数据刷盘")
    void testCloseFlushesData() throws Exception {
        writer = new WalWriter(TestUtils.createSlowFlushWalConfig(tempDir), "test-close-flush");
        writer.start();

        // 写入数据
        for (int i = 0; i < 10; i++) {
            writer.appendAsync("close-" + i, null, EventType.CREATE,
                System.currentTimeMillis(), TestUtils.testPayload("data-" + i));
        }

        // 关闭应该刷空缓冲区
        writer.close();

        // 验证统计
        assertTrue(writer.getStats().getWriteCount() >= 10);
    }

    // ==================== 健康监控测试 ====================

    @Test
    @Order(10)
    @DisplayName("健康监控 - 初始状态")
    void testHealthInitial() throws IOException {
        writer = new WalWriter(TestUtils.createFastFlushWalConfig(tempDir), "health-initial");
        writer.start();

        try {
            assertTrue(writer.isHealthy());

            WalWriter.HealthStatus status = writer.getHealthStatus();
            assertTrue(status.healthy());
            assertTrue(status.running());
            assertEquals(0, status.errorCount());
        } finally {
            writer.close();
        }
    }

    @Test
    @Order(11)
    @DisplayName("健康监控 - 刷盘时间更新")
    void testHealthFlushTimeUpdate() throws Exception {
        writer = new WalWriter(TestUtils.createFastFlushWalConfig(tempDir), "health-flush-time");
        writer.start();

        try {
            long beforeTime = writer.getLastFlushTime();

            // 写入并等待刷盘
            writer.appendAsync("flush-test", null, EventType.CREATE,
                System.currentTimeMillis(), "data".getBytes());

            Thread.sleep(100); // 等待刷盘

            // 强制刷盘确保更新
            writer.flushNow();

            long afterTime = writer.getLastFlushTime();
            assertTrue(afterTime >= beforeTime,
                "Last flush time should be updated: before=" + beforeTime + ", after=" + afterTime);
        } finally {
            writer.close();
        }
    }

    @Test
    @Order(12)
    @DisplayName("健康监控 - 错误计数器初始为0")
    void testHealthErrorCountInitial() throws IOException {
        writer = new WalWriter(TestUtils.createFastFlushWalConfig(tempDir), "health-error");
        writer.start();

        try {
            assertEquals(0, writer.getFlushErrorCount());
        } finally {
            writer.close();
        }
    }

    @Test
    @Order(13)
    @DisplayName("健康监控 - HealthStatus 详情")
    void testHealthStatusDetails() throws Exception {
        writer = new WalWriter(TestUtils.createFastFlushWalConfig(tempDir), "health-details");
        writer.start();

        try {
            // 写入多条记录
            for (int i = 0; i < 5; i++) {
                writer.appendAsync("status-" + i, null, EventType.CREATE,
                    System.currentTimeMillis(), new byte[0]);
            }

            WalWriter.HealthStatus status = writer.getHealthStatus();
            assertTrue(status.running());
            assertEquals(0, status.errorCount());

            // 统计信息
            WalWriter.Stats stats = writer.getStats();
            assertTrue(stats.getWriteCount() >= 5);
        } finally {
            writer.close();
        }
    }

    // ==================== 写入语义测试 ====================

    @Test
    @Order(20)
    @DisplayName("写入语义 - ASYNC 立即返回")
    void testAsyncSemantics() throws Exception {
        writer = new WalWriter(TestUtils.createSlowFlushWalConfig(tempDir), "async-test");
        writer.start();

        try {
            long startTime = System.currentTimeMillis();

            // ASYNC: 应该立即返回
            CompletableFuture<Long> future = writer.appendAsync("async-task", null,
                EventType.CREATE, System.currentTimeMillis(), "data".getBytes());

            long elapsed = System.currentTimeMillis() - startTime;

            assertTrue(elapsed < 50, "ASYNC should return immediately, took " + elapsed + "ms");
            assertNotNull(future);

            // 等待入队完成
            assertTrue(future.get(1, TimeUnit.SECONDS) > 0);
        } finally {
            writer.close();
        }
    }

    @Test
    @Order(21)
    @DisplayName("写入语义 - DURABLE 等待刷盘")
    void testDurableSemantics() throws Exception {
        writer = new WalWriter(TestUtils.createFastFlushWalConfig(tempDir), "durable-test");
        writer.start();

        try {
            // 写入并获取序列号
            var committed = writer.appendAsync("durable-task", null,
                EventType.CREATE, System.currentTimeMillis(), "data".getBytes());

            long position = committed.get(1, TimeUnit.SECONDS);
            assertTrue(position > 0);

            // 等待刷盘
            long sequence = writer.getRecordCount();
            CompletableFuture<Long> flushed = writer.waitForFlush(sequence);

            Long flushPosition = flushed.get(2, TimeUnit.SECONDS);
            assertNotNull(flushPosition);
            assertTrue(flushPosition >= position);
        } finally {
            writer.close();
        }
    }

    @Test
    @Order(22)
    @DisplayName("写入语义 - flushNow 强制刷盘")
    void testFlushNow() throws Exception {
        writer = new WalWriter(TestUtils.createSlowFlushWalConfig(tempDir), "flush-now-test");
        writer.start();

        try {
            long beforeFlush = writer.getLastFlushTime();

            // 写入数据
            writer.appendAsync("flush-now", null, EventType.CREATE,
                System.currentTimeMillis(), "data".getBytes());

            // 强制刷盘
            writer.flushNow();

            long afterFlush = writer.getLastFlushTime();
            assertTrue(afterFlush > beforeFlush, "flushNow should update last flush time");
        } finally {
            writer.close();
        }
    }

    // ==================== 背压控制测试 ====================

    @Test
    @Order(30)
    @DisplayName("背压控制 - 超时写入正常返回")
    void testBackpressureTimeoutSuccess() throws Exception {
        writer = new WalWriter(TestUtils.createFastFlushWalConfig(tempDir), "backpressure-timeout");
        writer.start();

        try {
            // 正常情况应该成功
            var future = writer.appendAsyncWithTimeout("timeout-test", null,
                EventType.CREATE, System.currentTimeMillis(),
                "data".getBytes(), 100, TimeUnit.MILLISECONDS);

            assertNotNull(future);
            assertTrue(future.get(500, TimeUnit.MILLISECONDS) > 0);
        } finally {
            writer.close();
        }
    }

    @Test
    @Order(31)
    @DisplayName("背压控制 - 批量写入无溢出")
    void testBackpressureBatchWrite() throws Exception {
        writer = new WalWriter(TestUtils.createFastFlushWalConfig(tempDir), "backpressure-batch");
        writer.start();

        try {
            int count = 100;
            for (int i = 0; i < count; i++) {
                var future = writer.appendAsync("batch-" + i, null, EventType.CREATE,
                    System.currentTimeMillis(), TestUtils.testPayload("data-" + i));
                assertTrue(future.get(1, TimeUnit.SECONDS) > 0);
            }

            // 检查无溢出
            assertEquals(0, writer.getStats().getOverflowCount());
        } finally {
            writer.close();
        }
    }

    // ==================== 统计测试 ====================

    @Test
    @Order(40)
    @DisplayName("统计 - 写入计数准确")
    void testStatsWriteCount() throws Exception {
        writer = new WalWriter(TestUtils.createFastFlushWalConfig(tempDir), "stats-test");
        writer.start();

        try {
            int count = 20;
            for (int i = 0; i < count; i++) {
                writer.appendAsync("stats-" + i, null, EventType.CREATE,
                    System.currentTimeMillis(), new byte[0]);
            }

            WalWriter.Stats stats = writer.getStats();
            assertTrue(stats.getWriteCount() >= count);
            assertTrue(stats.toString().contains("writes="));
        } finally {
            writer.close();
        }
    }

    @Test
    @Order(41)
    @DisplayName("统计 - 初始状态")
    void testStatsInitial() throws IOException {
        writer = new WalWriter(TestUtils.createFastFlushWalConfig(tempDir), "stats-initial");

        WalWriter.Stats stats = writer.getStats();
        assertEquals(0, stats.getWriteCount());
        assertEquals(0, stats.getOverflowCount());
    }
}

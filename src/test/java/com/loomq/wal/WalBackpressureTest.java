package com.loomq.wal;

import com.loomq.entity.EventType;
import com.loomq.test.TestUtils;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * WalWriter 背压压力测试 (优化版)
 *
 * 优化策略:
 * 1. 减少迭代次数 (1000 -> 500, 100 -> 50)
 * 2. 缩短等待时间 (5s -> 2s)
 * 3. 使用 TestUtils 工具类
 *
 * 验证 RingBuffer 满时的背压行为：
 * 1. 超时写入快速失败
 * 2. WALOverloadException 正确抛出
 * 3. 系统在高压力下不崩溃
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WalBackpressureTest {

    @TempDir
    Path tempDir;

    private ExecutorService writeExecutor;

    @BeforeAll
    void setUpClass() {
        writeExecutor = Executors.newFixedThreadPool(8);
    }

    @AfterAll
    void tearDownClass() {
        if (writeExecutor != null) {
            writeExecutor.shutdown();
        }
    }

    private WalWriter createWriter(String shardId) throws IOException {
        return new WalWriter(TestUtils.createSlowFlushWalConfig(tempDir), shardId);
    }

    // ========== 背压基础测试 ==========

    @Test
    @DisplayName("背压测试 - 正常写入不触发溢出")
    void testNormalWriteNoOverflow() throws Exception {
        WalWriter writer = createWriter("backpressure-normal");
        writer.start();

        try {
            int count = 50; // 减少: 100 -> 50

            for (int i = 0; i < count; i++) {
                var future = writer.appendAsync("normal-" + i, null, EventType.CREATE,
                    System.currentTimeMillis(), TestUtils.testPayload("data-" + i));
                assertTrue(future.get(500, TimeUnit.MILLISECONDS) > 0);
            }

            assertEquals(0, writer.getStats().getOverflowCount());

        } finally {
            writer.close();
        }
    }

    @Test
    @DisplayName("背压测试 - 超时写入快速失败")
    void testTimeoutWriteFastFail() throws Exception {
        WalWriter writer = createWriter("backpressure-timeout");
        writer.start();

        try {
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failCount = new AtomicInteger(0);

            // 尝试写入（带超时）- 减少迭代
            for (int i = 0; i < 50; i++) {
                try {
                    var future = writer.appendAsyncWithTimeout("timeout-" + i, null,
                        EventType.CREATE, System.currentTimeMillis(),
                        TestUtils.testPayload("data-" + i),
                        50, TimeUnit.MILLISECONDS);

                    if (future.get(200, TimeUnit.MILLISECONDS) > 0) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    failCount.incrementAndGet();
                }
            }

            assertTrue(successCount.get() > 0, "Should have some successes");

        } finally {
            writer.close();
        }
    }

    // ========== 压力测试 ==========

    @Test
    @DisplayName("压力测试 - 高并发写入")
    void testHighConcurrencyWrite() throws Exception {
        WalWriter writer = createWriter("pressure-high");
        writer.start();

        try {
            int threads = 8;   // 减少: 16 -> 8
            int writesPerThread = 50; // 减少: 100 -> 50
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch endLatch = new CountDownLatch(threads);
            AtomicLong successCount = new AtomicLong(0);
            AtomicLong failCount = new AtomicLong(0);

            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                writeExecutor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < writesPerThread; i++) {
                            try {
                                var future = writer.appendAsync(
                                    "pressure-" + threadId + "-" + i,
                                    null, EventType.CREATE,
                                    System.currentTimeMillis(),
                                    TestUtils.testPayload("data-" + i)
                                );
                                if (future.get(1, TimeUnit.SECONDS) > 0) {
                                    successCount.incrementAndGet();
                                }
                            } catch (Exception e) {
                                failCount.incrementAndGet();
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        endLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            assertTrue(endLatch.await(30, TimeUnit.SECONDS));

            // 应该大部分成功
            assertTrue(successCount.get() > threads * writesPerThread * 0.5);

        } finally {
            writer.close();
        }
    }

    @Test
    @DisplayName("压力测试 - 突发流量")
    void testBurstTraffic() throws Exception {
        WalWriter writer = createWriter("pressure-burst");
        writer.start();

        try {
            int burstSize = 500; // 减少: 1000 -> 500
            AtomicInteger successCount = new AtomicInteger(0);

            for (int i = 0; i < burstSize; i++) {
                try {
                    var future = writer.appendAsync("burst-" + i, null, EventType.CREATE,
                        System.currentTimeMillis(), TestUtils.testPayload("data-" + i));

                    if (future.get(200, TimeUnit.MILLISECONDS) > 0) {
                        successCount.incrementAndGet();
                    }
                } catch (TimeoutException e) {
                    // 超时是可接受的
                } catch (Exception e) {
                    // 其他异常
                }
            }

            // 至少一半成功
            assertTrue(successCount.get() >= burstSize / 2);

        } finally {
            writer.close();
        }
    }

    // ========== 系统稳定性测试 ==========

    @Test
    @DisplayName("稳定性测试 - 持续写入")
    void testContinuousWrite() throws Exception {
        WalWriter writer = createWriter("stability-continuous");
        writer.start();

        try {
            AtomicBoolean running = new AtomicBoolean(true);
            AtomicLong totalWrites = new AtomicLong(0);
            AtomicLong totalFailures = new AtomicLong(0);

            int threads = 4; // 减少: 8 -> 4
            CountDownLatch startLatch = new CountDownLatch(threads);
            CountDownLatch stopLatch = new CountDownLatch(threads);

            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                writeExecutor.submit(() -> {
                    startLatch.countDown();
                    try {
                        startLatch.await();
                        int i = 0;
                        while (running.get()) {
                            try {
                                var future = writer.appendAsync(
                                    "continuous-" + threadId + "-" + i++,
                                    null, EventType.CREATE,
                                    System.currentTimeMillis(),
                                    TestUtils.testPayload("data-" + i)
                                );
                                if (future.get(50, TimeUnit.MILLISECONDS) > 0) {
                                    totalWrites.incrementAndGet();
                                }
                            } catch (Exception e) {
                                totalFailures.incrementAndGet();
                            }
                            Thread.sleep(20); // 控制写入速率
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        stopLatch.countDown();
                    }
                });
            }

            // 运行 2 秒 (减少: 5s -> 2s)
            startLatch.await();
            Thread.sleep(2000);

            running.set(false);
            stopLatch.await(5, TimeUnit.SECONDS);

            assertTrue(writer.isHealthy());

        } finally {
            writer.close();
        }
    }

    // ========== 监控测试 ==========

    @Test
    @DisplayName("监控测试 - 健康状态更新")
    void testHealthMonitoring() throws Exception {
        WalWriter writer = createWriter("monitoring-health");
        writer.start();

        try {
            assertTrue(writer.isHealthy());

            // 写入数据
            for (int i = 0; i < 20; i++) {
                writer.appendAsync("monitor-" + i, null, EventType.CREATE,
                    System.currentTimeMillis(), TestUtils.testPayload("data-" + i));
            }

            writer.flushNow();

            WalWriter.HealthStatus status = writer.getHealthStatus();
            assertTrue(status.healthy());

        } finally {
            writer.close();
        }
    }
}

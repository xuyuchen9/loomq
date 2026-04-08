package com.loomq.wal.v2;

import com.loomq.config.WalConfig;
import com.loomq.entity.EventType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * WalWriterV2 单元测试
 */
class WalWriterV2Test {

    @TempDir
    Path tempDir;

    private WalConfig config;
    private WalWriterV2 writer;

    @BeforeEach
    void setUp() {
        config = new WalConfig() {
            @Override
            public String dataDir() { return tempDir.toString(); }
            @Override
            public int segmentSizeMb() { return 64; }
            @Override
            public String flushStrategy() { return "batch"; }
            @Override
            public long batchFlushIntervalMs() { return 100; }
            @Override
            public boolean syncOnWrite() { return false; }
        };
    }

    @AfterEach
    void tearDown() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    @Test
    @DisplayName("基本写入应成功")
    void basicWrite() throws IOException {
        writer = new WalWriterV2(config, "shard-0");
        writer.start();

        byte[] payload = "test payload".getBytes();
        long position = writer.append("task-1", "biz-1", EventType.CREATE, System.currentTimeMillis(), payload);

        assertTrue(position > 0);
        assertEquals(1, writer.getRecordCount());
    }

    @Test
    @DisplayName("批量写入应高效（异步批量模式）")
    void batchWritePerformance() throws IOException, InterruptedException {
        writer = new WalWriterV2(config, "shard-0");
        writer.start();

        int count = 100000;
        int batchSize = 1000;
        byte[] payload = new byte[100];

        long start = System.currentTimeMillis();

        // 批量异步写入（真正的 Group Commit 模式）
        List<CompletableFuture<Long>> futures = new ArrayList<>(batchSize);

        for (int i = 0; i < count; i++) {
            CompletableFuture<Long> future = writer.appendAsync(
                    "task-" + i, "biz-" + i, EventType.CREATE,
                    System.currentTimeMillis(), payload);
            futures.add(future);

            // 每 batchSize 个等待一次，避免内存溢出
            if (futures.size() >= batchSize) {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                futures.clear();
            }
        }

        // 等待剩余的
        if (!futures.isEmpty()) {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        }

        long elapsed = System.currentTimeMillis() - start;
        double qps = count / (elapsed / 1000.0);

        System.out.println("Write " + count + " records in " + elapsed + "ms, QPS: " + String.format("%.0f", qps));
        System.out.println("Stats: " + writer.getStats());

        assertTrue(qps > 50000, "QPS should be > 50,000, actual: " + qps);
        assertEquals(count, writer.getRecordCount());
    }

    @Test
    @DisplayName("多线程写入应正确")
    void concurrentWrite() throws IOException, InterruptedException, ExecutionException {
        writer = new WalWriterV2(config, "shard-0");
        writer.start();

        int threads = 4;
        int countPerThread = 10000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        List<Future<Integer>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                byte[] payload = new byte[100];
                int success = 0;
                for (int i = 0; i < countPerThread; i++) {
                    try {
                        writer.append("t" + threadId + "-task-" + i, "biz-" + i,
                                EventType.CREATE, System.currentTimeMillis(), payload);
                        success++;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                return success;
            }));
        }

        int total = 0;
        for (Future<Integer> f : futures) {
            total += f.get();
        }

        executor.shutdown();

        assertEquals(threads * countPerThread, total);
        assertEquals(total, writer.getRecordCount());
    }

    @Test
    @DisplayName("统计信息应正确")
    void statsTracking() throws IOException {
        writer = new WalWriterV2(config, "shard-0");
        writer.start();

        byte[] payload = "test".getBytes();

        for (int i = 0; i < 100; i++) {
            writer.append("task-" + i, "biz-" + i, EventType.CREATE, System.currentTimeMillis(), payload);
        }

        WalWriterV2.Stats stats = writer.getStats();

        assertTrue(stats.getWriteCount() >= 100);
        assertTrue(stats.getFlushCount() > 0);
        assertEquals(0, stats.getOverflowCount());

        System.out.println("Stats: " + stats);
    }
}

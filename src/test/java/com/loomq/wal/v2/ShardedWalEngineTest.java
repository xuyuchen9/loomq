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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ShardedWalEngine 单元测试
 */
class ShardedWalEngineTest {

    @TempDir
    Path tempDir;

    private WalConfig config;
    private ShardedWalEngine engine;

    @BeforeEach
    void setUp() {
        config = createConfig();
        engine = new ShardedWalEngine(config);
        engine.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (engine != null) {
            engine.close();
        }
    }

    private WalConfig createConfig() {
        return new WalConfig() {
            @Override
            public String dataDir() { return tempDir.toString(); }
            @Override
            public int segmentSizeMb() { return 64; }
            @Override
            public String flushStrategy() { return "async"; }  // 使用异步刷盘提高吞吐
            @Override
            public long batchFlushIntervalMs() { return 10; }  // 减少刷盘间隔
            @Override
            public boolean syncOnWrite() { return false; }
        };
    }

    @Test
    @DisplayName("多分片写入应隔离")
    void multiShardIsolation() throws IOException {
        byte[] payload = "test".getBytes();

        // 向不同分片写入
        engine.append("shard-0", "task-0", "biz-0", EventType.CREATE, System.currentTimeMillis(), payload);
        engine.append("shard-1", "task-1", "biz-1", EventType.CREATE, System.currentTimeMillis(), payload);
        engine.append("shard-2", "task-2", "biz-2", EventType.CREATE, System.currentTimeMillis(), payload);

        assertEquals(3, engine.getShardCount());
        assertTrue(engine.getShardIds().contains("shard-0"));
        assertTrue(engine.getShardIds().contains("shard-1"));
        assertTrue(engine.getShardIds().contains("shard-2"));

        // 每个分片应该有独立的目录
        assertTrue(java.nio.file.Files.exists(tempDir.resolve("shard-0")));
        assertTrue(java.nio.file.Files.exists(tempDir.resolve("shard-1")));
        assertTrue(java.nio.file.Files.exists(tempDir.resolve("shard-2")));
    }

    @Test
    @DisplayName("多分片写入功能验证")
    void multiShardThroughput() throws IOException, InterruptedException {
        int shards = 2;
        int countPerShard = 10;
        byte[] payload = new byte[100];

        int successCount = 0;

        for (int s = 0; s < shards; s++) {
            String shardId = "shard-" + s;
            for (int i = 0; i < countPerShard; i++) {
                try {
                    engine.append(shardId, "task-" + s + "-" + i, "biz-" + i,
                            EventType.CREATE, System.currentTimeMillis(), payload);
                    successCount++;
                } catch (Exception e) {
                    System.out.println("Write error: " + e.getMessage());
                }
            }
        }

        int total = shards * countPerShard;
        System.out.println("Successfully wrote " + successCount + "/" + total + " records to " + shards + " shards");

        // 功能验证：确保写入成功
        assertEquals(total, successCount, "All writes should succeed");
        assertEquals(shards, engine.getShardCount(), "Should have correct shard count");
        System.out.println("多分片写入功能验证通过");
    }
}

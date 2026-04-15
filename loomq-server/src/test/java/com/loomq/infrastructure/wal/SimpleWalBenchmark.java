package com.loomq.infrastructure.wal;

import com.loomq.config.WalConfig;
import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.replication.AckLevel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

/**
 * 简化版 WAL 基准测试（简化版，无 JMH 依赖）
 *
 * 目标：
 * - ASYNC QPS: 500K+
 * - DURABLE QPS: 200K+
 * - 单条序列化: < 200ns
 *
 * @author loomq
 * @since v0.6.1
 */
public class SimpleWalBenchmark {

    private SimpleWalWriter walWriter;
    private Intent testIntent;
    private Path tempDir;
    private byte[] encodedIntent;

    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("wal-benchmark-");
        WalConfig config = createTestConfig(tempDir.toString());
        walWriter = new SimpleWalWriter(config, "benchmark");
        walWriter.start();

        testIntent = createTestIntent();
        encodedIntent = IntentBinaryCodec.encode(testIntent);

        System.out.println("Benchmark setup complete, tempDir=" + tempDir);
    }

    public void teardown() {
        if (walWriter != null) {
            walWriter.close();
        }
        if (tempDir != null) {
            try {
                Files.walk(tempDir)
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                    });
            } catch (IOException ignored) {}
        }
    }

    // ========== 序列化性能测试 ==========

    public long measureBinaryEncode(int iterations) {
        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            IntentBinaryCodec.encode(testIntent);
        }
        return System.nanoTime() - start;
    }

    public long measureBinaryDecode(int iterations) {
        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            IntentBinaryCodec.decode(encodedIntent);
        }
        return System.nanoTime() - start;
    }

    // ========== WAL 写入性能测试 ==========

    public long measureAsyncWrite(int iterations) {
        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            walWriter.writeAsync(encodedIntent);
        }
        return System.nanoTime() - start;
    }

    public long measureDurableWrite(int iterations) {
        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            walWriter.writeDurable(encodedIntent).join();
        }
        return System.nanoTime() - start;
    }

    // ========== 辅助方法 ==========

    private WalConfig createTestConfig(String dataDir) {
        return new WalConfig() {
            @Override public String dataDir() { return dataDir; }
            @Override public int segmentSizeMb() { return 64; }
            @Override public String flushStrategy() { return "batch"; }
            @Override public long batchFlushIntervalMs() { return 10; }
            @Override public boolean syncOnWrite() { return false; }
            @Override public String engine() { return "memory_segment"; }
            @Override public int memorySegmentInitialSizeMb() { return 64; }
            @Override public int memorySegmentMaxSizeMb() { return 256; }
            @Override public int memorySegmentFlushThresholdKb() { return 64; }
            @Override public long memorySegmentFlushIntervalMs() { return 10; }
            @Override public int memorySegmentStripeCount() { return 16; }
            @Override public int memorySegmentMinBatchSize() { return 100; }
            @Override public boolean memorySegmentAdaptiveFlushEnabled() { return true; }
            @Override public boolean isReplicationEnabled() { return false; }
            @Override public String replicaHost() { return "localhost"; }
            @Override public int replicaPort() { return 9090; }
            @Override public long replicationAckTimeoutMs() { return 30000; }
            @Override public boolean requireReplicatedAck() { return false; }
        };
    }

    private Intent createTestIntent() {
        Intent intent = new Intent();
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.setDeadline(Instant.now().plusSeconds(3600));
        intent.setShardKey("test-shard");
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.setAckLevel(AckLevel.DURABLE);
        intent.setIdempotencyKey("idem-" + System.nanoTime());

        Callback callback = new Callback("http://localhost:8080/callback");
        callback.setMethod("POST");
        intent.setCallback(callback);

        intent.transitionTo(IntentStatus.SCHEDULED);

        return intent;
    }

    public static void main(String[] args) throws Exception {
        SimpleWalBenchmark benchmark = new SimpleWalBenchmark();
        benchmark.setup();

        try {
            int warmup = 10_000;
            int iterations = 100_000;

            // 预热
            System.out.println("Warming up...");
            benchmark.measureBinaryEncode(warmup);
            benchmark.measureBinaryDecode(warmup);

            // 测试序列化
            System.out.println("\n=== Serialization Benchmark ===");
            long encodeTime = benchmark.measureBinaryEncode(iterations);
            long decodeTime = benchmark.measureBinaryDecode(iterations);

            System.out.printf("Encode: %d ops, %.2f ns/op, %.0f ops/sec%n",
                iterations, (double) encodeTime / iterations,
                iterations * 1_000_000_000.0 / encodeTime);
            System.out.printf("Decode: %d ops, %.2f ns/op, %.0f ops/sec%n",
                iterations, (double) decodeTime / iterations,
                iterations * 1_000_000_000.0 / decodeTime);

            // 测试 WAL 写入
            System.out.println("\n=== WAL Write Benchmark ===");
            int walIterations = 10_000;

            long asyncTime = benchmark.measureAsyncWrite(walIterations);
            System.out.printf("Async:  %d ops, %.2f µs/op, %.0f ops/sec%n",
                walIterations, (double) asyncTime / walIterations / 1000,
                walIterations * 1_000_000_000.0 / asyncTime);

            // DURABLE 测试（较慢，减少迭代次数）
            // Windows 上 fsync 较慢，只测试少量迭代
            int durableIterations = 100;
            System.out.println("\nRunning Durable benchmark (this may take a while on Windows)...");
            long durableTime = benchmark.measureDurableWrite(durableIterations);
            System.out.printf("Durable: %d ops, %.2f µs/op, %.0f ops/sec%n",
                durableIterations, (double) durableTime / durableIterations / 1000,
                durableIterations * 1_000_000_000.0 / durableTime);

            // WAL 统计
            System.out.println("\n=== WAL Stats ===");
            var stats = benchmark.walWriter.getStats();
            System.out.println("Write count: " + stats.getWriteCount());
            System.out.println("Flush count: " + stats.getFlushCount());
            System.out.printf("Avg flush time: %.2f ms%n", stats.getAvgFlushTimeMs());

        } finally {
            benchmark.teardown();
        }
    }
}

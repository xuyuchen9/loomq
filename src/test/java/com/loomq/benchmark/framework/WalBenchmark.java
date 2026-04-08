package com.loomq.benchmark.framework;

import com.loomq.wal.WalRecord;
import com.loomq.wal.WalSegment;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * WAL 态性能压测 (v0.4.3)
 *
 * 测试 WAL 写入性能。
 * 指标：WAL 写入吞吐量、同步延迟。
 *
 * @author loomq
 * @since v0.4.3
 */
public class WalBenchmark extends BenchmarkBase {

    private final int recordCount;
    private final Path tempDir;
    private WalSegment walSegment;

    public WalBenchmark(int recordCount) throws IOException {
        super();
        this.recordCount = recordCount;
        this.tempDir = Files.createTempDirectory("wal-benchmark");
    }

    public WalBenchmark(int recordCount, BenchmarkConfig config) throws IOException {
        super(config);
        this.recordCount = recordCount;
        this.tempDir = Files.createTempDirectory("wal-benchmark");
    }

    @Override
    protected String getBenchmarkName() {
        return "WAL态性能压测 (" + formatNumber(recordCount) + " 记录)";
    }

    @Override
    protected void warmup() {
        // WAL 预热
        try {
            File warmupFile = tempDir.resolve("warmup.wal").toFile();
            try (WalSegment segment = new WalSegment(warmupFile, 0, 100 * 1024 * 1024)) {
                for (int i = 0; i < 1000; i++) {
                    WalRecord record = createRecord(i, "warmup-" + i);
                    segment.write(record);
                }
                segment.sync();
            }
            warmupFile.delete();
        } catch (IOException e) {
            throw new RuntimeException("WAL 预热失败", e);
        }
    }

    @Override
    protected BenchmarkRunResult runSingle() {
        try {
            File walFile = tempDir.resolve("benchmark-" + System.nanoTime() + ".wal").toFile();
            walSegment = new WalSegment(walFile, 1, 1024 * 1024 * 1024);

            LatencyRecorder latencyRecorder = new LatencyRecorder();
            ThroughputTimer timer = new ThroughputTimer();

            long startMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

            for (int i = 0; i < recordCount; i++) {
                long opStart = System.nanoTime();

                WalRecord record = createRecord(i, "task-" + i);
                walSegment.write(record);

                if (config.recordLatencies() && i % config.latencySampleRate() == 0) {
                    latencyRecorder.recordNano(System.nanoTime() - opStart);
                }

                timer.increment();
            }

            // 强制刷盘
            walSegment.sync();

            long endMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            long memoryUsedMB = (endMem - startMem) / 1024 / 1024;

            long elapsedMs = timer.getElapsedMs();

            walSegment.close();
            walFile.delete();

            return BenchmarkRunResult.of(0, recordCount, elapsedMs)
                    .withLatencies(latencyRecorder.getLatencies())
                    .withMemoryUsed(memoryUsedMB);

        } catch (IOException e) {
            throw new RuntimeException("WAL 压测失败", e);
        }
    }

    @Override
    protected void cleanup() {
        if (walSegment != null) {
            try {
                walSegment.close();
            } catch (Exception e) {
                // ignore
            }
        }

        // 清理临时文件
        try {
            Files.list(tempDir).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    // ignore
                }
            });
        } catch (IOException e) {
            // ignore
        }

        System.gc();
    }

    private WalRecord createRecord(long seq, String taskId) {
        return WalRecord.create(
                1,
                seq,
                taskId,
                null,
                com.loomq.entity.EventType.CREATE,
                System.currentTimeMillis(),
                new byte[100] // 模拟 100 字节 payload
        );
    }

    private String formatNumber(int n) {
        if (n >= 1_000_000) return (n / 1_000_000) + "M";
        if (n >= 1_000) return (n / 1_000) + "K";
        return String.valueOf(n);
    }
}

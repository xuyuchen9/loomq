package com.loomq.benchmark.framework;

import com.loomq.entity.EventType;
import com.loomq.recovery.WalRecoveryService;
import com.loomq.store.TaskStore;
import com.loomq.wal.WalRecord;
import com.loomq.wal.WalSegment;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 恢复态性能压测 (v0.4.3)
 *
 * 测试 WAL 恢复速度。
 * 指标：恢复吞吐量（任务/秒）、恢复时间。
 *
 * @author loomq
 * @since v0.4.3
 */
public class RecoveryBenchmark extends BenchmarkBase {

    private final int taskCount;
    private final Path tempDir;

    public RecoveryBenchmark(int taskCount) throws IOException {
        super();
        this.taskCount = taskCount;
        this.tempDir = Files.createTempDirectory("recovery-benchmark");
    }

    public RecoveryBenchmark(int taskCount, BenchmarkConfig config) throws IOException {
        super(config);
        this.taskCount = taskCount;
        this.tempDir = Files.createTempDirectory("recovery-benchmark");
    }

    @Override
    protected String getBenchmarkName() {
        return "恢复态性能压测 (" + formatNumber(taskCount) + " 任务)";
    }

    @Override
    protected void warmup() {
        // 创建小规模的 WAL 文件进行预热
        try {
            Path warmupDir = tempDir.resolve("warmup");
            Files.createDirectories(warmupDir);

            try (WalSegment segment = new WalSegment(
                    warmupDir.resolve("00000001.wal").toFile(),
                    1,
                    100 * 1024 * 1024)) {
                for (int i = 0; i < 100; i++) {
                    segment.write(createRecord(1, i, "warmup-" + i, EventType.CREATE));
                }
            }

            // 执行一次恢复
            TaskStore store = new TaskStore();
            WalRecoveryService recoveryService = new WalRecoveryService(
                    warmupDir.toString(),
                    store,
                    WalRecoveryService.RecoveryConfig.defaultConfig()
            );
            recoveryService.recover();

            // 清理
            Files.walk(warmupDir)
                    .sorted((a, b) -> -a.compareTo(b))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            // ignore
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException("恢复预热失败", e);
        }
    }

    @Override
    protected BenchmarkRunResult runSingle() {
        try {
            Path walDir = tempDir.resolve("run-" + System.nanoTime());
            Files.createDirectories(walDir);

            // 准备 WAL 数据：模拟多种状态的任务
            int recordsPerTask = 3; // CREATE, SCHEDULE, READY
            int recordsToWrite = taskCount * recordsPerTask;
            int recordsPerSegment = 10000;
            int segmentCount = (recordsToWrite + recordsPerSegment - 1) / recordsPerSegment;

            for (int seg = 0; seg < segmentCount; seg++) {
                String fileName = String.format("%08d.wal", seg + 1);
                try (WalSegment segment = new WalSegment(
                        walDir.resolve(fileName).toFile(),
                        seg + 1,
                        100 * 1024 * 1024)) {

                    int startIdx = seg * recordsPerSegment / recordsPerTask;
                    int endIdx = Math.min(startIdx + recordsPerSegment / recordsPerTask, taskCount);

                    long recordSeq = 0;
                    for (int i = startIdx; i < endIdx; i++) {
                        String taskId = "task-" + i;
                        segment.write(createRecord(seg + 1, recordSeq++, taskId, EventType.CREATE));
                        segment.write(createRecord(seg + 1, recordSeq++, taskId, EventType.SCHEDULE));
                        segment.write(createRecord(seg + 1, recordSeq++, taskId, EventType.READY));
                    }
                }
            }

            // 执行恢复并计时
            TaskStore taskStore = new TaskStore();
            WalRecoveryService recoveryService = new WalRecoveryService(
                    walDir.toString(),
                    taskStore,
                    WalRecoveryService.RecoveryConfig.defaultConfig()
            );

            long startTime = System.nanoTime();
            WalRecoveryService.RecoveryResult result = recoveryService.recover();
            long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;

            // 清理
            Files.walk(walDir)
                    .sorted((a, b) -> -a.compareTo(b))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            // ignore
                        }
                    });

            return BenchmarkRunResult.of(0, elapsedMs, result.tasksRestored());

        } catch (IOException e) {
            throw new RuntimeException("恢复压测失败", e);
        }
    }

    @Override
    protected void cleanup() {
        // 清理临时目录
        try {
            Files.walk(tempDir)
                    .sorted((a, b) -> -a.compareTo(b))
                    .forEach(path -> {
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

    private WalRecord createRecord(int segmentSeq, long recordSeq, String taskId, EventType eventType) {
        return WalRecord.create(
                segmentSeq,
                recordSeq,
                taskId,
                null,
                eventType,
                System.currentTimeMillis(),
                new byte[50]
        );
    }

    private String formatNumber(int n) {
        if (n >= 1_000_000) return (n / 1_000_000) + "M";
        if (n >= 1_000) return (n / 1_000) + "K";
        return String.valueOf(n);
    }
}

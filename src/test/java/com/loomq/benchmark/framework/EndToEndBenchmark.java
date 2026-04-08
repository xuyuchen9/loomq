package com.loomq.benchmark.framework;

import com.loomq.entity.Task;
import com.loomq.scheduler.WalWriter;
import com.loomq.store.TaskStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 端到端性能压测 (v0.4.3)
 *
 * 测试完整链路：调度 → 执行
 * 指标：端到端吞吐量、调度延迟、执行延迟
 *
 * @author loomq
 * @since v0.4.3
 */
public class EndToEndBenchmark extends BenchmarkBase {

    private final int taskCount;
    private final Path tempDir;

    private TaskStore taskStore;
    private WalWriter walWriter;
    private ExecutorService executor;

    public EndToEndBenchmark(int taskCount) throws IOException {
        super();
        this.taskCount = taskCount;
        this.tempDir = Files.createTempDirectory("e2e-benchmark");
    }

    public EndToEndBenchmark(int taskCount, BenchmarkConfig config) throws IOException {
        super(config);
        this.taskCount = taskCount;
        this.tempDir = Files.createTempDirectory("e2e-benchmark");
    }

    @Override
    protected String getBenchmarkName() {
        return "端到端性能压测 (" + formatNumber(taskCount) + " 任务)";
    }

    @Override
    protected void warmup() {
        // 简单预热
        try {
            TaskStore warmupStore = new TaskStore();
            for (int i = 0; i < 1000; i++) {
                Task task = createTask("warmup-" + i);
                warmupStore.add(task);
            }
            warmupStore.clear();
        } catch (Exception e) {
            // ignore
        }
    }

    @Override
    protected BenchmarkRunResult runSingle() {
        try {
            // 初始化组件
            taskStore = new TaskStore();
            walWriter = new WalWriter();
            executor = Executors.newVirtualThreadPerTaskExecutor();

            // 统计
            AtomicInteger completedCount = new AtomicInteger(0);
            CountDownLatch completionLatch = new CountDownLatch(taskCount);
            LatencyRecorder latencyRecorder = new LatencyRecorder();

            long startTime = System.nanoTime();

            // 并发创建任务
            final int threads = Math.max(1, Math.min(taskCount / 100, 50));

            CountDownLatch createLatch = new CountDownLatch(threads);
            final int tasksPerThread = taskCount / threads;

            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        int startIdx = threadId * tasksPerThread;
                        int endIdx = (threadId == threads - 1) ? taskCount : startIdx + tasksPerThread;

                        for (int i = startIdx; i < endIdx; i++) {
                            long opStart = System.nanoTime();

                            Task task = createTask("task-" + i);
                            taskStore.add(task);

                            // 模拟调度
                            task.transitionToScheduled();
                            task.transitionToReady();

                            if (config.recordLatencies() && i % config.latencySampleRate() == 0) {
                                latencyRecorder.recordNano(System.nanoTime() - opStart);
                            }

                            completedCount.incrementAndGet();
                            completionLatch.countDown();
                        }
                    } finally {
                        createLatch.countDown();
                    }
                });
            }

            // 等待完成
            boolean completed = completionLatch.await(60, TimeUnit.SECONDS);
            long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;

            if (!completed) {
                logger.warn("端到端压测超时，已完成 {}/{} 任务", completedCount.get(), taskCount);
            }

            return BenchmarkRunResult.of(0, elapsedMs, completedCount.get())
                    .withLatencies(latencyRecorder.getLatencies());

        } catch (Exception e) {
            throw new RuntimeException("端到端压测失败", e);
        }
    }

    @Override
    protected void cleanup() {
        if (executor != null) {
            executor.shutdownNow();
        }

        if (walWriter != null) {
            try {
                walWriter.close();
            } catch (Exception e) {
                // ignore
            }
        }

        if (taskStore != null) {
            taskStore.clear();
        }

        // 清理临时目录
        try {
            Files.walk(tempDir)
                    .sorted((a, b) -> -a.compareTo(b))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (Exception e) {
                            // ignore
                        }
                    });
        } catch (Exception e) {
            // ignore
        }

        System.gc();
    }

    private Task createTask(String taskId) {
        return Task.builder()
                .taskId(taskId)
                .webhookUrl("https://example.com/webhook")
                .bizKey(taskId + "-biz")
                .wakeTime(System.currentTimeMillis()) // 立即触发
                .build();
    }

    private String formatNumber(int n) {
        if (n >= 1_000_000) return (n / 1_000_000) + "M";
        if (n >= 1_000) return (n / 1_000) + "K";
        return String.valueOf(n);
    }
}

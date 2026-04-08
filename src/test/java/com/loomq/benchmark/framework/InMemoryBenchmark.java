package com.loomq.benchmark.framework;

import com.loomq.entity.Task;
import com.loomq.store.TaskStore;

import java.util.ArrayList;
import java.util.List;

/**
 * 内存态性能压测 (v0.4.3)
 *
 * 测试纯内存操作性能，无 WAL 持久化。
 * 指标：任务创建、查询、状态转换的纯内存吞吐。
 *
 * @author loomq
 * @since v0.4.3
 */
public class InMemoryBenchmark extends BenchmarkBase {

    private final int taskCount;
    private TaskStore taskStore;

    public InMemoryBenchmark(int taskCount) {
        super();
        this.taskCount = taskCount;
    }

    public InMemoryBenchmark(int taskCount, BenchmarkConfig config) {
        super(config);
        this.taskCount = taskCount;
    }

    @Override
    protected String getBenchmarkName() {
        return "内存态性能压测 (" + formatNumber(taskCount) + " 任务)";
    }

    @Override
    protected void warmup() {
        TaskStore warmupStore = new TaskStore();
        for (int i = 0; i < 10000; i++) {
            Task task = createTask("warmup-" + i);
            warmupStore.add(task);
        }
        warmupStore.clear();
    }

    @Override
    protected BenchmarkRunResult runSingle() {
        taskStore = new TaskStore();
        LatencyRecorder latencyRecorder = new LatencyRecorder();
        ThroughputTimer timer = new ThroughputTimer();

        long startMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        for (int i = 0; i < taskCount; i++) {
            long opStart = System.nanoTime();

            Task task = createTask("task-" + i);
            taskStore.add(task);

            if (config.recordLatencies() && i % config.latencySampleRate() == 0) {
                latencyRecorder.recordNano(System.nanoTime() - opStart);
            }

            timer.increment();
        }

        long endMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long memoryUsedMB = (endMem - startMem) / 1024 / 1024;

        return BenchmarkRunResult.of(0, timer.getElapsedMs(), taskCount)
                .withLatencies(latencyRecorder.getLatencies())
                .withMemoryUsed(memoryUsedMB);
    }

    @Override
    protected void cleanup() {
        if (taskStore != null) {
            taskStore.clear();
        }
        System.gc();
    }

    private Task createTask(String taskId) {
        return Task.builder()
                .taskId(taskId)
                .webhookUrl("https://example.com/webhook")
                .bizKey(taskId + "-biz")
                .wakeTime(System.currentTimeMillis() + 3600000)
                .build();
    }

    private String formatNumber(int n) {
        if (n >= 1_000_000) return (n / 1_000_000) + "M";
        if (n >= 1_000) return (n / 1_000) + "K";
        return String.valueOf(n);
    }
}

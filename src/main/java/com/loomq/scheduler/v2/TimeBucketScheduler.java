package com.loomq.scheduler.v2;

import com.loomq.entity.Task;
import com.loomq.entity.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 时间桶调度器 (V0.3 优化版)
 *
 * 核心设计：
 * - 可配置 bucket 粒度（默认 100ms）
 * - ConcurrentSkipListMap 有序索引
 * - taskBucketMap 支持 O(1) 取消
 * - 批量到期任务获取
 *
 * 时间精度模型：
 * - 实际延迟 = bucketSize + schedulerTick + 调度抖动
 * - 延迟分布 ~ U(0, bucketSize) + tick
 *
 * V0.3 改进：
 * - 更细粒度控制（100ms 桶 vs 1s 桶）
 * - O(1) 任务取消（通过 taskBucketMap）
 * - 更高效的批量扫描
 */
public class TimeBucketScheduler {

    private static final Logger logger = LoggerFactory.getLogger(TimeBucketScheduler.class);

    // 默认 bucket 大小：100ms
    public static final long DEFAULT_BUCKET_SIZE_MS = 100;

    // 时间桶：key = bucketId (triggerTime / bucketSizeMs)
    private final ConcurrentSkipListMap<Long, Bucket> timeBuckets;

    // 任务 ID -> bucketId 映射（用于 O(1) 取消）
    private final ConcurrentHashMap<String, Long> taskBucketMap;

    // 待执行队列（有界）
    private final BlockingQueue<Task> readyQueue;

    // 调度线程
    private final ScheduledExecutorService scheduler;

    // 执行线程池
    private final ExecutorService executor;

    // 任务回调
    private final TaskDispatcher dispatcher;

    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);

    // 统计
    private final Stats stats = new Stats();

    // 配置
    private final int readyQueueCapacity;
    private final long bucketSizeMs;  // 桶粒度（默认 100ms）
    private final int maxDispatchPerBatch;  // 每批次最大分发数量

    public TimeBucketScheduler(TaskDispatcher dispatcher, int readyQueueCapacity) {
        this(dispatcher, readyQueueCapacity, DEFAULT_BUCKET_SIZE_MS, 1000);
    }

    public TimeBucketScheduler(TaskDispatcher dispatcher, int readyQueueCapacity,
                               long bucketSizeMs, int maxDispatchPerBatch) {
        if (bucketSizeMs <= 0) {
            throw new IllegalArgumentException("bucketSizeMs must be positive");
        }
        this.dispatcher = dispatcher;
        this.readyQueueCapacity = readyQueueCapacity;
        this.bucketSizeMs = bucketSizeMs;
        this.maxDispatchPerBatch = maxDispatchPerBatch;

        this.timeBuckets = new ConcurrentSkipListMap<>();
        this.taskBucketMap = new ConcurrentHashMap<>();
        this.readyQueue = new ArrayBlockingQueue<>(readyQueueCapacity);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bucket-scheduler");
            t.setDaemon(true);
            return t;
        });

        this.executor = Executors.newVirtualThreadPerTaskExecutor();

        logger.info("TimeBucketScheduler created, bucketSize={}ms", bucketSizeMs);
    }

    /**
     * 启动调度器
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        // 启动桶扫描线程（每 100ms 检查一次）
        scheduler.scheduleAtFixedRate(this::processBuckets, 100, 100, TimeUnit.MILLISECONDS);

        // 启动执行线程
        scheduler.submit(this::dispatchLoop);

        logger.info("TimeBucketScheduler started, bucketSize={}ms, readyQueueCapacity={}, maxDispatchPerBatch={}",
                bucketSizeMs, readyQueueCapacity, maxDispatchPerBatch);
    }

    /**
     * 停止调度器
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        scheduler.shutdown();
        executor.shutdown();

        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        logger.info("TimeBucketScheduler stopped, stats={}", stats);
    }

    /**
     * 调度任务
     * 核心逻辑：将任务放入对应的时间桶，并记录到 taskBucketMap 支持 O(1) 取消
     */
    public boolean schedule(Task task) {
        if (!running.get()) {
            return false;
        }
        if (task == null || task.getTaskId() == null) {
            return false;
        }

        // 计算 bucket ID
        long bucketId = calculateBucketId(task.getTriggerTime());

        // 获取或创建 bucket
        Bucket bucket = timeBuckets.computeIfAbsent(bucketId, k ->
                new Bucket(k, k * bucketSizeMs, (k + 1) * bucketSizeMs)
        );

        // 添加到 bucket
        if (bucket.add(task)) {
            // 记录任务位置（O(1) 取消）
            taskBucketMap.put(task.getTaskId(), bucketId);
            stats.recordSchedule();

            logger.debug("Task {} scheduled to bucket {} (triggerTime={}, delayMs={})",
                    task.getTaskId(), bucketId, task.getTriggerTime(),
                    task.getTriggerTime() - System.currentTimeMillis());
            return true;
        }

        return false;
    }

    /**
     * 取消任务（O(1) 查找）
     */
    public boolean cancel(String taskId) {
        if (taskId == null) {
            return false;
        }

        // O(1) 查找任务所在 bucket
        Long bucketId = taskBucketMap.remove(taskId);
        if (bucketId == null) {
            // 可能已在 ready 队列
            return readyQueue.removeIf(t -> t.getTaskId().equals(taskId));
        }

        // 从 bucket 中移除
        Bucket bucket = timeBuckets.get(bucketId);
        if (bucket != null && bucket.remove(taskId)) {
            stats.recordCancel();
            logger.debug("Task {} cancelled from bucket {}", taskId, bucketId);

            // 清理空 bucket
            if (bucket.isEmpty()) {
                timeBuckets.remove(bucketId);
            }
            return true;
        }

        return false;
    }

    /**
     * 重新调度
     */
    public boolean reschedule(Task task) {
        cancel(task.getTaskId());
        return schedule(task);
    }

    /**
     * 处理到期的时间桶（V0.3 优化版）
     * 批量获取所有到期 bucket 的任务
     */
    private void processBuckets() {
        if (!running.get()) {
            return;
        }

        long now = System.currentTimeMillis();
        long currentBucketId = calculateBucketId(now);

        // 获取所有到期的 bucket
        List<Task> dueTasks = new ArrayList<>();

        // headMap 返回所有 key <= currentBucketId 的条目
        var expiredBuckets = timeBuckets.headMap(currentBucketId, true);

        for (var entry : expiredBuckets.entrySet()) {
            Bucket bucket = entry.getValue();
            List<Task> tasks = bucket.drainAll();

            if (!tasks.isEmpty()) {
                for (Task task : tasks) {
                    // 从 taskBucketMap 中移除
                    taskBucketMap.remove(task.getTaskId());

                    // 检查是否真正到期
                    if (task.getTriggerTime() <= now) {
                        dueTasks.add(task);
                    } else {
                        // 还未到期（边界情况），重新调度
                        schedule(task);
                    }
                }
            }

            // 移除空 bucket
            timeBuckets.remove(entry.getKey());

            if (dueTasks.size() >= maxDispatchPerBatch) {
                break;
            }
        }

        if (!dueTasks.isEmpty()) {
            stats.recordExpire(dueTasks.size());
            logger.debug("Processing {} due tasks from buckets", dueTasks.size());

            // 放入 ready 队列
            for (Task task : dueTasks) {
                if (!readyQueue.offer(task)) {
                    logger.warn("Ready queue full, task {} dropped", task.getTaskId());
                }
            }
        }
    }

    /**
     * 执行循环
     * 核心逻辑：从 ready 队列取任务，批量执行
     */
    private void dispatchLoop() {
        while (running.get()) {
            try {
                // 阻塞等待任务
                Task task = readyQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }

                // 检查状态
                if (task.getStatus().isTerminal()) {
                    logger.debug("Task {} already terminal, skip", task.getTaskId());
                    continue;
                }

                // 原子状态转换
                if (!task.tryStartExecution()) {
                    logger.debug("Task {} cannot start execution, status={}",
                            task.getTaskId(), task.getStatus());
                    continue;
                }

                // 提交执行
                executor.submit(() -> {
                    try {
                        dispatcher.dispatch(task);
                        stats.recordDispatch();
                    } catch (Exception e) {
                        logger.error("Dispatch task {} failed", task.getTaskId(), e);
                    }
                });

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * 计算 bucket ID
     */
    private long calculateBucketId(long triggerTime) {
        return triggerTime / bucketSizeMs;
    }

    // ========== 统计接口 ==========

    public long getBucketSizeMs() {
        return bucketSizeMs;
    }

    public int getBucketCount() {
        return timeBuckets.size();
    }

    public int getReadyQueueSize() {
        return readyQueue.size();
    }

    public int getPendingTaskCount() {
        return taskBucketMap.size();
    }

    public Stats getStats() {
        return stats;
    }

    public SchedulerStats getSchedulerStats() {
        return new SchedulerStats(
                timeBuckets.size(),
                readyQueue.size(),
                stats.getScheduledCount(),
                stats.getExpiredCount(),
                running.get()
        );
    }

    public record SchedulerStats(
            int bucketCount,
            int readyQueueSize,
            long totalScheduled,
            long totalExpired,
            boolean running
    ) {}

    // ========== 内部类 ==========

    /**
     * 时间桶
     */
    public static class Bucket {
        private final long bucketId;
        private final long startTime;
        private final long endTime;
        private final ConcurrentHashMap<String, Task> tasks;

        Bucket(long bucketId, long startTime, long endTime) {
            this.bucketId = bucketId;
            this.startTime = startTime;
            this.endTime = endTime;
            this.tasks = new ConcurrentHashMap<>();
        }

        boolean add(Task task) {
            return tasks.putIfAbsent(task.getTaskId(), task) == null;
        }

        boolean remove(String taskId) {
            return tasks.remove(taskId) != null;
        }

        List<Task> drainAll() {
            List<Task> result = new ArrayList<>(tasks.values());
            tasks.clear();
            return result;
        }

        boolean isEmpty() {
            return tasks.isEmpty();
        }

        int size() {
            return tasks.size();
        }

        public long getBucketId() { return bucketId; }
        public long getStartTime() { return startTime; }
        public long getEndTime() { return endTime; }
    }

    /**
     * 统计信息
     */
    public static class Stats {
        private final java.util.concurrent.atomic.LongAdder scheduledCount = new java.util.concurrent.atomic.LongAdder();
        private final java.util.concurrent.atomic.LongAdder cancelledCount = new java.util.concurrent.atomic.LongAdder();
        private final java.util.concurrent.atomic.LongAdder expiredCount = new java.util.concurrent.atomic.LongAdder();
        private final java.util.concurrent.atomic.LongAdder dispatchedCount = new java.util.concurrent.atomic.LongAdder();

        void recordSchedule() { scheduledCount.increment(); }
        void recordCancel() { cancelledCount.increment(); }
        void recordExpire(int count) { expiredCount.add(count); }
        void recordDispatch() { dispatchedCount.increment(); }

        public long getScheduledCount() { return scheduledCount.sum(); }
        public long getCancelledCount() { return cancelledCount.sum(); }
        public long getExpiredCount() { return expiredCount.sum(); }
        public long getDispatchedCount() { return dispatchedCount.sum(); }

        @Override
        public String toString() {
            return String.format("Stats{scheduled=%d, cancelled=%d, expired=%d, dispatched=%d}",
                    getScheduledCount(), getCancelledCount(), getExpiredCount(), getDispatchedCount());
        }
    }
}

package com.loomq.scheduler.v2;

import com.loomq.entity.Task;
import com.loomq.entity.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 精确调度器 (V0.3+ 核心实现)
 *
 * 设计原理 (基于设计文档 §6)：
 * 1. 使用最小堆结构（PriorityBlockingQueue）按 triggerTime 排序
 * 2. 调度线程精确睡眠到最早任务到期
 * 3. 新任务插入时，如果比当前等待的任务更早，唤醒调度线程
 *
 * 性能特性：
 * - 插入：O(logN)
 * - 取最小：O(1)
 * - 取消：O(1)（通过辅助 Map）
 *
 * 与 TimeBucketScheduler 对比：
 * | 维度    | TimeBucket        | PrecisionScheduler |
 * |--------|-------------------|-------------------|
 * | 精度    | 受 bucket 限制     | 精确时间           |
 * | 扫描    | O(N bucket)       | O(1)              |
 * | 插入    | O(1)              | O(logN)           |
 * | 空转    | 每 100ms 轮询      | 无空转            |
 *
 * @author loomq
 * @since v0.3+
 */
public class PrecisionScheduler {

    private static final Logger logger = LoggerFactory.getLogger(PrecisionScheduler.class);

    // 默认队列容量
    public static final int DEFAULT_QUEUE_CAPACITY = 100000;

    // 任务队列（最小堆，按 triggerTime 排序）
    private final PriorityBlockingQueue<ScheduledTask> taskQueue;

    // 任务索引（用于 O(1) 取消）
    private final ConcurrentHashMap<String, ScheduledTask> taskIndex;

    // 分发器
    private final TaskDispatcher dispatcher;

    // 执行线程池（虚拟线程）
    private final ExecutorService executor;

    // 调度线程
    private final Thread schedulerThread;

    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);

    // 唤醒锁（用于精确唤醒调度线程）
    private final ReentrantLock wakeLock = new ReentrantLock();
    private final Condition wakeCondition = wakeLock.newCondition();

    // 当前等待的任务截止时间（用于判断是否需要唤醒）
    private final AtomicLong currentWaitDeadline = new AtomicLong(Long.MAX_VALUE);

    // 统计
    private final Stats stats = new Stats();

    // 配置
    private final int queueCapacity;

    /**
     * 创建精确调度器
     *
     * @param dispatcher 任务分发器
     * @param queueCapacity 队列容量
     */
    public PrecisionScheduler(TaskDispatcher dispatcher, int queueCapacity) {
        this.dispatcher = dispatcher;
        this.queueCapacity = queueCapacity;

        // 创建最小堆队列（按 triggerTime 升序）
        this.taskQueue = new PriorityBlockingQueue<>(
                queueCapacity,
                Comparator.comparingLong(ScheduledTask::getTriggerTime)
        );

        this.taskIndex = new ConcurrentHashMap<>();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();

        // 创建调度线程
        this.schedulerThread = new Thread(this::scheduleLoop, "precision-scheduler");
        this.schedulerThread.setDaemon(true);

        logger.info("PrecisionScheduler created, queueCapacity={}", queueCapacity);
    }

    /**
     * 启动调度器
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        schedulerThread.start();
        logger.info("PrecisionScheduler started");
    }

    /**
     * 停止调度器
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        // 唤醒调度线程
        signalWakeUp();

        try {
            schedulerThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        logger.info("PrecisionScheduler stopped, stats={}", stats);
    }

    /**
     * 调度任务
     * 核心逻辑：插入队列，如果比当前等待的任务更早，唤醒调度线程
     */
    public boolean schedule(Task task) {
        if (!running.get()) {
            return false;
        }
        if (task == null || task.getTaskId() == null) {
            return false;
        }

        String taskId = task.getTaskId();

        // 将任务状态设置为 SCHEDULED（如果当前是 PENDING）
        if (task.getStatus() == TaskStatus.PENDING) {
            task.setStatus(TaskStatus.SCHEDULED);
        }

        // 创建调度任务包装
        ScheduledTask scheduledTask = new ScheduledTask(task);

        // 记录到索引（用于 O(1) 取消）
        ScheduledTask existing = taskIndex.putIfAbsent(taskId, scheduledTask);
        if (existing != null) {
            // 任务已存在
            logger.warn("Task {} already scheduled", taskId);
            return false;
        }

        // 插入队列
        taskQueue.offer(scheduledTask);
        stats.recordSchedule();

        // 检查是否需要唤醒调度线程
        long triggerTime = task.getTriggerTime();
        long currentDeadline = currentWaitDeadline.get();

        if (triggerTime < currentDeadline) {
            // 新任务比当前等待的任务更早，需要唤醒
            logger.debug("Task {} is earlier than current deadline, wake up scheduler (triggerTime={}, currentDeadline={})",
                    taskId, triggerTime, currentDeadline);
            signalWakeUp();
        }

        logger.debug("Task {} scheduled, triggerTime={}, delayMs={}",
                taskId, triggerTime, triggerTime - System.currentTimeMillis());

        return true;
    }

    /**
     * 取消任务（O(1)）
     */
    public boolean cancel(String taskId) {
        if (taskId == null) {
            return false;
        }

        // 从索引中移除
        ScheduledTask scheduledTask = taskIndex.remove(taskId);
        if (scheduledTask == null) {
            return false;
        }

        // 标记为已取消（惰性删除，不立即从队列移除）
        scheduledTask.cancel();
        stats.recordCancel();

        logger.debug("Task {} cancelled", taskId);
        return true;
    }

    /**
     * 重新调度
     */
    public boolean reschedule(Task task) {
        cancel(task.getTaskId());
        return schedule(task);
    }

    /**
     * 立即触发任务
     */
    public boolean fireNow(String taskId) {
        ScheduledTask scheduledTask = taskIndex.get(taskId);
        if (scheduledTask == null) {
            return false;
        }

        // 标记原任务为取消（惰性删除）
        scheduledTask.cancel();

        // 创建新任务并立即执行
        Task task = scheduledTask.getTask();
        ScheduledTask newTask = new ScheduledTask(task);
        newTask.updateTriggerTime(System.currentTimeMillis());

        // 更新索引
        taskIndex.put(taskId, newTask);

        // 加入队列
        taskQueue.offer(newTask);

        // 唤醒调度线程
        signalWakeUp();

        logger.debug("Task {} fired now", taskId);
        return true;
    }

    /**
     * 调度循环（核心算法）
     *
     * while (true):
     *     task = peek()
     *     if task.execute_time > now:
     *         sleep(task.execute_time - now)  // 精确睡眠
     *     else:
     *         pop()
     *         dispatch(task)
     */
    private void scheduleLoop() {
        while (running.get()) {
            try {
                // 获取最早到期的任务
                ScheduledTask scheduledTask = taskQueue.peek();

                if (scheduledTask == null) {
                    // 队列为空，等待唤醒
                    waitForWakeUp(Long.MAX_VALUE);
                    continue;
                }

                // 检查是否已取消（惰性删除）
                if (scheduledTask.isCancelled()) {
                    taskQueue.poll();  // 移除已取消任务
                    continue;
                }

                long now = System.currentTimeMillis();
                long triggerTime = scheduledTask.getTriggerTime();
                long delayMs = triggerTime - now;

                if (delayMs > 0) {
                    // 任务尚未到期，精确等待
                    currentWaitDeadline.set(triggerTime);
                    waitForWakeUp(delayMs);
                    currentWaitDeadline.set(Long.MAX_VALUE);
                } else {
                    // 任务已到期，取出并执行
                    scheduledTask = taskQueue.poll();
                    if (scheduledTask == null || scheduledTask.isCancelled()) {
                        continue;
                    }

                    // 从索引中移除
                    taskIndex.remove(scheduledTask.getTaskId());

                    // 检查状态
                    Task task = scheduledTask.getTask();
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
                    stats.recordExpire();
                    dispatchTask(task);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Schedule loop error", e);
            }
        }
    }

    /**
     * 分发任务（虚拟线程执行）
     */
    private void dispatchTask(Task task) {
        executor.submit(() -> {
            try {
                dispatcher.dispatch(task);
                stats.recordDispatch();
            } catch (Exception e) {
                logger.error("Dispatch task {} failed", task.getTaskId(), e);
            }
        });
    }

    /**
     * 等待唤醒（精确睡眠）
     *
     * @param timeoutMs 超时时间（毫秒），Long.MAX_VALUE 表示无限等待
     */
    private void waitForWakeUp(long timeoutMs) throws InterruptedException {
        wakeLock.lock();
        try {
            if (timeoutMs == Long.MAX_VALUE) {
                // 无限等待，直到被唤醒
                wakeCondition.await();
            } else {
                // 精确等待
                wakeCondition.await(timeoutMs, TimeUnit.MILLISECONDS);
            }
        } finally {
            wakeLock.unlock();
        }
    }

    /**
     * 唤醒调度线程
     */
    private void signalWakeUp() {
        wakeLock.lock();
        try {
            wakeCondition.signalAll();
        } finally {
            wakeLock.unlock();
        }
    }

    // ========== 统计接口 ==========

    public int getQueueSize() {
        return taskQueue.size();
    }

    public int getPendingTaskCount() {
        return taskIndex.size();
    }

    public Stats getStats() {
        return stats;
    }

    public SchedulerStats getSchedulerStats() {
        return new SchedulerStats(
                taskQueue.size(),
                stats.getScheduledCount(),
                stats.getExpiredCount(),
                running.get()
        );
    }

    public record SchedulerStats(
            int queueSize,
            long totalScheduled,
            long totalExpired,
            boolean running
    ) {}

    // ========== 内部类 ==========

    /**
     * 调度任务包装（支持动态修改 triggerTime 和取消标记）
     */
    private static class ScheduledTask {
        private final Task task;
        private volatile long triggerTime;
        private volatile boolean cancelled = false;

        ScheduledTask(Task task) {
            this.task = task;
            this.triggerTime = task.getTriggerTime();
        }

        Task getTask() {
            return task;
        }

        long getTriggerTime() {
            return triggerTime;
        }

        void updateTriggerTime(long newTriggerTime) {
            this.triggerTime = newTriggerTime;
        }

        void cancel() {
            this.cancelled = true;
        }

        boolean isCancelled() {
            return cancelled;
        }

        String getTaskId() {
            return task.getTaskId();
        }
    }

    /**
     * 统计信息
     */
    public static class Stats {
        private final AtomicLong scheduledCount = new AtomicLong(0);
        private final AtomicLong cancelledCount = new AtomicLong(0);
        private final AtomicLong expiredCount = new AtomicLong(0);
        private final AtomicLong dispatchedCount = new AtomicLong(0);

        void recordSchedule() { scheduledCount.incrementAndGet(); }
        void recordCancel() { cancelledCount.incrementAndGet(); }
        void recordExpire() { expiredCount.incrementAndGet(); }
        void recordDispatch() { dispatchedCount.incrementAndGet(); }

        public long getScheduledCount() { return scheduledCount.get(); }
        public long getCancelledCount() { return cancelledCount.get(); }
        public long getExpiredCount() { return expiredCount.get(); }
        public long getDispatchedCount() { return dispatchedCount.get(); }

        @Override
        public String toString() {
            return String.format("Stats{scheduled=%d, cancelled=%d, expired=%d, dispatched=%d}",
                    getScheduledCount(), getCancelledCount(), getExpiredCount(), getDispatchedCount());
        }
    }
}

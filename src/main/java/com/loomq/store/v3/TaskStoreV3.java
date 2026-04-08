package com.loomq.store.v3;

import com.loomq.entity.v3.TaskStatusV3;
import com.loomq.entity.v3.TaskV3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 任务存储 V3
 *
 * 内存索引存储，支持多种查询维度。
 *
 * 索引结构：
 * - 主索引：taskId -> TaskV3
 * - 幂等索引：idempotencyKey -> taskId
 * - 业务键索引：bizKey -> taskId
 * - 时间索引：wakeTime -> Set<taskId>（TreeMap 实现）
 * - 状态索引：status -> Set<taskId>
 *
 * @author loomq
 * @since v0.4
 */
public class TaskStoreV3 {

    private static final Logger logger = LoggerFactory.getLogger(TaskStoreV3.class);

    // 主索引：taskId -> TaskV3
    private final ConcurrentHashMap<String, TaskV3> taskById;

    // 幂等索引：idempotencyKey -> taskId
    private final ConcurrentHashMap<String, String> taskIdByIdempotencyKey;

    // 业务键索引：bizKey -> taskId
    private final ConcurrentHashMap<String, String> taskIdByBizKey;

    // 时间索引：wakeTime -> Set<taskId>
    private final TreeMap<Long, Set<String>> timeIndex;

    // 状态索引：status -> Set<taskId>
    private final Map<TaskStatusV3, Set<String>> taskIdsByStatus;

    // 统计
    private final AtomicLong totalTasks = new AtomicLong(0);
    private final AtomicLong version = new AtomicLong(0);

    // 锁对象
    private final Object timeIndexLock = new Object();
    private final Object idempotencyLock = new Object();

    public TaskStoreV3() {
        this.taskById = new ConcurrentHashMap<>();
        this.taskIdByIdempotencyKey = new ConcurrentHashMap<>();
        this.taskIdByBizKey = new ConcurrentHashMap<>();
        this.timeIndex = new TreeMap<>();
        this.taskIdsByStatus = new ConcurrentHashMap<>();

        // 初始化状态索引
        for (TaskStatusV3 status : TaskStatusV3.values()) {
            taskIdsByStatus.put(status, ConcurrentHashMap.newKeySet());
        }
    }

    // ========== 写操作 ==========

    /**
     * 添加任务
     *
     * @return 是否添加成功
     */
    public boolean add(TaskV3 task) {
        String taskId = task.getTaskId();

        // 添加到主索引
        TaskV3 existing = taskById.putIfAbsent(taskId, task);
        if (existing != null) {
            logger.warn("Task already exists: {}", taskId);
            return false;
        }

        // 添加幂等索引
        if (task.getIdempotencyKey() != null && !task.getIdempotencyKey().isEmpty()) {
            taskIdByIdempotencyKey.put(task.getIdempotencyKey(), taskId);
        }

        // 添加业务键索引
        if (task.getBizKey() != null && !task.getBizKey().isEmpty()) {
            taskIdByBizKey.put(task.getBizKey(), taskId);
        }

        // 添加时间索引
        addToTimeIndex(task.getWakeTime(), taskId);

        // 添加状态索引
        taskIdsByStatus.get(task.getStatus()).add(taskId);

        // 更新统计
        totalTasks.incrementAndGet();
        version.incrementAndGet();

        logger.debug("Added task: {}", taskId);
        return true;
    }

    /**
     * 带幂等检查的原子添加
     *
     * 确保幂等检查和任务添加在同一原子操作中完成，
     * 避免并发场景下的 TOCTOU 竞态条件。
     *
     * @param task 要添加的任务
     * @return 幂等结果：
     *         - NOT_FOUND + 创建成功：新任务已创建
     *         - ACTIVE_EXISTS：存在活跃任务，未创建新任务
     *         - TERMINAL_EXISTS：存在终态任务，未创建新任务
     */
    public IdempotencyResult addWithIdempotency(TaskV3 task) {
        String idempotencyKey = task.getIdempotencyKey();

        // 无幂等键，直接添加
        if (idempotencyKey == null || idempotencyKey.isEmpty()) {
            boolean added = add(task);
            return added ? IdempotencyResult.notFound() : IdempotencyResult.activeExists(task);
        }

        synchronized (idempotencyLock) {
            // 1. 原子性检查幂等键
            IdempotencyResult existing = getByIdempotencyKey(idempotencyKey);
            if (existing.exists()) {
                logger.debug("Task with idempotencyKey {} already exists: {}",
                        idempotencyKey, existing.getTask().getTaskId());
                return existing;
            }

            // 2. 添加到主索引
            String taskId = task.getTaskId();
            TaskV3 previous = taskById.putIfAbsent(taskId, task);
            if (previous != null) {
                // taskId 已存在（理论上不应该发生）
                logger.warn("Task already exists by taskId: {}", taskId);
                return IdempotencyResult.activeExists(previous);
            }

            // 3. 添加幂等索引（必须在同一同步块中）
            taskIdByIdempotencyKey.put(idempotencyKey, taskId);

            // 4. 添加业务键索引
            if (task.getBizKey() != null && !task.getBizKey().isEmpty()) {
                taskIdByBizKey.put(task.getBizKey(), taskId);
            }

            // 5. 添加时间索引
            addToTimeIndex(task.getWakeTime(), taskId);

            // 6. 添加状态索引
            taskIdsByStatus.get(task.getStatus()).add(taskId);

            // 7. 更新统计
            totalTasks.incrementAndGet();
            version.incrementAndGet();

            logger.debug("Atomically added task: {} with idempotencyKey: {}", taskId, idempotencyKey);
            return IdempotencyResult.notFound();
        }
    }

    /**
     * 更新任务
     */
    public void update(TaskV3 task) {
        String taskId = task.getTaskId();

        TaskV3 existing = taskById.get(taskId);
        if (existing == null) {
            throw new NoSuchElementException("Task not found: " + taskId);
        }

        // 保存旧值
        TaskStatusV3 oldStatus = existing.getStatus();
        long oldWakeTime = existing.getWakeTime();
        TaskStatusV3 newStatus = task.getStatus();
        long newWakeTime = task.getWakeTime();

        // 更新主索引
        taskById.put(taskId, task);

        // 更新时间索引
        if (oldWakeTime != newWakeTime) {
            removeFromTimeIndex(oldWakeTime, taskId);
            addToTimeIndex(newWakeTime, taskId);
        }

        // 更新状态索引
        if (oldStatus != newStatus) {
            taskIdsByStatus.get(oldStatus).remove(taskId);
            taskIdsByStatus.get(newStatus).add(taskId);
        }

        version.incrementAndGet();
        logger.debug("Updated task: {}, status: {} -> {}", taskId, oldStatus, newStatus);
    }

    /**
     * 更新任务状态
     */
    public void updateStatus(String taskId, TaskStatusV3 oldStatus, TaskStatusV3 newStatus) {
        if (oldStatus != newStatus) {
            taskIdsByStatus.get(oldStatus).remove(taskId);
            taskIdsByStatus.get(newStatus).add(taskId);
            version.incrementAndGet();
            logger.debug("Updated task status: {}, {} -> {}", taskId, oldStatus, newStatus);
        }
    }

    /**
     * 移除任务
     */
    public TaskV3 remove(String taskId) {
        TaskV3 task = taskById.remove(taskId);
        if (task == null) {
            return null;
        }

        // 移除幂等索引
        if (task.getIdempotencyKey() != null) {
            taskIdByIdempotencyKey.remove(task.getIdempotencyKey());
        }

        // 移除业务键索引
        if (task.getBizKey() != null) {
            taskIdByBizKey.remove(task.getBizKey());
        }

        // 移除时间索引
        removeFromTimeIndex(task.getWakeTime(), taskId);

        // 移除状态索引
        taskIdsByStatus.get(task.getStatus()).remove(taskId);

        // 更新统计
        totalTasks.decrementAndGet();
        version.incrementAndGet();

        logger.debug("Removed task: {}", taskId);
        return task;
    }

    // ========== 读操作 ==========

    /**
     * 根据 taskId 获取任务
     */
    public TaskV3 get(String taskId) {
        return taskById.get(taskId);
    }

    /**
     * 幂等查询（关键方法）
     *
     * 返回三种结果：
     * 1. NOT_FOUND - 可以创建新任务
     * 2. ACTIVE_EXISTS - 返回已存在的未终态任务
     * 3. TERMINAL_EXISTS - 任务已终态，拒绝创建
     */
    public IdempotencyResult getByIdempotencyKey(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isEmpty()) {
            return IdempotencyResult.notFound();
        }

        String taskId = taskIdByIdempotencyKey.get(idempotencyKey);
        if (taskId == null) {
            return IdempotencyResult.notFound();
        }

        TaskV3 task = taskById.get(taskId);
        if (task == null) {
            // 索引不一致，清理
            taskIdByIdempotencyKey.remove(idempotencyKey);
            return IdempotencyResult.notFound();
        }

        if (task.getStatus().isTerminal()) {
            return IdempotencyResult.terminalExists(task);
        }

        return IdempotencyResult.activeExists(task);
    }

    /**
     * 根据业务键获取任务
     */
    public TaskV3 getByBizKey(String bizKey) {
        if (bizKey == null || bizKey.isEmpty()) {
            return null;
        }
        String taskId = taskIdByBizKey.get(bizKey);
        return taskId != null ? taskById.get(taskId) : null;
    }

    /**
     * 检查 taskId 是否存在
     */
    public boolean exists(String taskId) {
        return taskById.containsKey(taskId);
    }

    /**
     * 获取到期任务
     *
     * @param now 当前时间戳
     * @return 到期任务列表
     */
    public List<TaskV3> getDueTasks(long now) {
        List<String> taskIds = queryTimeRange(0, now);
        return taskIds.stream()
                .map(taskById::get)
                .filter(Objects::nonNull)
                .filter(t -> t.getStatus() == TaskStatusV3.SCHEDULED || t.getStatus() == TaskStatusV3.RETRY_WAIT)
                .collect(Collectors.toList());
    }

    /**
     * 获取指定状态的任务列表
     */
    public List<TaskV3> getByStatus(TaskStatusV3 status) {
        Set<String> taskIds = taskIdsByStatus.get(status);
        return taskIds.stream()
                .map(taskById::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * 获取指定状态的任务数量
     */
    public int countByStatus(TaskStatusV3 status) {
        return taskIdsByStatus.get(status).size();
    }

    // ========== 统计 ==========

    public long size() {
        return totalTasks.get();
    }

    public long getVersion() {
        return version.get();
    }

    public Map<String, Long> getStats() {
        Map<String, Long> stats = new HashMap<>();
        stats.put("total", totalTasks.get());

        for (TaskStatusV3 status : TaskStatusV3.values()) {
            stats.put(status.name().toLowerCase(), (long) countByStatus(status));
        }

        return stats;
    }

    public void clear() {
        taskById.clear();
        taskIdByIdempotencyKey.clear();
        taskIdByBizKey.clear();
        synchronized (timeIndexLock) {
            timeIndex.clear();
        }
        for (TaskStatusV3 status : TaskStatusV3.values()) {
            taskIdsByStatus.get(status).clear();
        }
        totalTasks.set(0);
        version.incrementAndGet();
        logger.info("Task store cleared");
    }

    // ========== 私有方法 ==========

    private void addToTimeIndex(long wakeTime, String taskId) {
        synchronized (timeIndexLock) {
            timeIndex.computeIfAbsent(wakeTime, k -> ConcurrentHashMap.newKeySet()).add(taskId);
        }
    }

    private void removeFromTimeIndex(long wakeTime, String taskId) {
        synchronized (timeIndexLock) {
            Set<String> tasks = timeIndex.get(wakeTime);
            if (tasks != null) {
                tasks.remove(taskId);
                if (tasks.isEmpty()) {
                    timeIndex.remove(wakeTime);
                }
            }
        }
    }

    private List<String> queryTimeRange(long from, long to) {
        synchronized (timeIndexLock) {
            List<String> result = new ArrayList<>();
            NavigableMap<Long, Set<String>> subMap = timeIndex.subMap(from, true, to, true);
            for (Set<String> tasks : subMap.values()) {
                result.addAll(tasks);
            }
            return result;
        }
    }
}

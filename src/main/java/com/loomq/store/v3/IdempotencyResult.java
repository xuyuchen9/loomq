package com.loomq.store.v3;

import com.loomq.entity.v3.TaskV3;

/**
 * 幂等查询结果
 *
 * 用于区分不同的幂等场景：
 * 1. NOT_FOUND - 幂等键不存在，可以创建新任务
 * 2. ACTIVE_EXISTS - 幂等键存在且任务未终态，返回已存在任务
 * 3. TERMINAL_EXISTS - 幂等键存在且任务已终态，拒绝创建
 *
 * @author loomq
 * @since v0.4
 */
public class IdempotencyResult {

    private final IdempotencyStatus status;
    private final TaskV3 task;
    private final String message;

    private IdempotencyResult(IdempotencyStatus status, TaskV3 task, String message) {
        this.status = status;
        this.task = task;
        this.message = message;
    }

    /**
     * 幂等键不存在
     */
    public static IdempotencyResult notFound() {
        return new IdempotencyResult(IdempotencyStatus.NOT_FOUND, null, null);
    }

    /**
     * 存在未终态任务
     */
    public static IdempotencyResult activeExists(TaskV3 task) {
        return new IdempotencyResult(IdempotencyStatus.ACTIVE_EXISTS, task,
                "Task already exists and is active");
    }

    /**
     * 存在已终态任务
     */
    public static IdempotencyResult terminalExists(TaskV3 task) {
        return new IdempotencyResult(IdempotencyStatus.TERMINAL_EXISTS, task,
                "Task already completed with status: " + task.getStatus());
    }

    public IdempotencyStatus getStatus() {
        return status;
    }

    public TaskV3 getTask() {
        return task;
    }

    public String getMessage() {
        return message;
    }

    public boolean isNotFound() {
        return status == IdempotencyStatus.NOT_FOUND;
    }

    public boolean exists() {
        return status != IdempotencyStatus.NOT_FOUND;
    }

    public boolean isTerminal() {
        return status == IdempotencyStatus.TERMINAL_EXISTS;
    }

    public boolean isActive() {
        return status == IdempotencyStatus.ACTIVE_EXISTS;
    }

    @Override
    public String toString() {
        return "IdempotencyResult{" +
                "status=" + status +
                ", taskId=" + (task != null ? task.getTaskId() : "null") +
                '}';
    }

    /**
     * 幂等状态
     */
    public enum IdempotencyStatus {
        /**
         * 幂等键不存在
         */
        NOT_FOUND,

        /**
         * 存在未终态任务
         */
        ACTIVE_EXISTS,

        /**
         * 存在已终态任务
         */
        TERMINAL_EXISTS
    }
}

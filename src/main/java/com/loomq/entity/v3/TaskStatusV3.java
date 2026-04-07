package com.loomq.entity.v3;

/**
 * 任务状态 V3（统一状态机）
 *
 * 状态流转：
 * PENDING → SCHEDULED → READY → RUNNING → SUCCESS
 *                              ↘ RETRY_WAIT → SCHEDULED (重试)
 *                              ↘ FAILED
 *                              ↘ DEAD_LETTER (重试耗尽)
 * PENDING/SCHEDULED/READY/RETRY_WAIT → CANCELLED
 * SCHEDULED/READY → EXPIRED
 *
 * 状态说明：
 * - PENDING: 已创建，等待调度
 * - SCHEDULED: 已进入调度系统，等待到期
 * - READY: 已到期，等待执行
 * - RUNNING: 执行中
 * - RETRY_WAIT: 等待重试
 * - SUCCESS: 执行成功（终态）
 * - FAILED: 执行失败（终态）
 * - CANCELLED: 已取消（终态）
 * - EXPIRED: 已过期（终态）
 * - DEAD_LETTER: 死信（终态，重试耗尽）
 *
 * @author loomq
 * @since v0.4
 */
public enum TaskStatusV3 {

    // ========== 非终态 ==========

    /**
     * 已创建，等待调度
     */
    PENDING(false, false),

    /**
     * 已进入调度系统，等待到期
     */
    SCHEDULED(false, false),

    /**
     * 已到期，等待执行
     */
    READY(false, false),

    /**
     * 执行中
     */
    RUNNING(false, false),

    /**
     * 等待重试
     */
    RETRY_WAIT(false, false),

    // ========== 终态 ==========

    /**
     * 执行成功（终态）
     */
    SUCCESS(true, false),

    /**
     * 执行失败（终态）
     */
    FAILED(true, false),

    /**
     * 已取消（终态）
     */
    CANCELLED(true, false),

    /**
     * 已过期（终态）
     */
    EXPIRED(true, false),

    /**
     * 死信（终态，重试耗尽）
     */
    DEAD_LETTER(true, false);

    private final boolean terminal;
    private final boolean dead;

    TaskStatusV3(boolean terminal, boolean dead) {
        this.terminal = terminal;
        this.dead = dead;
    }

    /**
     * 判断是否为终态
     */
    public boolean isTerminal() {
        return terminal;
    }

    /**
     * 判断是否可取消
     */
    public boolean isCancellable() {
        return this == PENDING || this == SCHEDULED || this == READY || this == RETRY_WAIT;
    }

    /**
     * 判断是否可修改
     */
    public boolean isModifiable() {
        return !terminal;
    }

    /**
     * 判断是否可执行
     */
    public boolean isExecutable() {
        return this == READY;
    }

    /**
     * 判断是否为死信
     */
    public boolean isDeadLetter() {
        return this == DEAD_LETTER;
    }

    /**
     * 判断是否成功
     */
    public boolean isSuccess() {
        return this == SUCCESS;
    }

    /**
     * 判断是否失败
     */
    public boolean isFailed() {
        return this == FAILED || this == DEAD_LETTER;
    }
}

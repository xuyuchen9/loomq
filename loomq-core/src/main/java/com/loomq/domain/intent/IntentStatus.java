package com.loomq.domain.intent;

/**
 * Intent 状态枚举 (v0.5)
 *
 * 状态流转：
 * CREATED → SCHEDULED ─────────────────► CANCELED
 *              │
 *              ▼
 *            DUE
 *              │
 *              ▼
 *         DISPATCHING
 *              │
 *              ├─────────────────────► DEAD_LETTERED (达到 maxAttempts)
 *              │
 *              ▼
 *          DELIVERED
 *              │
 *              ├─────────────────────► EXPIRED (超过 deadline)
 *              │
 *              ▼
 *            ACKED
 *
 * 状态说明：
 * - CREATED: 初始状态（瞬时）
 * - SCHEDULED: 已成功创建并进入调度器
 * - DUE: 到达执行时间，等待投递
 * - DISPATCHING: 正在执行 HTTP 投递
 * - DELIVERED: 已发出请求，等待对端 ACK
 * - ACKED: 对端已确认，终态
 * - CANCELED: 用户主动取消，终态
 * - EXPIRED: 超过 deadline 且 expiredAction=DISCARD，终态
 * - DEAD_LETTERED: 达到最大重试次数或进入死信，终态
 *
 * @author loomq
 * @since v0.5.0
 */
public enum IntentStatus {

    // ========== 非终态 ==========

    /**
     * 初始状态（瞬时）
     */
    CREATED(false),

    /**
     * 已成功创建并进入调度器
     */
    SCHEDULED(false),

    /**
     * 到达执行时间，等待投递
     */
    DUE(false),

    /**
     * 正在执行 HTTP 投递
     */
    DISPATCHING(false),

    /**
     * 已发出请求，等待对端 ACK 或 Redelivery 决策
     */
    DELIVERED(false),

    // ========== 终态 ==========

    /**
     * 对端已确认，终态
     */
    ACKED(true),

    /**
     * 用户主动取消，终态
     */
    CANCELED(true),

    /**
     * 超过 deadline 且 expiredAction=DISCARD，终态
     */
    EXPIRED(true),

    /**
     * 达到最大重试次数或进入死信，终态
     */
    DEAD_LETTERED(true);

    private final boolean terminal;

    IntentStatus(boolean terminal) {
        this.terminal = terminal;
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
        return this == SCHEDULED || this == DUE;
    }

    /**
     * 判断是否可修改
     */
    public boolean isModifiable() {
        return !terminal && this != DELIVERED && this != ACKED;
    }

    /**
     * 判断是否需要投递
     */
    public boolean needsDispatch() {
        return this == DUE || this == DELIVERED;
    }

    /**
     * 判断是否允许重投
     */
    public boolean allowsRedelivery() {
        return this == DELIVERED;
    }

    /**
     * 判断是否已过期处理
     */
    public boolean isExpiredOrDeadLetter() {
        return this == EXPIRED || this == DEAD_LETTERED;
    }
}

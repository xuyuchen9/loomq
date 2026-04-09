package com.loomq.entity.v5;

/**
 * 过期后动作枚举
 *
 * @author loomq
 * @since v0.5.0
 */
public enum ExpiredAction {

    /**
     * 丢弃（默认）
     * 超过 deadline 后，Intent 状态变为 EXPIRED，不再投递
     */
    DISCARD,

    /**
     * 进入死信队列
     * 超过 deadline 后，Intent 状态变为 DEAD_LETTERED，可手动处理
     */
    DEAD_LETTER
}

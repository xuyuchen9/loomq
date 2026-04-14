package com.loomq.domain;

/**
 * 事件类型枚举
 *
 * 对齐需求文档定义的事件类型。
 */
public enum EventType {

    /**
     * 任务创建
     */
    CREATE(1),

    /**
     * 进入调度
     */
    SCHEDULE(2),

    /**
     * 到期，准备执行
     */
    READY(3),

    /**
     * 开始执行
     */
    DISPATCH(4),

    /**
     * 执行成功
     */
    ACK(5),

    /**
     * 进入重试等待
     */
    RETRY(6),

    /**
     * 最终失败
     */
    FAIL(7),

    /**
     * 任务取消
     */
    CANCEL(8),

    /**
     * 任务过期
     */
    EXPIRE(9),

    /**
     * 进入死信
     */
    DEAD_LETTER(10),

    /**
     * 任务修改
     */
    MODIFY(11),

    /**
     * 立即触发
     */
    FIRE_NOW(12),

    /**
     * 检查点
     */
    CHECKPOINT(100);

    private final int code;

    EventType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static EventType fromCode(int code) {
        for (EventType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown event type code: " + code);
    }
}

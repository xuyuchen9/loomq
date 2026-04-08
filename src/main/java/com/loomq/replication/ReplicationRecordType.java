package com.loomq.replication;

/**
 * 复制记录类型枚举
 *
 * 定义复制对象范围（决策 #5）：必须包含任务全生命周期
 *
 * @author loomq
 * @since v0.4.8
 */
public enum ReplicationRecordType {

    // ========== 任务生命周期 ==========
    /**
     * 任务创建（含完整 Task 对象）
     */
    TASK_CREATE((byte) 0x01),

    /**
     * 任务取消
     */
    TASK_CANCEL((byte) 0x02),

    /**
     * 延迟修改
     */
    TASK_MODIFY_DELAY((byte) 0x03),

    /**
     * 立即触发
     */
    TASK_TRIGGER_NOW((byte) 0x04),

    // ========== 状态迁移 ==========
    /**
     * 状态变更：PENDING -> RUNNING -> SUCCESS/FAILURE
     */
    STATE_TRANSITION((byte) 0x10),

    /**
     * 超时处理
     */
    STATE_TIMEOUT((byte) 0x11),

    /**
     * 重试计数增加
     */
    STATE_RETRY((byte) 0x12),

    // ========== 调度索引（硬约束 #3：必须包含）==========
    /**
     * 插入时间桶索引
     */
    INDEX_INSERT((byte) 0x20),

    /**
     * 从时间桶移除
     */
    INDEX_REMOVE((byte) 0x21),

    /**
     * 更新调度时间
     */
    INDEX_UPDATE((byte) 0x22),

    // ========== 死信队列 ==========
    /**
     * 进入死信队列
     */
    DLQ_ENTER((byte) 0x30),

    /**
     * 从死信队列恢复
     */
    DLQ_EXIT((byte) 0x31),

    // ========== 检查点 ==========
    /**
     * 手动检查点（用于 snapshot 对齐）
     */
    CHECKPOINT((byte) 0x40),

    // ========== 系统事件 ==========
    /**
     * 节点提升为 primary
     */
    NODE_PROMOTION((byte) 0x50),

    /**
     * 节点降级为 replica
     */
    NODE_DEMOTION((byte) 0x51),

    /**
     * 租约续约
     */
    LEASE_RENEWAL((byte) 0x52);

    private final byte code;

    ReplicationRecordType(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    /**
     * 根据 code 获取类型
     */
    public static ReplicationRecordType fromCode(byte code) {
        for (ReplicationRecordType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown replication record type code: " + code);
    }

    /**
     * 是否为任务生命周期相关类型
     */
    public boolean isTaskLifecycle() {
        return this == TASK_CREATE || this == TASK_CANCEL
            || this == TASK_MODIFY_DELAY || this == TASK_TRIGGER_NOW;
    }

    /**
     * 是否为状态迁移类型
     */
    public boolean isStateTransition() {
        return this == STATE_TRANSITION || this == STATE_TIMEOUT || this == STATE_RETRY;
    }

    /**
     * 是否为调度索引类型（硬约束 #3）
     */
    public boolean isSchedulerIndex() {
        return this == INDEX_INSERT || this == INDEX_REMOVE || this == INDEX_UPDATE;
    }

    /**
     * 是否为系统事件类型
     */
    public boolean isSystemEvent() {
        return this == NODE_PROMOTION || this == NODE_DEMOTION || this == LEASE_RENEWAL;
    }
}

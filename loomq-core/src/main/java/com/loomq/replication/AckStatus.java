package com.loomq.replication;

/**
 * ACK 状态枚举
 *
 * 对应三种 ACK 级别：
 * - ASYNC: 进入内存队列即返回
 * - DURABLE: primary WAL fsync 后返回
 * - REPLICATED: primary + replica 都确认后返回
 *
 * @author loomq
 * @since v0.4.8
 */
public enum AckStatus {

    /**
     * 已接收，进入内存队列
     */
    RECEIVED((byte) 0x01),

    /**
     * 已持久化（primary WAL fsync 完成）
     */
    PERSISTED((byte) 0x02),

    /**
     * 已复制（replica 也确认持久化）
     */
    REPLICATED((byte) 0x03),

    /**
     * 失败
     */
    FAILED((byte) 0x10),

    /**
     * 超时
     */
    TIMEOUT((byte) 0x11),

    /**
     * 被拒绝（如 fencing token 过期）
     */
    REJECTED((byte) 0x12);

    private final byte code;

    AckStatus(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    /**
     * 是否表示成功
     */
    public boolean isSuccess() {
        return this == RECEIVED || this == PERSISTED || this == REPLICATED;
    }

    /**
     * 是否表示失败
     */
    public boolean isFailure() {
        return this == FAILED || this == TIMEOUT || this == REJECTED;
    }

    public static AckStatus fromCode(byte code) {
        for (AckStatus status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown ack status code: " + code);
    }
}

package com.loomq.common;

/**
 * 错误码定义
 */
public enum ErrorCode {
    // 成功
    SUCCESS(0, "success"),

    // 通用错误 1-99
    INVALID_PARAM(1, "invalid parameter"),
    INTERNAL_ERROR(2, "internal error"),
    SERVICE_UNAVAILABLE(3, "service unavailable"),

    // Intent 相关错误 100-199
    TASK_NOT_FOUND(100, "intent not found"),
    TASK_ALREADY_EXISTS(101, "intent already exists"),
    TASK_STATUS_INVALID(102, "intent status invalid for this operation"),
    TASK_ALREADY_TERMINATED(103, "intent already terminated"),
    TASK_CANNOT_CANCEL(104, "intent cannot be cancelled"),
    TASK_CANNOT_MODIFY(105, "intent cannot be modified"),
    VERSION_CONFLICT(106, "version conflict, intent has been modified"),
    DUPLICATE_TASK(107, "duplicate intent"),
    TASK_ALREADY_COMPLETED(108, "intent already completed"),

    // 幂等相关错误 200-299
    IDEMPOTENCY_KEY_CONFLICT(200, "idempotency key conflict"),

    // 配置相关错误 300-399
    CONFIG_ERROR(300, "configuration error"),

    // WAL相关错误 400-499
    WAL_WRITE_ERROR(400, "WAL write error"),
    WAL_READ_ERROR(401, "WAL read error"),
    WAL_CORRUPTED(402, "WAL file corrupted"),

    // 调度相关错误 500-599
    SCHEDULER_FULL(500, "scheduler is full"),

    // 执行相关错误 600-699
    WEBHOOK_FAILED(600, "webhook call failed"),
    WEBHOOK_TIMEOUT(601, "webhook call timeout"),
    WEBHOOK_INVALID_URL(602, "invalid webhook url"),

    // 恢复相关错误 700-799
    RECOVERY_FAILED(700, "recovery failed");

    private final int code;
    private final String message;

    ErrorCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}

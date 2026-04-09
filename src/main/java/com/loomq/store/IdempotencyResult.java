package com.loomq.store;

import com.loomq.entity.v5.Intent;

/**
 * 幂等查询结果 (v0.5)
 *
 * 用于区分不同的幂等场景：
 * 1. NEW - 幂等键不存在，可以创建新 Intent
 * 2. DUPLICATE_ACTIVE - 幂等键存在且 Intent 未终态，返回已存在 Intent
 * 3. DUPLICATE_TERMINAL - 幂等键存在且 Intent 已终态，拒绝创建
 * 4. WINDOW_EXPIRED - 窗口期外，视为新业务请求
 *
 * @author loomq
 * @since v0.5.0
 */
public class IdempotencyResult {

    private final IdempotencyStatus status;
    private final Intent intent;
    private final String message;

    private IdempotencyResult(IdempotencyStatus status, Intent intent, String message) {
        this.status = status;
        this.intent = intent;
        this.message = message;
    }

    /**
     * 新请求，允许创建
     */
    public static IdempotencyResult newRequest() {
        return new IdempotencyResult(IdempotencyStatus.NEW, null,
            "New request, proceed to create");
    }

    /**
     * 窗口期内重复请求，Intent 非终态，返回已存在 Intent
     */
    public static IdempotencyResult duplicateActive(Intent intent) {
        return new IdempotencyResult(IdempotencyStatus.DUPLICATE_ACTIVE, intent,
            "Duplicate request, returning existing active intent");
    }

    /**
     * 窗口期内重复请求，Intent 已终态，返回 409 Conflict
     */
    public static IdempotencyResult duplicateTerminal(Intent intent) {
        return new IdempotencyResult(IdempotencyStatus.DUPLICATE_TERMINAL, intent,
            "Duplicate request with terminal intent, reject with 409");
    }

    /**
     * 窗口期外，视为新业务请求
     */
    public static IdempotencyResult windowExpired() {
        return new IdempotencyResult(IdempotencyStatus.WINDOW_EXPIRED, null,
            "Idempotency window expired, treat as new request");
    }

    // ========== 判定方法 ==========

    /**
     * 是否允许创建新 Intent
     */
    public boolean isAllowed() {
        return status == IdempotencyStatus.NEW ||
               status == IdempotencyStatus.WINDOW_EXPIRED;
    }

    public IdempotencyStatus getStatus() {
        return status;
    }

    public Intent getIntent() {
        return intent;
    }

    public String getMessage() {
        return message;
    }

    public boolean isNotFound() {
        return status == IdempotencyStatus.NEW;
    }

    public boolean exists() {
        return status != IdempotencyStatus.NEW &&
               status != IdempotencyStatus.WINDOW_EXPIRED;
    }

    public boolean isTerminal() {
        return status == IdempotencyStatus.DUPLICATE_TERMINAL;
    }

    public boolean isActive() {
        return status == IdempotencyStatus.DUPLICATE_ACTIVE;
    }

    /**
     * 是否返回 409 Conflict
     */
    public boolean isDuplicateTerminal() {
        return status == IdempotencyStatus.DUPLICATE_TERMINAL;
    }

    /**
     * 是否是非终态重复（返回 200 OK）
     */
    public boolean isDuplicateActive() {
        return status == IdempotencyStatus.DUPLICATE_ACTIVE;
    }

    /**
     * 获取 HTTP 状态码
     */
    public int getHttpStatus() {
        return switch (status) {
            case NEW, WINDOW_EXPIRED -> 201; // Created
            case DUPLICATE_ACTIVE -> 200;    // OK, return existing
            case DUPLICATE_TERMINAL -> 409;  // Conflict
        };
    }

    /**
     * 获取错误码（用于 DUPLICATE_TERMINAL）
     */
    public String getErrorCode() {
        return isDuplicateTerminal() ? "40901" : null;
    }

    @Override
    public String toString() {
        String id = intent != null ? intent.getIntentId() : "null";
        return "IdempotencyResult{" +
                "status=" + status +
                ", id=" + id +
                '}';
    }

    /**
     * 幂等状态
     */
    public enum IdempotencyStatus {
        NEW,
        DUPLICATE_ACTIVE,
        DUPLICATE_TERMINAL,
        WINDOW_EXPIRED
    }
}

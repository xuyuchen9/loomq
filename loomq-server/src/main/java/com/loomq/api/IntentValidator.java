package com.loomq.api;

import java.time.Instant;

/**
 * Intent 验证器
 *
 * 统一 IntentController 和 IntentHandler 的验证逻辑
 *
 * @author loomq
 * @since v0.6.2
 */
public final class IntentValidator {

    // 错误码常量
    public static final String ERR_EXECUTE_AT_REQUIRED = "42201";
    public static final String ERR_DEADLINE_REQUIRED = "42202";
    public static final String ERR_SHARD_KEY_REQUIRED = "42203";
    public static final String ERR_CALLBACK_URL_REQUIRED = "42204";
    public static final String ERR_DEADLINE_ORDER = "42205";
    public static final String ERR_EXECUTE_AT_FUTURE = "42206";

    private IntentValidator() {
        // 工具类不允许实例化
    }

    /**
     * 验证创建 Intent 的必填字段
     *
     * @param executeAtStr 执行时间字符串
     * @param deadlineStr  截止时间字符串
     * @param shardKey     分片键
     * @return 验证结果
     */
    public static ValidationResult validateCreate(
            String executeAtStr,
            String deadlineStr,
            String shardKey) {
        return validateCreate(executeAtStr, deadlineStr, shardKey, null);
    }

    /**
     * 验证创建 Intent 的必填字段（包含回调 URL 验证）
     *
     * @param executeAtStr 执行时间字符串
     * @param deadlineStr  截止时间字符串
     * @param shardKey     分片键
     * @param callbackUrl  回调 URL
     * @return 验证结果
     */
    public static ValidationResult validateCreate(
            String executeAtStr,
            String deadlineStr,
            String shardKey,
            String callbackUrl) {

        // 验证 executeAt 必填
        if (executeAtStr == null || executeAtStr.isBlank()) {
            return ValidationResult.error(ERR_EXECUTE_AT_REQUIRED, "executeAt is required");
        }

        // 验证 deadline 必填
        if (deadlineStr == null || deadlineStr.isBlank()) {
            return ValidationResult.error(ERR_DEADLINE_REQUIRED, "deadline is required");
        }

        // 验证 shardKey 必填
        if (shardKey == null || shardKey.isBlank()) {
            return ValidationResult.error(ERR_SHARD_KEY_REQUIRED, "shardKey is required");
        }

        // 验证 callbackUrl 必填（可选）
        if (callbackUrl != null && callbackUrl.isBlank() == false) {
            // Netty handler 需要验证
        }

        // 解析并验证时间
        try {
            Instant executeAt = Instant.parse(executeAtStr);
            Instant deadline = Instant.parse(deadlineStr);

            // 验证 deadline > executeAt
            if (deadline.isBefore(executeAt)) {
                return ValidationResult.error(ERR_DEADLINE_ORDER, "deadline must be after executeAt");
            }

        } catch (Exception e) {
            return ValidationResult.error(ERR_EXECUTE_AT_REQUIRED, "invalid timestamp format");
        }

        return ValidationResult.VALID;
    }

    /**
     * 验证创建 Intent（使用 Instant 对象）
     *
     * @param executeAt 执行时间
     * @param deadline  截止时间
     * @param shardKey  分片键
     * @return 验证结果
     */
    public static ValidationResult validateCreate(
            Instant executeAt,
            Instant deadline,
            String shardKey) {

        // 验证 executeAt 必填
        if (executeAt == null) {
            return ValidationResult.error(ERR_EXECUTE_AT_REQUIRED, "executeAt is required");
        }

        // 验证 deadline 必填
        if (deadline == null) {
            return ValidationResult.error(ERR_DEADLINE_REQUIRED, "deadline is required");
        }

        // 验证 shardKey 必填
        if (shardKey == null || shardKey.isBlank()) {
            return ValidationResult.error(ERR_SHARD_KEY_REQUIRED, "shardKey is required");
        }

        // 验证 deadline > executeAt
        if (deadline.isBefore(executeAt)) {
            return ValidationResult.error(ERR_DEADLINE_ORDER, "deadline must be after executeAt");
        }

        return ValidationResult.VALID;
    }

    /**
     * 验证 executeAt 是否为未来时间
     *
     * @param executeAt 执行时间
     * @return 验证结果
     */
    public static ValidationResult validateExecuteAtFuture(Instant executeAt) {
        if (executeAt != null && executeAt.isBefore(Instant.now())) {
            return ValidationResult.error(ERR_EXECUTE_AT_FUTURE, "executeAt must be in the future");
        }
        return ValidationResult.VALID;
    }

    /**
     * 验证回调 URL 必填
     *
     * @param callbackUrl 回调 URL
     * @return 验证结果
     */
    public static ValidationResult validateCallbackUrl(String callbackUrl) {
        if (callbackUrl == null || callbackUrl.isBlank()) {
            return ValidationResult.error(ERR_CALLBACK_URL_REQUIRED, "callback.url is required");
        }
        return ValidationResult.VALID;
    }
}

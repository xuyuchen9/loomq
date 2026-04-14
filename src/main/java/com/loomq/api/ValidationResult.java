package com.loomq.api;

/**
 * 验证结果
 *
 * @author loomq
 * @since v0.6.2
 */
public record ValidationResult(
    boolean valid,
    String errorCode,
    String errorMessage
) {
    /** 验证通过 */
    public static final ValidationResult VALID = new ValidationResult(true, null, null);

    /**
     * 创建验证失败结果
     */
    public static ValidationResult error(String errorCode, String errorMessage) {
        return new ValidationResult(false, errorCode, errorMessage);
    }

    /**
     * 是否有效
     */
    public boolean isValid() {
        return valid;
    }

    /**
     * 是否无效
     */
    public boolean isInvalid() {
        return !valid;
    }
}

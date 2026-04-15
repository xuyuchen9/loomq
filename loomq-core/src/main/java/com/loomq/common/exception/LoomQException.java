package com.loomq.common.exception;

/**
 * LoomQ 基础异常类
 *
 * 所有 LoomQ 自定义异常的基类
 *
 * @author loomq
 * @since v0.6.2
 */
public class LoomQException extends RuntimeException {

    private final String errorCode;

    public LoomQException(String message) {
        super(message);
        this.errorCode = "50000";
    }

    public LoomQException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public LoomQException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = "50000";
    }

    public LoomQException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return errorCode;
    }
}

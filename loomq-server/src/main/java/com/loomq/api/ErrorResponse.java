package com.loomq.api;

import java.util.Map;

/**
 * 错误响应
 *
 * @author loomq
 * @since v0.5.0
 */
public record ErrorResponse(
    String code,
    String message,
    Map<String, Object> details
) {

    public static ErrorResponse of(String code, String message) {
        return new ErrorResponse(code, message, null);
    }

    public static ErrorResponse of(String code, String message, Map<String, Object> details) {
        return new ErrorResponse(code, message, details);
    }

}

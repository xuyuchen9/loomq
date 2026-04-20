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

    /**
     * Lease 过期，返回 Fencing 错误
     */
    public static ErrorResponse fencingExpired(String intentId, String newPrimary) {
        String redirectUrl = String.format("http://%s/v1/intents/%s/fire-now", newPrimary, intentId);

        return new ErrorResponse(
            "50304",
            "Lease expired, current primary is " + newPrimary,
            Map.of(
                "intentId", intentId,
                "redirectTo", redirectUrl,
                "newPrimary", newPrimary
            )
        );
    }
}

package com.loomq.common.exception;

/**
 * 投递异常
 *
 * 当意图投递失败时抛出
 *
 * @author loomq
 * @since v0.6.2
 */
public class DeliveryException extends LoomQException {

    /** 投递超时 */
    public static final String ERR_DELIVERY_TIMEOUT = "60101";
    /** 投递失败 */
    public static final String ERR_DELIVERY_FAILED = "60102";
    /** 无效的回调 URL */
    public static final String ERR_INVALID_CALLBACK_URL = "60103";
    /** 连接被拒绝 */
    public static final String ERR_CONNECTION_REFUSED = "60104";

    public DeliveryException(String message) {
        super(message);
    }

    public DeliveryException(String errorCode, String message) {
        super(errorCode, message);
    }

    public DeliveryException(String message, Throwable cause) {
        super(message, cause);
    }

    public DeliveryException(String errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }

    /**
     * 创建投递超时异常
     */
    public static DeliveryException timeout(String intentId, String url) {
        return new DeliveryException(ERR_DELIVERY_TIMEOUT,
            "Delivery timeout for intent " + intentId + " to " + url);
    }

    /**
     * 创建投递失败异常
     */
    public static DeliveryException failed(String intentId, String reason, Throwable cause) {
        return new DeliveryException(ERR_DELIVERY_FAILED,
            "Delivery failed for intent " + intentId + ": " + reason, cause);
    }

    /**
     * 创建无效回调 URL 异常
     */
    public static DeliveryException invalidUrl(String intentId, String url) {
        return new DeliveryException(ERR_INVALID_CALLBACK_URL,
            "Invalid callback URL for intent " + intentId + ": " + url);
    }

    /**
     * 创建连接被拒绝异常
     */
    public static DeliveryException connectionRefused(String intentId, String url, Throwable cause) {
        return new DeliveryException(ERR_CONNECTION_REFUSED,
            "Connection refused for intent " + intentId + " to " + url, cause);
    }
}

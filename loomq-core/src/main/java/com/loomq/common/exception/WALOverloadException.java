package com.loomq.common.exception;

/**
 * WAL 写入压力过大异常
 *
 * 当 RingBuffer 已满且无法继续写入时抛出
 *
 * @author loomq
 * @since v0.5.0
 */
public class WALOverloadException extends RuntimeException {

    public WALOverloadException(String message) {
        super(message);
    }

    public WALOverloadException(String message, Throwable cause) {
        super(message, cause);
    }
}

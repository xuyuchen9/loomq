package com.loomq.dispatcher;

import com.loomq.common.exception.DeliveryException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DeliveryException 单元测试
 */
class DeliveryExceptionTest {

    @Test
    void testConstructor_Message() {
        DeliveryException ex = new DeliveryException("Delivery failed");

        assertEquals("Delivery failed", ex.getMessage());
    }

    @Test
    void testConstructor_ErrorCodeAndMessage() {
        DeliveryException ex = new DeliveryException(DeliveryException.ERR_DELIVERY_FAILED, "Failed");

        assertEquals("Failed", ex.getMessage());
        assertEquals(DeliveryException.ERR_DELIVERY_FAILED, ex.getErrorCode());
    }

    @Test
    void testConstructor_MessageAndCause() {
        Exception cause = new RuntimeException("Connection refused");
        DeliveryException ex = new DeliveryException("Delivery error", cause);

        assertEquals("Delivery error", ex.getMessage());
        assertSame(cause, ex.getCause());
    }

    @Test
    void testTimeout() {
        DeliveryException ex = DeliveryException.timeout("intent-123", "http://localhost:8080/callback");

        assertEquals(DeliveryException.ERR_DELIVERY_TIMEOUT, ex.getErrorCode());
        assertTrue(ex.getMessage().contains("intent-123"));
        assertTrue(ex.getMessage().contains("timeout"));
        assertTrue(ex.getMessage().contains("localhost:8080"));
    }

    @Test
    void testFailed() {
        Exception cause = new RuntimeException("Connection reset");
        DeliveryException ex = DeliveryException.failed("intent-456", "network error", cause);

        assertEquals(DeliveryException.ERR_DELIVERY_FAILED, ex.getErrorCode());
        assertTrue(ex.getMessage().contains("intent-456"));
        assertTrue(ex.getMessage().contains("network error"));
        assertSame(cause, ex.getCause());
    }

    @Test
    void testInvalidUrl() {
        DeliveryException ex = DeliveryException.invalidUrl("intent-789", "invalid-url");

        assertEquals(DeliveryException.ERR_INVALID_CALLBACK_URL, ex.getErrorCode());
        assertTrue(ex.getMessage().contains("intent-789"));
        assertTrue(ex.getMessage().contains("Invalid callback URL"));
    }

    @Test
    void testConnectionRefused() {
        Exception cause = new RuntimeException("Connection refused");
        DeliveryException ex = DeliveryException.connectionRefused("intent-abc", "http://localhost:8080", cause);

        assertEquals(DeliveryException.ERR_CONNECTION_REFUSED, ex.getErrorCode());
        assertTrue(ex.getMessage().contains("intent-abc"));
        assertTrue(ex.getMessage().contains("Connection refused"));
        assertSame(cause, ex.getCause());
    }

    @Test
    void testErrorCodes() {
        assertEquals("60101", DeliveryException.ERR_DELIVERY_TIMEOUT);
        assertEquals("60102", DeliveryException.ERR_DELIVERY_FAILED);
        assertEquals("60103", DeliveryException.ERR_INVALID_CALLBACK_URL);
        assertEquals("60104", DeliveryException.ERR_CONNECTION_REFUSED);
    }
}

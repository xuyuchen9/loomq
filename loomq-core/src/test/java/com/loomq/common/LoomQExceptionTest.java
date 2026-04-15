package com.loomq.common;

import com.loomq.common.exception.LoomQException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LoomQException 单元测试
 */
class LoomQExceptionTest {

    @Test
    void testConstructor_Message() {
        LoomQException ex = new LoomQException("Test error");

        assertEquals("Test error", ex.getMessage());
        assertEquals("50000", ex.getErrorCode());
    }

    @Test
    void testConstructor_ErrorCodeAndMessage() {
        LoomQException ex = new LoomQException("40001", "Bad request");

        assertEquals("Bad request", ex.getMessage());
        assertEquals("40001", ex.getErrorCode());
    }

    @Test
    void testConstructor_MessageAndCause() {
        Exception cause = new RuntimeException("Root cause");
        LoomQException ex = new LoomQException("Wrapped error", cause);

        assertEquals("Wrapped error", ex.getMessage());
        assertEquals("50000", ex.getErrorCode());
        assertSame(cause, ex.getCause());
    }

    @Test
    void testConstructor_ErrorCodeMessageAndCause() {
        Exception cause = new RuntimeException("Root cause");
        LoomQException ex = new LoomQException("50001", "Internal error", cause);

        assertEquals("Internal error", ex.getMessage());
        assertEquals("50001", ex.getErrorCode());
        assertSame(cause, ex.getCause());
    }

    @Test
    void testIsRuntimeException() {
        LoomQException ex = new LoomQException("Test");

        assertTrue(ex instanceof RuntimeException);
    }
}

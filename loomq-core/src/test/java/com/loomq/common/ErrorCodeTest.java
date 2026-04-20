package com.loomq.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ErrorCode 单元测试
 */
class ErrorCodeTest {

    @Test
    void testSuccessCode() {
        assertEquals(0, ErrorCode.SUCCESS.getCode());
        assertEquals("success", ErrorCode.SUCCESS.getMessage());
    }

    @Test
    void testGeneralErrorCodes() {
        assertEquals(1, ErrorCode.INVALID_PARAM.getCode());
        assertEquals(2, ErrorCode.INTERNAL_ERROR.getCode());
        assertEquals(3, ErrorCode.SERVICE_UNAVAILABLE.getCode());
    }

    @Test
    void testTaskErrorCodes() {
        assertEquals(100, ErrorCode.TASK_NOT_FOUND.getCode());
        assertEquals(101, ErrorCode.TASK_ALREADY_EXISTS.getCode());
        assertEquals(102, ErrorCode.TASK_STATUS_INVALID.getCode());
        assertEquals(103, ErrorCode.TASK_ALREADY_TERMINATED.getCode());
        assertEquals(104, ErrorCode.TASK_CANNOT_CANCEL.getCode());
        assertEquals(105, ErrorCode.TASK_CANNOT_MODIFY.getCode());
        assertEquals(106, ErrorCode.VERSION_CONFLICT.getCode());
        assertEquals(107, ErrorCode.DUPLICATE_TASK.getCode());
        assertEquals(108, ErrorCode.TASK_ALREADY_COMPLETED.getCode());
    }

    @Test
    void testIdempotencyErrorCodes() {
        assertEquals(200, ErrorCode.IDEMPOTENCY_KEY_CONFLICT.getCode());
    }

    @Test
    void testConfigErrorCodes() {
        assertEquals(300, ErrorCode.CONFIG_ERROR.getCode());
    }

    @Test
    void testWalErrorCodes() {
        assertEquals(400, ErrorCode.WAL_WRITE_ERROR.getCode());
        assertEquals(401, ErrorCode.WAL_READ_ERROR.getCode());
        assertEquals(402, ErrorCode.WAL_CORRUPTED.getCode());
    }

    @Test
    void testSchedulerErrorCodes() {
        assertEquals(500, ErrorCode.SCHEDULER_FULL.getCode());
    }

    @Test
    void testWebhookErrorCodes() {
        assertEquals(600, ErrorCode.WEBHOOK_FAILED.getCode());
        assertEquals(601, ErrorCode.WEBHOOK_TIMEOUT.getCode());
        assertEquals(602, ErrorCode.WEBHOOK_INVALID_URL.getCode());
    }

    @Test
    void testRecoveryErrorCodes() {
        assertEquals(700, ErrorCode.RECOVERY_FAILED.getCode());
    }

    @Test
    void testGetMessage() {
        assertEquals("intent not found", ErrorCode.TASK_NOT_FOUND.getMessage());
        assertEquals("WAL write error", ErrorCode.WAL_WRITE_ERROR.getMessage());
    }

    @Test
    void testAllErrorCodesHaveUniqueCodes() {
        ErrorCode[] codes = ErrorCode.values();
        java.util.Set<Integer> uniqueCodes = new java.util.HashSet<>();

        for (ErrorCode code : codes) {
            assertTrue(uniqueCodes.add(code.getCode()),
                "Duplicate code found: " + code.getCode() + " for " + code.name());
        }
    }
}

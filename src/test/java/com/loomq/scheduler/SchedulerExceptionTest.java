package com.loomq.scheduler;

import com.loomq.common.exception.SchedulerException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SchedulerException 单元测试
 */
class SchedulerExceptionTest {

    @Test
    void testConstructor_Message() {
        SchedulerException ex = new SchedulerException("Schedule failed");

        assertEquals("Schedule failed", ex.getMessage());
    }

    @Test
    void testConstructor_ErrorCodeAndMessage() {
        SchedulerException ex = new SchedulerException(SchedulerException.ERR_SCHEDULE_FAILED, "Failed to schedule");

        assertEquals("Failed to schedule", ex.getMessage());
        assertEquals(SchedulerException.ERR_SCHEDULE_FAILED, ex.getErrorCode());
    }

    @Test
    void testConstructor_MessageAndCause() {
        Exception cause = new RuntimeException("Root cause");
        SchedulerException ex = new SchedulerException("Schedule error", cause);

        assertEquals("Schedule error", ex.getMessage());
        assertSame(cause, ex.getCause());
    }

    @Test
    void testSchedulerFull() {
        SchedulerException ex = SchedulerException.schedulerFull("intent-123");

        assertEquals(SchedulerException.ERR_SCHEDULER_FULL, ex.getErrorCode());
        assertTrue(ex.getMessage().contains("intent-123"));
        assertTrue(ex.getMessage().contains("Scheduler is full"));
    }

    @Test
    void testScheduleFailed() {
        Exception cause = new RuntimeException("Connection refused");
        SchedulerException ex = SchedulerException.scheduleFailed("intent-456", cause);

        assertEquals(SchedulerException.ERR_SCHEDULE_FAILED, ex.getErrorCode());
        assertTrue(ex.getMessage().contains("intent-456"));
        assertSame(cause, ex.getCause());
    }

    @Test
    void testErrorCodes() {
        assertEquals("50001", SchedulerException.ERR_SCHEDULER_FULL);
        assertEquals("50002", SchedulerException.ERR_SCHEDULE_FAILED);
        assertEquals("50003", SchedulerException.ERR_INVALID_SCHEDULE_TIME);
    }
}

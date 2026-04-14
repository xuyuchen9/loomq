package com.loomq.api;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IntentValidator 单元测试
 */
class IntentValidatorTest {

    @Test
    void testValidateCreate_AllFieldsValid() {
        Instant executeAt = Instant.now().plusSeconds(60);
        Instant deadline = executeAt.plusSeconds(3600);

        ValidationResult result = IntentValidator.validateCreate(executeAt, deadline, "shard-1");

        assertTrue(result.isValid());
        assertNull(result.errorCode());
        assertNull(result.errorMessage());
    }

    @Test
    void testValidateCreate_ExecuteAtNull() {
        Instant deadline = Instant.now().plusSeconds(3600);

        ValidationResult result = IntentValidator.validateCreate(null, deadline, "shard-1");

        assertTrue(result.isInvalid());
        assertEquals(IntentValidator.ERR_EXECUTE_AT_REQUIRED, result.errorCode());
        assertEquals("executeAt is required", result.errorMessage());
    }

    @Test
    void testValidateCreate_DeadlineNull() {
        Instant executeAt = Instant.now().plusSeconds(60);

        ValidationResult result = IntentValidator.validateCreate(executeAt, null, "shard-1");

        assertTrue(result.isInvalid());
        assertEquals(IntentValidator.ERR_DEADLINE_REQUIRED, result.errorCode());
    }

    @Test
    void testValidateCreate_ShardKeyNull() {
        Instant executeAt = Instant.now().plusSeconds(60);
        Instant deadline = executeAt.plusSeconds(3600);

        ValidationResult result = IntentValidator.validateCreate(executeAt, deadline, null);

        assertTrue(result.isInvalid());
        assertEquals(IntentValidator.ERR_SHARD_KEY_REQUIRED, result.errorCode());
    }

    @Test
    void testValidateCreate_ShardKeyBlank() {
        Instant executeAt = Instant.now().plusSeconds(60);
        Instant deadline = executeAt.plusSeconds(3600);

        ValidationResult result = IntentValidator.validateCreate(executeAt, deadline, "  ");

        assertTrue(result.isInvalid());
        assertEquals(IntentValidator.ERR_SHARD_KEY_REQUIRED, result.errorCode());
    }

    @Test
    void testValidateCreate_DeadlineBeforeExecuteAt() {
        Instant executeAt = Instant.now().plusSeconds(3600);
        Instant deadline = Instant.now().plusSeconds(60);

        ValidationResult result = IntentValidator.validateCreate(executeAt, deadline, "shard-1");

        assertTrue(result.isInvalid());
        assertEquals(IntentValidator.ERR_DEADLINE_ORDER, result.errorCode());
        assertEquals("deadline must be after executeAt", result.errorMessage());
    }

    @Test
    void testValidateCreate_StringInputs_Valid() {
        String executeAtStr = Instant.now().plusSeconds(60).toString();
        String deadlineStr = Instant.now().plusSeconds(3600).toString();

        ValidationResult result = IntentValidator.validateCreate(executeAtStr, deadlineStr, "shard-1");

        assertTrue(result.isValid());
    }

    @Test
    void testValidateCreate_StringInputs_ExecuteAtNull() {
        String deadlineStr = Instant.now().plusSeconds(3600).toString();

        ValidationResult result = IntentValidator.validateCreate(null, deadlineStr, "shard-1");

        assertTrue(result.isInvalid());
        assertEquals(IntentValidator.ERR_EXECUTE_AT_REQUIRED, result.errorCode());
    }

    @Test
    void testValidateCreate_StringInputs_ExecuteAtBlank() {
        String deadlineStr = Instant.now().plusSeconds(3600).toString();

        ValidationResult result = IntentValidator.validateCreate("  ", deadlineStr, "shard-1");

        assertTrue(result.isInvalid());
        assertEquals(IntentValidator.ERR_EXECUTE_AT_REQUIRED, result.errorCode());
    }

    @Test
    void testValidateExecuteAtFuture_PastTime() {
        Instant pastTime = Instant.now().minusSeconds(60);

        ValidationResult result = IntentValidator.validateExecuteAtFuture(pastTime);

        assertTrue(result.isInvalid());
        assertEquals(IntentValidator.ERR_EXECUTE_AT_FUTURE, result.errorCode());
    }

    @Test
    void testValidateExecuteAtFuture_FutureTime() {
        Instant futureTime = Instant.now().plusSeconds(60);

        ValidationResult result = IntentValidator.validateExecuteAtFuture(futureTime);

        assertTrue(result.isValid());
    }

    @Test
    void testValidateExecuteAtFuture_Null() {
        ValidationResult result = IntentValidator.validateExecuteAtFuture(null);

        assertTrue(result.isValid());
    }

    @Test
    void testValidateCallbackUrl_Valid() {
        ValidationResult result = IntentValidator.validateCallbackUrl("http://localhost:8080/callback");

        assertTrue(result.isValid());
    }

    @Test
    void testValidateCallbackUrl_Null() {
        ValidationResult result = IntentValidator.validateCallbackUrl(null);

        assertTrue(result.isInvalid());
        assertEquals(IntentValidator.ERR_CALLBACK_URL_REQUIRED, result.errorCode());
    }

    @Test
    void testValidateCallbackUrl_Blank() {
        ValidationResult result = IntentValidator.validateCallbackUrl("  ");

        assertTrue(result.isInvalid());
        assertEquals(IntentValidator.ERR_CALLBACK_URL_REQUIRED, result.errorCode());
    }
}

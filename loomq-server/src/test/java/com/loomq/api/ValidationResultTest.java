package com.loomq.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ValidationResult 单元测试
 */
class ValidationResultTest {

    @Test
    void testValidConstant() {
        assertTrue(ValidationResult.VALID.isValid());
        assertFalse(ValidationResult.VALID.isInvalid());
        assertNull(ValidationResult.VALID.errorCode());
        assertNull(ValidationResult.VALID.errorMessage());
    }

    @Test
    void testError_Factory() {
        ValidationResult result = ValidationResult.error("42201", "executeAt is required");

        assertFalse(result.isValid());
        assertTrue(result.isInvalid());
        assertEquals("42201", result.errorCode());
        assertEquals("executeAt is required", result.errorMessage());
    }

    @Test
    void testError_DifferentCodes() {
        ValidationResult result1 = ValidationResult.error("42201", "Error 1");
        ValidationResult result2 = ValidationResult.error("42202", "Error 2");

        assertNotEquals(result1.errorCode(), result2.errorCode());
        assertNotEquals(result1.errorMessage(), result2.errorMessage());
    }
}

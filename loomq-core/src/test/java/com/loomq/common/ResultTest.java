package com.loomq.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Result 单元测试
 */
class ResultTest {

    @Test
    void testSuccess_NoData() {
        Result<Void> result = Result.success();

        assertTrue(result.isSuccess());
        assertFalse(result.isError());
        assertEquals(0, result.getCode());
        assertEquals("success", result.getMessage());
        assertNull(result.getData());
    }

    @Test
    void testSuccess_WithData() {
        String data = "test-data";
        Result<String> result = Result.success(data);

        assertTrue(result.isSuccess());
        assertEquals(0, result.getCode());
        assertEquals("success", result.getMessage());
        assertEquals(data, result.getData());
    }

    @Test
    void testSuccess_WithMessageAndData() {
        String data = "test-data";
        Result<String> result = Result.success("Operation completed", data);

        assertTrue(result.isSuccess());
        assertEquals(0, result.getCode());
        assertEquals("Operation completed", result.getMessage());
        assertEquals(data, result.getData());
    }

    @Test
    void testError_WithErrorCode() {
        Result<Void> result = Result.error(ErrorCode.TASK_NOT_FOUND);

        assertTrue(result.isError());
        assertFalse(result.isSuccess());
        assertEquals(100, result.getCode());
        assertEquals("intent not found", result.getMessage());
    }

    @Test
    void testError_WithErrorCodeAndMessage() {
        Result<Void> result = Result.error(ErrorCode.INVALID_PARAM, "Field 'name' is required");

        assertTrue(result.isError());
        assertEquals(1, result.getCode());
        assertEquals("Field 'name' is required", result.getMessage());
    }

    @Test
    void testError_WithCodeAndMessage() {
        Result<Void> result = Result.error(404, "Not found");

        assertTrue(result.isError());
        assertEquals(404, result.getCode());
        assertEquals("Not found", result.getMessage());
    }

    @Test
    void testIsSuccess() {
        assertTrue(Result.success().isSuccess());
        assertFalse(Result.error(ErrorCode.INTERNAL_ERROR).isSuccess());
    }

    @Test
    void testIsError() {
        assertTrue(Result.error(ErrorCode.INTERNAL_ERROR).isError());
        assertFalse(Result.success().isError());
    }

    @Test
    void testDataCanBeNull() {
        Result<String> result = Result.success(null);

        assertTrue(result.isSuccess());
        assertNull(result.getData());
    }

    @Test
    void testErrorDataIsNull() {
        Result<Void> result = Result.error(ErrorCode.INTERNAL_ERROR);

        assertNull(result.getData());
    }
}

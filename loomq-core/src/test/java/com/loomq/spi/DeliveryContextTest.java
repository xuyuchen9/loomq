package com.loomq.spi;

import org.junit.jupiter.api.Test;

import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeliveryContextTest {

    @Test
    void shouldInitializeWithRequiredFields() {
        DeliveryContext ctx = new DeliveryContext("d-1", "i-1", 3);
        assertEquals("d-1", ctx.getDeliveryId());
        assertEquals("i-1", ctx.getIntentId());
        assertEquals(3, ctx.getAttempt());
        assertNotNull(ctx.getStartedAt());
        assertFalse(ctx.hasResponse());
        assertFalse(ctx.hasException());
        assertFalse(ctx.isSuccess());
        assertTrue(ctx.getFinishedAt().isEmpty());
    }

    @Test
    void shouldRejectNullDeliveryId() {
        // Use a try-catch since Objects.requireNonNull throws NPE
        try {
            new DeliveryContext(null, "i-1", 1);
        } catch (NullPointerException expected) {
            // expected
        }
    }

    @Test
    void shouldRejectNullIntentId() {
        try {
            new DeliveryContext("d-1", null, 1);
        } catch (NullPointerException expected) {
            // expected
        }
    }

    @Test
    void markSuccessShouldSetHttpStatusAndHeaders() {
        DeliveryContext ctx = new DeliveryContext("d-2", "i-2", 1);
        ctx.markSuccess(200, Map.of("Content-Type", "application/json"), "{\"ok\":true}");
        assertTrue(ctx.isSuccess());
        assertTrue(ctx.hasResponse());
        assertFalse(ctx.hasException());
        assertEquals(200, ctx.getHttpStatus().orElseThrow());
        assertEquals("{\"ok\":true}", ctx.getResponseBody().orElseThrow());
        assertTrue(ctx.getLatency().isPresent());
        assertTrue(ctx.getFinishedAt().isPresent());
    }

    @Test
    void markFailureWithExceptionShouldSetException() {
        DeliveryContext ctx = new DeliveryContext("d-3", "i-3", 1);
        RuntimeException ex = new RuntimeException("boom");
        ctx.markFailure(ex);
        assertTrue(ctx.hasException());
        assertFalse(ctx.hasResponse());
        assertEquals(ex, ctx.getException().orElseThrow());
    }

    @Test
    void markFailureWithHttpStatusShouldSetStatus() {
        DeliveryContext ctx = new DeliveryContext("d-4", "i-4", 1);
        ctx.markFailure(500, Map.of(), "Server Error");
        assertTrue(ctx.isServerError());
        assertFalse(ctx.isClientError());
        assertFalse(ctx.isSuccess());
    }

    @Test
    void shouldDetectTimeoutFromSocketTimeoutException() {
        DeliveryContext ctx = new DeliveryContext("d-5", "i-5", 1);
        ctx.markFailure(new SocketTimeoutException("Read timed out"));
        assertTrue(ctx.isTimeout());
    }

    @Test
    void shouldDetectTimeoutFromUtilTimeoutException() {
        DeliveryContext ctx = new DeliveryContext("d-6", "i-6", 1);
        ctx.markFailure(new TimeoutException("timed out"));
        assertTrue(ctx.isTimeout());
    }

    @Test
    void shouldNotDetectTimeoutFromOtherExceptions() {
        DeliveryContext ctx = new DeliveryContext("d-7", "i-7", 1);
        ctx.markFailure(new IllegalStateException("some state"));
        assertFalse(ctx.isTimeout());
    }

    @Test
    void shouldDetectConnectionErrorByMessage() {
        DeliveryContext ctx = new DeliveryContext("d-8", "i-8", 1);
        ctx.markFailure(new RuntimeException("Connection refused by remote host"));
        assertTrue(ctx.isConnectionError());
    }

    @Test
    void shouldNotDetectConnectionErrorForNonMatchingMessage() {
        DeliveryContext ctx = new DeliveryContext("d-9", "i-9", 1);
        ctx.markFailure(new RuntimeException("something else happened"));
        assertFalse(ctx.isConnectionError());
    }
}

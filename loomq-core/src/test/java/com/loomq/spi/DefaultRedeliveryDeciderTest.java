package com.loomq.spi;

import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultRedeliveryDeciderTest {

    private final DefaultRedeliveryDecider decider = new DefaultRedeliveryDecider();

    @Test
    void shouldNotRedeliverOnSuccess() {
        DeliveryContext ctx = new DeliveryContext("d-1", "i-1", 1);
        ctx.markSuccess(200, Map.of(), "OK");
        assertFalse(decider.shouldRedeliver(ctx));
    }

    @Test
    void shouldRedeliverOnServerError5xx() {
        DeliveryContext ctx = new DeliveryContext("d-2", "i-2", 1);
        ctx.markFailure(500, Map.of(), "Internal Server Error");
        assertTrue(decider.shouldRedeliver(ctx));
    }

    @Test
    void shouldRedeliverOn503ServiceUnavailable() {
        DeliveryContext ctx = new DeliveryContext("d-3", "i-3", 1);
        ctx.markFailure(503, Map.of(), "Service Unavailable");
        assertTrue(decider.shouldRedeliver(ctx));
    }

    @Test
    void shouldNotRedeliverOnClientError4xx() {
        DeliveryContext ctx = new DeliveryContext("d-4", "i-4", 1);
        ctx.markFailure(400, Map.of(), "Bad Request");
        assertFalse(decider.shouldRedeliver(ctx));
    }

    @Test
    void shouldNotRedeliverOn404NotFound() {
        DeliveryContext ctx = new DeliveryContext("d-5", "i-5", 1);
        ctx.markFailure(404, Map.of(), "Not Found");
        assertFalse(decider.shouldRedeliver(ctx));
    }

    @Test
    void shouldRedeliverOnSocketTimeoutException() {
        DeliveryContext ctx = new DeliveryContext("d-6", "i-6", 1);
        ctx.markFailure(new SocketTimeoutException("Read timed out"));
        assertTrue(decider.shouldRedeliver(ctx));
    }

    @Test
    void shouldRedeliverOnTimeoutException() {
        DeliveryContext ctx = new DeliveryContext("d-7", "i-7", 1);
        ctx.markFailure(new TimeoutException("timed out"));
        assertTrue(decider.shouldRedeliver(ctx));
    }

    @Test
    void shouldRedeliverOnConnectionRefused() {
        DeliveryContext ctx = new DeliveryContext("d-8", "i-8", 1);
        ctx.markFailure(new RuntimeException("Connection refused"));
        assertTrue(decider.shouldRedeliver(ctx));
    }

    @Test
    void shouldRedeliverOnConnectionReset() {
        DeliveryContext ctx = new DeliveryContext("d-9", "i-9", 1);
        ctx.markFailure(new RuntimeException("Connection reset"));
        assertTrue(decider.shouldRedeliver(ctx));
    }

    @Test
    void shouldRedeliverOnConnectException() {
        DeliveryContext ctx = new DeliveryContext("d-10", "i-10", 1);
        ctx.markFailure(new ConnectException("Connection refused: no further information"));
        assertTrue(decider.shouldRedeliver(ctx));
    }

    @Test
    void shouldRedeliverOnUnknownException() {
        DeliveryContext ctx = new DeliveryContext("d-11", "i-11", 1);
        ctx.markFailure(new RuntimeException("something unexpected"));
        assertTrue(decider.shouldRedeliver(ctx));
    }

    @Test
    void shouldNotRedeliverOnUnmarkedContext() {
        DeliveryContext ctx = new DeliveryContext("d-12", "i-12", 1);
        assertFalse(decider.shouldRedeliver(ctx));
    }

    @Test
    void shouldReturnCorrectName() {
        assertEquals("DefaultRedeliveryDecider", decider.getName());
    }

    // ========== priority: isSuccess() short-circuits before timeout/connection checks ==========

    @Test
    void successShouldOverrideTimeout() {
        // markSuccess then markFailure — demonstrates that markSuccess wins
        DeliveryContext ctx = new DeliveryContext("d-13", "i-13", 1);
        ctx.markSuccess(200, Map.of(), "OK");
        ctx.markFailure(new SocketTimeoutException("Read timed out"));
        // isSuccess checks httpStatus which is still 200 even after markFailure overwrites exception
        assertFalse(decider.shouldRedeliver(ctx));
    }
}

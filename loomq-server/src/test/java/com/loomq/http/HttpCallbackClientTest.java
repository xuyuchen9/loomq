package com.loomq.http;

import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.Intent;
import org.junit.jupiter.api.Test;

import java.net.http.HttpRequest;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * HttpCallbackClient 单元测试
 */
class HttpCallbackClientTest {

    @Test
    void testBuildCallbackRequest_BasicPost() {
        Intent intent = createTestIntent("intent-1", "http://localhost:8080/callback");

        HttpRequest request = HttpCallbackClient.buildCallbackRequest(intent);

        assertNotNull(request);
        assertEquals("http://localhost:8080/callback", request.uri().toString());
        assertEquals("POST", request.method());
        assertEquals("application/json", request.headers().firstValue("Content-Type").orElse(null));
        assertEquals("intent-1", request.headers().firstValue("X-LoomQ-Intent-Id").orElse(null));
    }

    @Test
    void testBuildCallbackRequest_WithCustomTimeout() {
        Intent intent = createTestIntent("intent-2", "http://localhost:8080/callback");

        HttpRequest request = HttpCallbackClient.buildCallbackRequest(intent, 60);

        assertNotNull(request);
        assertEquals("http://localhost:8080/callback", request.uri().toString());
    }

    @Test
    void testBuildCallbackRequest_WithCustomHeaders() {
        Intent intent = createTestIntent("intent-3", "http://localhost:8080/callback");
        intent.getCallback().setHeaders(Map.of("X-Custom-Header", "custom-value"));

        HttpRequest request = HttpCallbackClient.buildCallbackRequest(intent);

        assertNotNull(request);
        assertEquals("custom-value", request.headers().firstValue("X-Custom-Header").orElse(null));
    }

    @Test
    void testBuildCallbackRequest_PutMethod() {
        Intent intent = createTestIntent("intent-4", "http://localhost:8080/callback");
        intent.getCallback().setMethod("PUT");

        HttpRequest request = HttpCallbackClient.buildCallbackRequest(intent);

        assertEquals("PUT", request.method());
    }

    @Test
    void testBuildCallbackRequest_PatchMethod() {
        Intent intent = createTestIntent("intent-5", "http://localhost:8080/callback");
        intent.getCallback().setMethod("PATCH");

        HttpRequest request = HttpCallbackClient.buildCallbackRequest(intent);

        assertEquals("PATCH", request.method());
    }

    @Test
    void testBuildCallbackRequest_NullMethodDefaultsToPost() {
        Intent intent = createTestIntent("intent-6", "http://localhost:8080/callback");
        intent.getCallback().setMethod(null);

        HttpRequest request = HttpCallbackClient.buildCallbackRequest(intent);

        assertEquals("POST", request.method());
    }

    @Test
    void testBuildCallbackRequest_UnknownMethodDefaultsToPost() {
        Intent intent = createTestIntent("intent-7", "http://localhost:8080/callback");
        intent.getCallback().setMethod("DELETE");

        HttpRequest request = HttpCallbackClient.buildCallbackRequest(intent);

        assertEquals("POST", request.method());
    }

    @Test
    void testBuildCallbackRequest_WithBody() {
        Intent intent = createTestIntent("intent-8", "http://localhost:8080/callback");
        intent.getCallback().setBody("{\"key\":\"value\"}");

        HttpRequest request = HttpCallbackClient.buildCallbackRequest(intent);

        assertNotNull(request);
        // BodyPublisher is not directly comparable, but we can verify the request was built
        assertEquals("POST", request.method());
    }

    @Test
    void testBuildCallbackRequest_WithNullBody() {
        Intent intent = createTestIntent("intent-9", "http://localhost:8080/callback");
        intent.getCallback().setBody(null);

        HttpRequest request = HttpCallbackClient.buildCallbackRequest(intent);

        assertNotNull(request);
    }

    @Test
    void testBuildCallbackRequest_EmptyHeaders() {
        Intent intent = createTestIntent("intent-10", "http://localhost:8080/callback");
        intent.getCallback().setHeaders(Map.of());

        HttpRequest request = HttpCallbackClient.buildCallbackRequest(intent);

        assertNotNull(request);
        assertEquals("application/json", request.headers().firstValue("Content-Type").orElse(null));
    }

    // ========== Helper Methods ==========

    private Intent createTestIntent(String intentId, String callbackUrl) {
        Intent intent = new Intent(intentId);
        Callback callback = new Callback(callbackUrl);
        intent.setCallback(callback);
        return intent;
    }
}

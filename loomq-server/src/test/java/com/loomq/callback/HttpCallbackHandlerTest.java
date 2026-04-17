package com.loomq.callback;

import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.CallbackHandler.EventType;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * HttpCallbackHandler 单元测试
 *
 * @author loomq
 * @since v0.7.0
 */
class HttpCallbackHandlerTest {

    private MockWebServer mockWebServer;
    private HttpCallbackHandler handler;
    private String baseUrl;

    @BeforeEach
    void setUp() throws Exception {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        baseUrl = mockWebServer.url("/callback").toString();

        handler = HttpCallbackHandler.builder()
            .maxRetries(2)
            .connectTimeout(Duration.ofSeconds(1))
            .requestTimeout(Duration.ofSeconds(2))
            .build();
    }

    @AfterEach
    void tearDown() throws Exception {
        handler.close();
        mockWebServer.shutdown();
    }

    @Test
    void testDueEventCallback() throws Exception {
        // 配置 Mock 服务器返回成功
        mockWebServer.enqueue(new MockResponse()
            .setResponseCode(200)
            .setBody("{\"status\":\"ok\"}"));

        // 创建测试 Intent
        Intent intent = createTestIntent("test-1", baseUrl);

        // 触发回调
        handler.onIntentEvent(intent, EventType.DUE, null);

        // 等待异步处理
        RecordedRequest request = mockWebServer.takeRequest(3, TimeUnit.SECONDS);

        // 验证请求
        assertNotNull(request);
        assertEquals("POST", request.getMethod());
        assertEquals("/callback", request.getPath());
        assertEquals("application/json", request.getHeader("Content-Type"));
        assertEquals("test-1", request.getHeader("X-LoomQ-Intent-Id"));
        assertEquals("DUE", request.getHeader("X-LoomQ-Event-Type"));

        // 验证请求体
        String body = request.getBody().readUtf8();
        assertTrue(body.contains("\"intentId\":\"test-1\""));
        assertTrue(body.contains("\"eventType\":\"DUE\""));
        assertTrue(body.contains("\"status\":\"SCHEDULED\""));
    }

    @Test
    void testCancelledEventCallback() throws Exception {
        mockWebServer.enqueue(new MockResponse()
            .setResponseCode(200));

        Intent intent = createTestIntent("test-cancel", baseUrl);
        intent.transitionTo(IntentStatus.CANCELED);

        handler.onIntentEvent(intent, EventType.CANCELLED, null);

        RecordedRequest request = mockWebServer.takeRequest(3, TimeUnit.SECONDS);
        assertNotNull(request);
        assertEquals("CANCELLED", request.getHeader("X-LoomQ-Event-Type"));

        String body = request.getBody().readUtf8();
        assertTrue(body.contains("\"eventType\":\"CANCELLED\""));
        assertTrue(body.contains("\"status\":\"CANCELED\""));
    }

    @Test
    void testFailedEventCallback() throws Exception {
        mockWebServer.enqueue(new MockResponse()
            .setResponseCode(200));

        Intent intent = createTestIntent("test-fail", baseUrl);
        RuntimeException error = new RuntimeException("Connection timeout");

        handler.onIntentEvent(intent, EventType.FAILED, error);

        RecordedRequest request = mockWebServer.takeRequest(3, TimeUnit.SECONDS);
        assertNotNull(request);
        assertEquals("FAILED", request.getHeader("X-LoomQ-Event-Type"));

        String body = request.getBody().readUtf8();
        assertTrue(body.contains("\"eventType\":\"FAILED\""));
        assertTrue(body.contains("\"error\""));
        assertTrue(body.contains("Connection timeout"));
    }

    @Test
    void testRetryOnFailure() throws Exception {
        // 前两次返回失败，第三次成功
        mockWebServer.enqueue(new MockResponse().setResponseCode(500));
        mockWebServer.enqueue(new MockResponse().setResponseCode(503));
        mockWebServer.enqueue(new MockResponse().setResponseCode(200));

        Intent intent = createTestIntent("test-retry", baseUrl);

        handler.onIntentEvent(intent, EventType.DUE, null);

        // 应该有 3 次请求（2 次失败 + 1 次成功）
        RecordedRequest request1 = mockWebServer.takeRequest(3, TimeUnit.SECONDS);
        RecordedRequest request2 = mockWebServer.takeRequest(3, TimeUnit.SECONDS);
        RecordedRequest request3 = mockWebServer.takeRequest(3, TimeUnit.SECONDS);

        assertNotNull(request1);
        assertNotNull(request2);
        assertNotNull(request3);
        assertEquals("test-retry", request1.getHeader("X-LoomQ-Intent-Id"));
        assertEquals("test-retry", request2.getHeader("X-LoomQ-Intent-Id"));
        assertEquals("test-retry", request3.getHeader("X-LoomQ-Intent-Id"));
    }

    @Test
    void testMaxRetriesExceeded() throws Exception {
        // 全部返回失败
        mockWebServer.enqueue(new MockResponse().setResponseCode(500));
        mockWebServer.enqueue(new MockResponse().setResponseCode(503));
        mockWebServer.enqueue(new MockResponse().setResponseCode(502));

        Intent intent = createTestIntent("test-max-retry", baseUrl);

        handler.onIntentEvent(intent, EventType.DUE, null);

        // 等待重试完成
        Thread.sleep(1000);

        // 应该有 3 次请求（初始 + 2 次重试）
        assertEquals(3, mockWebServer.getRequestCount());
    }

    @Test
    void testNullCallbackConfig() {
        // 创建没有回调配置的 Intent
        Intent intent = new Intent("test-no-callback");
        intent.setExecuteAt(Instant.now().plus(Duration.ofMinutes(1)));
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        // 不设置 callback

        // 应该不会抛出异常
        assertDoesNotThrow(() -> handler.onIntentEvent(intent, EventType.DUE, null));
    }

    @Test
    void testEmptyCallbackUrl() {
        Intent intent = new Intent("test-empty-url");
        intent.setExecuteAt(Instant.now().plus(Duration.ofMinutes(1)));
        intent.setPrecisionTier(PrecisionTier.STANDARD);

        Callback callback = new Callback();
        callback.setUrl(""); // 空 URL
        intent.setCallback(callback);

        // 应该不会抛出异常
        assertDoesNotThrow(() -> handler.onIntentEvent(intent, EventType.DUE, null));
    }

    @Test
    void testCustomHeaders() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200));

        Intent intent = createTestIntent("test-headers", baseUrl);
        intent.getCallback().setHeaders(Map.of(
            "X-Custom-Header", "custom-value",
            "Authorization", "Bearer token123"
        ));

        handler.onIntentEvent(intent, EventType.DUE, null);

        RecordedRequest request = mockWebServer.takeRequest(3, TimeUnit.SECONDS);
        assertNotNull(request);
        assertEquals("custom-value", request.getHeader("X-Custom-Header"));
        assertEquals("Bearer token123", request.getHeader("Authorization"));
    }

    @Test
    void testPutMethod() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200));

        Intent intent = createTestIntent("test-put", baseUrl);
        intent.getCallback().setMethod("PUT");

        handler.onIntentEvent(intent, EventType.DUE, null);

        RecordedRequest request = mockWebServer.takeRequest(3, TimeUnit.SECONDS);
        assertNotNull(request);
        assertEquals("PUT", request.getMethod());
    }

    @Test
    void testPatchMethod() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200));

        Intent intent = createTestIntent("test-patch", baseUrl);
        intent.getCallback().setMethod("PATCH");

        handler.onIntentEvent(intent, EventType.DUE, null);

        RecordedRequest request = mockWebServer.takeRequest(3, TimeUnit.SECONDS);
        assertNotNull(request);
        assertEquals("PATCH", request.getMethod());
    }

    @Test
    void testPayloadInBody() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200));

        Intent intent = createTestIntent("test-payload", baseUrl);
        intent.getCallback().setBody(Map.of("orderId", "12345", "amount", 99.99));

        handler.onIntentEvent(intent, EventType.DUE, null);

        RecordedRequest request = mockWebServer.takeRequest(3, TimeUnit.SECONDS);
        assertNotNull(request);

        String body = request.getBody().readUtf8();
        assertTrue(body.contains("\"payload\""));
        assertTrue(body.contains("\"orderId\""));
        assertTrue(body.contains("12345"));
    }

    @Test
    void testAsyncDelivery() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200));

        Intent intent = createTestIntent("test-async", baseUrl);

        var future = handler.deliverCallbackAsync(intent, EventType.DUE, null);

        // 等待完成
        Boolean result = future.get(5, TimeUnit.SECONDS);
        assertTrue(result);

        RecordedRequest request = mockWebServer.takeRequest(1, TimeUnit.SECONDS);
        assertNotNull(request);
    }

    // ========== Helper Methods ==========

    private Intent createTestIntent(String intentId, String callbackUrl) {
        Intent intent = new Intent(intentId);
        intent.setExecuteAt(Instant.now().plus(Duration.ofMinutes(1)));
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);

        Callback callback = new Callback(callbackUrl);
        callback.setMethod("POST");
        intent.setCallback(callback);

        return intent;
    }
}

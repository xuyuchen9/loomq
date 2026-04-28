package com.loomq.callback;

import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.spi.DeliveryContext;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.RedeliveryDecider;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.netty.http.client.HttpClient;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NettyHttpDeliveryHandlerTest {

    private MockWebServer mockServer;
    private NettyHttpDeliveryHandler handler;
    private RedeliveryDecider decider;

    @BeforeEach
    void setUp() throws IOException {
        mockServer = new MockWebServer();
        mockServer.start();
        // Default decider: retry on server errors, connections errors, timeouts
        decider = new RedeliveryDecider() {
            @Override
            public boolean shouldRedeliver(DeliveryContext context) {
                if (context.isSuccess()) return false;
                if (context.isTimeout() || context.isConnectionError()) return true;
                if (context.isServerError()) return true;
                if (context.isClientError()) return false;
                if (context.hasException()) return true;
                return false;
            }

            @Override
            public String getName() {
                return "test-decider";
            }
        };
        HttpClient client = HttpClient.create()
            .responseTimeout(Duration.ofSeconds(5));
        handler = new NettyHttpDeliveryHandler(client, decider);
    }

    @AfterEach
    void tearDown() throws IOException {
        mockServer.shutdown();
    }

    private Intent intentWithUrl(String id, String url) {
        Intent intent = new Intent(id);
        intent.transitionTo(IntentStatus.SCHEDULED);
        Callback callback = new Callback();
        callback.setUrl(url);
        intent.setCallback(callback);
        return intent;
    }

    private Intent intentWithUrlAndHeaders(String id, String url, Map<String, String> headers) {
        Intent intent = intentWithUrl(id, url);
        intent.getCallback().setHeaders(headers);
        return intent;
    }

    // ========== Success ==========

    @Test
    void shouldReturnSuccessOn200() throws Exception {
        mockServer.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));
        Intent intent = intentWithUrl("test-200", mockServer.url("/webhook").toString());

        DeliveryResult result = handler.deliverAsync(intent).get(5, TimeUnit.SECONDS);
        assertEquals(DeliveryResult.SUCCESS, result);
    }

    @Test
    void shouldReturnSuccessOn201() throws Exception {
        mockServer.enqueue(new MockResponse().setResponseCode(201).setBody("Created"));
        Intent intent = intentWithUrl("test-201", mockServer.url("/webhook").toString());

        DeliveryResult result = handler.deliverAsync(intent).get(5, TimeUnit.SECONDS);
        assertEquals(DeliveryResult.SUCCESS, result);
    }

    // ========== Server Error (5xx) → RETRY ==========

    @Test
    void shouldReturnRetryOn500() throws Exception {
        mockServer.enqueue(new MockResponse().setResponseCode(500).setBody("Error"));
        Intent intent = intentWithUrl("test-500", mockServer.url("/webhook").toString());

        DeliveryResult result = handler.deliverAsync(intent).get(5, TimeUnit.SECONDS);
        assertEquals(DeliveryResult.RETRY, result);
    }

    @Test
    void shouldReturnRetryOn503() throws Exception {
        mockServer.enqueue(new MockResponse().setResponseCode(503).setBody("Unavailable"));
        Intent intent = intentWithUrl("test-503", mockServer.url("/webhook").toString());

        DeliveryResult result = handler.deliverAsync(intent).get(5, TimeUnit.SECONDS);
        assertEquals(DeliveryResult.RETRY, result);
    }

    // ========== Client Error (4xx) → DEAD_LETTER ==========

    @Test
    void shouldReturnDeadLetterOn400() throws Exception {
        mockServer.enqueue(new MockResponse().setResponseCode(400).setBody("Bad Request"));
        Intent intent = intentWithUrl("test-400", mockServer.url("/webhook").toString());

        DeliveryResult result = handler.deliverAsync(intent).get(5, TimeUnit.SECONDS);
        assertEquals(DeliveryResult.DEAD_LETTER, result);
    }

    @Test
    void shouldReturnDeadLetterOn404() throws Exception {
        mockServer.enqueue(new MockResponse().setResponseCode(404).setBody("Not Found"));
        Intent intent = intentWithUrl("test-404", mockServer.url("/webhook").toString());

        DeliveryResult result = handler.deliverAsync(intent).get(5, TimeUnit.SECONDS);
        assertEquals(DeliveryResult.DEAD_LETTER, result);
    }

    // ========== Null/Blank URL ==========

    @Test
    void shouldReturnDeadLetterForNullUrl() throws Exception {
        Intent intent = new Intent("test-null-url");
        intent.transitionTo(IntentStatus.SCHEDULED);
        // no callback set → url is null

        DeliveryResult result = handler.deliverAsync(intent).get(5, TimeUnit.SECONDS);
        assertEquals(DeliveryResult.DEAD_LETTER, result);
    }

    @Test
    void shouldReturnDeadLetterForBlankUrl() throws Exception {
        Intent intent = intentWithUrl("test-blank", "");

        DeliveryResult result = handler.deliverAsync(intent).get(5, TimeUnit.SECONDS);
        assertEquals(DeliveryResult.DEAD_LETTER, result);
    }

    // ========== Connection Refused → RETRY ==========

    @Test
    void shouldReturnRetryOnConnectionRefused() throws Exception {
        // Use a port that nothing is listening on
        Intent intent = intentWithUrl("test-conn-refused", "http://127.0.0.1:19998/webhook");

        DeliveryResult result = handler.deliverAsync(intent).get(10, TimeUnit.SECONDS);
        // onErrorResume should catch and return RETRY (decider says connectionError → retry)
        assertEquals(DeliveryResult.RETRY, result);
    }

    // ========== Custom Headers ==========

    @Test
    void shouldSendHeaders() throws Exception {
        mockServer.enqueue(new MockResponse().setResponseCode(200));
        Intent intent = intentWithUrlAndHeaders("test-headers", mockServer.url("/webhook").toString(),
            Map.of("X-Custom", "value1", "Authorization", "Bearer token"));

        handler.deliverAsync(intent).get(5, TimeUnit.SECONDS);

        var request = mockServer.takeRequest();
        assertEquals("value1", request.getHeader("X-Custom"));
        assertEquals("Bearer token", request.getHeader("Authorization"));
        assertEquals("application/json", request.getHeader("Content-Type"));
    }

    @Test
    void shouldSendIntentIdHeader() throws Exception {
        mockServer.enqueue(new MockResponse().setResponseCode(200));
        Intent intent = intentWithUrl("test-id-header", mockServer.url("/webhook").toString());

        handler.deliverAsync(intent).get(5, TimeUnit.SECONDS);

        var request = mockServer.takeRequest();
        assertEquals("test-id-header", request.getHeader("X-LoomQ-Intent-Id"));
    }

    @Test
    void shouldSendPayloadAsJson() throws Exception {
        mockServer.enqueue(new MockResponse().setResponseCode(200));
        Intent intent = intentWithUrl("test-payload", mockServer.url("/webhook").toString());

        handler.deliverAsync(intent).get(5, TimeUnit.SECONDS);

        var request = mockServer.takeRequest();
        String body = request.getBody().readUtf8();
        assertEquals("{\"intentId\":\"test-payload\",\"precisionTier\":\"STANDARD\"}", body);
    }

    // ========== RedeliveryDecider overrides success ==========

    @Test
    void shouldHonorDeciderOverridingSuccess() throws Exception {
        mockServer.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));

        // Custom decider that always says retry
        RedeliveryDecider alwaysRetry = new RedeliveryDecider() {
            @Override
            public boolean shouldRedeliver(DeliveryContext context) {
                return true;
            }

            @Override
            public String getName() {
                return "always-retry";
            }
        };
        HttpClient client = HttpClient.create().responseTimeout(Duration.ofSeconds(5));
        NettyHttpDeliveryHandler customHandler = new NettyHttpDeliveryHandler(client, alwaysRetry);

        Intent intent = intentWithUrl("test-override", mockServer.url("/webhook").toString());
        DeliveryResult result = customHandler.deliverAsync(intent).get(5, TimeUnit.SECONDS);
        assertEquals(DeliveryResult.RETRY, result);
    }
}

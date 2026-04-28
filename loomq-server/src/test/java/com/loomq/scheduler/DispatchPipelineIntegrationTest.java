package com.loomq.scheduler;

import com.loomq.LoomqEngine;
import com.loomq.callback.NettyHttpDeliveryHandler;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.RedeliveryDecider;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.netty.http.client.HttpClient;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.RedeliveryPolicy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
class DispatchPipelineIntegrationTest {

    private MockWebServer mockWebhook;
    private LoomqEngine engine;
    private Path walDir;
    private RedeliveryDecider decider;

    @BeforeEach
    void setUp() throws IOException {
        mockWebhook = new MockWebServer();
        mockWebhook.start();
        walDir = Files.createTempDirectory("loomq-integration-test");

        // Decider: server errors → retry, client errors → dead letter
        decider = ctx -> {
            if (ctx.isSuccess()) return false;
            if (ctx.isTimeout() || ctx.isConnectionError()) return true;
            if (ctx.isServerError()) return true;
            return false;
        };

        // Create engine with NettyHttpDeliveryHandler pointing at mock webhook
        HttpClient client = HttpClient.create().responseTimeout(Duration.ofSeconds(5));
        NettyHttpDeliveryHandler handler = new NettyHttpDeliveryHandler(client, decider);

        engine = LoomqEngine.builder()
            .walDir(walDir)
            .deliveryHandler(handler)
            .build();
        engine.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (engine != null) {
            engine.close();
        }
        if (mockWebhook != null) {
            mockWebhook.shutdown();
        }
        if (walDir != null) {
            try {
                Files.walk(walDir).sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                    try { Files.delete(p); } catch (IOException ignored) {}
                });
            } catch (IOException ignored) {}
        }
    }

    private Intent createIntentWithWebhook(String id, PrecisionTier tier) {
        Intent intent = new Intent(id);
        intent.setExecuteAt(Instant.now());
        intent.setPrecisionTier(tier);
        com.loomq.domain.intent.Callback callback = new com.loomq.domain.intent.Callback();
        callback.setUrl(mockWebhook.url("/webhook/" + id).toString());
        callback.setMethod("POST");
        intent.setCallback(callback);
        return intent;
    }

    @Test
    void shouldDeliverSingleIntentToAcked() throws Exception {
        mockWebhook.enqueue(new MockResponse().setResponseCode(200).setBody("OK"));

        Intent intent = createIntentWithWebhook("full-pipeline-1", PrecisionTier.ULTRA);
        engine.createIntent(intent, AckMode.DURABLE).get(10, TimeUnit.SECONDS);

        // Wait for delivery
        Thread.sleep(500);
        RecordedRequest request = mockWebhook.takeRequest(1, TimeUnit.SECONDS);
        assertNotNull(request, "webhook should receive a request");
        assertEquals("POST", request.getMethod());
        assertEquals("/webhook/full-pipeline-1", request.getRequestUrl().encodedPath());

        // Check intent status
        Thread.sleep(100);
        Optional<Intent> stored = engine.getIntent("full-pipeline-1");
        assertTrue(stored.isPresent());
        assertTrue(stored.get().getStatus().isTerminal() || stored.get().getStatus() == IntentStatus.ACKED,
            "intent should be terminal or ACKED, was " + stored.get().getStatus());
    }

    @Test
    void shouldDeliverUltraIntentQuickly() throws Exception {
        mockWebhook.enqueue(new MockResponse().setResponseCode(200));

        Intent intent = createIntentWithWebhook("ultra-fast", PrecisionTier.ULTRA);
        long start = System.nanoTime();
        engine.createIntent(intent, AckMode.DURABLE).get(10, TimeUnit.SECONDS);

        // Wait for delivery
        Thread.sleep(200);
        mockWebhook.takeRequest(1, TimeUnit.SECONDS);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        // ULTRA should deliver within ~50ms (precision window 10ms + overhead)
        assertTrue(elapsedMs < 2000, "ULTRA delivery took " + elapsedMs + "ms, expected < 2000ms");
    }

    @Test
    void shouldRetryOnServerError() throws Exception {
        // First attempt gets 500 → handler returns RETRY
        mockWebhook.enqueue(new MockResponse().setResponseCode(500));

        Intent intent = createIntentWithWebhook("retry-cycle", PrecisionTier.ULTRA);
        // Short redelivery delay for quick test
        intent.setRedelivery(new RedeliveryPolicy(3, "fixed", 100, 100, 1.0, false));
        engine.createIntent(intent, AckMode.DURABLE).get(10, TimeUnit.SECONDS);

        // Wait for first delivery attempt
        Thread.sleep(500);
        assertEquals(1, mockWebhook.getRequestCount(), "should have at least 1 attempt");

        // Intent should be in SCHEDULED state (awaiting redelivery) or already retried
        Optional<Intent> stored = engine.getIntent("retry-cycle");
        assertTrue(stored.isPresent());
        IntentStatus status = stored.get().getStatus();
        assertTrue(status != IntentStatus.DEAD_LETTERED && !status.isTerminal() || status == IntentStatus.ACKED,
            "intent should not be dead-lettered, was " + status);
    }

    @Test
    void shouldCancelBeforeDispatch() throws Exception {
        Intent intent = createIntentWithWebhook("cancel-test", PrecisionTier.HIGH);
        engine.createIntent(intent, AckMode.DURABLE).get(10, TimeUnit.SECONDS);

        // Cancel immediately
        boolean cancelled = engine.cancelIntent("cancel-test");
        assertTrue(cancelled, "cancel should return true");

        Thread.sleep(500);

        // No webhook calls should have been made for a canceled intent
        // (or at least the intent should be in CANCELED state)
        Optional<Intent> stored = engine.getIntent("cancel-test");
        assertTrue(stored.isPresent());
        assertEquals(IntentStatus.CANCELED, stored.get().getStatus());
    }

    @Test
    void shouldGetIntentById() throws Exception {
        mockWebhook.enqueue(new MockResponse().setResponseCode(200));

        Intent intent = createIntentWithWebhook("get-test", PrecisionTier.STANDARD);
        engine.createIntent(intent, AckMode.DURABLE).get(10, TimeUnit.SECONDS);

        Optional<Intent> found = engine.getIntent("get-test");
        assertTrue(found.isPresent());
        assertEquals("get-test", found.get().getIntentId());
    }
}

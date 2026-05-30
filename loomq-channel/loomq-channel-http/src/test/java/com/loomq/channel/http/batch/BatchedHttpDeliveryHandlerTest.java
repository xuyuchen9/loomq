package com.loomq.channel.http.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("fast")
class BatchedHttpDeliveryHandlerTest {

    private static final Logger logger = LoggerFactory.getLogger(BatchedHttpDeliveryHandlerTest.class);

    @Test
    void testBatchDeliveryConfig() {
        BatchDeliveryConfig config = new BatchDeliveryConfig(100, 5, 200, true, 10000);
        assertEquals(100, config.maxBatchSize());
        assertEquals(5, config.flushIntervalMs());
        assertEquals(200, config.maxConnections());
        assertEquals(true, config.tcpNoDelay());
        assertEquals(10000, config.batchTimeoutMs());
    }

    @Test
    void testBatchDeliveryConfigValidation() {
        // 验证参数校验
        try {
            new BatchDeliveryConfig(0, 5, 200, true, 10000);
            // Should throw
        } catch (IllegalArgumentException e) {
            assertEquals("maxBatchSize must be positive", e.getMessage());
        }

        try {
            new BatchDeliveryConfig(100, -1, 200, true, 10000);
            // Should throw
        } catch (IllegalArgumentException e) {
            assertEquals("flushIntervalMs must be positive", e.getMessage());
        }
    }

    @Test
    void testDefaultConfig() {
        BatchDeliveryConfig config = BatchDeliveryConfig.DEFAULT;
        assertEquals(200, config.maxBatchSize());
        assertEquals(10, config.flushIntervalMs());
        assertEquals(500, config.maxConnections());
        assertEquals(true, config.tcpNoDelay());
        assertEquals(30000, config.batchTimeoutMs());
    }

    @Test
    void testLowLatencyConfig() {
        BatchDeliveryConfig config = BatchDeliveryConfig.LOW_LATENCY;
        assertEquals(50, config.maxBatchSize());
        assertEquals(2, config.flushIntervalMs());
        assertEquals(500, config.maxConnections());
        assertEquals(true, config.tcpNoDelay());
        assertEquals(10000, config.batchTimeoutMs());
    }

    @Test
    void testDeadLetterOnMissingUrl() throws Exception {
        BatchDeliveryConfig config = new BatchDeliveryConfig(10, 5, 10, true, 1000);
        BatchedHttpDeliveryHandler handler = new BatchedHttpDeliveryHandler(config);

        Intent intent = new Intent("test-1");
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        // No callback URL set

        CompletableFuture<DeliveryResult> future = handler.deliverAsync(intent);
        DeliveryResult result = future.get(1, TimeUnit.SECONDS);
        assertEquals(DeliveryResult.DEAD_LETTER, result);

        handler.shutdown();
    }

    @Test
    void testDeadLetterOnBlankUrl() throws Exception {
        BatchDeliveryConfig config = new BatchDeliveryConfig(10, 5, 10, true, 1000);
        BatchedHttpDeliveryHandler handler = new BatchedHttpDeliveryHandler(config);

        Intent intent = new Intent("test-2");
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        Callback callback = new Callback();
        callback.setUrl("   ");
        intent.setCallback(callback);

        CompletableFuture<DeliveryResult> future = handler.deliverAsync(intent);
        DeliveryResult result = future.get(1, TimeUnit.SECONDS);
        assertEquals(DeliveryResult.DEAD_LETTER, result);

        handler.shutdown();
    }

    @Test
    void testBatchWebhookHandler() {
        List<BatchWebhookHandler.IntentEvent> received = new ArrayList<>();
        BatchWebhookHandler handler = new BatchWebhookHandler(received::add);

        // 测试批量请求
        String batchRequest = """
            {"intents":[
              {"intentId":"id-1","precisionTier":"ULTRA"},
              {"intentId":"id-2","precisionTier":"FAST"},
              {"intentId":"id-3","precisionTier":"HIGH"}
            ]}
            """;

        String response = handler.handleRequest(batchRequest);
        assertNotNull(response);

        // 验证接收到的 intent
        assertEquals(3, received.size());
        assertEquals("id-1", received.get(0).intentId());
        assertEquals("ULTRA", received.get(0).precisionTier());
        assertEquals("id-2", received.get(1).intentId());
        assertEquals("FAST", received.get(1).precisionTier());
        assertEquals("id-3", received.get(2).intentId());
        assertEquals("HIGH", received.get(2).precisionTier());

        logger.info("Batch response: {}", response);
    }

    @Test
    void testBatchWebhookHandlerSingleIntent() {
        List<BatchWebhookHandler.IntentEvent> received = new ArrayList<>();
        BatchWebhookHandler handler = new BatchWebhookHandler(received::add);

        // 测试单个 intent 请求（向后兼容）
        String singleRequest = """
            {"intentId":"single-1","precisionTier":"STANDARD"}
            """;

        String response = handler.handleRequest(singleRequest);
        assertNotNull(response);

        assertEquals(1, received.size());
        assertEquals("single-1", received.get(0).intentId());
        assertEquals("STANDARD", received.get(0).precisionTier());
    }

    @Test
    void testBatchWebhookHandlerError() {
        BatchWebhookHandler handler = new BatchWebhookHandler(event -> {
            throw new RuntimeException("Processing failed");
        });

        String request = """
            {"intents":[{"intentId":"fail-1","precisionTier":"ULTRA"}]}
            """;

        String response = handler.handleRequest(request);
        assertNotNull(response);
        // 应该包含 FAILED 状态
        assertEquals(true, response.contains("FAILED"));
    }

    @Test
    void testBatchWebhookHandlerEmptyBody() {
        List<BatchWebhookHandler.IntentEvent> received = new ArrayList<>();
        BatchWebhookHandler handler = new BatchWebhookHandler(received::add);

        String response = handler.handleRequest("");
        assertNotNull(response);
        assertEquals(true, response.contains("error"));
        assertEquals(0, received.size());
    }

    @Test
    void testBatchDeliveryMetrics() {
        BatchedHttpDeliveryHandler.BatchDeliveryMetrics metrics =
            new BatchedHttpDeliveryHandler.BatchDeliveryMetrics(
                10, 1500, 5.5, 2.3, 7, 3
            );

        assertEquals(10, metrics.totalBatches());
        assertEquals(1500, metrics.totalIntentsBatched());
        assertEquals(5.5, metrics.avgBatchDwellMs(), 0.01);
        assertEquals(2.3, metrics.avgHttpRttMs(), 0.01);
        assertEquals(7, metrics.flushByFullBatch());
        assertEquals(3, metrics.flushByTimer());
        assertEquals(150.0, metrics.avgBatchSize(), 0.01);
        assertEquals(70.0, metrics.fullBatchPercentage(), 0.01);
    }

    private Intent createTestIntent(String id, PrecisionTier tier, String url) {
        Intent intent = new Intent(id);
        intent.setPrecisionTier(tier);
        intent.setExecuteAt(Instant.now());
        intent.transitionTo(IntentStatus.SCHEDULED);
        if (url != null) {
            Callback callback = new Callback();
            callback.setUrl(url);
            intent.setCallback(callback);
        }
        return intent;
    }
}

package com.loomq.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.loomq.channel.grpc.server.DeliveryStreamRegistry;
import com.loomq.channel.grpc.server.GrpcStreamDeliveryHandler;
import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.grpc.gen.DeliveryAckMode;
import com.loomq.grpc.gen.DeliveryEvent;
import com.loomq.grpc.gen.WatchDeliveriesRequest;
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

/**
 * gRPC 流投递处理器单元测试。
 *
 * @author loomq
 * @since v0.9.2
 */
@Tag("fast")
class GrpcStreamDeliveryTest {

    private static final Logger logger = LoggerFactory.getLogger(GrpcStreamDeliveryTest.class);

    @Test
    void testAutoAckDelivery() throws Exception {
        logger.info("=== AUTO_ACK 投递测试 ===");

        DeliveryStreamRegistry registry = new DeliveryStreamRegistry();
        GrpcStreamDeliveryHandler handler = new GrpcStreamDeliveryHandler(
            DeliveryAckMode.AUTO_ACK, registry);

        // 模拟 Gateway 注册流
        MockStreamObserver observer = new MockStreamObserver();
        WatchDeliveriesRequest request = WatchDeliveriesRequest.newBuilder()
            .setAckMode(DeliveryAckMode.AUTO_ACK)
            .build();

        var entry = registry.register(request, observer);

        // 创建 Intent
        Intent intent = createTestIntent("test-001", PrecisionTier.ULTRA);

        // 投递
        CompletableFuture<DeliveryResult> future = handler.deliverAsync(intent);
        DeliveryResult result = future.get(5, TimeUnit.SECONDS);

        // 验证
        assertEquals(DeliveryResult.SUCCESS, result);
        assertEquals(1, observer.getEvents().size());

        DeliveryEvent event = observer.getEvents().get(0);
        assertEquals("test-001", event.getIntentId());
        assertEquals("ULTRA", event.getPrecisionTier());
        assertEquals(1, event.getAttempt());

        // 清理
        registry.unregister(entry);
        handler.close();

        logger.info("AUTO_ACK 测试通过");
    }

    @Test
    void testDeadLetterOnNoStreams() throws Exception {
        logger.info("=== 无流时降级测试 ===");

        DeliveryStreamRegistry registry = new DeliveryStreamRegistry();
        GrpcStreamDeliveryHandler handler = new GrpcStreamDeliveryHandler(
            DeliveryAckMode.AUTO_ACK, registry);

        // 创建 Intent（无活跃流）
        Intent intent = createTestIntent("test-002", PrecisionTier.ULTRA);

        // 投递
        CompletableFuture<DeliveryResult> future = handler.deliverAsync(intent);
        DeliveryResult result = future.get(5, TimeUnit.SECONDS);

        // 验证降级为 DEAD_LETTER
        assertEquals(DeliveryResult.DEAD_LETTER, result);
        assertEquals(1, handler.getNoStreamCount());

        handler.close();
        logger.info("无流降级测试通过");
    }

    @Test
    void testTierFiltering() throws Exception {
        logger.info("=== 档位过滤测试 ===");

        DeliveryStreamRegistry registry = new DeliveryStreamRegistry();
        GrpcStreamDeliveryHandler handler = new GrpcStreamDeliveryHandler(
            DeliveryAckMode.AUTO_ACK, registry);

        // 注册只接收 ULTRA 档位的流
        MockStreamObserver ultraObserver = new MockStreamObserver();
        WatchDeliveriesRequest ultraRequest = WatchDeliveriesRequest.newBuilder()
            .addPrecisionTiers("ULTRA")
            .setAckMode(DeliveryAckMode.AUTO_ACK)
            .build();
        var ultraEntry = registry.register(ultraRequest, ultraObserver);

        // 注册只接收 FAST 档位的流
        MockStreamObserver fastObserver = new MockStreamObserver();
        WatchDeliveriesRequest fastRequest = WatchDeliveriesRequest.newBuilder()
            .addPrecisionTiers("FAST")
            .setAckMode(DeliveryAckMode.AUTO_ACK)
            .build();
        var fastEntry = registry.register(fastRequest, fastObserver);

        // 投递 ULTRA Intent
        Intent ultraIntent = createTestIntent("ultra-001", PrecisionTier.ULTRA);
        CompletableFuture<DeliveryResult> ultraFuture = handler.deliverAsync(ultraIntent);
        assertEquals(DeliveryResult.SUCCESS, ultraFuture.get(5, TimeUnit.SECONDS));

        // 投递 FAST Intent
        Intent fastIntent = createTestIntent("fast-001", PrecisionTier.FAST);
        CompletableFuture<DeliveryResult> fastFuture = handler.deliverAsync(fastIntent);
        assertEquals(DeliveryResult.SUCCESS, fastFuture.get(5, TimeUnit.SECONDS));

        // 验证过滤
        assertEquals(1, ultraObserver.getEvents().size());
        assertEquals("ultra-001", ultraObserver.getEvents().get(0).getIntentId());

        assertEquals(1, fastObserver.getEvents().size());
        assertEquals("fast-001", fastObserver.getEvents().get(0).getIntentId());

        // 清理
        registry.unregister(ultraEntry);
        registry.unregister(fastEntry);
        handler.close();

        logger.info("档位过滤测试通过");
    }

    @Test
    void testWildcardStream() throws Exception {
        logger.info("=== 全档位流测试 ===");

        DeliveryStreamRegistry registry = new DeliveryStreamRegistry();
        GrpcStreamDeliveryHandler handler = new GrpcStreamDeliveryHandler(
            DeliveryAckMode.AUTO_ACK, registry);

        // 注册全档位流
        MockStreamObserver observer = new MockStreamObserver();
        WatchDeliveriesRequest request = WatchDeliveriesRequest.newBuilder()
            .setAckMode(DeliveryAckMode.AUTO_ACK)
            .build();
        var entry = registry.register(request, observer);

        // 投递不同档位的 Intent
        for (PrecisionTier tier : PrecisionTier.values()) {
            Intent intent = createTestIntent("test-" + tier.name(), tier);
            CompletableFuture<DeliveryResult> future = handler.deliverAsync(intent);
            assertEquals(DeliveryResult.SUCCESS, future.get(5, TimeUnit.SECONDS));
        }

        // 验证所有档位都被接收
        assertEquals(5, observer.getEvents().size());

        // 清理
        registry.unregister(entry);
        handler.close();

        logger.info("全档位流测试通过");
    }

    private Intent createTestIntent(String intentId, PrecisionTier tier) {
        Intent intent = new Intent(intentId);
        intent.setPrecisionTier(tier);
        intent.setExecuteAt(Instant.now());
        intent.transitionTo(IntentStatus.SCHEDULED);
        Callback callback = new Callback();
        callback.setUrl("http://localhost:8080/webhook");
        intent.setCallback(callback);
        return intent;
    }

    /**
     * 模拟 ServerCallStreamObserver（用于单元测试）。
     */
    private static class MockStreamObserver extends io.grpc.stub.ServerCallStreamObserver<DeliveryEvent> {
        private final List<DeliveryEvent> events = new ArrayList<>();
        private boolean ready = true;

        List<DeliveryEvent> getEvents() {
            return events;
        }

        @Override
        public void onNext(DeliveryEvent value) {
            events.add(value);
        }

        @Override
        public void onError(Throwable t) {
            // ignore
        }

        @Override
        public void onCompleted() {
            // ignore
        }

        @Override
        public boolean isReady() {
            return ready;
        }

        @Override
        public void setOnReadyHandler(Runnable onReadyHandler) {
            // ignore
        }

        @Override
        public void disableAutoInboundFlowControl() {
            // ignore
        }

        @Override
        public void request(int count) {
            // ignore
        }

        @Override
        public void setMessageCompression(boolean enable) {
            // ignore
        }

        @Override
        public void setOnCancelHandler(Runnable onCancelHandler) {
            // ignore
        }

        @Override
        public void setOnCloseHandler(Runnable onCloseHandler) {
            // ignore
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void setCompression(String compression) {
            // ignore
        }
    }
}

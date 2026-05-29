package com.loomq.channel.grpc.server;

import com.loomq.channel.grpc.converter.ProtoConverter;
import com.loomq.domain.intent.Intent;
import com.loomq.grpc.gen.DeliveryAckMode;
import com.loomq.grpc.gen.DeliveryEvent;
import com.loomq.spi.DeliveryHandler;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC 流投递处理器。
 *
 * <p>将 Intent 通过持久 gRPC 流推送到 Gateway，替代 HTTP Webhook 投递。
 * 支持 AUTO_ACK（推送到流即成功）和 MANUAL_ACK（等待 Gateway 显式确认）两种模式。
 *
 * <p>当无活跃的 Gateway 连接时，降级为 DEAD_LETTER。
 *
 * @author loomq
 * @since v0.9.2
 */
public class GrpcStreamDeliveryHandler implements DeliveryHandler {

    private static final Logger logger = LoggerFactory.getLogger(GrpcStreamDeliveryHandler.class);

    private final DeliveryStreamRegistry registry;
    private final DeliveryAckMode defaultAckMode;

    // 异步 dispatch 线程池
    private final ExecutorService dispatchExecutor;

    // 指标
    private final AtomicInteger totalDeliveries = new AtomicInteger(0);
    private final AtomicInteger deliveredCount = new AtomicInteger(0);
    private final AtomicInteger deadLetterCount = new AtomicInteger(0);
    private final AtomicInteger noStreamCount = new AtomicInteger(0);

    public GrpcStreamDeliveryHandler() {
        this(DeliveryAckMode.AUTO_ACK);
    }

    public GrpcStreamDeliveryHandler(DeliveryAckMode defaultAckMode) {
        this(defaultAckMode, new DeliveryStreamRegistry());
    }

    public GrpcStreamDeliveryHandler(DeliveryAckMode defaultAckMode, DeliveryStreamRegistry registry) {
        this.defaultAckMode = defaultAckMode;
        this.registry = registry;
        this.dispatchExecutor = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(),
            r -> {
                Thread t = new Thread(r, "grpc-delivery-dispatch");
                t.setDaemon(true);
                return t;
            }
        );

        logger.info("GrpcStreamDeliveryHandler initialized: defaultAckMode={}", defaultAckMode);
    }

    /**
     * 获取流注册表（用于 LoomqGrpcService 注册/注销流）。
     */
    public DeliveryStreamRegistry getRegistry() {
        return registry;
    }

    @Override
    public CompletableFuture<DeliveryResult> deliverAsync(Intent intent) {
        totalDeliveries.incrementAndGet();

        // 1. 查找匹配的活跃流
        List<DeliveryStreamRegistry.DeliveryStreamEntry> streams =
            registry.findStreams(intent.getPrecisionTier());

        if (streams.isEmpty()) {
            noStreamCount.incrementAndGet();
            logger.debug("No active delivery stream for tier {}, dead-lettering intent {}",
                intent.getPrecisionTier(), intent.getIntentId());
            return CompletableFuture.completedFuture(DeliveryResult.DEAD_LETTER);
        }

        // 2. 构建 DeliveryEvent
        DeliveryEvent event = ProtoConverter.toDeliveryEvent(intent);
        CompletableFuture<DeliveryResult> result = new CompletableFuture<>();

        // 3. 异步推送到所有匹配的流
        dispatchExecutor.submit(() -> {
            try {
                for (DeliveryStreamRegistry.DeliveryStreamEntry stream : streams) {
                    if (stream.ackMode() == DeliveryAckMode.AUTO_ACK) {
                        // AUTO_ACK: 推送即成功
                        stream.deliver(event);
                        result.complete(DeliveryResult.SUCCESS);
                        deliveredCount.incrementAndGet();
                    } else {
                        // MANUAL_ACK: 注册 pending，等待 ACK
                        registry.registerPending(event.getDeliveryId(), result);
                        stream.deliver(event);
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to deliver intent {} via gRPC stream: {}",
                    intent.getIntentId(), e.getMessage());
                result.complete(DeliveryResult.RETRY);
            }
        });

        return result;
    }

    /**
     * 关闭投递处理器。
     */
    public void close() {
        dispatchExecutor.shutdown();
        registry.close();

        logger.info("GrpcStreamDeliveryHandler closed: total={}, delivered={}, deadLetter={}, noStream={}",
            totalDeliveries.get(), deliveredCount.get(), deadLetterCount.get(), noStreamCount.get());
    }

    // ── 指标 ──

    public int getTotalDeliveries() {
        return totalDeliveries.get();
    }

    public int getDeliveredCount() {
        return deliveredCount.get();
    }

    public int getDeadLetterCount() {
        return deadLetterCount.get();
    }

    public int getNoStreamCount() {
        return noStreamCount.get();
    }

    public int getActiveStreamCount() {
        return registry.getActiveStreamCount();
    }

    public int getPendingCount() {
        return registry.getPendingCount();
    }
}

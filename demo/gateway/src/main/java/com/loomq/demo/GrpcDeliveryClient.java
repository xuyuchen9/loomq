package com.loomq.demo;

import com.loomq.grpc.gen.DeliveryAckMode;
import com.loomq.grpc.gen.DeliveryEvent;
import com.loomq.grpc.gen.LoomQServiceGrpc;
import com.loomq.grpc.gen.WatchDeliveriesRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC 投递流客户端。
 *
 * <p>连接 LoomQ Server 的 gRPC 服务，订阅投递流，
 * 收到 DeliveryEvent 后回调给 Gateway。
 *
 * @author loomq
 * @since v0.9.2
 */
public class GrpcDeliveryClient {

    private static final Logger logger = LoggerFactory.getLogger(GrpcDeliveryClient.class);

    private final String host;
    private final int port;
    private ManagedChannel channel;
    private LoomQServiceGrpc.LoomQServiceStub stub;

    public GrpcDeliveryClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * 连接 LoomQ Server 并订阅投递流。
     *
     * @param onDelivery 收到投递事件时的回调
     */
    public void connect(Consumer<DeliveryEvent> onDelivery) {
        logger.info("Connecting to LoomQ gRPC server at {}:{}", host, port);

        channel = ManagedChannelBuilder
            .forAddress(host, port)
            .usePlaintext()
            .build();

        stub = LoomQServiceGrpc.newStub(channel);

        // 订阅投递流（AUTO_ACK 模式）
        WatchDeliveriesRequest request = WatchDeliveriesRequest.newBuilder()
            .setAckMode(DeliveryAckMode.AUTO_ACK)
            .build();

        stub.watchDeliveries(request, new StreamObserver<DeliveryEvent>() {
            @Override
            public void onNext(DeliveryEvent event) {
                logger.debug("Received delivery event: {} (tier={})",
                    event.getIntentId(), event.getPrecisionTier());
                onDelivery.accept(event);
            }

            @Override
            public void onError(Throwable t) {
                logger.error("Delivery stream error: {}", t.getMessage());
                // 重连逻辑
                reconnect(onDelivery);
            }

            @Override
            public void onCompleted() {
                logger.info("Delivery stream completed");
            }
        });

        logger.info("Connected to LoomQ gRPC server, watching deliveries...");
    }

    /**
     * 重新连接。
     */
    private void reconnect(Consumer<DeliveryEvent> onDelivery) {
        logger.info("Reconnecting in 5 seconds...");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        if (channel != null) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        connect(onDelivery);
    }

    /**
     * 关闭连接。
     */
    public void close() {
        logger.info("Closing gRPC delivery client");
        if (channel != null) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

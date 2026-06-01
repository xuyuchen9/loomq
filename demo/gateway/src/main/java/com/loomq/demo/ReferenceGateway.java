package com.loomq.demo;

import com.loomq.grpc.gen.DeliveryEvent;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 参考 Gateway 实现。
 *
 * <p>展示完整的投递链路：LoomQ → gRPC 流 → Gateway → WebSocket → 浏览器
 *
 * <p>使用方式：
 * <pre>{@code
 * # 1. 启动 LoomQ Server（gRPC 流投递模式）
 * java -Dloomq.delivery.handler=grpc-stream -Dloomq.grpc.enabled=true -jar loomq-server.jar
 *
 * # 2. 启动 ReferenceGateway
 * cd demo/gateway
 * mvn exec:java
 *
 * # 3. 打开浏览器
 * open http://localhost:8080
 *
 * # 4. 通过 API 创建 Intent
 * curl -X POST http://localhost:7928/v1/intents \
 *   -H "Content-Type: application/json" \
 *   -d '{"intentId":"demo-001","executeAt":"2026-05-29T12:00:00Z","precisionTier":"ULTRA","callback":{"url":"http://localhost:8080/webhook"}}'
 * }</pre>
 *
 * @author loomq
 * @since v0.9.2
 */
public class ReferenceGateway {

    private static final Logger logger = LoggerFactory.getLogger(ReferenceGateway.class);

    // 配置
    private static final String DEFAULT_LOOMQ_HOST = "localhost";
    private static final int DEFAULT_GRPC_PORT = 8928;
    private static final int DEFAULT_WS_PORT = 8080;

    private final GrpcDeliveryClient grpcClient;
    private final WebSocketServer wsServer;
    private final AtomicLong eventCount = new AtomicLong(0);

    public ReferenceGateway(String loomqHost, int grpcPort, int wsPort) {
        this.grpcClient = new GrpcDeliveryClient(loomqHost, grpcPort);
        this.wsServer = new WebSocketServer(wsPort);
    }

    /**
     * 启动 Gateway。
     */
    public void start() throws InterruptedException {
        // 1. 启动 WebSocket 服务器
        wsServer.start();

        // 2. 连接 LoomQ gRPC 服务
        grpcClient.connect(this::handleDeliveryEvent);

        logger.info("ReferenceGateway started");
        logger.info("  WebSocket: ws://localhost:{}", DEFAULT_WS_PORT);
        logger.info("  Status: http://localhost:{}/status", DEFAULT_WS_PORT);
    }

    /**
     * 处理投递事件。
     */
    private void handleDeliveryEvent(DeliveryEvent event) {
        long count = eventCount.incrementAndGet();

        // 转换为 JSON
        try {
            String json = toJson(event);

            // 广播到 WebSocket 客户端
            wsServer.broadcast(json);

            if (count % 100 == 0) {
                logger.info("Processed {} delivery events, active connections: {}",
                    count, wsServer.getActiveConnections());
            }
        } catch (Exception e) {
            logger.error("Failed to process delivery event: {}", e.getMessage());
        }
    }

    /**
     * 将 DeliveryEvent 转换为 JSON 字符串。
     */
    private String toJson(DeliveryEvent event) {
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"deliveryId\":\"").append(escapeJson(event.getDeliveryId())).append("\"");
        sb.append(",\"intentId\":\"").append(escapeJson(event.getIntentId())).append("\"");
        sb.append(",\"precisionTier\":\"").append(escapeJson(event.getPrecisionTier())).append("\"");
        sb.append(",\"attempt\":").append(event.getAttempt());

        if (event.hasExecuteAt()) {
            long seconds = event.getExecuteAt().getSeconds();
            int nanos = event.getExecuteAt().getNanos();
            sb.append(",\"executeAt\":{\"seconds\":").append(seconds);
            sb.append(",\"nanos\":").append(nanos).append("}");
        }

        sb.append("}");
        return sb.toString();
    }

    private String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * 关闭 Gateway。
     */
    public void stop() {
        logger.info("Stopping ReferenceGateway");
        grpcClient.close();
        wsServer.stop();
        logger.info("ReferenceGateway stopped, total events: {}", eventCount.get());
    }

    /**
     * 主入口。
     */
    public static void main(String[] args) throws Exception {
        String loomqHost = System.getenv("LOOMQ_HOST");
        if (loomqHost == null || loomqHost.isBlank()) {
            loomqHost = DEFAULT_LOOMQ_HOST;
        }

        int grpcPort = DEFAULT_GRPC_PORT;
        String grpcPortStr = System.getenv("LOOMQ_GRPC_PORT");
        if (grpcPortStr != null && !grpcPortStr.isBlank()) {
            grpcPort = Integer.parseInt(grpcPortStr);
        }

        int wsPort = DEFAULT_WS_PORT;
        String wsPortStr = System.getenv("GATEWAY_WS_PORT");
        if (wsPortStr != null && !wsPortStr.isBlank()) {
            wsPort = Integer.parseInt(wsPortStr);
        }

        printBanner(loomqHost, grpcPort, wsPort);

        ReferenceGateway gateway = new ReferenceGateway(loomqHost, grpcPort, wsPort);

        // 关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            gateway.stop();
        }));

        // 启动
        gateway.start();

        // 阻塞主线程
        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }

    private static void printBanner(String loomqHost, int grpcPort, int wsPort) {
        System.out.println();
        System.out.println("╔════════════════════════════════════════════════════════════╗");
        System.out.println("║              LoomQ Reference Gateway                       ║");
        System.out.println("║  LoomQ → gRPC Stream → Gateway → WebSocket → Browser     ║");
        System.out.println("╚════════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("  LoomQ gRPC: " + loomqHost + ":" + grpcPort);
        System.out.println("  WebSocket:  ws://localhost:" + wsPort + "/ws");
        System.out.println("  Status:     http://localhost:" + wsPort + "/status");
        System.out.println();
    }
}

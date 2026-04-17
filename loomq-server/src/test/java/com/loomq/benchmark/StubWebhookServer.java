package com.loomq.benchmark;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地 Stub Webhook Server
 * 用于测试系统真实调度能力，消除外部依赖影响
 */
public class StubWebhookServer {

    private static final AtomicLong requestCount = new AtomicLong(0);
    private static final AtomicLong totalLatency = new AtomicLong(0);
    private static final AtomicInteger delayMs = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("用法: StubWebhookServer <port> [delayMs]");
            System.out.println("  port    - 监听端口");
            System.out.println("  delayMs - 每个请求延迟(ms)，默认0");
            return;
        }

        int port = Integer.parseInt(args[0]);
        if (args.length > 1) {
            delayMs.set(Integer.parseInt(args[1]));
        }

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 10000);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

        // 主 webhook 端点
        server.createContext("/webhook", new WebhookHandler());

        // 健康检查
        server.createContext("/health", exchange -> {
            String response = "OK";
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        });

        // 统计端点
        server.createContext("/stats", exchange -> {
            long count = requestCount.get();
            long latency = totalLatency.get();
            double avg = count > 0 ? (double) latency / count : 0;
            String response = String.format(
                "{\"requests\":%d,\"avgLatencyMs\":%.1f,\"delayMs\":%d}",
                count, avg, delayMs.get()
            );
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        });

        // 重置统计
        server.createContext("/reset", exchange -> {
            requestCount.set(0);
            totalLatency.set(0);
            String response = "Reset";
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        });

        server.start();

        System.out.println("========================================");
        System.out.printf("Stub Webhook Server 启动%n");
        System.out.printf("端口: %d%n", port);
        System.out.printf("延迟: %d ms%n", delayMs.get());
        System.out.println("========================================");
        System.out.println("端点:");
        System.out.printf("  POST http://localhost:%d/webhook%n", port);
        System.out.printf("  GET  http://localhost:%d/health%n", port);
        System.out.printf("  GET  http://localhost:%d/stats%n", port);
        System.out.printf("  GET  http://localhost:%d/reset%n", port);
        System.out.println("========================================");
    }

    static class WebhookHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            long start = System.currentTimeMillis();

            // 模拟延迟
            int delay = delayMs.get();
            if (delay > 0) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            // 读取请求体（模拟真实处理）
            byte[] body = exchange.getRequestBody().readAllBytes();

            // 返回响应
            String response = "{\"status\":\"ok\",\"timestamp\":" + System.currentTimeMillis() + "}";
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length());

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }

            long elapsed = System.currentTimeMillis() - start;
            requestCount.incrementAndGet();
            totalLatency.addAndGet(elapsed);
        }
    }
}

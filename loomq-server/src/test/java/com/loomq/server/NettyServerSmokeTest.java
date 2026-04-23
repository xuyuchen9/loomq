package com.loomq.server;

import com.loomq.LoomqEngine;
import com.loomq.callback.HttpDeliveryHandler;
import com.loomq.config.ServerConfig;
import com.loomq.http.netty.IntentHandler;
import com.loomq.http.netty.NettyHttpServer;
import com.loomq.http.netty.RadixRouter;
import io.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NettyServerSmokeTest {

    @Test
    @Timeout(30)
    @DisplayName("Netty standalone server should serve health and JSON intent APIs")
    void nettySmokeTest() throws Exception {
        Path walDir = Files.createTempDirectory("loomq-netty-smoke");
        LoomqEngine engine = LoomqEngine.builder()
            .nodeId("smoke-node")
            .walDir(walDir)
            .deliveryHandler(new HttpDeliveryHandler())
            .build();

        NettyHttpServer server = null;

        try {
            engine.start();

            RadixRouter router = new RadixRouter();
            new IntentHandler(engine).register(router);
            router.add(HttpMethod.GET, "/health", (method, uri, body, headers, pathParams) -> java.util.Map.of("status", "UP"));

            server = new NettyHttpServer(testConfig(), router);
            server.start();

            HttpClient client = HttpClient.newHttpClient();

            HttpResponse<String> health = client.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/health"))
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(200, health.statusCode());
            assertTrue(health.body().contains("\"status\":\"UP\""));

            String intentId = "smoke-" + System.nanoTime();
            Instant executeAt = Instant.now().plusSeconds(5);
            String payload = """
                {
                  "intentId":"%s",
                  "executeAt":"%s",
                  "deadline":"%s",
                  "precisionTier":"STANDARD",
                  "shardKey":"orders"
                }
                """.formatted(intentId, executeAt, executeAt.plusSeconds(60));

            HttpResponse<String> created = client.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/v1/intents"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(201, created.statusCode());
            assertTrue(created.body().contains(intentId));

            HttpResponse<String> fetched = client.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/v1/intents/" + intentId))
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(200, fetched.statusCode());
            assertTrue(fetched.body().contains(intentId));
            assertTrue(fetched.body().contains("\"status\":\"SCHEDULED\""));
        } finally {
            if (server != null) {
                server.stop();
            }
            try {
                engine.close();
            } catch (Exception ignored) {
            }
            try {
                Files.walk(walDir)
                    .sorted((a, b) -> b.getNameCount() - a.getNameCount())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (Exception ignored) {
                        }
                    });
            } catch (Exception ignored) {
            }
        }
    }

    private ServerConfig testConfig() {
        Properties props = new Properties();
        props.setProperty("host", "127.0.0.1");
        props.setProperty("port", "0");
        props.setProperty("bossThreads", "1");
        props.setProperty("workerThreads", "0");
        props.setProperty("maxContentLength", String.valueOf(10 * 1024 * 1024));
        props.setProperty("useEpoll", "false");
        props.setProperty("pooledAllocator", "true");
        props.setProperty("soBacklog", "1024");
        props.setProperty("tcpNoDelay", "true");
        props.setProperty("connectionTimeoutMs", "30000");
        props.setProperty("idleTimeoutSeconds", "60");
        props.setProperty("maxConnections", "1000");
        props.setProperty("writeBufferHighWaterMark", String.valueOf(1024 * 1024));
        props.setProperty("writeBufferLowWaterMark", String.valueOf(512 * 1024));
        props.setProperty("maxConcurrentBusinessRequests", "100");
        props.setProperty("gracefulShutdownTimeoutMs", "5000");
        props.setProperty("server.host", "127.0.0.1");
        props.setProperty("server.port", "0");
        props.setProperty("server.backlog", "1024");
        props.setProperty("server.virtual_threads", "true");
        props.setProperty("server.max_request_size", String.valueOf(10 * 1024 * 1024));
        props.setProperty("server.thread_pool_size", "200");
        return ServerConfig.fromProperties(props);
    }
}

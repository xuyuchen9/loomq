package com.loomq.v5;

import com.loomq.LoomqEngine;
import com.loomq.api.CreateIntentRequest;
import com.loomq.entity.v5.Callback;
import com.loomq.entity.v5.Intent;
import com.loomq.entity.v5.RedeliveryPolicy;
import com.loomq.replication.AckLevel;
import com.loomq.store.IntentStore;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LoomQ v0.5 集成测试
 *
 * 验证 Intent API、幂等性、ACK 级别等核心功能
 *
 * @author loomq
 * @since v0.5.0
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class V5IntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(V5IntegrationTest.class);
    private static int port;
    private static String baseUrl;

    private static LoomqEngine engine;
    private static Path tempDir;

    @BeforeAll
    static void setUp() throws Exception {
        // 获取可用端口
        port = findAvailablePort();
        baseUrl = "http://localhost:" + port;

        // 创建临时数据目录
        tempDir = Files.createTempDirectory("loomq-v5-test");
        logger.info("Test data directory: {}", tempDir);

        // 启动引擎
        engine = new LoomqEngine("test-node", "test-shard", tempDir.toString(), port);
        engine.start();

        // 等待服务就绪
        Thread.sleep(1000);

        // 配置 Unirest
        Unirest.config().reset();
        Unirest.config()
            .socketTimeout(5000)
            .connectTimeout(5000);

        logger.info("LoomQ v0.5 engine started on port {}", port);
    }

    /**
     * 查找可用端口
     */
    static int findAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (Exception e) {
            throw new RuntimeException("Failed to find available port", e);
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        logger.info("Stopping engine...");
        if (engine != null) {
            engine.stop();
        }
        // 清理临时目录
        if (tempDir != null) {
            deleteDirectory(tempDir.toFile());
        }
    }

    static void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
    }

    @Test
    @Order(1)
    @DisplayName("FT-01: 创建 DURABLE Intent")
    void testCreateDurableIntent() {
        String intentId = "test-durable-" + UUID.randomUUID().toString().substring(0, 8);
        Instant executeAt = Instant.now().plus(60, ChronoUnit.SECONDS);
        Instant deadline = executeAt.plus(300, ChronoUnit.SECONDS);

        String json = String.format("""
            {
                "intentId": "%s",
                "executeAt": "%s",
                "deadline": "%s",
                "shardKey": "test-shard",
                "ackLevel": "DURABLE",
                "callback": {
                    "url": "https://example.com/webhook",
                    "method": "POST"
                }
            }
            """, intentId, executeAt.toString(), deadline.toString());

        HttpResponse<JsonNode> response = Unirest.post(baseUrl + "/v1/intents")
            .header("Content-Type", "application/json")
            .body(json)
            .asJson();

        assertEquals(201, response.getStatus());
        assertEquals(intentId, response.getBody().getObject().getString("intentId"));
        assertEquals("SCHEDULED", response.getBody().getObject().getString("status"));

        logger.info("✅ FT-01 PASSED: DURABLE Intent created");
    }

    @Test
    @Order(2)
    @DisplayName("FT-02: 创建 ASYNC Intent")
    void testCreateAsyncIntent() {
        String intentId = "test-async-" + UUID.randomUUID().toString().substring(0, 8);
        Instant executeAt = Instant.now().plus(30, ChronoUnit.SECONDS);
        Instant deadline = executeAt.plus(300, ChronoUnit.SECONDS);

        String json = String.format("""
            {
                "intentId": "%s",
                "executeAt": "%s",
                "deadline": "%s",
                "shardKey": "test-shard",
                "ackLevel": "ASYNC",
                "callback": {
                    "url": "https://example.com/webhook",
                    "method": "POST"
                }
            }
            """, intentId, executeAt.toString(), deadline.toString());

        long start = System.currentTimeMillis();
        HttpResponse<JsonNode> response = Unirest.post(baseUrl + "/v1/intents")
            .header("Content-Type", "application/json")
            .body(json)
            .asJson();
        long latency = System.currentTimeMillis() - start;

        assertEquals(201, response.getStatus());
        assertTrue(latency < 10, "ASYNC latency should be < 10ms, got " + latency + "ms");

        logger.info("✅ FT-02 PASSED: ASYNC Intent created in {}ms", latency);
    }

    @Test
    @Order(3)
    @DisplayName("FT-04: 查询 Intent")
    void testGetIntent() {
        // 先创建一个 Intent
        String intentId = "test-get-" + UUID.randomUUID().toString().substring(0, 8);
        Instant executeAt = Instant.now().plus(60, ChronoUnit.SECONDS);
        Instant deadline = executeAt.plus(300, ChronoUnit.SECONDS);

        String json = String.format("""
            {
                "intentId": "%s",
                "executeAt": "%s",
                "deadline": "%s",
                "shardKey": "test-shard",
                "ackLevel": "DURABLE",
                "callback": {
                    "url": "https://example.com/webhook",
                    "method": "POST"
                }
            }
            """, intentId, executeAt.toString(), deadline.toString());

        Unirest.post(baseUrl + "/v1/intents")
            .header("Content-Type", "application/json")
            .body(json)
            .asJson();

        // 查询
        HttpResponse<JsonNode> response = Unirest.get(baseUrl + "/v1/intents/" + intentId)
            .asJson();

        assertEquals(200, response.getStatus());
        assertEquals(intentId, response.getBody().getObject().getString("intentId"));
        assertEquals("SCHEDULED", response.getBody().getObject().getString("status"));

        logger.info("✅ FT-04 PASSED: Intent queried successfully");
    }

    @Test
    @Order(4)
    @DisplayName("FT-05: 取消 Intent")
    void testCancelIntent() {
        // 先创建一个 Intent
        String intentId = "test-cancel-" + UUID.randomUUID().toString().substring(0, 8);
        Instant executeAt = Instant.now().plus(60, ChronoUnit.SECONDS);
        Instant deadline = executeAt.plus(300, ChronoUnit.SECONDS);

        String json = String.format("""
            {
                "intentId": "%s",
                "executeAt": "%s",
                "deadline": "%s",
                "shardKey": "test-shard",
                "ackLevel": "DURABLE",
                "callback": {
                    "url": "https://example.com/webhook",
                    "method": "POST"
                }
            }
            """, intentId, executeAt.toString(), deadline.toString());

        Unirest.post(baseUrl + "/v1/intents")
            .header("Content-Type", "application/json")
            .body(json)
            .asJson();

        // 取消
        HttpResponse<JsonNode> response = Unirest.post(baseUrl + "/v1/intents/" + intentId + "/cancel")
            .asJson();

        assertEquals(200, response.getStatus());
        assertEquals("CANCELED", response.getBody().getObject().getString("status"));

        logger.info("✅ FT-05 PASSED: Intent cancelled successfully");
    }

    @Test
    @Order(5)
    @DisplayName("FT-08: 幂等性 - 重复创建返回已存在")
    void testIdempotencyDuplicateActive() {
        String idempotencyKey = "idem-test-" + UUID.randomUUID().toString().substring(0, 8);
        Instant executeAt = Instant.now().plus(60, ChronoUnit.SECONDS);
        Instant deadline = executeAt.plus(300, ChronoUnit.SECONDS);

        String json = String.format("""
            {
                "intentId": "intent-1-%s",
                "executeAt": "%s",
                "deadline": "%s",
                "shardKey": "test-shard",
                "ackLevel": "DURABLE",
                "idempotencyKey": "%s",
                "callback": {
                    "url": "https://example.com/webhook",
                    "method": "POST"
                }
            }
            """, idempotencyKey, executeAt.toString(), deadline.toString(), idempotencyKey);

        // 第一次创建
        HttpResponse<JsonNode> response1 = Unirest.post(baseUrl + "/v1/intents")
            .header("Content-Type", "application/json")
            .body(json)
            .asJson();

        assertEquals(201, response1.getStatus());
        String firstIntentId = response1.getBody().getObject().getString("intentId");

        // 第二次创建（相同幂等键）
        HttpResponse<JsonNode> response2 = Unirest.post(baseUrl + "/v1/intents")
            .header("Content-Type", "application/json")
            .body(json)
            .asJson();

        // 应该返回 200 和已存在的 Intent
        assertEquals(200, response2.getStatus());
        assertEquals(firstIntentId, response2.getBody().getObject().getString("intentId"));

        logger.info("✅ FT-08 PASSED: Idempotency works correctly");
    }

    @Test
    @Order(6)
    @DisplayName("Health check endpoints")
    void testHealthEndpoints() {
        HttpResponse<String> live = Unirest.get(baseUrl + "/health/live").asString();
        assertEquals(200, live.getStatus());

        HttpResponse<String> ready = Unirest.get(baseUrl + "/health/ready").asString();
        assertEquals(200, ready.getStatus());

        HttpResponse<JsonNode> health = Unirest.get(baseUrl + "/health").asJson();
        assertEquals(200, health.getStatus());
        assertEquals("UP", health.getBody().getObject().getString("status"));

        logger.info("✅ Health checks PASSED");
    }

    @Test
    @Order(7)
    @DisplayName("Metrics endpoint")
    void testMetricsEndpoint() {
        HttpResponse<String> response = Unirest.get(baseUrl + "/metrics").asString();
        assertEquals(200, response.getStatus());
        assertTrue(response.getBody().contains("loomq_intents_created_total"));

        logger.info("✅ Metrics endpoint PASSED");
    }
}

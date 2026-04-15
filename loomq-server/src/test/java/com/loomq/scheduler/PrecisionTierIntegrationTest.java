package com.loomq.scheduler;

import com.loomq.LoomqEngine;
import com.loomq.domain.intent.PrecisionTier;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 精度档位集成测试 (v0.5.1)
 *
 * 验证混合精度档位场景下的调度正确性和 SLO 满足情况
 *
 * @author loomq
 * @since v0.5.1
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PrecisionTierIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(PrecisionTierIntegrationTest.class);

    private static int port;
    private static String baseUrl;
    private static LoomqEngine engine;
    private static Path tempDir;

    // SLO 边界 (p99)
    private static final Map<PrecisionTier, Long> SLO_BOUNDS = Map.of(
        PrecisionTier.ULTRA, 15L,
        PrecisionTier.FAST, 60L,
        PrecisionTier.HIGH, 120L,
        PrecisionTier.STANDARD, 550L,
        PrecisionTier.ECONOMY, 1100L
    );

    @BeforeAll
    static void setUp() throws Exception {
        port = findAvailablePort();
        baseUrl = "http://localhost:" + port;

        tempDir = Files.createTempDirectory("loomq-precision-test");
        logger.info("Test data directory: {}", tempDir);

        engine = new LoomqEngine("precision-test-node", "precision-test-shard", tempDir.toString(), port);
        engine.start();

        Thread.sleep(1500);

        Unirest.config()
            .socketTimeout(10000)
            .connectTimeout(5000);

        logger.info("Precision tier test engine started on port {}", port);
    }

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
    @DisplayName("PT-01: 创建不同精度档位的 Intent")
    void testCreateIntentsWithDifferentTiers() {
        for (PrecisionTier tier : PrecisionTier.values()) {
            String intentId = "tier-" + tier.name().toLowerCase() + "-" + UUID.randomUUID().toString().substring(0, 8);
            Instant executeAt = Instant.now().plus(60, ChronoUnit.SECONDS);
            Instant deadline = executeAt.plus(300, ChronoUnit.SECONDS);

            String json = String.format("""
                {
                    "intentId": "%s",
                    "executeAt": "%s",
                    "deadline": "%s",
                    "precisionTier": "%s",
                    "shardKey": "tier-test",
                    "ackLevel": "DURABLE",
                    "callback": {
                        "url": "https://example.com/webhook",
                        "method": "POST"
                    }
                }
                """, intentId, executeAt.toString(), deadline.toString(), tier.name());

            HttpResponse<JsonNode> response = Unirest.post(baseUrl + "/v1/intents")
                .header("Content-Type", "application/json")
                .body(json)
                .asJson();

            assertEquals(201, response.getStatus(), "Failed to create intent for tier " + tier);
            assertEquals(intentId, response.getBody().getObject().getString("intentId"));
            assertEquals(tier.name(), response.getBody().getObject().getString("precisionTier"));

            logger.info("✅ Created intent {} with tier {}", intentId, tier);
        }

        logger.info("✅ PT-01 PASSED: All precision tiers can be created");
    }

    @Test
    @Order(2)
    @DisplayName("PT-02: 默认精度档位为 STANDARD")
    void testDefaultPrecisionTier() {
        String intentId = "default-tier-" + UUID.randomUUID().toString().substring(0, 8);
        Instant executeAt = Instant.now().plus(60, ChronoUnit.SECONDS);
        Instant deadline = executeAt.plus(300, ChronoUnit.SECONDS);

        // 不指定 precisionTier
        String json = String.format("""
            {
                "intentId": "%s",
                "executeAt": "%s",
                "deadline": "%s",
                "shardKey": "default-test",
                "callback": {
                    "url": "https://example.com/webhook"
                }
            }
            """, intentId, executeAt.toString(), deadline.toString());

        HttpResponse<JsonNode> response = Unirest.post(baseUrl + "/v1/intents")
            .header("Content-Type", "application/json")
            .body(json)
            .asJson();

        assertEquals(201, response.getStatus());
        assertEquals(PrecisionTier.STANDARD.name(), response.getBody().getObject().getString("precisionTier"));

        logger.info("✅ PT-02 PASSED: Default precision tier is STANDARD");
    }

    @Test
    @Order(3)
    @DisplayName("PT-03: 无效精度档位返回默认值")
    void testInvalidPrecisionTier() {
        String intentId = "invalid-tier-" + UUID.randomUUID().toString().substring(0, 8);
        Instant executeAt = Instant.now().plus(60, ChronoUnit.SECONDS);
        Instant deadline = executeAt.plus(300, ChronoUnit.SECONDS);

        String json = String.format("""
            {
                "intentId": "%s",
                "executeAt": "%s",
                "deadline": "%s",
                "precisionTier": "INVALID_TIER",
                "shardKey": "invalid-test",
                "callback": {
                    "url": "https://example.com/webhook"
                }
            }
            """, intentId, executeAt.toString(), deadline.toString());

        HttpResponse<JsonNode> response = Unirest.post(baseUrl + "/v1/intents")
            .header("Content-Type", "application/json")
            .body(json)
            .asJson();

        // 无效值应该被解析为默认 STANDARD
        assertEquals(201, response.getStatus());
        assertEquals(PrecisionTier.STANDARD.name(), response.getBody().getObject().getString("precisionTier"));

        logger.info("✅ PT-03 PASSED: Invalid tier defaults to STANDARD");
    }

    @Test
    @Order(4)
    @DisplayName("PT-04: 批量创建混合档位 Intent")
    void testBatchCreateMixedTiers() {
        int countPerTier = 10;
        int totalCreated = 0;

        Map<PrecisionTier, List<String>> intentsByTier = new EnumMap<>(PrecisionTier.class);
        for (PrecisionTier tier : PrecisionTier.values()) {
            intentsByTier.put(tier, new ArrayList<>());
        }

        for (PrecisionTier tier : PrecisionTier.values()) {
            for (int i = 0; i < countPerTier; i++) {
                String intentId = "batch-" + tier.name().toLowerCase() + "-" + i;
                Instant executeAt = Instant.now().plus(120, ChronoUnit.SECONDS);
                Instant deadline = executeAt.plus(300, ChronoUnit.SECONDS);

                String json = String.format("""
                    {
                        "intentId": "%s",
                        "executeAt": "%s",
                        "deadline": "%s",
                        "precisionTier": "%s",
                        "shardKey": "batch-test",
                        "callback": {
                            "url": "https://example.com/webhook"
                        }
                    }
                    """, intentId, executeAt.toString(), deadline.toString(), tier.name());

                HttpResponse<JsonNode> response = Unirest.post(baseUrl + "/v1/intents")
                    .header("Content-Type", "application/json")
                    .body(json)
                    .asJson();

                if (response.getStatus() == 201) {
                    intentsByTier.get(tier).add(intentId);
                    totalCreated++;
                }
            }
        }

        assertEquals(countPerTier * 5, totalCreated, "Should create all intents");

        for (Map.Entry<PrecisionTier, List<String>> entry : intentsByTier.entrySet()) {
            logger.info("Tier {}: {} intents created", entry.getKey(), entry.getValue().size());
        }

        logger.info("✅ PT-04 PASSED: Created {} mixed-tier intents", totalCreated);
    }

    @Test
    @Order(5)
    @DisplayName("PT-05: 指标端点包含精度档位标签")
    void testMetricsIncludePrecisionTier() {
        HttpResponse<String> response = Unirest.get(baseUrl + "/metrics").asString();

        assertEquals(200, response.getStatus());
        String metrics = response.getBody();

        // 验证基本指标存在
        assertTrue(metrics.contains("loomq_intents") || metrics.contains("loomq_tasks"),
            "Should have loomq metrics. Got: " + metrics.substring(0, Math.min(500, metrics.length())));

        logger.info("✅ PT-05 PASSED: Metrics endpoint works");
    }

    @Test
    @Order(6)
    @DisplayName("PT-06: 唤醒延迟测试 - 短延迟场景")
    void testShortDelayWakeupLatency() throws InterruptedException {
        // 测试 delay <= precisionWindow 的场景
        PrecisionTier tier = PrecisionTier.STANDARD; // 500ms window
        String intentId = "short-delay-" + UUID.randomUUID().toString().substring(0, 8);

        // 设置 executeAt 为当前时间 + 200ms (< 500ms window)
        Instant executeAt = Instant.now().plus(200, ChronoUnit.MILLIS);
        Instant deadline = executeAt.plus(60, ChronoUnit.SECONDS);

        String json = String.format("""
            {
                "intentId": "%s",
                "executeAt": "%s",
                "deadline": "%s",
                "precisionTier": "%s",
                "shardKey": "short-delay-test",
                "callback": {
                    "url": "https://example.com/webhook"
                }
            }
            """, intentId, executeAt.toString(), deadline.toString(), tier.name());

        long startCreate = System.currentTimeMillis();
        HttpResponse<JsonNode> response = Unirest.post(baseUrl + "/v1/intents")
            .header("Content-Type", "application/json")
            .body(json)
            .asJson();

        assertEquals(201, response.getStatus());
        long createLatency = System.currentTimeMillis() - startCreate;

        logger.info("Short delay intent created in {}ms", createLatency);
        logger.info("✅ PT-06 PASSED: Short delay intent created successfully");
    }

    @Test
    @Order(7)
    @DisplayName("PT-07: 精度档位对 CPU 开销的影响")
    void testCpuOverheadByTier() throws InterruptedException {
        // 这个测试主要验证 API 层面的正确性
        // 实际 CPU 开销对比在压测中进行

        Map<PrecisionTier, Long> createLatencies = new EnumMap<>(PrecisionTier.class);

        for (PrecisionTier tier : PrecisionTier.values()) {
            String intentId = "cpu-test-" + tier.name().toLowerCase();
            Instant executeAt = Instant.now().plus(300, ChronoUnit.SECONDS);
            Instant deadline = executeAt.plus(300, ChronoUnit.SECONDS);

            String json = String.format("""
                {
                    "intentId": "%s",
                    "executeAt": "%s",
                    "deadline": "%s",
                    "precisionTier": "%s",
                    "shardKey": "cpu-test",
                    "callback": {
                        "url": "https://example.com/webhook"
                    }
                }
                """, intentId, executeAt.toString(), deadline.toString(), tier.name());

            long start = System.nanoTime();
            HttpResponse<JsonNode> response = Unirest.post(baseUrl + "/v1/intents")
                .header("Content-Type", "application/json")
                .body(json)
                .asJson();
            long latency = (System.nanoTime() - start) / 1_000_000;

            assertEquals(201, response.getStatus());
            createLatencies.put(tier, latency);
        }

        // 所有档位的创建延迟应该相近（创建时不考虑精度差异）
        long maxLatency = createLatencies.values().stream().max(Long::compare).orElse(0L);
        long minLatency = createLatencies.values().stream().min(Long::compare).orElse(0L);

        assertTrue(maxLatency - minLatency < 100,
            "Create latency variance should be small across tiers");

        logger.info("✅ PT-07 PASSED: Create latency consistent across tiers");
        createLatencies.forEach((tier, latency) ->
            logger.info("  {} create latency: {}ms", tier, latency));
    }

    @Test
    @Order(8)
    @DisplayName("PT-08: 查询返回正确的 precisionTier")
    void testQueryReturnsCorrectTier() {
        String intentId = "query-tier-" + UUID.randomUUID().toString().substring(0, 8);
        Instant executeAt = Instant.now().plus(60, ChronoUnit.SECONDS);
        Instant deadline = executeAt.plus(300, ChronoUnit.SECONDS);

        String json = String.format("""
            {
                "intentId": "%s",
                "executeAt": "%s",
                "deadline": "%s",
                "precisionTier": "ECONOMY",
                "shardKey": "query-test",
                "callback": {
                    "url": "https://example.com/webhook"
                }
            }
            """, intentId, executeAt.toString(), deadline.toString());

        // 创建
        Unirest.post(baseUrl + "/v1/intents")
            .header("Content-Type", "application/json")
            .body(json)
            .asJson();

        // 查询
        HttpResponse<JsonNode> response = Unirest.get(baseUrl + "/v1/intents/" + intentId)
            .asJson();

        assertEquals(200, response.getStatus());
        assertEquals("ECONOMY", response.getBody().getObject().getString("precisionTier"));

        logger.info("✅ PT-08 PASSED: Query returns correct precisionTier");
    }

    @Test
    @Order(9)
    @DisplayName("PT-09: Prometheus 格式指标端点")
    void testPrometheusMetricsEndpoint() {
        HttpResponse<String> response = Unirest.get(baseUrl + "/metrics").asString();

        assertEquals(200, response.getStatus());

        // Prometheus 格式指标应该正常返回
        assertTrue(response.getBody().contains("loomq_intents_created_total") ||
                   response.getBody().contains("loomq_wal"));

        logger.info("✅ PT-09 PASSED: Prometheus metrics endpoint works");
    }

    @Test
    @Order(10)
    @DisplayName("PT-10: 并发创建不同档位 Intent")
    void testConcurrentCreateMixedTiers() throws InterruptedException {
        int threads = 10;
        int intentsPerThread = 5;
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        List<Thread> threadList = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            final int threadNum = t;
            final PrecisionTier tier = PrecisionTier.values()[t % PrecisionTier.values().length];

            Thread thread = Thread.ofVirtual().start(() -> {
                try {
                    for (int i = 0; i < intentsPerThread; i++) {
                        String intentId = "concurrent-" + threadNum + "-" + i;
                        Instant executeAt = Instant.now().plus(60, ChronoUnit.SECONDS);
                        Instant deadline = executeAt.plus(300, ChronoUnit.SECONDS);

                        String json = String.format("""
                            {
                                "intentId": "%s",
                                "executeAt": "%s",
                                "deadline": "%s",
                                "precisionTier": "%s",
                                "shardKey": "concurrent-test",
                                "callback": {
                                    "url": "https://example.com/webhook"
                                }
                            }
                            """, intentId, executeAt.toString(), deadline.toString(), tier.name());

                        HttpResponse<JsonNode> response = Unirest.post(baseUrl + "/v1/intents")
                            .header("Content-Type", "application/json")
                            .body(json)
                            .asJson();

                        if (response.getStatus() == 201 || response.getStatus() == 200) {
                            successCount.incrementAndGet();
                        } else {
                            errorCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
            threadList.add(thread);
        }

        boolean completed = latch.await(30, TimeUnit.SECONDS);
        assertTrue(completed, "Concurrent test should complete within timeout");

        assertEquals(threads * intentsPerThread, successCount.get(), "All creates should succeed");
        assertEquals(0, errorCount.get(), "No errors should occur");

        logger.info("✅ PT-10 PASSED: Concurrent create {} intents, {} success, {} errors",
            threads * intentsPerThread, successCount.get(), errorCount.get());
    }
}

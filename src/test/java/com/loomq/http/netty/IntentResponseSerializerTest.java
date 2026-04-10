package com.loomq.http.netty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.loomq.entity.v5.Intent;
import com.loomq.entity.v5.IntentStatus;
import com.loomq.entity.v5.PrecisionTier;
import com.loomq.replication.AckLevel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * IntentResponseSerializer 测试
 *
 * 验证手写序列化器输出与 Jackson 一致
 */
class IntentResponseSerializerTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Test
    @DisplayName("基本字段序列化应与 Jackson 一致")
    void testBasicSerialization() throws Exception {
        // Given
        Intent intent = new Intent("test-intent-123");
        intent.setExecuteAt(Instant.parse("2026-04-10T12:30:45.123456789Z"));
        intent.setDeadline(Instant.parse("2026-04-10T13:30:45Z"));
        intent.setShardKey("shard-01");
        intent.setPrecisionTier(PrecisionTier.HIGH);
        intent.setAckLevel(AckLevel.DURABLE);
        intent.transitionTo(IntentStatus.SCHEDULED);

        // When - 手写序列化器
        byte[] customBytes = IntentResponseSerializer.toBytes(intent);
        String customJson = new String(customBytes, StandardCharsets.UTF_8);

        // Then - 解析并验证字段
        @SuppressWarnings("unchecked")
        Map<String, Object> parsed = objectMapper.readValue(customJson, Map.class);

        assertEquals("test-intent-123", parsed.get("intentId"));
        assertEquals("SCHEDULED", parsed.get("status"));
        assertEquals("2026-04-10T12:30:45.123456789Z", parsed.get("executeAt"));
        assertEquals("2026-04-10T13:30:45Z", parsed.get("deadline"));
        assertEquals("shard-01", parsed.get("shardKey"));
        assertEquals("HIGH", parsed.get("precisionTier"));
        assertEquals("DURABLE", parsed.get("ackLevel"));
    }

    @Test
    @DisplayName("null 字段应正确处理")
    void testNullFields() throws Exception {
        // Given
        Intent intent = new Intent("test-null");
        intent.setExecuteAt(null);
        intent.setDeadline(null);
        intent.setShardKey(null);
        intent.setPrecisionTier(null);
        intent.setAckLevel(null);

        // When
        byte[] bytes = IntentResponseSerializer.toBytes(intent);
        String json = new String(bytes, StandardCharsets.UTF_8);

        // Debug: 打印生成的 JSON
        System.out.println("Generated JSON: " + json);

        // Then
        @SuppressWarnings("unchecked")
        Map<String, Object> parsed = objectMapper.readValue(json, Map.class);

        assertEquals("test-null", parsed.get("intentId"));
        assertEquals("", parsed.get("executeAt")); // null 值序列化为空字符串
        assertEquals("", parsed.get("deadline"));
        assertEquals("", parsed.get("shardKey"));
        assertEquals("STANDARD", parsed.get("precisionTier"));
        assertEquals("DURABLE", parsed.get("ackLevel"));
    }

    @Test
    @DisplayName("需要转义的字符串应正确处理")
    void testEscapedStrings() throws Exception {
        // Given
        Intent intent = new Intent("intent-with-\"quotes\"");
        intent.setShardKey("shard\\with\\backslash");
        intent.setExecuteAt(Instant.now());
        intent.setDeadline(Instant.now().plusSeconds(3600));

        // When
        byte[] bytes = IntentResponseSerializer.toBytes(intent);
        String json = new String(bytes, StandardCharsets.UTF_8);

        // Then - 应该是合法 JSON
        @SuppressWarnings("unchecked")
        Map<String, Object> parsed = objectMapper.readValue(json, Map.class);
        assertEquals("intent-with-\"quotes\"", parsed.get("intentId"));
        assertEquals("shard\\with\\backslash", parsed.get("shardKey"));
    }

    @Test
    @DisplayName("控制字符应正确转义")
    void testControlCharacters() throws Exception {
        // Given
        Intent intent = new Intent("intent\nwith\nnewlines");
        intent.setShardKey("shard\twith\ttabs");
        intent.setExecuteAt(Instant.now());
        intent.setDeadline(Instant.now().plusSeconds(3600));

        // When
        byte[] bytes = IntentResponseSerializer.toBytes(intent);
        String json = new String(bytes, StandardCharsets.UTF_8);

        // Then - 应该是合法 JSON
        @SuppressWarnings("unchecked")
        Map<String, Object> parsed = objectMapper.readValue(json, Map.class);
        assertEquals("intent\nwith\nnewlines", parsed.get("intentId"));
        assertEquals("shard\twith\ttabs", parsed.get("shardKey"));
    }

    @Test
    @DisplayName("直接写入 ByteBuf 应正常工作")
    void testWriteToByteBuf() {
        // Given
        Intent intent = new Intent("bytebuf-test");
        intent.setExecuteAt(Instant.now());
        intent.setDeadline(Instant.now().plusSeconds(3600));
        intent.setShardKey("shard-01");

        ByteBuf buf = Unpooled.buffer(256);

        try {
            // When
            IntentResponseSerializer.writeIntentResponse(intent, buf);

            // Then
            assertTrue(buf.readableBytes() > 0);

            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            String json = new String(bytes, StandardCharsets.UTF_8);

            assertTrue(json.contains("\"intentId\":\"bytebuf-test\""));
            assertTrue(json.contains("\"shardKey\":\"shard-01\""));
        } finally {
            buf.release();
        }
    }

    @Test
    @DisplayName("IntentResponseData 应正确实现 DirectSerializedResponse")
    void testIntentResponseData() {
        // Given
        Intent intent = new Intent("data-test");
        intent.setExecuteAt(Instant.parse("2026-04-10T10:00:00Z"));
        intent.setDeadline(Instant.parse("2026-04-10T11:00:00Z"));
        intent.setShardKey("shard-01");
        intent.setPrecisionTier(PrecisionTier.ULTRA);
        intent.setAckLevel(AckLevel.ASYNC);
        intent.transitionTo(IntentStatus.SCHEDULED);

        IntentResponseData data = new IntentResponseData(intent);

        // When
        ByteBuf buf = Unpooled.buffer(data.estimateSize());
        try {
            data.writeTo(buf);

            // Then
            assertTrue(buf.readableBytes() > 0);

            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            String json = new String(bytes, StandardCharsets.UTF_8);

            assertTrue(json.contains("\"intentId\":\"data-test\""));
            assertTrue(json.contains("\"status\":\"SCHEDULED\""));
            assertTrue(json.contains("\"precisionTier\":\"ULTRA\""));
            assertTrue(json.contains("\"ackLevel\":\"ASYNC\""));
        } finally {
            buf.release();
        }
    }

    @Test
    @DisplayName("所有枚举值应正确序列化")
    void testAllEnumValues() throws Exception {
        for (IntentStatus status : IntentStatus.values()) {
            for (PrecisionTier tier : PrecisionTier.values()) {
                for (AckLevel ackLevel : AckLevel.values()) {
                    Intent intent = new Intent("enum-test-" + status + "-" + tier + "-" + ackLevel);
                    intent.setExecuteAt(Instant.now());
                    intent.setDeadline(Instant.now().plusSeconds(3600));
                    intent.setShardKey("shard");
                    intent.setPrecisionTier(tier);
                    intent.setAckLevel(ackLevel);

                    byte[] bytes = IntentResponseSerializer.toBytes(intent);
                    String json = new String(bytes, StandardCharsets.UTF_8);

                    @SuppressWarnings("unchecked")
                    Map<String, Object> parsed = objectMapper.readValue(json, Map.class);

                    assertEquals(tier.name(), parsed.get("precisionTier"));
                    assertEquals(ackLevel.name(), parsed.get("ackLevel"));
                }
            }
        }
    }

    @Test
    @DisplayName("性能测试：对比手写与 Jackson")
    void testPerformance() {
        Intent intent = new Intent("perf-test");
        intent.setExecuteAt(Instant.now());
        intent.setDeadline(Instant.now().plusSeconds(3600));
        intent.setShardKey("shard-01");
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.setAckLevel(AckLevel.DURABLE);

        // Warmup
        for (int i = 0; i < 1000; i++) {
            IntentResponseSerializer.toBytes(intent);
        }

        // Custom serializer
        long startCustom = System.nanoTime();
        int iterations = 10000;
        for (int i = 0; i < iterations; i++) {
            IntentResponseSerializer.toBytes(intent);
        }
        long customTime = System.nanoTime() - startCustom;

        // Jackson
        IntentResponseData data = new IntentResponseData(intent);
        long startJackson = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            objectMapper.valueToTree(data);
        }
        long jacksonTime = System.nanoTime() - startJackson;

        System.out.println("Custom serializer: " + (customTime / iterations) + " ns/op");
        System.out.println("Jackson: " + (jacksonTime / iterations) + " ns/op");
        System.out.println("Speedup: " + (double) jacksonTime / customTime + "x");

        // 手写序列化器应该更快
        assertTrue(customTime < jacksonTime, "Custom serializer should be faster than Jackson");
    }
}

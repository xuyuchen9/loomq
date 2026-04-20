package com.loomq.http.netty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.loomq.api.IntentResponse;
import com.loomq.domain.intent.ExpiredAction;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.replication.AckLevel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IntentResponseSerializer 测试
 *
 * 验证手写序列化器输出与 Jackson 一致。
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
    @DisplayName("直接序列化输出应与 Jackson 一致")
    void testSerializationMatchesJackson() throws Exception {
        IntentResponse response = sampleResponse();

        byte[] customBytes = IntentResponseSerializer.toBytes(response);
        String customJson = new String(customBytes, StandardCharsets.UTF_8);
        String jacksonJson = objectMapper.writeValueAsString(response);

        assertEquals(jacksonJson, customJson);
    }

    @Test
    @DisplayName("null 字段应正确处理")
    void testNullFields() throws Exception {
        Intent intent = new Intent("test-null");
        intent.setExecuteAt(null);
        intent.setDeadline(null);
        intent.setExpiredAction(null);
        intent.setLastDeliveryId(null);

        IntentResponse response = IntentResponse.from(intent);
        String json = new String(IntentResponseSerializer.toBytes(response), StandardCharsets.UTF_8);

        @SuppressWarnings("unchecked")
        Map<String, Object> parsed = objectMapper.readValue(json, Map.class);

        assertEquals("test-null", parsed.get("intentId"));
        assertTrue(parsed.containsKey("executeAt"));
        assertTrue(parsed.containsKey("deadline"));
        assertTrue(parsed.containsKey("expiredAction"));
        assertTrue(parsed.containsKey("lastDeliveryId"));
        assertEquals(null, parsed.get("executeAt"));
        assertEquals(null, parsed.get("deadline"));
        assertEquals(null, parsed.get("expiredAction"));
        assertEquals(null, parsed.get("lastDeliveryId"));
    }

    @Test
    @DisplayName("需要转义的字符串应正确处理")
    void testEscapedStrings() throws Exception {
        Intent intent = new Intent("intent-with-\"quotes\"");
        intent.setExecuteAt(Instant.parse("2026-04-10T12:30:45Z"));
        intent.setDeadline(Instant.parse("2026-04-10T13:30:45Z"));
        intent.setLastDeliveryId("delivery\\with\\backslash");

        IntentResponse response = IntentResponse.from(intent);
        String json = new String(IntentResponseSerializer.toBytes(response), StandardCharsets.UTF_8);

        @SuppressWarnings("unchecked")
        Map<String, Object> parsed = objectMapper.readValue(json, Map.class);
        assertEquals("intent-with-\"quotes\"", parsed.get("intentId"));
        assertEquals("delivery\\with\\backslash", parsed.get("lastDeliveryId"));
    }

    @Test
    @DisplayName("直接写入 ByteBuf 应正常工作")
    void testWriteToByteBuf() {
        IntentResponse response = sampleResponse();

        ByteBuf buf = Unpooled.buffer(256);
        try {
            IntentResponseSerializer.writeIntentResponse(response, buf);

            assertTrue(buf.readableBytes() > 0);

            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            String json = new String(bytes, StandardCharsets.UTF_8);

            assertTrue(json.contains("\"intentId\":\"test-intent-123\""));
            assertTrue(json.contains("\"status\":\"SCHEDULED\""));
            assertTrue(json.contains("\"precisionTier\":\"HIGH\""));
            assertTrue(json.contains("\"ackLevel\":\"DURABLE\""));
        } finally {
            buf.release();
        }
    }

    private IntentResponse sampleResponse() {
        Intent intent = new Intent("test-intent-123");
        intent.setExecuteAt(Instant.parse("2026-04-10T12:30:45.123456789Z"));
        intent.setDeadline(Instant.parse("2026-04-10T13:30:45Z"));
        intent.setExpiredAction(ExpiredAction.DEAD_LETTER);
        intent.setPrecisionTier(PrecisionTier.HIGH);
        intent.setAckLevel(AckLevel.DURABLE);
        intent.setLastDeliveryId("delivery-1");
        intent.incrementAttempts();
        intent.transitionTo(IntentStatus.SCHEDULED);

        return IntentResponse.from(intent);
    }
}

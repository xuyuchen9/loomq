package com.loomq.http.json;

import com.loomq.api.CreateIntentRequest;
import com.loomq.api.IntentResponse;
import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.RedeliveryPolicy;
import com.loomq.http.netty.DirectSerializedResponse;
import com.loomq.http.netty.IntentResponseSerializer;
import com.loomq.replication.AckLevel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonCodecTest {

    private final JsonCodec jsonCodec = JsonCodec.instance();

    @Test
    @DisplayName("CreateIntentRequest should round-trip Instant and nested records")
    void createIntentRequestRoundTrip() throws Exception {
        String json = """
            {
              "intentId":"intent-1",
              "executeAt":"2026-04-20T10:00:00Z",
              "deadline":"2026-04-20T11:00:00Z",
              "expiredAction":"DISCARD",
              "precisionTier":"FAST",
              "shardKey":"orders",
              "ackLevel":"DURABLE",
              "callback":{"url":"http://localhost:9999/webhook","method":"POST","headers":{"X-Test":"1"}},
              "redelivery":{"maxAttempts":3,"backoff":"fixed","initialDelayMs":1000,"maxDelayMs":10000,"multiplier":2.0,"jitter":false},
              "idempotencyKey":"idem-1",
              "tags":{"env":"test"}
            }
            """;

        CreateIntentRequest request = jsonCodec.read(json.getBytes(), CreateIntentRequest.class);

        assertEquals("intent-1", request.intentId());
        assertEquals(Instant.parse("2026-04-20T10:00:00Z"), request.executeAt());
        assertEquals(Instant.parse("2026-04-20T11:00:00Z"), request.deadline());
        assertEquals(PrecisionTier.FAST, request.precisionTier());
        assertEquals("orders", request.shardKey());
        assertEquals(AckLevel.DURABLE, request.ackLevel());
        assertEquals("http://localhost:9999/webhook", request.callback().getUrl());
        assertEquals("POST", request.callback().getMethod());
        assertEquals("1", request.callback().getHeaders().get("X-Test"));
        assertEquals(3, request.redelivery().getMaxAttempts());
        assertEquals("idem-1", request.idempotencyKey());
        assertEquals("test", request.tags().get("env"));
    }

    @Test
    @DisplayName("IntentResponse should serialize Instant fields as ISO-8601 strings")
    void intentResponseSerialization() throws Exception {
        Intent intent = new Intent("intent-2");
        intent.setExecuteAt(Instant.parse("2026-04-20T10:00:00Z"));
        intent.setDeadline(Instant.parse("2026-04-20T11:00:00Z"));
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.setAckLevel(AckLevel.DURABLE);
        intent.setCallback(new Callback("http://localhost:9999/webhook"));
        intent.setRedelivery(new RedeliveryPolicy());
        intent.setTags(Map.of("env", "test"));

        String json = jsonCodec.writeString(IntentResponse.from(intent));

        assertTrue(json.contains("\"executeAt\":\"2026-04-20T10:00:00Z\""));
        assertTrue(json.contains("\"deadline\":\"2026-04-20T11:00:00Z\""));
        assertTrue(json.contains("\"status\":\"CREATED\""));
    }

    @Test
    @DisplayName("IntentResponse should support direct ByteBuf serialization")
    void intentResponseDirectSerialization() throws Exception {
        Intent intent = new Intent("intent-3");
        intent.setExecuteAt(Instant.parse("2026-04-20T10:00:00Z"));
        intent.setDeadline(Instant.parse("2026-04-20T11:00:00Z"));
        intent.setExpiredAction(com.loomq.domain.intent.ExpiredAction.DEAD_LETTER);
        intent.setPrecisionTier(PrecisionTier.HIGH);
        intent.setAckLevel(AckLevel.REPLICATED);
        intent.setLastDeliveryId("delivery-1");
        intent.incrementAttempts();
        intent.setTags(Map.of("env", "test"));

        IntentResponse response = IntentResponse.from(intent);
        assertTrue(response instanceof DirectSerializedResponse);

        String jacksonJson = jsonCodec.writeString(response);
        String directJson = new String(IntentResponseSerializer.toBytes(response), StandardCharsets.UTF_8);

        assertEquals(jacksonJson, directJson);
    }
}

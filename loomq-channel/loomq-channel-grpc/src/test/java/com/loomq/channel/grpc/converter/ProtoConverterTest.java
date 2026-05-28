package com.loomq.channel.grpc.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.protobuf.Timestamp;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.ExpiredAction;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.RedeliveryPolicy;
import com.loomq.domain.intent.WalMode;
import com.loomq.grpc.gen.CallbackMessage;
import com.loomq.grpc.gen.CreateIntentRequest;
import com.loomq.grpc.gen.IntentMessage;
import com.loomq.grpc.gen.RedeliveryPolicyMessage;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ProtoConverterTest {

    // ── Timestamp ↔ Instant ──

    @Test
    void timestampRoundTrip() {
        Instant now = Instant.now();
        Timestamp proto = ProtoConverter.toProto(now);
        Instant back = ProtoConverter.toDomain(proto);
        assertEquals(now.getEpochSecond(), back.getEpochSecond());
        assertEquals(now.getNano(), back.getNano());
    }

    @Test
    void nullTimestampReturnsNull() {
        assertNull(ProtoConverter.toProto((Instant) null));
        assertNull(ProtoConverter.toDomain((Timestamp) null));
    }

    // ── Callback ──

    @Test
    void callbackRoundTrip() {
        Callback c = new Callback();
        c.setUrl("http://example.com/hook");
        c.setMethod("PUT");
        c.setHeaders(Map.of("X-Key", "val"));
        c.setBody("{\"a\":1}");

        CallbackMessage proto = ProtoConverter.toProto(c);
        assertEquals("http://example.com/hook", proto.getUrl());
        assertEquals("PUT", proto.getMethod());
        assertEquals("val", proto.getHeadersMap().get("X-Key"));

        Callback back = ProtoConverter.toDomain(proto);
        assertEquals("http://example.com/hook", back.getUrl());
        assertEquals("PUT", back.getMethod());
        assertEquals("val", back.getHeaders().get("X-Key"));
        assertEquals("{\"a\":1}", back.getBody());
    }

    @Test
    void nullCallbackReturnsNull() {
        assertNull(ProtoConverter.toProto((Callback) null));
        assertNull(ProtoConverter.toDomain((CallbackMessage) null));
    }

    // ── RedeliveryPolicy ──

    @Test
    void redeliveryPolicyRoundTrip() {
        RedeliveryPolicy r = new RedeliveryPolicy();
        r.setMaxAttempts(5);
        r.setBackoff("exponential");
        r.setInitialDelayMs(100);
        r.setMaxDelayMs(5000);
        r.setMultiplier(2.0);
        r.setJitter(true);

        RedeliveryPolicyMessage proto = ProtoConverter.toProto(r);
        assertEquals(5, proto.getMaxAttempts());
        assertEquals("exponential", proto.getBackoff());
        assertEquals(100, proto.getInitialDelayMs());
        assertEquals(5000, proto.getMaxDelayMs());
        assertEquals(2.0, proto.getMultiplier());
        assertEquals(true, proto.getJitter());

        RedeliveryPolicy back = ProtoConverter.toDomain(proto);
        assertEquals(5, back.getMaxAttempts());
        assertEquals("exponential", back.getBackoff());
    }

    // ── Enum parsers ──

    @Test
    void parsePrecisionTier() {
        assertEquals(PrecisionTier.ULTRA, ProtoConverter.parsePrecisionTier("ULTRA"));
        assertEquals(PrecisionTier.FAST, ProtoConverter.parsePrecisionTier("fast"));
        assertNull(ProtoConverter.parsePrecisionTier(null));
        assertNull(ProtoConverter.parsePrecisionTier(""));
        assertNull(ProtoConverter.parsePrecisionTier("INVALID"));
    }

    @Test
    void parseAckMode() {
        assertEquals(AckMode.DURABLE, ProtoConverter.parseAckMode("DURABLE"));
        assertEquals(AckMode.ASYNC, ProtoConverter.parseAckMode("async"));
        assertNull(ProtoConverter.parseAckMode(null));
        assertNull(ProtoConverter.parseAckMode("INVALID"));
    }

    @Test
    void parseWalMode() {
        assertEquals(WalMode.DURABLE, ProtoConverter.parseWalMode("DURABLE"));
        assertNull(ProtoConverter.parseWalMode(null));
        assertNull(ProtoConverter.parseWalMode("INVALID"));
    }

    @Test
    void parseExpiredAction() {
        assertEquals(ExpiredAction.DEAD_LETTER, ProtoConverter.parseExpiredAction("DEAD_LETTER"));
        assertNull(ProtoConverter.parseExpiredAction(null));
        assertNull(ProtoConverter.parseExpiredAction("INVALID"));
    }

    // ── Intent → IntentMessage ──

    @Test
    void intentToProto() {
        Intent intent = new Intent("test-1");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.setDeadline(Instant.now().plusSeconds(120));
        intent.setPrecisionTier(PrecisionTier.FAST);
        intent.setAckMode(AckMode.DURABLE);
        intent.setShardKey("orders");
        intent.transitionTo(IntentStatus.SCHEDULED);

        IntentMessage msg = ProtoConverter.toProto(intent);
        assertEquals("test-1", msg.getIntentId());
        assertEquals("SCHEDULED", msg.getStatus());
        assertEquals("FAST", msg.getPrecisionTier());
        assertEquals("DURABLE", msg.getAckLevel());
        assertEquals("orders", msg.getShardKey());
    }

    // ── CreateIntentRequest → Intent ──

    @Test
    void createRequestToDomain() {
        Instant executeAt = Instant.now().plusSeconds(60);
        Instant deadline = Instant.now().plusSeconds(120);

        CreateIntentRequest request = CreateIntentRequest.newBuilder()
            .setIntentId("req-1")
            .setExecuteAt(ProtoConverter.toProto(executeAt))
            .setDeadline(ProtoConverter.toProto(deadline))
            .setPrecisionTier("STANDARD")
            .setShardKey("shard-1")
            .setCallback(CallbackMessage.newBuilder().setUrl("http://cb").build())
            .build();

        Intent intent = ProtoConverter.toDomain(request);
        assertEquals("req-1", intent.getIntentId());
        assertEquals(PrecisionTier.STANDARD, intent.getPrecisionTier());
        assertEquals("shard-1", intent.getShardKey());
        assertNotNull(intent.getCallback());
        assertEquals("http://cb", intent.getCallback().getUrl());
    }

    @Test
    void createRequestWithOverrideId() {
        CreateIntentRequest request = CreateIntentRequest.newBuilder()
            .setIntentId("original")
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setShardKey("s")
            .build();

        Intent intent = ProtoConverter.toDomain(request, "override");
        assertEquals("override", intent.getIntentId());
    }

    @Test
    void createRequestWithSloFields() {
        CreateIntentRequest request = CreateIntentRequest.newBuilder()
            .setIntentId("slo-1")
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setShardKey("s")
            .setAckLevel("DURABLE")
            .setWalMode("ASYNC")
            .setExpiredAction("DEAD_LETTER")
            .setIdempotencyKey("idem-1")
            .putTags("env", "test")
            .build();

        Intent intent = ProtoConverter.toDomain(request);
        assertEquals(AckMode.DURABLE, intent.getAckMode());
        assertEquals(WalMode.ASYNC, intent.getWalMode());
        assertEquals(ExpiredAction.DEAD_LETTER, intent.getExpiredAction());
        assertEquals("idem-1", intent.getIdempotencyKey());
        assertEquals("test", intent.getTags().get("env"));
    }
}

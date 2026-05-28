package com.loomq.grpc;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.Timestamp;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.ExpiredAction;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.RedeliveryPolicy;
import com.loomq.domain.intent.WalMode;
import com.loomq.grpc.converter.ProtoConverter;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ProtoConverter}.
 *
 * <p>Verifies bidirectional conversion between Protobuf messages
 * and LoomQ domain objects.
 */
class ProtoConverterTest {

    // ── Timestamp ↔ Instant ──

    @Test
    void shouldConvertInstantToProtoAndBack() {
        Instant original = Instant.parse("2026-05-26T10:00:00.123456789Z");
        Timestamp proto = ProtoConverter.toProto(original);
        assertNotNull(proto);
        assertEquals(original.getEpochSecond(), proto.getSeconds());
        assertEquals(original.getNano(), proto.getNanos());

        Instant back = ProtoConverter.toDomain(proto);
        assertEquals(original, back);
    }

    @Test
    void shouldHandleNullInstant() {
        assertNull(ProtoConverter.toProto((Instant) null));
        assertNull(ProtoConverter.toDomain((Timestamp) null));
    }

    // ── Callback ──

    @Test
    void shouldConvertCallbackToProtoAndBack() {
        Callback original = new Callback();
        original.setUrl("https://example.com/hook");
        original.setMethod("PUT");
        original.setHeaders(Map.of("Authorization", "Bearer token"));
        original.setBody("{\"key\":\"value\"}");

        com.loomq.grpc.gen.CallbackMessage proto = ProtoConverter.toProto(original);
        assertNotNull(proto);
        assertEquals("https://example.com/hook", proto.getUrl());
        assertEquals("PUT", proto.getMethod());
        assertEquals("Bearer token", proto.getHeadersMap().get("Authorization"));
        assertEquals("{\"key\":\"value\"}", proto.getBody().toStringUtf8());

        Callback back = ProtoConverter.toDomain(proto);
        assertEquals(original.getUrl(), back.getUrl());
        assertEquals(original.getMethod(), back.getMethod());
        assertEquals(original.getHeaders(), back.getHeaders());
        assertEquals(original.getBody().toString(), back.getBody().toString());
    }

    @Test
    void shouldHandleNullCallback() {
        assertNull(ProtoConverter.toProto((Callback) null));
        assertNull(ProtoConverter.toDomain((com.loomq.grpc.gen.CallbackMessage) null));
    }

    // ── RedeliveryPolicy ──

    @Test
    void shouldConvertRedeliveryPolicyToProtoAndBack() {
        RedeliveryPolicy original = new RedeliveryPolicy();
        original.setMaxAttempts(10);
        original.setBackoff("fixed");
        original.setInitialDelayMs(500);
        original.setMaxDelayMs(30000);
        original.setMultiplier(1.5);
        original.setJitter(false);

        var proto = ProtoConverter.toProto(original);
        assertEquals(10, proto.getMaxAttempts());
        assertEquals("fixed", proto.getBackoff());

        RedeliveryPolicy back = ProtoConverter.toDomain(proto);
        assertEquals(original.getMaxAttempts(), back.getMaxAttempts());
        assertEquals(original.getBackoff(), back.getBackoff());
        assertEquals(original.getInitialDelayMs(), back.getInitialDelayMs());
        assertEquals(original.getMaxDelayMs(), back.getMaxDelayMs());
        assertEquals(original.getMultiplier(), back.getMultiplier(), 1e-9);
        assertEquals(original.isJitter(), back.isJitter());
    }

    // ── Enum parsing ──

    @Test
    void shouldParseValidEnums() {
        assertEquals(PrecisionTier.ULTRA, ProtoConverter.parsePrecisionTier("ULTRA"));
        assertEquals(PrecisionTier.FAST, ProtoConverter.parsePrecisionTier("fast"));
        assertEquals(PrecisionTier.ECONOMY, ProtoConverter.parsePrecisionTier("Economy"));
        assertEquals(AckMode.DURABLE, ProtoConverter.parseAckMode("DURABLE"));
        assertEquals(WalMode.ASYNC, ProtoConverter.parseWalMode("ASYNC"));
        assertEquals(ExpiredAction.DISCARD, ProtoConverter.parseExpiredAction("DISCARD"));
    }

    @Test
    void shouldReturnNullForInvalidEnum() {
        assertNull(ProtoConverter.parsePrecisionTier("UNKNOWN"));
        assertNull(ProtoConverter.parsePrecisionTier(""));
        assertNull(ProtoConverter.parsePrecisionTier(null));
    }

    // ── Intent → IntentMessage ──

    @Test
    void shouldConvertIntentToProto() {
        Intent intent = new Intent("test-1");
        intent.setExecuteAt(Instant.parse("2026-05-26T12:00:00Z"));
        intent.setDeadline(Instant.parse("2026-05-26T13:00:00Z"));
        intent.setExpiredAction(ExpiredAction.DEAD_LETTER);
        intent.setPrecisionTier(PrecisionTier.HIGH);
        intent.setWalMode(WalMode.DURABLE);
        intent.setAckMode(AckMode.REPLICATED);
        intent.setShardKey("shard-a");
        intent.setIdempotencyKey("idem-1");
        intent.setTags(Map.of("env", "test"));
        intent.setAttempts(2);

        var proto = ProtoConverter.toProto(intent);
        assertEquals("test-1", proto.getIntentId());
        assertEquals("CREATED", proto.getStatus());
        assertEquals("DEAD_LETTER", proto.getExpiredAction());
        assertEquals("HIGH", proto.getPrecisionTier());
        assertEquals("DURABLE", proto.getWalMode());
        assertEquals("REPLICATED", proto.getAckLevel());
        assertEquals("shard-a", proto.getShardKey());
        assertEquals("idem-1", proto.getIdempotencyKey());
        assertEquals("test", proto.getTagsMap().get("env"));
        assertEquals(2, proto.getAttempts());
        assertEquals(0, proto.getRevision());
        assertTrue(proto.hasExecuteAt());
        assertTrue(proto.hasCreatedAt());
    }

    // ── CreateIntentRequest (proto) → Intent (domain) ──

    @Test
    void shouldConvertCreateRequestToDomain() {
        var proto = com.loomq.grpc.gen.CreateIntentRequest.newBuilder()
            .setIntentId("req-1")
            .setExecuteAt(ProtoConverter.toProto(Instant.parse("2026-05-26T12:00:00Z")))
            .setDeadline(ProtoConverter.toProto(Instant.parse("2026-05-26T13:00:00Z")))
            .setExpiredAction("DEAD_LETTER")
            .setPrecisionTier("FAST")
            .setWalMode("ASYNC")
            .setShardKey("shard-b")
            .setAckLevel("DURABLE")
            .setIdempotencyKey("idem-req")
            .putTags("env", "prod")
            .build();

        Intent intent = ProtoConverter.toDomain(proto);
        assertEquals("req-1", intent.getIntentId());
        assertEquals(Instant.parse("2026-05-26T12:00:00Z"), intent.getExecuteAt());
        assertEquals(Instant.parse("2026-05-26T13:00:00Z"), intent.getDeadline());
        assertEquals(ExpiredAction.DEAD_LETTER, intent.getExpiredAction());
        assertEquals(PrecisionTier.FAST, intent.getPrecisionTier());
        assertEquals(WalMode.ASYNC, intent.getWalMode());
        assertEquals("shard-b", intent.getShardKey());
        assertEquals(AckMode.DURABLE, intent.getAckMode());
        assertEquals("idem-req", intent.getIdempotencyKey());
        assertEquals("prod", intent.getTags().get("env"));
    }

    @Test
    void shouldApplyOverrideId() {
        var proto = com.loomq.grpc.gen.CreateIntentRequest.newBuilder()
            .setIntentId("original-id")
            .build();
        Intent intent = ProtoConverter.toDomain(proto, "overridden-id");
        assertEquals("overridden-id", intent.getIntentId());
    }

    @Test
    void shouldConvertIntentMessageToDomain() {
        var proto = com.loomq.grpc.gen.IntentMessage.newBuilder()
            .setIntentId("test-id")
            .setStatus("SCHEDULED")
            .setExpiredAction("DEAD_LETTER")
            .setPrecisionTier("FAST")
            .setWalMode("ASYNC")
            .setAckLevel("DURABLE")
            .setShardKey("shard-a")
            .setShardId("3")
            .setExecuteAt(ProtoConverter.toProto(Instant.ofEpochMilli(1000)))
            .setCreatedAt(ProtoConverter.toProto(Instant.ofEpochMilli(500)))
            .setAttempts(2)
            .setRevision(5)
            .putTags("k", "v")
            .build();

        assertEquals("test-id", proto.getIntentId());
        assertEquals("SCHEDULED", proto.getStatus());
        assertEquals("DEAD_LETTER", proto.getExpiredAction());
        assertEquals("FAST", proto.getPrecisionTier());
        assertEquals("ASYNC", proto.getWalMode());
        assertEquals("DURABLE", proto.getAckLevel());
        assertEquals("shard-a", proto.getShardKey());
        assertEquals("3", proto.getShardId());
        assertEquals(2, proto.getAttempts());
        assertEquals(5, proto.getRevision());
        assertEquals("v", proto.getTagsMap().get("k"));
    }

    @Test
    void shouldHandleNullRedeliveryPolicy() {
        var callback = ProtoConverter.toProto(new com.loomq.domain.intent.Callback("http://example.com", "POST", null, null));
        assertNotNull(callback);
        assertEquals("http://example.com", callback.getUrl());

        // Test null RedeliveryPolicy in IntentMessage
        var intent = new com.loomq.domain.intent.Intent("test");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        var msg = ProtoConverter.toProto(intent);
        assertFalse(msg.hasRedelivery());
    }
}

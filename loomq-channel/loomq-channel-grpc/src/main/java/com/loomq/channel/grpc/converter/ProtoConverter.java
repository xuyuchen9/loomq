package com.loomq.channel.grpc.converter;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.ExpiredAction;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.RedeliveryPolicy;
import com.loomq.domain.intent.WalMode;
import java.time.Instant;
import java.util.HashMap;

/**
 * Converts between Protobuf messages and LoomQ domain objects.
 *
 * <p>All enum types are represented as strings in the proto schema
 * to keep the wire format flexible and avoid proto enum evolution issues.
 */
public final class ProtoConverter {

    private ProtoConverter() {}

    // ── Timestamp ↔ Instant ──

    public static Timestamp toProto(Instant instant) {
        if (instant == null) return null;
        return Timestamp.newBuilder()
            .setSeconds(instant.getEpochSecond())
            .setNanos(instant.getNano())
            .build();
    }

    public static Instant toDomain(Timestamp ts) {
        if (ts == null) return null;
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }

    // ── Callback ──

    public static com.loomq.grpc.gen.CallbackMessage toProto(Callback c) {
        if (c == null) return null;
        var b = com.loomq.grpc.gen.CallbackMessage.newBuilder()
            .setUrl(c.getUrl() != null ? c.getUrl() : "")
            .setMethod(c.getMethod() != null ? c.getMethod() : "POST");
        if (c.getHeaders() != null) b.putAllHeaders(c.getHeaders());
        if (c.getBody() != null) b.setBody(ByteString.copyFromUtf8(c.getBody().toString()));
        return b.build();
    }

    public static Callback toDomain(com.loomq.grpc.gen.CallbackMessage p) {
        if (p == null) return null;
        Callback c = new Callback();
        c.setUrl(p.getUrl());
        c.setMethod(p.getMethod());
        if (!p.getHeadersMap().isEmpty()) c.setHeaders(new HashMap<>(p.getHeadersMap()));
        if (!p.getBody().isEmpty()) c.setBody(p.getBody().toStringUtf8());
        return c;
    }

    // ── RedeliveryPolicy ──

    public static com.loomq.grpc.gen.RedeliveryPolicyMessage toProto(RedeliveryPolicy p) {
        if (p == null) return null;
        return com.loomq.grpc.gen.RedeliveryPolicyMessage.newBuilder()
            .setMaxAttempts(p.getMaxAttempts())
            .setBackoff(p.getBackoff() != null ? p.getBackoff() : "exponential")
            .setInitialDelayMs(p.getInitialDelayMs())
            .setMaxDelayMs(p.getMaxDelayMs())
            .setMultiplier(p.getMultiplier())
            .setJitter(p.isJitter())
            .build();
    }

    public static RedeliveryPolicy toDomain(com.loomq.grpc.gen.RedeliveryPolicyMessage p) {
        if (p == null) return null;
        RedeliveryPolicy r = new RedeliveryPolicy();
        r.setMaxAttempts(p.getMaxAttempts());
        r.setBackoff(p.getBackoff());
        r.setInitialDelayMs(p.getInitialDelayMs());
        r.setMaxDelayMs(p.getMaxDelayMs());
        r.setMultiplier(p.getMultiplier());
        r.setJitter(p.getJitter());
        return r;
    }

    // ── Enum helpers ──

    public static PrecisionTier parsePrecisionTier(String s) {
        if (s == null || s.isBlank()) return null;
        try { return PrecisionTier.valueOf(s.toUpperCase()); }
        catch (IllegalArgumentException e) { return null; }
    }

    public static AckMode parseAckMode(String s) {
        if (s == null || s.isBlank()) return null;
        try { return AckMode.valueOf(s.toUpperCase()); }
        catch (IllegalArgumentException e) { return null; }
    }

    public static WalMode parseWalMode(String s) {
        if (s == null || s.isBlank()) return null;
        try { return WalMode.valueOf(s.toUpperCase()); }
        catch (IllegalArgumentException e) { return null; }
    }

    public static ExpiredAction parseExpiredAction(String s) {
        if (s == null || s.isBlank()) return null;
        try { return ExpiredAction.valueOf(s.toUpperCase()); }
        catch (IllegalArgumentException e) { return null; }
    }

    // ── Intent → IntentMessage ──

    public static com.loomq.grpc.gen.IntentMessage toProto(Intent intent) {
        var b = com.loomq.grpc.gen.IntentMessage.newBuilder()
            .setIntentId(intent.getIntentId())
            .setStatus(intent.getStatus().name());
        if (intent.getExecuteAt() != null) b.setExecuteAt(toProto(intent.getExecuteAt()));
        if (intent.getDeadline() != null) b.setDeadline(toProto(intent.getDeadline()));
        if (intent.getExpiredAction() != null) b.setExpiredAction(intent.getExpiredAction().name());
        if (intent.getPrecisionTier() != null) b.setPrecisionTier(intent.getPrecisionTier().name());
        if (intent.getWalMode() != null) b.setWalMode(intent.getWalMode().name());
        if (intent.getAckMode() != null) b.setAckLevel(intent.getAckMode().name());
        if (intent.getShardKey() != null) b.setShardKey(intent.getShardKey());
        if (intent.getShardId() != null) b.setShardId(intent.getShardId());
        if (intent.getCallback() != null) b.setCallback(toProto(intent.getCallback()));
        if (intent.getRedelivery() != null) b.setRedelivery(toProto(intent.getRedelivery()));
        if (intent.getIdempotencyKey() != null) b.setIdempotencyKey(intent.getIdempotencyKey());
        if (intent.getTags() != null) b.putAllTags(intent.getTags());
        b.setAttempts(intent.getAttempts());
        if (intent.getLastDeliveryId() != null) b.setLastDeliveryId(intent.getLastDeliveryId());
        b.setRevision(intent.getRevision());
        if (intent.getCreatedAt() != null) b.setCreatedAt(toProto(intent.getCreatedAt()));
        if (intent.getUpdatedAt() != null) b.setUpdatedAt(toProto(intent.getUpdatedAt()));
        if (intent.getTraceId() != null) b.setTraceId(intent.getTraceId());
        return b.build();
    }

    // ── CreateIntentRequest (proto) → Intent (domain) ──

    public static Intent toDomain(com.loomq.grpc.gen.CreateIntentRequest proto) {
        return toDomain(proto, null);
    }

    public static Intent toDomain(com.loomq.grpc.gen.CreateIntentRequest proto, String overrideId) {
        String intentId = overrideId != null ? overrideId : proto.getIntentId();
        Intent intent = new Intent(intentId);
        if (proto.hasExecuteAt()) intent.setExecuteAt(toDomain(proto.getExecuteAt()));
        if (proto.hasDeadline()) intent.setDeadline(toDomain(proto.getDeadline()));
        String expiredAction = proto.getExpiredAction();
        if (!expiredAction.isEmpty()) {
            ExpiredAction ea = parseExpiredAction(expiredAction);
            if (ea != null) intent.setExpiredAction(ea);
        }
        String tier = proto.getPrecisionTier();
        if (!tier.isEmpty()) {
            PrecisionTier pt = parsePrecisionTier(tier);
            if (pt != null) intent.setPrecisionTier(pt);
        }
        String walMode = proto.getWalMode();
        if (!walMode.isEmpty()) {
            WalMode wm = parseWalMode(walMode);
            if (wm != null) intent.setWalMode(wm);
        }
        if (!proto.getShardKey().isEmpty()) intent.setShardKey(proto.getShardKey());
        String ackLevel = proto.getAckLevel();
        if (!ackLevel.isEmpty()) {
            AckMode am = parseAckMode(ackLevel);
            if (am != null) intent.setAckMode(am);
        }
        if (proto.hasCallback()) intent.setCallback(toDomain(proto.getCallback()));
        if (proto.hasRedelivery()) intent.setRedelivery(toDomain(proto.getRedelivery()));
        if (!proto.getIdempotencyKey().isEmpty()) intent.setIdempotencyKey(proto.getIdempotencyKey());
        if (!proto.getTagsMap().isEmpty()) intent.setTags(new HashMap<>(proto.getTagsMap()));
        return intent;
    }
}

package com.loomq.api;

import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.RedeliveryPolicy;
import com.loomq.domain.intent.WalMode;
import com.loomq.replication.AckLevel;

import java.time.Instant;
import java.util.Map;

/**
 * 创建 Intent 请求
 *
 * @author loomq
 * @since v0.5.0
 */
public record CreateIntentRequest(
    String intentId,
    Instant executeAt,
    Instant deadline,
    com.loomq.domain.intent.ExpiredAction expiredAction,
    PrecisionTier precisionTier,
    WalMode walMode,
    String shardKey,
    AckLevel ackLevel,
    Callback callback,
    RedeliveryPolicy redelivery,
    String idempotencyKey,
    Map<String, String> tags
) {}

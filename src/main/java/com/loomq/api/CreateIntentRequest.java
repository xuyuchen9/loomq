package com.loomq.api;

import com.loomq.entity.v5.Callback;
import com.loomq.entity.v5.PrecisionTier;
import com.loomq.entity.v5.RedeliveryPolicy;
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
    com.loomq.entity.v5.ExpiredAction expiredAction,
    PrecisionTier precisionTier,
    String shardKey,
    AckLevel ackLevel,
    Callback callback,
    RedeliveryPolicy redelivery,
    String idempotencyKey,
    Map<String, String> tags
) {}

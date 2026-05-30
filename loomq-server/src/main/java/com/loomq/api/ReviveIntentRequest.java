package com.loomq.api;

import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.RedeliveryPolicy;
import java.time.Instant;

/**
 * Revive 请求 — 将死信 Intent 重新调度。
 *
 * <p>所有字段可选。仅提供需要更新的字段，其余保持原值。</p>
 */
public record ReviveIntentRequest(
    Instant executeAt,
    Instant deadline,
    Callback callback,
    RedeliveryPolicy redelivery
) {}

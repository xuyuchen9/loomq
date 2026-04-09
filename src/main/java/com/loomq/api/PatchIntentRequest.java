package com.loomq.api;

import com.loomq.entity.v5.RedeliveryPolicy;

import java.time.Instant;
import java.util.Map;

/**
 * 修改 Intent 请求
 *
 * @author loomq
 * @since v0.5.0
 */
public record PatchIntentRequest(
    Instant executeAt,
    Instant deadline,
    com.loomq.entity.v5.ExpiredAction expiredAction,
    RedeliveryPolicy redelivery,
    Map<String, String> tags
) {}

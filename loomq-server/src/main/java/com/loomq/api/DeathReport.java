package com.loomq.api;

/**
 * 死信报告，包含 Intent 死亡的详细信息。
 */
public record DeathReport(
    int finalAttempt,
    int maxAttempts,
    String lastAttemptAt,
    String failureReason,
    Integer failureHttpStatus,
    String callbackUrl,
    String tier,
    long lifetimeMs
) {}

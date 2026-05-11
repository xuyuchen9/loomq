package com.loomq.application.swap;

import com.loomq.domain.intent.PrecisionTier;

/**
 * Metadata retained in memory after a long-delay Intent is swapped out of IntentStore.
 *
 * Minimal footprint: ~80 bytes per cold intent vs ~500+ bytes for a full Intent object.
 * At 100k cold intents, this saves ~40MB+ of heap.
 *
 * @param intentId      系统唯一标识
 * @param walPosition    WAL 文件中记录的起始位置（SimpleWalWriter.writeInternal 返回值）
 * @param recordLength   完整 WAL 记录长度 = HEADER_SIZE + payloadLen + CHECKSUM_SIZE
 * @param executeAtEpochMs  计划执行时间（epoch millis），用于排序索引
 * @param tier          精度档位，用于调度恢复
 */
public record ColdIntentEntry(
    String intentId,
    long walPosition,
    int recordLength,
    long executeAtEpochMs,
    PrecisionTier tier
) {
    public ColdIntentEntry {
        if (intentId == null || intentId.isBlank()) {
            throw new IllegalArgumentException("intentId must not be blank");
        }
        if (walPosition < 0) {
            throw new IllegalArgumentException("walPosition must be >= 0");
        }
        if (recordLength <= 0) {
            throw new IllegalArgumentException("recordLength must be > 0");
        }
        if (executeAtEpochMs <= 0) {
            throw new IllegalArgumentException("executeAtEpochMs must be > 0");
        }
        if (tier == null) {
            throw new IllegalArgumentException("tier must not be null");
        }
    }
}

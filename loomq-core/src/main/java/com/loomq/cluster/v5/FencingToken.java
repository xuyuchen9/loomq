package com.loomq.cluster.v5;

import java.util.Objects;

/**
 * Fencing Token (v0.5)
 *
 * 用于防止脑裂的分区令牌，每次写操作都需要携带单调递增的 token
 * 存储层会拒绝比当前 token 小的写入请求
 *
 * 结构：高 32 位为 epoch，低 32 位为 sequence
 *
 * @author loomq
 * @since v0.5.0
 */
public final class FencingToken implements Comparable<FencingToken> {

    private final long epoch;
    private final long sequence;

    public FencingToken(long epoch, long sequence) {
        this.epoch = epoch;
        this.sequence = sequence;
    }

    /**
     * 创建一个初始 token（用于 epoch 0）
     */
    public static FencingToken initial() {
        return new FencingToken(0, 0);
    }

    /**
     * 创建一个仅基于序列的新 token（相同 epoch）
     */
    public FencingToken next() {
        return new FencingToken(this.epoch, this.sequence + 1);
    }

    /**
     * 创建一个新 epoch 的 token
     */
    public FencingToken withEpoch(long newEpoch) {
        return new FencingToken(newEpoch, 0);
    }

    /**
     * 检查此 token 是否比另一个 token 更新
     */
    public boolean isNewerThan(FencingToken other) {
        if (other == null) return true;
        if (this.epoch != other.epoch) {
            return this.epoch > other.epoch;
        }
        return this.sequence > other.sequence;
    }

    /**
     * 检查 token 是否有效（未被 fence）
     */
    public boolean isValidAgainst(FencingToken currentMax) {
        if (currentMax == null) return true;
        return !currentMax.isNewerThan(this);
    }

    /**
     * 转换为 long 值用于存储/传输
     * 高 32 位为 epoch，低 32 位为 sequence
     */
    public long toLong() {
        return (epoch << 32) | (sequence & 0xFFFFFFFFL);
    }

    /**
     * 从 long 值解析
     */
    public static FencingToken fromLong(long value) {
        long epoch = value >>> 32;
        long sequence = value & 0xFFFFFFFFL;
        return new FencingToken(epoch, sequence);
    }

    @Override
    public int compareTo(FencingToken other) {
        if (other == null) return 1;
        if (this.epoch != other.epoch) {
            return Long.compare(this.epoch, other.epoch);
        }
        return Long.compare(this.sequence, other.sequence);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FencingToken that = (FencingToken) o;
        return epoch == that.epoch && sequence == that.sequence;
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, sequence);
    }

    @Override
    public String toString() {
        return String.format("FencingToken{epoch=%d, seq=%d}", epoch, sequence);
    }

    // Getters

    public long getEpoch() {
        return epoch;
    }

    public long getSequence() {
        return sequence;
    }
}

/**
 * Fencing Token 验证器
 */
class FencingTokenValidator {

    private FencingToken currentMax;

    public synchronized void updateMaxToken(FencingToken token) {
        if (token != null && (currentMax == null || token.isNewerThan(currentMax))) {
            this.currentMax = token;
        }
    }

    public synchronized FencingToken getCurrentMax() {
        return currentMax;
    }

    public synchronized boolean validate(FencingToken token) {
        return token != null && token.isValidAgainst(currentMax);
    }
}

/**
 * Fencing Token 过期异常
 */
class FencingTokenExpiredException extends RuntimeException {
    public FencingTokenExpiredException(String message) {
        super(message);
    }
}

package com.loomq.domain.cluster;

/**
 * Fencing Token - 用于防止脑裂的令牌
 *
 * 用于确保只有持有最新令牌的节点才能写入存储。
 *
 * @author loomq
 * @since v0.5.0
 */
public record FencingToken(long epoch, long sequence) implements Comparable<FencingToken> {

    /**
     * 比较两个令牌的大小
     *
     * 先比较 epoch，epoch 相同再比较 sequence
     */
    @Override
    public int compareTo(FencingToken other) {
        int epochCompare = Long.compare(this.epoch, other.epoch);
        if (epochCompare != 0) {
            return epochCompare;
        }
        return Long.compare(this.sequence, other.sequence);
    }

    /**
     * 判断当前令牌是否比另一个令牌新
     */
    public boolean isNewerThan(FencingToken other) {
        return this.compareTo(other) > 0;
    }

    /**
     * 验证令牌是否有效
     *
     * 用于验证请求的 fencing token 是否仍然有效（不比当前 lease 的 token 旧）
     *
     * @param currentToken 当前有效的令牌
     * @return true 如果当前令牌不旧于有效令牌
     */
    public boolean isValidAgainst(FencingToken currentToken) {
        return this.compareTo(currentToken) >= 0;
    }

    @Override
    public String toString() {
        return String.format("FencingToken{epoch=%d, seq=%d}", epoch, sequence);
    }
}

package com.loomq.replication.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * WAL 追赶请求消息
 *
 * Week 4 Phase 4 - T4.2: Replica 向 Primary 请求缺失的 WAL 记录
 *
 * 消息格式：
 * - Magic (4 bytes): 0x4C515255 (LQCU - LoomQ Catch Up)
 * - Version (2 bytes): 1
 * - Shard ID Length (2 bytes)
 * - Shard ID (variable)
 * - Start Offset (8 bytes): 请求的起始 offset（包含）
 * - Batch Size (4 bytes): 请求的批量大小
 * - Replica Node ID Length (2 bytes)
 * - Replica Node ID (variable)
 *
 * @author loomq
 * @since v0.4.8
 */
public class CatchUpRequest {

    // ==================== 常量 ====================

    /**
     * Magic number: "LQCU" (LoomQ Catch Up)
     */
    public static final int MAGIC_NUMBER = 0x4C515255;

    /**
     * 协议版本
     */
    public static final short VERSION = 1;

    // ==================== 字段 ====================

    private final String shardId;
    private final long startOffset;
    private final int batchSize;
    private final String replicaNodeId;

    // ==================== 构造函数 ====================

    /**
     * 创建追赶请求
     *
     * @param shardId 分片 ID
     * @param startOffset 起始 offset（包含）
     * @param batchSize 批量大小
     * @param replicaNodeId Replica 节点 ID
     */
    public CatchUpRequest(String shardId, long startOffset, int batchSize, String replicaNodeId) {
        this.shardId = Objects.requireNonNull(shardId, "shardId cannot be null");
        this.startOffset = startOffset;
        this.batchSize = Math.max(1, batchSize);
        this.replicaNodeId = Objects.requireNonNull(replicaNodeId, "replicaNodeId cannot be null");

        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset must be non-negative: " + startOffset);
        }
    }

    // ==================== Getter ====================

    public String getShardId() {
        return shardId;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public String getReplicaNodeId() {
        return replicaNodeId;
    }

    // ==================== 编码/解码 ====================

    /**
     * 编码为字节数组
     */
    public byte[] encode() {
        byte[] shardIdBytes = shardId.getBytes(StandardCharsets.UTF_8);
        byte[] nodeIdBytes = replicaNodeId.getBytes(StandardCharsets.UTF_8);

        // 计算总大小
        int totalSize = 4 + 2 + 2 + shardIdBytes.length + 8 + 4 + 2 + nodeIdBytes.length;

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        // Magic (4 bytes)
        buffer.putInt(MAGIC_NUMBER);

        // Version (2 bytes)
        buffer.putShort(VERSION);

        // Shard ID Length (2 bytes) + Shard ID
        buffer.putShort((short) shardIdBytes.length);
        buffer.put(shardIdBytes);

        // Start Offset (8 bytes)
        buffer.putLong(startOffset);

        // Batch Size (4 bytes)
        buffer.putInt(batchSize);

        // Replica Node ID Length (2 bytes) + Node ID
        buffer.putShort((short) nodeIdBytes.length);
        buffer.put(nodeIdBytes);

        return buffer.array();
    }

    /**
     * 从字节数组解码
     */
    public static CatchUpRequest decode(byte[] data) {
        if (data.length < 22) { // 最小长度: 4 + 2 + 2 + 0 + 8 + 4 + 2 + 0 = 22
            throw new IllegalArgumentException("Data too short for CatchUpRequest: " + data.length);
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);

        // Verify magic number
        int magic = buffer.getInt();
        if (magic != MAGIC_NUMBER) {
            throw new IllegalArgumentException(
                "Invalid magic number: expected " + MAGIC_NUMBER + ", got " + magic);
        }

        // Verify version
        short version = buffer.getShort();
        if (version != VERSION) {
            throw new IllegalArgumentException(
                "Unsupported version: expected " + VERSION + ", got " + version);
        }

        // Shard ID
        short shardIdLen = buffer.getShort();
        if (shardIdLen < 0 || shardIdLen > 1024) {
            throw new IllegalArgumentException("Invalid shardId length: " + shardIdLen);
        }
        byte[] shardIdBytes = new byte[shardIdLen];
        buffer.get(shardIdBytes);
        String shardId = new String(shardIdBytes, StandardCharsets.UTF_8);

        // Start Offset
        long startOffset = buffer.getLong();

        // Batch Size
        int batchSize = buffer.getInt();

        // Replica Node ID
        short nodeIdLen = buffer.getShort();
        if (nodeIdLen < 0 || nodeIdLen > 1024) {
            throw new IllegalArgumentException("Invalid replicaNodeId length: " + nodeIdLen);
        }
        byte[] nodeIdBytes = new byte[nodeIdLen];
        buffer.get(nodeIdBytes);
        String replicaNodeId = new String(nodeIdBytes, StandardCharsets.UTF_8);

        return new CatchUpRequest(shardId, startOffset, batchSize, replicaNodeId);
    }

    // ==================== 辅助方法 ====================

    /**
     * 计算结束 offset（不包含）
     */
    public long getEndOffset() {
        return startOffset + batchSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CatchUpRequest that = (CatchUpRequest) o;
        return startOffset == that.startOffset &&
               batchSize == that.batchSize &&
               shardId.equals(that.shardId) &&
               replicaNodeId.equals(that.replicaNodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, startOffset, batchSize, replicaNodeId);
    }

    @Override
    public String toString() {
        return String.format("CatchUpRequest{shard='%s', offset=%d, batch=%d, node='%s'}",
            shardId, startOffset, batchSize, replicaNodeId);
    }
}

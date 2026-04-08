package com.loomq.replication;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * 复制记录
 *
 * 这是 v0.4.8 复制的核心数据结构，用于在 primary 和 replica 之间传输任务变更。
 *
 * 格式设计：
 * - Magic (8 bytes)
 * - Version (2 bytes)
 * - Header Size (2 bytes)
 * - Offset (8 bytes)
 * - Timestamp (8 bytes)
 * - Type (1 byte)
 * - Padding (7 bytes)
 * - Checksum (8 bytes) - CRC32C 覆盖 header + sourceLen + source + payload
 * - Source Node ID Length (2 bytes)
 * - Source Node ID (variable)
 * - Payload (variable)
 *
 * @author loomq
 * @since v0.4.8
 */
public class ReplicationRecord {

    // ==================== 常量 ====================

    /**
     * Magic number: "LQRP" (LoomQ Replication Protocol)
     */
    public static final long MAGIC_NUMBER = 0x4C51525000000001L;

    /**
     * 协议版本
     */
    public static final short VERSION = 1;

    /**
     * 固定头部大小（不包括 checksum 和变长部分）
     * Magic(8) + Version(2) + HeaderSize(2) + Offset(8) + Timestamp(8) + Type(1) + Padding(7) = 36
     */
    public static final int HEADER_SIZE = 36;

    // ==================== 字段 ====================

    private final long offset;
    private final long timestamp;
    private final ReplicationRecordType type;
    private final String sourceNodeId;
    private final byte[] payload;
    private final long checksum;

    // ==================== 构造函数 ====================

    private ReplicationRecord(long offset, long timestamp, ReplicationRecordType type,
                              String sourceNodeId, byte[] payload, long checksum) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.type = type;
        this.sourceNodeId = sourceNodeId != null ? sourceNodeId : "";
        this.payload = payload != null ? payload : new byte[0];
        this.checksum = checksum;
    }

    // ==================== Builder ====================

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long offset = -1;
        private long timestamp = System.currentTimeMillis();
        private ReplicationRecordType type;
        private String sourceNodeId = "";
        private byte[] payload = new byte[0];

        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder timestamp(Instant instant) {
            this.timestamp = instant.toEpochMilli();
            return this;
        }

        public Builder type(ReplicationRecordType type) {
            this.type = type;
            return this;
        }

        public Builder sourceNodeId(String sourceNodeId) {
            this.sourceNodeId = sourceNodeId;
            return this;
        }

        public Builder payload(byte[] payload) {
            this.payload = payload != null ? payload : new byte[0];
            return this;
        }

        public ReplicationRecord build() {
            if (offset < 0) {
                throw new IllegalArgumentException("offset must be non-negative");
            }
            if (type == null) {
                throw new IllegalArgumentException("type is required");
            }

            // 先计算校验和（使用临时 buffer）
            byte[] sourceBytes = sourceNodeId.getBytes(java.nio.charset.StandardCharsets.UTF_8);

            // 计算 header + sourceLen + source + payload 的 CRC
            CRC32C crc32c = new CRC32C();

            // header
            ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE);
            headerBuf.putLong(MAGIC_NUMBER);
            headerBuf.putShort(VERSION);
            headerBuf.putShort((short) HEADER_SIZE);
            headerBuf.putLong(offset);
            headerBuf.putLong(timestamp);
            headerBuf.put(type.getCode());
            headerBuf.put(new byte[7]); // padding
            crc32c.update(headerBuf.array());

            // source length
            ByteBuffer lenBuf = ByteBuffer.allocate(2);
            lenBuf.putShort((short) sourceBytes.length);
            crc32c.update(lenBuf.array());

            // source + payload
            crc32c.update(sourceBytes);
            crc32c.update(payload);

            long checksum = crc32c.getValue();

            return new ReplicationRecord(offset, timestamp, type, sourceNodeId, payload, checksum);
        }
    }

    // ==================== Getter ====================

    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Instant getTimestampInstant() {
        return Instant.ofEpochMilli(timestamp);
    }

    public ReplicationRecordType getType() {
        return type;
    }

    public String getSourceNodeId() {
        return sourceNodeId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public long getChecksum() {
        return checksum;
    }

    // ==================== 编码/解码 ====================

    /**
     * 编码为字节数组（网络传输或持久化）
     */
    public byte[] encode() {
        byte[] sourceBytes = sourceNodeId.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        // 计算总大小
        int totalSize = HEADER_SIZE + 8 + 2 + sourceBytes.length + payload.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        // Header (32 bytes)
        buffer.putLong(MAGIC_NUMBER);           // 8 bytes
        buffer.putShort(VERSION);               // 2 bytes
        buffer.putShort((short) HEADER_SIZE);   // 2 bytes
        buffer.putLong(offset);                 // 8 bytes
        buffer.putLong(timestamp);              // 8 bytes
        buffer.put(type.getCode());             // 1 byte
        buffer.put(new byte[7]);                // 7 bytes padding

        // Checksum (8 bytes)
        buffer.putLong(checksum);

        // Source node ID length (2 bytes)
        buffer.putShort((short) sourceBytes.length);

        // Source node ID
        buffer.put(sourceBytes);

        // Payload
        buffer.put(payload);

        return buffer.array();
    }

    /**
     * 从字节数组解码
     */
    public static ReplicationRecord decode(byte[] data) {
        if (data.length < HEADER_SIZE + 8 + 2) {
            throw new IllegalArgumentException(
                "Data too short for replication record: " + data.length);
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);

        // Verify magic number
        long magic = buffer.getLong();
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

        short headerSize = buffer.getShort();
        if (headerSize != HEADER_SIZE) {
            throw new IllegalArgumentException(
                "Unexpected header size: expected " + HEADER_SIZE + ", got " + headerSize);
        }

        long offset = buffer.getLong();
        long timestamp = buffer.getLong();
        ReplicationRecordType type = ReplicationRecordType.fromCode(buffer.get());

        // Skip padding
        buffer.position(buffer.position() + 7);

        // Checksum
        long storedChecksum = buffer.getLong();

        // Source node ID length
        short sourceLength = buffer.getShort();

        if (data.length < HEADER_SIZE + 8 + 2 + sourceLength) {
            throw new IllegalArgumentException(
                "Data too short for source node ID: expected " + sourceLength +
                ", available " + (data.length - HEADER_SIZE - 8 - 2));
        }

        // Read source node ID
        byte[] sourceBytes = new byte[sourceLength];
        buffer.get(sourceBytes);
        String sourceNodeId = new String(sourceBytes, java.nio.charset.StandardCharsets.UTF_8);

        // Read payload
        int payloadLength = data.length - buffer.position();
        byte[] payload = new byte[payloadLength];
        buffer.get(payload);

        // 验证 checksum
        CRC32C crc32c = new CRC32C();
        crc32c.update(data, 0, HEADER_SIZE); // header (不包含 checksum 本身)
        crc32c.update(data, HEADER_SIZE + 8, data.length - HEADER_SIZE - 8); // sourceLen + source + payload
        long calculatedChecksum = crc32c.getValue();

        if (calculatedChecksum != storedChecksum) {
            throw new IllegalArgumentException(
                "Checksum mismatch: expected " + storedChecksum + ", calculated " + calculatedChecksum);
        }

        return new ReplicationRecord(offset, timestamp, type, sourceNodeId, payload, storedChecksum);
    }

    // ==================== 辅助方法 ====================

    /**
     * 获取记录总大小
     */
    public int getTotalSize() {
        byte[] sourceBytes = sourceNodeId.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return HEADER_SIZE + 8 + 2 + sourceBytes.length + payload.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicationRecord that = (ReplicationRecord) o;
        return offset == that.offset &&
            timestamp == that.timestamp &&
            checksum == that.checksum &&
            type == that.type &&
            Objects.equals(sourceNodeId, that.sourceNodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, timestamp, type, sourceNodeId, checksum);
    }

    @Override
    public String toString() {
        return String.format("ReplicationRecord{offset=%d, type=%s, source='%s', payloadSize=%d, checksum=%d}",
            offset, type, sourceNodeId, payload.length, checksum);
    }
}

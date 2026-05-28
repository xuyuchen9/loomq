package com.loomq.replication;

import java.time.Instant;
import java.util.Objects;

/**
 * 复制确认消息
 *
 * Replica 收到并处理 ReplicationRecord 后返回的确认
 *
 * @author loomq
 * @since v0.4.8
 */
public class Ack {

    /**
     * 确认的 offset
     */
    private final long offset;

    /**
     * ACK 状态
     */
    private final AckStatus status;

    /**
     * 确认时间戳
     */
    private final Instant timestamp;

    /**
     * 可选：错误信息（失败时）
     */
    private final String errorMessage;

    /**
     * 可选：replica 当前已应用的 offset（用于进度同步）
     */
    private final long replicaAppliedOffset;

    /** Protocol version: legacy format without raftResponse */
    private static final byte ACK_VERSION_V1 = 0x01;
    /** Protocol version: includes raftResponse extension */
    private static final byte ACK_VERSION_V2 = 0x02;
    /**
     * 可选：Raft 响应 payload（Raft RPC 使用，null 表示普通 Ack）
     */
    private final byte[] raftResponse;

    public Ack(long offset, AckStatus status) {
        this(offset, status, Instant.now(), null, offset, null);
    }

    public Ack(long offset, AckStatus status, Instant timestamp,
               String errorMessage, long replicaAppliedOffset) {
        this(offset, status, timestamp, errorMessage, replicaAppliedOffset, null);
    }
    public Ack(long offset, AckStatus status, Instant timestamp,
               String errorMessage, long replicaAppliedOffset, byte[] raftResponse) {
        this.offset = offset;
        this.status = status;
        this.timestamp = timestamp != null ? timestamp : Instant.now();
        this.errorMessage = errorMessage;
        this.replicaAppliedOffset = replicaAppliedOffset;
        this.raftResponse = raftResponse;
    }

    /**
     * 创建携带 Raft 响应的 Ack。
     */
    public static Ack raftResponse(long offset, AckStatus status, byte[] raftResponse) {
        return new Ack(offset, status, Instant.now(), null, offset, raftResponse);
    }

    /**
     * 从字节数组解码。
     *
     * 支持 v1（无 raftResponse）和 v2（含 raftResponse）格式。
     * 首字节为版本号：0x01=v1, 0x02=v2。若首字节为其他值，
     * 按 v1 兼容解码（不消耗首字节作为版本号）。
     */
    public static Ack decode(byte[] data) {
        if (data.length < 27) {
            throw new IllegalArgumentException("Invalid ack data length: " + data.length);
        }

        int pos = 0;
        byte version = data[pos];

        // Detect version: v1 data starts with offset high byte (typically 0x00),
        // v2 data has explicit version byte 0x01 or 0x02.
        boolean hasVersionByte = (version == ACK_VERSION_V1 || version == ACK_VERSION_V2);
        if (hasVersionByte) {
            pos++; // consume version byte
        }

        long offset = readLong(data, pos);                    pos += 8;
        AckStatus status = AckStatus.fromCode(data[pos++]);
        long timestampMillis = readLong(data, pos);           pos += 8;
        long replicaAppliedOffset = readLong(data, pos);      pos += 8;
        short errorLength = readShort(data, pos);             pos += 2;

        String errorMessage = null;
        if (errorLength > 0) {
            errorMessage = new String(data, pos, errorLength,
                java.nio.charset.StandardCharsets.UTF_8);
            pos += errorLength;
        }

        byte[] raftResponse = null;
        if (hasVersionByte && version == ACK_VERSION_V2 && pos + 4 <= data.length) {
            int raftLen = readInt(data, pos);                 pos += 4;
            if (raftLen > 0 && pos + raftLen <= data.length) {
                raftResponse = new byte[raftLen];
                System.arraycopy(data, pos, raftResponse, 0, raftLen);
            }
        }

        return new Ack(offset, status, Instant.ofEpochMilli(timestampMillis),
            errorMessage, replicaAppliedOffset, raftResponse);
    }

    private static void writeInt(byte[] buf, int pos, int value) {
        buf[pos] = (byte) (value >>> 24);
        buf[pos + 1] = (byte) (value >>> 16);
        buf[pos + 2] = (byte) (value >>> 8);
        buf[pos + 3] = (byte) value;
    }

    private static int readInt(byte[] buf, int pos) {
        return ((buf[pos] & 0xFF) << 24)
            | ((buf[pos + 1] & 0xFF) << 16)
            | ((buf[pos + 2] & 0xFF) << 8)
            | (buf[pos + 3] & 0xFF);
    }

    public long getOffset() { return offset; }

    public AckStatus getStatus() { return status; }

    // ==================== 工厂方法 ====================

    public static Ack success(long offset) {
        return new Ack(offset, AckStatus.REPLICATED);
    }

    public static Ack persisted(long offset) {
        return new Ack(offset, AckStatus.PERSISTED);
    }

    public static Ack failed(long offset, String errorMessage) {
        return new Ack(offset, AckStatus.FAILED, Instant.now(), errorMessage, -1);
    }

    public static Ack timeout(long offset) {
        return new Ack(offset, AckStatus.TIMEOUT, Instant.now(),
            "Replication timeout", -1);
    }

    public static Ack rejected(long offset, String reason) {
        return new Ack(offset, AckStatus.REJECTED, Instant.now(),
            "Rejected: " + reason, -1);
    }

    public Instant getTimestamp() { return timestamp; }

    // ==================== 编码/解码 ====================

    public String getErrorMessage() { return errorMessage; }

    public long getReplicaAppliedOffset() { return replicaAppliedOffset; }

    public byte[] getRaftResponse() { return raftResponse; }

    public boolean isSuccess() { return status.isSuccess(); }

    // ==================== 辅助方法 ====================

    private static void writeLong(byte[] buf, int pos, long value) {
        buf[pos] = (byte) (value >>> 56);
        buf[pos + 1] = (byte) (value >>> 48);
        buf[pos + 2] = (byte) (value >>> 40);
        buf[pos + 3] = (byte) (value >>> 32);
        buf[pos + 4] = (byte) (value >>> 24);
        buf[pos + 5] = (byte) (value >>> 16);
        buf[pos + 6] = (byte) (value >>> 8);
        buf[pos + 7] = (byte) value;
    }

    private static long readLong(byte[] buf, int pos) {
        return ((long) (buf[pos] & 0xFF) << 56)
            | ((long) (buf[pos + 1] & 0xFF) << 48)
            | ((long) (buf[pos + 2] & 0xFF) << 40)
            | ((long) (buf[pos + 3] & 0xFF) << 32)
            | ((long) (buf[pos + 4] & 0xFF) << 24)
            | ((long) (buf[pos + 5] & 0xFF) << 16)
            | ((long) (buf[pos + 6] & 0xFF) << 8)
            | (buf[pos + 7] & 0xFF);
    }

    private static void writeShort(byte[] buf, int pos, short value) {
        buf[pos] = (byte) (value >>> 8);
        buf[pos + 1] = (byte) value;
    }

    private static short readShort(byte[] buf, int pos) {
        return (short) (((buf[pos] & 0xFF) << 8) | (buf[pos + 1] & 0xFF));
    }

    public boolean isFailure() { return status.isFailure(); }

    /**
     * 编码为字节数组（网络传输）
     *
     * 格式（v2）：
     * - version: 1 byte (0x02)
     * - offset: 8 bytes (long)
     * - status: 1 byte
     * - timestamp: 8 bytes (long millis)
     * - replicaAppliedOffset: 8 bytes (long)
     * - errorLength: 2 bytes (short)
     * - errorBytes: variable
     * - raftResponseLength: 4 bytes (int)
     * - raftResponse: variable
     */
    public byte[] encode() {
        byte[] errorBytes = errorMessage != null ?
            errorMessage.getBytes(java.nio.charset.StandardCharsets.UTF_8) : new byte[0];
        byte[] raftBytes = raftResponse != null ? raftResponse : new byte[0];

        int totalLength = 1 + 8 + 1 + 8 + 8 + 2 + errorBytes.length + 4 + raftBytes.length;
        byte[] result = new byte[totalLength];
        int pos = 0;

        result[pos++] = ACK_VERSION_V2;
        writeLong(result, pos, offset);           pos += 8;
        result[pos++] = status.getCode();
        writeLong(result, pos, timestamp.toEpochMilli()); pos += 8;
        writeLong(result, pos, replicaAppliedOffset);     pos += 8;
        writeShort(result, pos, (short) errorBytes.length); pos += 2;
        System.arraycopy(errorBytes, 0, result, pos, errorBytes.length); pos += errorBytes.length;
        writeInt(result, pos, raftBytes.length);  pos += 4;
        System.arraycopy(raftBytes, 0, result, pos, raftBytes.length);

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Ack ack = (Ack) o;
        return offset == ack.offset &&
            replicaAppliedOffset == ack.replicaAppliedOffset &&
            status == ack.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, status, replicaAppliedOffset);
    }

    @Override
    public String toString() {
        return String.format("Ack{offset=%d, status=%s, appliedOffset=%d, raftResp=%s, error='%s'}",
            offset, status, replicaAppliedOffset,
            raftResponse != null ? raftResponse.length + "B" : "none",
            errorMessage != null ? errorMessage : "none");
    }
}

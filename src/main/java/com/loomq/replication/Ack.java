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

    public Ack(long offset, AckStatus status) {
        this(offset, status, Instant.now(), null, offset);
    }

    public Ack(long offset, AckStatus status, Instant timestamp,
               String errorMessage, long replicaAppliedOffset) {
        this.offset = offset;
        this.status = status;
        this.timestamp = timestamp != null ? timestamp : Instant.now();
        this.errorMessage = errorMessage;
        this.replicaAppliedOffset = replicaAppliedOffset;
    }

    public long getOffset() {
        return offset;
    }

    public AckStatus getStatus() {
        return status;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public long getReplicaAppliedOffset() {
        return replicaAppliedOffset;
    }

    /**
     * 是否成功
     */
    public boolean isSuccess() {
        return status.isSuccess();
    }

    /**
     * 是否失败
     */
    public boolean isFailure() {
        return status.isFailure();
    }

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

    // ==================== 编码/解码 ====================

    /**
     * 编码为字节数组（网络传输）
     *
     * 格式：
     * - offset: 8 bytes (long)
     * - status: 1 byte
     * - timestamp: 8 bytes (long millis)
     * - replicaAppliedOffset: 8 bytes (long)
     * - errorLength: 2 bytes (short)
     * - errorBytes: variable
     */
    public byte[] encode() {
        byte[] errorBytes = errorMessage != null ?
            errorMessage.getBytes(java.nio.charset.StandardCharsets.UTF_8) : new byte[0];

        int totalLength = 8 + 1 + 8 + 8 + 2 + errorBytes.length;
        byte[] result = new byte[totalLength];
        int pos = 0;

        // offset
        writeLong(result, pos, offset);
        pos += 8;

        // status
        result[pos++] = status.getCode();

        // timestamp
        writeLong(result, pos, timestamp.toEpochMilli());
        pos += 8;

        // replicaAppliedOffset
        writeLong(result, pos, replicaAppliedOffset);
        pos += 8;

        // error message length
        writeShort(result, pos, (short) errorBytes.length);
        pos += 2;

        // error message
        System.arraycopy(errorBytes, 0, result, pos, errorBytes.length);

        return result;
    }

    /**
     * 从字节数组解码
     */
    public static Ack decode(byte[] data) {
        if (data.length < 27) {
            throw new IllegalArgumentException("Invalid ack data length: " + data.length);
        }

        int pos = 0;

        long offset = readLong(data, pos);
        pos += 8;

        AckStatus status = AckStatus.fromCode(data[pos++]);

        long timestampMillis = readLong(data, pos);
        pos += 8;

        long replicaAppliedOffset = readLong(data, pos);
        pos += 8;

        short errorLength = readShort(data, pos);
        pos += 2;

        String errorMessage = null;
        if (errorLength > 0) {
            errorMessage = new String(data, pos, errorLength,
                java.nio.charset.StandardCharsets.UTF_8);
        }

        return new Ack(offset, status, Instant.ofEpochMilli(timestampMillis),
            errorMessage, replicaAppliedOffset);
    }

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
        return String.format("Ack{offset=%d, status=%s, appliedOffset=%d, error='%s'}",
            offset, status, replicaAppliedOffset,
            errorMessage != null ? errorMessage : "none");
    }
}

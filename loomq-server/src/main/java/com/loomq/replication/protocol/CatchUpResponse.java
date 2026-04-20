package com.loomq.replication.protocol;

import com.loomq.replication.ReplicationRecord;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * WAL 追赶响应消息
 *
 * Week 4 Phase 4 - T4.2: Primary 返回给 Replica 的 WAL 记录
 *
 * 消息格式：
 * - Magic (4 bytes): 0x4C515256 (LQCZ - LoomQ Catch up responZe)
 * - Version (2 bytes): 1
 * - Success (1 byte): 0x01 = success, 0x00 = failure
 * - Error Message Length (2 bytes) - only if success = false
 * - Error Message (variable) - only if success = false
 * - End Offset (8 bytes)
 * - Has More (1 byte): 0x01 = has more, 0x00 = no more
 * - Primary Offset (8 bytes): primary 当前最新 offset
 * - Record Count (4 bytes)
 * - Records (variable): 每个记录前带 4 字节长度
 *
 * @author loomq
 * @since v0.4.8
 */
public class CatchUpResponse {

    // ==================== 常量 ====================

    /**
     * Magic number: "LQCZ" (LoomQ Catch up responZe)
     */
    public static final int MAGIC_NUMBER = 0x4C51525A;

    /**
     * 协议版本
     */
    public static final short VERSION = 1;

    // ==================== 字段 ====================

    private final boolean success;
    private final String errorMessage;
    private final List<ReplicationRecord> records;
    private final long endOffset;
    private final boolean hasMore;
    private final long primaryOffset;

    // ==================== 构造函数 ====================

    /**
     * 创建成功响应
     *
     * @param records WAL 记录列表
     * @param endOffset 结束 offset
     * @param hasMore 是否还有更多数据
     * @param primaryOffset Primary 当前最新 offset
     */
    public CatchUpResponse(List<ReplicationRecord> records, long endOffset,
                           boolean hasMore, long primaryOffset) {
        this(true, null, records, endOffset, hasMore, primaryOffset);
    }

    /**
     * 创建失败响应
     *
     * @param errorMessage 错误消息
     */
    public CatchUpResponse(String errorMessage) {
        this(false, errorMessage, Collections.emptyList(), 0, false, 0);
    }

    /**
     * 创建完整响应
     */
    public CatchUpResponse(boolean success, String errorMessage,
                           List<ReplicationRecord> records, long endOffset,
                           boolean hasMore, long primaryOffset) {
        this.success = success;
        this.errorMessage = errorMessage;
        this.records = records != null ? new ArrayList<>(records) : new ArrayList<>();
        this.endOffset = endOffset;
        this.hasMore = hasMore;
        this.primaryOffset = primaryOffset;
    }

    // ==================== Getter ====================

    public boolean isSuccess() {
        return success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public List<ReplicationRecord> getRecords() {
        return Collections.unmodifiableList(records);
    }

    public long getEndOffset() {
        return endOffset;
    }

    public boolean hasMore() {
        return hasMore;
    }

    public long getPrimaryOffset() {
        return primaryOffset;
    }

    /**
     * 获取记录数量
     */
    public int getRecordCount() {
        return records.size();
    }

    // ==================== 编码/解码 ====================

    /**
     * 编码为字节数组
     */
    public byte[] encode() {
        // 计算总大小
        int totalSize = 4 + 2 + 1; // Magic + Version + Success

        if (!success) {
            byte[] errorBytes = errorMessage != null ?
                errorMessage.getBytes(StandardCharsets.UTF_8) : new byte[0];
            totalSize += 2 + errorBytes.length;
        } else {
            totalSize += 8 + 1 + 8 + 4; // endOffset + hasMore + primaryOffset + recordCount

            // 计算 records 大小
            for (ReplicationRecord record : records) {
                byte[] recordBytes = record.encode();
                totalSize += 4 + recordBytes.length; // length prefix + data
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        // Magic (4 bytes)
        buffer.putInt(MAGIC_NUMBER);

        // Version (2 bytes)
        buffer.putShort(VERSION);

        // Success (1 byte)
        buffer.put(success ? (byte) 0x01 : (byte) 0x00);

        if (!success) {
            // Error Message
            byte[] errorBytes = errorMessage != null ?
                errorMessage.getBytes(StandardCharsets.UTF_8) : new byte[0];
            buffer.putShort((short) errorBytes.length);
            buffer.put(errorBytes);
        } else {
            // End Offset (8 bytes)
            buffer.putLong(endOffset);

            // Has More (1 byte)
            buffer.put(hasMore ? (byte) 0x01 : (byte) 0x00);

            // Primary Offset (8 bytes)
            buffer.putLong(primaryOffset);

            // Record Count (4 bytes)
            buffer.putInt(records.size());

            // Records
            for (ReplicationRecord record : records) {
                byte[] recordBytes = record.encode();
                buffer.putInt(recordBytes.length);
                buffer.put(recordBytes);
            }
        }

        return buffer.array();
    }

    /**
     * 从字节数组解码
     */
    public static CatchUpResponse decode(byte[] data) {
        if (data.length < 7) { // 最小长度: 4 + 2 + 1 = 7
            throw new IllegalArgumentException("Data too short for CatchUpResponse: " + data.length);
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

        // Success flag
        boolean success = buffer.get() == (byte) 0x01;

        if (!success) {
            // Error Message
            short errorLen = buffer.getShort();
            if (errorLen < 0 || errorLen > 1024) {
                throw new IllegalArgumentException("Invalid error message length: " + errorLen);
            }
            byte[] errorBytes = new byte[errorLen];
            buffer.get(errorBytes);
            String errorMessage = new String(errorBytes, StandardCharsets.UTF_8);

            return new CatchUpResponse(errorMessage);
        }

        // End Offset
        long endOffset = buffer.getLong();

        // Has More
        boolean hasMore = buffer.get() == (byte) 0x01;

        // Primary Offset
        long primaryOffset = buffer.getLong();

        // Record Count
        int recordCount = buffer.getInt();

        // Records
        List<ReplicationRecord> records = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            if (buffer.remaining() < 4) {
                throw new IllegalArgumentException(
                    "Insufficient data for record length at index " + i);
            }
            int recordLen = buffer.getInt();
            if (recordLen < 0 || recordLen > 100 * 1024 * 1024) { // max 100MB per record
                throw new IllegalArgumentException("Invalid record length: " + recordLen);
            }
            if (buffer.remaining() < recordLen) {
                throw new IllegalArgumentException(
                    "Insufficient data for record at index " + i + ", expected " + recordLen);
            }
            byte[] recordBytes = new byte[recordLen];
            buffer.get(recordBytes);
            records.add(ReplicationRecord.decode(recordBytes));
        }

        return new CatchUpResponse(records, endOffset, hasMore, primaryOffset);
    }

    // ==================== 便捷构造器 ====================

    /**
     * 创建空的成功响应（无更多数据）
     */
    public static CatchUpResponse empty(long primaryOffset) {
        return new CatchUpResponse(
            Collections.emptyList(), primaryOffset, false, primaryOffset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CatchUpResponse that = (CatchUpResponse) o;
        return success == that.success &&
               endOffset == that.endOffset &&
               hasMore == that.hasMore &&
               primaryOffset == that.primaryOffset &&
               Objects.equals(errorMessage, that.errorMessage) &&
               records.equals(that.records);
    }

    @Override
    public int hashCode() {
        return Objects.hash(success, errorMessage, records, endOffset, hasMore, primaryOffset);
    }

    @Override
    public String toString() {
        if (!success) {
            return String.format("CatchUpResponse{success=false, error='%s'}", errorMessage);
        }
        return String.format("CatchUpResponse{success=true, records=%d, endOffset=%d, " +
                           "hasMore=%s, primaryOffset=%d}",
            records.size(), endOffset, hasMore, primaryOffset);
    }
}

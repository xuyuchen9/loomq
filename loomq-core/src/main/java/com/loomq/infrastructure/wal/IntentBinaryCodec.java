package com.loomq.infrastructure.wal;

import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.ExpiredAction;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.RedeliveryPolicy;
import com.loomq.replication.AckLevel;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Intent 二进制编解码器
 *
 * 格式设计原则：
 * 1. 紧凑：字段类型(1B) + 长度(4B) + 值(N)
 * 2. 可扩展：字段类型允许新增，旧代码可跳过未知字段
 * 3. 零拷贝：直接操作 ByteBuffer，避免中间对象
 *
 * 二进制格式：
 * | 字段数量 (1B) |
 * | 字段类型 (1B) | 字段长度 (4B, 大端) | 字段值 (N) | (重复字段数次)
 *
 * 字段类型定义：
 * 0x01: intentId (String)
 * 0x02: status (byte ordinal)
 * 0x03: createdAt (long epochMillis)
 * 0x04: updatedAt (long epochMillis)
 * 0x05: executeAt (long epochMillis)
 * 0x06: deadline (long epochMillis)
 * 0x07: expiredAction (byte ordinal)
 * 0x08: precisionTier (byte ordinal)
 * 0x09: shardKey (String)
 * 0x0A: shardId (String)
 * 0x0B: ackLevel (byte ordinal)
 * 0x0C: callback (嵌套结构)
 * 0x0D: redelivery (嵌套结构)
 * 0x0E: idempotencyKey (String)
 * 0x0F: tags (Map<String, String>)
 * 0x10: attempts (int)
 * 0x11: lastDeliveryId (String)
 *
 * String 编码：length(2B) + UTF-8 bytes
 * Map 编码：entryCount(4B) + [key(String) + value(String)]*
 *
 * @author loomq
 * @since v0.6.1
 */
public class IntentBinaryCodec {

    // 字段类型常量
    private static final byte FIELD_INTENT_ID = 0x01;
    private static final byte FIELD_STATUS = 0x02;
    private static final byte FIELD_CREATED_AT = 0x03;
    private static final byte FIELD_UPDATED_AT = 0x04;
    private static final byte FIELD_EXECUTE_AT = 0x05;
    private static final byte FIELD_DEADLINE = 0x06;
    private static final byte FIELD_EXPIRED_ACTION = 0x07;
    private static final byte FIELD_PRECISION_TIER = 0x08;
    private static final byte FIELD_SHARD_KEY = 0x09;
    private static final byte FIELD_SHARD_ID = 0x0A;
    private static final byte FIELD_ACK_LEVEL = 0x0B;
    private static final byte FIELD_CALLBACK = 0x0C;
    private static final byte FIELD_REDELIVERY = 0x0D;
    private static final byte FIELD_IDEMPOTENCY_KEY = 0x0E;
    private static final byte FIELD_TAGS = 0x0F;
    private static final byte FIELD_ATTEMPTS = 0x10;
    private static final byte FIELD_LAST_DELIVERY_ID = 0x11;

    // 预分配缓冲区大小（估算平均 Intent 大小）
    private static final int DEFAULT_BUFFER_SIZE = 512;

    /**
     * 编码 Intent 为字节数组
     */
    public static byte[] encode(Intent intent) {
        ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);

        // 预留字段数量位置
        int fieldCountPos = buffer.position();
        buffer.put((byte) 0);  // 占位，稍后填充

        byte fieldCount = 0;

        // 系统字段（必编码）
        if (intent.getIntentId() != null) {
            writeStringField(buffer, FIELD_INTENT_ID, intent.getIntentId());
            fieldCount++;
        }
        if (intent.getStatus() != null) {
            writeByteField(buffer, FIELD_STATUS, (byte) intent.getStatus().ordinal());
            fieldCount++;
        }
        if (intent.getCreatedAt() != null) {
            writeLongField(buffer, FIELD_CREATED_AT, intent.getCreatedAt().toEpochMilli());
            fieldCount++;
        }
        if (intent.getUpdatedAt() != null) {
            writeLongField(buffer, FIELD_UPDATED_AT, intent.getUpdatedAt().toEpochMilli());
            fieldCount++;
        }

        // 调度字段
        if (intent.getExecuteAt() != null) {
            writeLongField(buffer, FIELD_EXECUTE_AT, intent.getExecuteAt().toEpochMilli());
            fieldCount++;
        }
        if (intent.getDeadline() != null) {
            writeLongField(buffer, FIELD_DEADLINE, intent.getDeadline().toEpochMilli());
            fieldCount++;
        }
        if (intent.getExpiredAction() != null) {
            writeByteField(buffer, FIELD_EXPIRED_ACTION, (byte) intent.getExpiredAction().ordinal());
            fieldCount++;
        }
        if (intent.getPrecisionTier() != null) {
            writeByteField(buffer, FIELD_PRECISION_TIER, (byte) intent.getPrecisionTier().ordinal());
            fieldCount++;
        }

        // 路由字段
        if (intent.getShardKey() != null) {
            writeStringField(buffer, FIELD_SHARD_KEY, intent.getShardKey());
            fieldCount++;
        }
        if (intent.getShardId() != null) {
            writeStringField(buffer, FIELD_SHARD_ID, intent.getShardId());
            fieldCount++;
        }

        // 可靠性字段
        if (intent.getAckLevel() != null) {
            writeByteField(buffer, FIELD_ACK_LEVEL, (byte) intent.getAckLevel().ordinal());
            fieldCount++;
        }

        // 回调字段
        if (intent.getCallback() != null) {
            byte[] callbackBytes = encodeCallback(intent.getCallback());
            writeBytesField(buffer, FIELD_CALLBACK, callbackBytes);
            fieldCount++;
        }

        // 重投策略
        if (intent.getRedelivery() != null) {
            byte[] redeliveryBytes = encodeRedelivery(intent.getRedelivery());
            writeBytesField(buffer, FIELD_REDELIVERY, redeliveryBytes);
            fieldCount++;
        }

        // 业务字段
        if (intent.getIdempotencyKey() != null) {
            writeStringField(buffer, FIELD_IDEMPOTENCY_KEY, intent.getIdempotencyKey());
            fieldCount++;
        }
        if (intent.getTags() != null && !intent.getTags().isEmpty()) {
            byte[] tagsBytes = encodeTags(intent.getTags());
            writeBytesField(buffer, FIELD_TAGS, tagsBytes);
            fieldCount++;
        }

        // 投递统计
        if (intent.getAttempts() > 0) {
            writeIntField(buffer, FIELD_ATTEMPTS, intent.getAttempts());
            fieldCount++;
        }
        if (intent.getLastDeliveryId() != null) {
            writeStringField(buffer, FIELD_LAST_DELIVERY_ID, intent.getLastDeliveryId());
            fieldCount++;
        }

        // 回填字段数量
        buffer.put(fieldCountPos, fieldCount);

        // 提取结果
        buffer.flip();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    /**
     * 解码字节数组为 Intent
     */
    public static Intent decode(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        String intentId = null;
        IntentStatus status = null;
        Instant createdAt = null;
        Instant updatedAt = null;
        Instant executeAt = null;
        Instant deadline = null;
        ExpiredAction expiredAction = null;
        PrecisionTier precisionTier = null;
        String shardKey = null;
        String shardId = null;
        AckLevel ackLevel = null;
        Callback callback = null;
        RedeliveryPolicy redelivery = null;
        String idempotencyKey = null;
        Map<String, String> tags = null;
        int attempts = 0;
        String lastDeliveryId = null;

        byte fieldCount = buffer.get();

        for (int i = 0; i < fieldCount; i++) {
            byte fieldType = buffer.get();
            int fieldLen = buffer.getInt();

            switch (fieldType) {
                case FIELD_INTENT_ID -> {
                    intentId = readString(buffer, fieldLen);
                }
                case FIELD_STATUS -> status = IntentStatus.values()[buffer.get()];
                case FIELD_CREATED_AT -> createdAt = Instant.ofEpochMilli(buffer.getLong());
                case FIELD_UPDATED_AT -> updatedAt = Instant.ofEpochMilli(buffer.getLong());
                case FIELD_EXECUTE_AT -> executeAt = Instant.ofEpochMilli(buffer.getLong());
                case FIELD_DEADLINE -> deadline = Instant.ofEpochMilli(buffer.getLong());
                case FIELD_EXPIRED_ACTION -> expiredAction = ExpiredAction.values()[buffer.get()];
                case FIELD_PRECISION_TIER -> precisionTier = PrecisionTier.values()[buffer.get()];
                case FIELD_SHARD_KEY -> shardKey = readString(buffer, fieldLen);
                case FIELD_SHARD_ID -> shardId = readString(buffer, fieldLen);
                case FIELD_ACK_LEVEL -> ackLevel = AckLevel.values()[buffer.get()];
                case FIELD_CALLBACK -> {
                    byte[] callbackBytes = new byte[fieldLen];
                    buffer.get(callbackBytes);
                    callback = decodeCallback(callbackBytes);
                }
                case FIELD_REDELIVERY -> {
                    byte[] redeliveryBytes = new byte[fieldLen];
                    buffer.get(redeliveryBytes);
                    redelivery = decodeRedelivery(redeliveryBytes);
                }
                case FIELD_IDEMPOTENCY_KEY -> idempotencyKey = readString(buffer, fieldLen);
                case FIELD_TAGS -> {
                    byte[] tagsBytes = new byte[fieldLen];
                    buffer.get(tagsBytes);
                    tags = decodeTags(tagsBytes);
                }
                case FIELD_ATTEMPTS -> {
                    attempts = buffer.getInt();
                }
                case FIELD_LAST_DELIVERY_ID -> lastDeliveryId = readString(buffer, fieldLen);
                default -> buffer.position(buffer.position() + fieldLen); // 跳过未知字段
            }
        }

        return Intent.restore(
            intentId,
            status,
            createdAt,
            updatedAt,
            executeAt,
            deadline,
            expiredAction,
            precisionTier,
            shardKey,
            shardId,
            ackLevel,
            callback,
            redelivery,
            idempotencyKey,
            tags,
            attempts,
            lastDeliveryId
        );
    }

    // ========== 辅助编码方法 ==========

    private static void writeByteField(ByteBuffer buffer, byte type, byte value) {
        buffer.put(type);
        buffer.putInt(1);
        buffer.put(value);
    }

    private static void writeIntField(ByteBuffer buffer, byte type, int value) {
        buffer.put(type);
        buffer.putInt(4);
        buffer.putInt(value);
    }

    private static void writeLongField(ByteBuffer buffer, byte type, long value) {
        buffer.put(type);
        buffer.putInt(8);
        buffer.putLong(value);
    }

    private static void writeStringField(ByteBuffer buffer, byte type, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        buffer.put(type);
        buffer.putInt(2 + bytes.length);
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    private static void writeBytesField(ByteBuffer buffer, byte type, byte[] value) {
        buffer.put(type);
        buffer.putInt(value.length);
        buffer.put(value);
    }

    private static String readString(ByteBuffer buffer, int fieldLen) {
        short len = buffer.getShort();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    // ========== Callback 编解码 ==========

    private static byte[] encodeCallback(Callback callback) {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        byte fieldCount = 0;

        int fieldCountPos = buffer.position();
        buffer.put((byte) 0);

        if (callback.getUrl() != null) {
            writeStringField(buffer, (byte) 0x01, callback.getUrl());
            fieldCount++;
        }
        if (callback.getMethod() != null) {
            writeStringField(buffer, (byte) 0x02, callback.getMethod());
            fieldCount++;
        }
        if (callback.getHeaders() != null && !callback.getHeaders().isEmpty()) {
            byte[] headersBytes = encodeTags(callback.getHeaders());
            writeBytesField(buffer, (byte) 0x03, headersBytes);
            fieldCount++;
        }
        if (callback.getBody() != null) {
            String bodyStr = callback.getBody() instanceof String
                ? (String) callback.getBody()
                : callback.getBody().toString();
            writeStringField(buffer, (byte) 0x04, bodyStr);
            fieldCount++;
        }

        buffer.put(fieldCountPos, fieldCount);
        buffer.flip();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private static Callback decodeCallback(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        Callback callback = new Callback();

        byte fieldCount = buffer.get();
        for (int i = 0; i < fieldCount; i++) {
            byte type = buffer.get();
            int len = buffer.getInt();
            switch (type) {
                case 0x01 -> callback.setUrl(readString(buffer, len));
                case 0x02 -> callback.setMethod(readString(buffer, len));
                case 0x03 -> {
                    byte[] headersBytes = new byte[len];
                    buffer.get(headersBytes);
                    callback.setHeaders(decodeTags(headersBytes));
                }
                case 0x04 -> callback.setBody(readString(buffer, len));
                default -> buffer.position(buffer.position() + len);
            }
        }
        return callback;
    }

    // ========== RedeliveryPolicy 编解码 ==========

    private static byte[] encodeRedelivery(RedeliveryPolicy policy) {
        ByteBuffer buffer = ByteBuffer.allocate(64);
        byte fieldCount = 0;

        int fieldCountPos = buffer.position();
        buffer.put((byte) 0);

        writeIntField(buffer, (byte) 0x01, policy.getMaxAttempts());
        fieldCount++;

        buffer.put(fieldCountPos, fieldCount);
        buffer.flip();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private static RedeliveryPolicy decodeRedelivery(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        RedeliveryPolicy policy = new RedeliveryPolicy();

        byte fieldCount = buffer.get();
        for (int i = 0; i < fieldCount; i++) {
            byte type = buffer.get();
            int len = buffer.getInt();
            if (type == 0x01) {
                policy.setMaxAttempts(buffer.getInt());
            } else {
                buffer.position(buffer.position() + len);
            }
        }
        return policy;
    }

    // ========== Tags 编解码 ==========

    private static byte[] encodeTags(Map<String, String> tags) {
        int estimatedSize = 4 + tags.size() * 64;
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);

        buffer.putInt(tags.size());
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            writeString(buffer, entry.getKey());
            writeString(buffer, entry.getValue());
        }

        buffer.flip();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private static Map<String, String> decodeTags(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        Map<String, String> tags = new HashMap<>();

        int count = buffer.getInt();
        for (int i = 0; i < count; i++) {
            String key = readString(buffer);
            String value = readString(buffer);
            tags.put(key, value);
        }
        return tags;
    }

    private static void writeString(ByteBuffer buffer, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    private static String readString(ByteBuffer buffer) {
        short len = buffer.getShort();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}

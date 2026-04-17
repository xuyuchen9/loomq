package com.loomq.http.netty;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.replication.AckLevel;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Intent 响应序列化器（零拷贝优化）
 *
 * 直接写入 ByteBuf，避免中间 byte[] 拷贝。
 * 预期性能提升：序列化耗时降低 50-70%。
 *
 * @author loomq
 * @since v0.6.0
 */
public final class IntentResponseSerializer {

    // 预定义 JSON 模板常量
    private static final byte[] PREFIX = "{\"intentId\":\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] STATUS_FIELD = "\",\"status\":\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] EXECUTE_AT_FIELD = "\",\"executeAt\":\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DEADLINE_FIELD = "\",\"deadline\":\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SHARD_KEY_FIELD = "\",\"shardKey\":\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PRECISION_TIER_FIELD = "\",\"precisionTier\":\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ACK_LEVEL_FIELD = "\",\"ackLevel\":\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SUFFIX = "\"}".getBytes(StandardCharsets.UTF_8);

    // 空值占位符（不写引号，因为模板已包含引号）
    private static final byte[] EMPTY_VALUE = new byte[0];

    private IntentResponseSerializer() {}

    /**
     * 序列化 Intent 响应到 ByteBuf
     *
     * @param intent Intent 对象
     * @param buf 目标 ByteBuf
     */
    public static void writeIntentResponse(Intent intent, ByteBuf buf) {
        buf.writeBytes(PREFIX);

        // intentId
        writeEscapedString(intent.getIntentId(), buf);

        buf.writeBytes(STATUS_FIELD);

        // status
        IntentStatus status = intent.getStatus();
        if (status != null) {
            buf.writeBytes(status.name().getBytes(StandardCharsets.UTF_8));
        }

        buf.writeBytes(EXECUTE_AT_FIELD);

        // executeAt
        writeInstant(intent.getExecuteAt(), buf);

        buf.writeBytes(DEADLINE_FIELD);

        // deadline
        writeInstant(intent.getDeadline(), buf);

        buf.writeBytes(SHARD_KEY_FIELD);

        // shardKey
        writeEscapedString(intent.getShardKey(), buf);

        buf.writeBytes(PRECISION_TIER_FIELD);

        // precisionTier
        PrecisionTier tier = intent.getPrecisionTier();
        if (tier != null) {
            buf.writeBytes(tier.name().getBytes(StandardCharsets.UTF_8));
        } else {
            buf.writeBytes(PrecisionTier.STANDARD.name().getBytes(StandardCharsets.UTF_8));
        }

        buf.writeBytes(ACK_LEVEL_FIELD);

        // ackLevel
        AckLevel ackLevel = intent.getAckLevel();
        if (ackLevel != null) {
            buf.writeBytes(ackLevel.name().getBytes(StandardCharsets.UTF_8));
        } else {
            buf.writeBytes(AckLevel.DURABLE.name().getBytes(StandardCharsets.UTF_8));
        }

        buf.writeBytes(SUFFIX);
    }

    /**
     * 写入 ISO-8601 格式时间戳
     */
    private static void writeInstant(Instant instant, ByteBuf buf) {
        if (instant == null) {
            // 不写任何内容，模板已包含引号
            return;
        }
        // Instant.toString() 输出 ISO-8601 格式，如 2026-04-10T12:30:45.123456789Z
        String str = instant.toString();
        buf.writeBytes(str.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 写入转义字符串
     *
     * 仅当字符串包含需要转义的字符时才进行转义处理，
     * 绝大多数情况下（UUID、枚举值等）走快速通道。
     */
    private static void writeEscapedString(String value, ByteBuf buf) {
        if (value == null) {
            // 不写任何内容，模板已包含引号
            return;
        }

        // 快速检查是否需要转义
        int len = value.length();
        int escapePos = -1;
        for (int i = 0; i < len; i++) {
            char c = value.charAt(i);
            if (c == '"' || c == '\\' || c < ' ') {
                escapePos = i;
                break;
            }
        }

        if (escapePos < 0) {
            // 快速通道：无需转义，直接写入
            buf.writeBytes(value.getBytes(StandardCharsets.UTF_8));
        } else {
            // 慢速通道：需要转义
            writeEscapedStringSlow(value, buf, escapePos);
        }
    }

    /**
     * 慢速通道：处理需要转义的字符串
     */
    private static void writeEscapedStringSlow(String value, ByteBuf buf, int startEscapePos) {
        int len = value.length();

        // 先写入转义位置之前的部分
        if (startEscapePos > 0) {
            buf.writeBytes(value.substring(0, startEscapePos).getBytes(StandardCharsets.UTF_8));
        }

        // 处理剩余部分
        for (int i = startEscapePos; i < len; i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> buf.writeBytes("\\\"".getBytes(StandardCharsets.UTF_8));
                case '\\' -> buf.writeBytes("\\\\".getBytes(StandardCharsets.UTF_8));
                case '\n' -> buf.writeBytes("\\n".getBytes(StandardCharsets.UTF_8));
                case '\r' -> buf.writeBytes("\\r".getBytes(StandardCharsets.UTF_8));
                case '\t' -> buf.writeBytes("\\t".getBytes(StandardCharsets.UTF_8));
                default -> {
                    if (c < ' ') {
                        // 其他控制字符，用 uXXXX 格式
                        buf.writeByte('\\');
                        buf.writeByte('u');
                        buf.writeBytes(String.format("%04x", (int) c).getBytes(StandardCharsets.UTF_8));
                    } else {
                        // 普通字符，直接写入（单字符）
                        buf.writeByte((byte) c);
                    }
                }
            }
        }
    }

    /**
     * 序列化简单响应（用于测试和错误响应）
     *
     * @param intentId Intent ID
     * @param status 状态
     * @param executeAt 执行时间
     * @param deadline 截止时间
     * @param shardKey 分片键
     * @param precisionTier 精度档位
     * @param ackLevel ACK 级别
     * @param buf 目标 ByteBuf
     */
    public static void writeIntentResponse(
            String intentId,
            IntentStatus status,
            Instant executeAt,
            Instant deadline,
            String shardKey,
            PrecisionTier precisionTier,
            AckLevel ackLevel,
            ByteBuf buf) {

        buf.writeBytes(PREFIX);
        writeEscapedString(intentId, buf);
        buf.writeBytes(STATUS_FIELD);
        if (status != null) {
            buf.writeBytes(status.name().getBytes(StandardCharsets.UTF_8));
        }
        buf.writeBytes(EXECUTE_AT_FIELD);
        writeInstant(executeAt, buf);
        buf.writeBytes(DEADLINE_FIELD);
        writeInstant(deadline, buf);
        buf.writeBytes(SHARD_KEY_FIELD);
        writeEscapedString(shardKey, buf);
        buf.writeBytes(PRECISION_TIER_FIELD);
        buf.writeBytes((precisionTier != null ? precisionTier : PrecisionTier.STANDARD)
                .name().getBytes(StandardCharsets.UTF_8));
        buf.writeBytes(ACK_LEVEL_FIELD);
        buf.writeBytes((ackLevel != null ? ackLevel : AckLevel.DURABLE)
                .name().getBytes(StandardCharsets.UTF_8));
        buf.writeBytes(SUFFIX);
    }

    /**
     * 序列化为 byte[]（用于测试）
     */
    public static byte[] toBytes(Intent intent) {
        // 预估大小：基础模板 + 字段值
        int estimatedSize = 128 +
                (intent.getIntentId() != null ? intent.getIntentId().length() : 0) +
                (intent.getShardKey() != null ? intent.getShardKey().length() : 0);

        io.netty.buffer.ByteBuf buf = io.netty.buffer.Unpooled.buffer(estimatedSize);
        try {
            writeIntentResponse(intent, buf);
            byte[] result = new byte[buf.readableBytes()];
            buf.readBytes(result);
            return result;
        } finally {
            buf.release();
        }
    }
}

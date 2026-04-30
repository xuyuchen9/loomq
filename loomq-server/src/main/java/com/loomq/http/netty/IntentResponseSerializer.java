package com.loomq.http.netty;

import com.loomq.api.IntentResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Intent 响应序列化器（零拷贝优化）
 *
 * 直接写入 ByteBuf，绕过 Jackson 和中间 byte[] 分配。
 */
public final class IntentResponseSerializer {

    private static final byte[] OPEN_OBJECT = "{".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CLOSE_OBJECT = "}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COMMA = ",".getBytes(StandardCharsets.UTF_8);
    private static final byte[] QUOTE = "\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] NULL_LITERAL = "null".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ESCAPED_QUOTE = "\\\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ESCAPED_BACKSLASH = "\\\\".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ESCAPED_NEWLINE = "\\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ESCAPED_CARRIAGE_RETURN = "\\r".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ESCAPED_TAB = "\\t".getBytes(StandardCharsets.UTF_8);
    private static final byte[] MIN_INT_BYTES = "-2147483648".getBytes(StandardCharsets.UTF_8);
    private static final DateTimeFormatter ISO_INSTANT = DateTimeFormatter.ISO_INSTANT;

    private static final byte[] FIELD_INTENT_ID = "\"intentId\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_STATUS = "\"status\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_EXECUTE_AT = "\"executeAt\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_DEADLINE = "\"deadline\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_EXPIRED_ACTION = "\"expiredAction\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_PRECISION_TIER = "\"precisionTier\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_WAL_MODE = "\"walMode\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_ACK_LEVEL = "\"ackLevel\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_ATTEMPTS = "\"attempts\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_LAST_DELIVERY_ID = "\"lastDeliveryId\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_CREATED_AT = "\"createdAt\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_UPDATED_AT = "\"updatedAt\":".getBytes(StandardCharsets.UTF_8);

    private IntentResponseSerializer() {}

    public static void writeIntentResponse(IntentResponse response, ByteBuf buf) {
        buf.writeBytes(OPEN_OBJECT);

        boolean first = true;
        first = writeStringField(buf, first, FIELD_INTENT_ID, response.intentId());
        first = writeEnumField(buf, first, FIELD_STATUS, response.status());
        first = writeInstantField(buf, first, FIELD_EXECUTE_AT, response.executeAt());
        first = writeInstantField(buf, first, FIELD_DEADLINE, response.deadline());
        first = writeEnumField(buf, first, FIELD_EXPIRED_ACTION, response.expiredAction());
        first = writeEnumField(buf, first, FIELD_PRECISION_TIER, response.precisionTier());
        first = writeEnumField(buf, first, FIELD_WAL_MODE, response.walMode());
        first = writeEnumField(buf, first, FIELD_ACK_LEVEL, response.ackLevel());
        first = writeIntField(buf, first, FIELD_ATTEMPTS, response.attempts());
        first = writeStringField(buf, first, FIELD_LAST_DELIVERY_ID, response.lastDeliveryId());
        first = writeInstantField(buf, first, FIELD_CREATED_AT, response.createdAt());
        writeInstantField(buf, first, FIELD_UPDATED_AT, response.updatedAt());

        buf.writeBytes(CLOSE_OBJECT);
    }

    public static int estimateSize(IntentResponse response) {
        int size = 256;
        size += length(response.intentId());
        size += quotedLength(response.status());
        size += quotedLength(response.executeAt());
        size += quotedLength(response.deadline());
        size += quotedLength(response.expiredAction());
        size += quotedLength(response.precisionTier());
        size += quotedLength(response.walMode());
        size += quotedLength(response.ackLevel());
        size += 12;
        size += length(response.lastDeliveryId());
        size += quotedLength(response.createdAt());
        size += quotedLength(response.updatedAt());
        return size;
    }

    public static byte[] toBytes(IntentResponse response) {
        ByteBuf buf = io.netty.buffer.Unpooled.buffer(estimateSize(response));
        try {
            writeIntentResponse(response, buf);
            byte[] result = new byte[buf.readableBytes()];
            buf.readBytes(result);
            return result;
        } finally {
            buf.release();
        }
    }

    private static boolean writeStringField(ByteBuf buf, boolean first, byte[] fieldName, String value) {
        writeFieldPrefix(buf, first, fieldName);
        if (value == null) {
            buf.writeBytes(NULL_LITERAL);
        } else {
            buf.writeBytes(QUOTE);
            writeEscapedString(value, buf);
            buf.writeBytes(QUOTE);
        }
        return false;
    }

    private static boolean writeEnumField(ByteBuf buf, boolean first, byte[] fieldName, Enum<?> value) {
        writeFieldPrefix(buf, first, fieldName);
        if (value == null) {
            buf.writeBytes(NULL_LITERAL);
        } else {
            buf.writeBytes(QUOTE);
            ByteBufUtil.writeUtf8(buf, value.name());
            buf.writeBytes(QUOTE);
        }
        return false;
    }

    private static boolean writeInstantField(ByteBuf buf, boolean first, byte[] fieldName, Instant value) {
        writeFieldPrefix(buf, first, fieldName);
        if (value == null) {
            buf.writeBytes(NULL_LITERAL);
        } else {
            buf.writeBytes(QUOTE);
            ISO_INSTANT.formatTo(value, new ByteBufAppendable(buf));
            buf.writeBytes(QUOTE);
        }
        return false;
    }

    private static boolean writeIntField(ByteBuf buf, boolean first, byte[] fieldName, int value) {
        writeFieldPrefix(buf, first, fieldName);
        writeInt(buf, value);
        return false;
    }

    private static void writeFieldPrefix(ByteBuf buf, boolean first, byte[] fieldName) {
        if (!first) {
            buf.writeBytes(COMMA);
        }
        buf.writeBytes(fieldName);
    }

    private static int length(String value) {
        return value != null ? value.length() : 4;
    }

    private static int length(Enum<?> value) {
        return value != null ? value.name().length() : 4;
    }

    private static int quotedLength(Enum<?> value) {
        return value != null ? value.name().length() + 2 : 4;
    }

    private static int quotedLength(Instant value) {
        return value != null ? 32 : 4;
    }

    private static void writeEscapedString(String value, ByteBuf buf) {
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
            ByteBufUtil.writeUtf8(buf, value);
            return;
        }

        if (escapePos > 0) {
            buf.writeCharSequence(value.subSequence(0, escapePos), StandardCharsets.UTF_8);
        }

        for (int i = escapePos; i < len; i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> buf.writeBytes(ESCAPED_QUOTE);
                case '\\' -> buf.writeBytes(ESCAPED_BACKSLASH);
                case '\n' -> buf.writeBytes(ESCAPED_NEWLINE);
                case '\r' -> buf.writeBytes(ESCAPED_CARRIAGE_RETURN);
                case '\t' -> buf.writeBytes(ESCAPED_TAB);
                default -> {
                    if (c < ' ') {
                        buf.writeByte('\\');
                        buf.writeByte('u');
                        writeHex4(c, buf);
                    } else {
                        buf.writeByte((byte) c);
                    }
                }
            }
        }
    }

    private static void writeHex4(char value, ByteBuf buf) {
        int n = value;
        buf.writeByte(toHex((n >>> 12) & 0xF));
        buf.writeByte(toHex((n >>> 8) & 0xF));
        buf.writeByte(toHex((n >>> 4) & 0xF));
        buf.writeByte(toHex(n & 0xF));
    }

    private static byte toHex(int value) {
        return (byte) (value < 10 ? ('0' + value) : ('a' + (value - 10)));
    }

    private static void writeInt(ByteBuf buf, int value) {
        if (value == 0) {
            buf.writeByte('0');
            return;
        }
        if (value == Integer.MIN_VALUE) {
            buf.writeBytes(MIN_INT_BYTES);
            return;
        }

        boolean negative = value < 0;
        int v = negative ? -value : value;
        int digits = digitCount(v) + (negative ? 1 : 0);
        int writerIndex = buf.writerIndex();
        buf.ensureWritable(digits);

        int pos = writerIndex + digits - 1;
        do {
            int q = v / 10;
            int r = v - (q * 10);
            buf.setByte(pos--, '0' + r);
            v = q;
        } while (v != 0);

        if (negative) {
            buf.setByte(pos, '-');
        }

        buf.writerIndex(writerIndex + digits);
    }

    private static int digitCount(int value) {
        int digits = 1;
        while ((value /= 10) != 0) {
            digits++;
        }
        return digits;
    }

    private static final class ByteBufAppendable implements Appendable {
        private final ByteBuf buf;

        private ByteBufAppendable(ByteBuf buf) {
            this.buf = buf;
        }

        @Override
        public Appendable append(CharSequence csq) {
            if (csq == null) {
                buf.writeBytes(NULL_LITERAL);
                return this;
            }
            return append(csq, 0, csq.length());
        }

        @Override
        public Appendable append(CharSequence csq, int start, int end) {
            if (csq == null) {
                buf.writeBytes(NULL_LITERAL);
                return this;
            }
            for (int i = start; i < end; i++) {
                buf.writeByte((byte) csq.charAt(i));
            }
            return this;
        }

        @Override
        public Appendable append(char c) {
            buf.writeByte((byte) c);
            return this;
        }
    }
}

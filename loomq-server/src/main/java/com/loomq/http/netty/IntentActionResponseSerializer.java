package com.loomq.http.netty;

import com.loomq.api.IntentActionResponse;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/**
 * fire-now 轻量响应的零拷贝序列化器。
 */
public final class IntentActionResponseSerializer {

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

    private static final byte[] FIELD_INTENT_ID = "\"intentId\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_STATUS = "\"status\":".getBytes(StandardCharsets.UTF_8);

    private IntentActionResponseSerializer() {}

    public static void write(IntentActionResponse response, ByteBuf buf) {
        buf.writeBytes(OPEN_OBJECT);
        writeStringField(buf, true, FIELD_INTENT_ID, response.intentId());
        writeStringField(buf, false, FIELD_STATUS, response.status());
        buf.writeBytes(CLOSE_OBJECT);
    }

    public static int estimateSize(IntentActionResponse response) {
        int size = 64;
        size += response.intentId() != null ? response.intentId().length() + 8 : 4;
        size += response.status() != null ? response.status().length() + 8 : 4;
        return size;
    }

    public static byte[] toBytes(IntentActionResponse response) {
        ByteBuf buf = io.netty.buffer.Unpooled.buffer(estimateSize(response));
        try {
            write(response, buf);
            byte[] result = new byte[buf.readableBytes()];
            buf.readBytes(result);
            return result;
        } finally {
            buf.release();
        }
    }

    private static void writeStringField(ByteBuf buf, boolean first, byte[] fieldName, String value) {
        if (!first) {
            buf.writeBytes(COMMA);
        }
        buf.writeBytes(fieldName);
        if (value == null) {
            buf.writeBytes(NULL_LITERAL);
            return;
        }
        buf.writeBytes(QUOTE);
        writeEscapedString(value, buf);
        buf.writeBytes(QUOTE);
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
            for (int i = 0; i < len; i++) {
                buf.writeByte((byte) value.charAt(i));
            }
            return;
        }

        for (int i = 0; i < escapePos; i++) {
            buf.writeByte((byte) value.charAt(i));
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
}

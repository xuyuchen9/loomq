package com.loomq.http.netty;

import com.loomq.api.ErrorResponse;
import com.loomq.http.json.JsonCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * ErrorResponse 的零拷贝序列化器。
 *
 * 仅对 details 使用一次 JSON 编码，其余字段直接写入 ByteBuf。
 */
public final class HttpErrorResponseSerializer {

    private static final byte[] OPEN_OBJECT = "{".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CLOSE_OBJECT = "}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COMMA = ",".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COLON = ":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] QUOTE = "\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] NULL_LITERAL = "null".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ESCAPED_QUOTE = "\\\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ESCAPED_BACKSLASH = "\\\\".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ESCAPED_NEWLINE = "\\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ESCAPED_CARRIAGE_RETURN = "\\r".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ESCAPED_TAB = "\\t".getBytes(StandardCharsets.UTF_8);
    private static final byte[] MIN_INT_BYTES = "-2147483648".getBytes(StandardCharsets.UTF_8);

    private static final byte[] FIELD_STATUS = "\"status\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_ERROR = "\"error\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_CODE = "\"code\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_MESSAGE = "\"message\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_DETAILS = "\"details\"".getBytes(StandardCharsets.UTF_8);

    private HttpErrorResponseSerializer() {}

    public static void write(HttpErrorResponse response, ByteBuf buf) {
        buf.writeBytes(OPEN_OBJECT);
        writeFieldName(buf, FIELD_STATUS);
        writeInt(buf, response.status());
        buf.writeBytes(COMMA);
        writeFieldName(buf, FIELD_ERROR);
        writeErrorObject(response.error(), buf);
        buf.writeBytes(CLOSE_OBJECT);
    }

    public static int estimateSize(HttpErrorResponse response) {
        int size = 64;
        size += digitCount(response.status());
        size += estimateErrorSize(response.error());
        return size;
    }

    public static byte[] toBytes(HttpErrorResponse response) {
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

    private static void writeErrorObject(ErrorResponse error, ByteBuf buf) {
        buf.writeBytes(OPEN_OBJECT);
        writeFieldName(buf, FIELD_CODE);
        writeStringValue(buf, error.code());
        buf.writeBytes(COMMA);
        writeFieldName(buf, FIELD_MESSAGE);
        writeStringValue(buf, error.message());
        buf.writeBytes(COMMA);
        writeFieldName(buf, FIELD_DETAILS);
        if (error.details() == null) {
            buf.writeBytes(NULL_LITERAL);
        } else {
            byte[] detailsBytes;
            try {
                detailsBytes = JsonCodec.instance().writeBytes(error.details());
            } catch (Exception e) {
                throw new IllegalStateException("Failed to serialize error details", e);
            }
            buf.writeBytes(detailsBytes);
        }
        buf.writeBytes(CLOSE_OBJECT);
    }

    private static int estimateErrorSize(ErrorResponse error) {
        int size = 64;
        size += length(error.code());
        size += length(error.message());
        size += error.details() != null ? 32 : 4;
        return size;
    }

    private static void writeFieldName(ByteBuf buf, byte[] fieldName) {
        buf.writeBytes(fieldName);
        buf.writeBytes(COLON);
    }

    private static void writeStringValue(ByteBuf buf, String value) {
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

    private static int length(String value) {
        return value != null ? value.length() : 4;
    }

    private static int digitCount(int value) {
        if (value == Integer.MIN_VALUE) {
            return 11;
        }

        int digits = 1;
        int v = value < 0 ? -value : value;
        while ((v /= 10) != 0) {
            digits++;
        }
        return value < 0 ? digits + 1 : digits;
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

}

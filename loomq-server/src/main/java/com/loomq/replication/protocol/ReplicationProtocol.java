package com.loomq.replication.protocol;

import com.loomq.replication.Ack;
import com.loomq.replication.ReplicationRecord;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;

import java.util.List;

/**
 * 复制协议编解码器
 *
 * Netty 处理器，用于编码 ReplicationRecord 和解码 Ack
 *
 * 消息格式：
 * - 4 bytes: 消息长度（不包括长度字段本身）
 * - 1 byte: 消息类型（0x01 = Record, 0x02 = Ack）
 * - N bytes: 消息体
 *
 * @author loomq
 * @since v0.4.8
 */
public class ReplicationProtocol extends ByteToMessageCodec<Object> {

    /**
     * 消息类型
     */
    public static final byte MSG_TYPE_RECORD = 0x01;
    public static final byte MSG_TYPE_ACK = 0x02;
    public static final byte MSG_TYPE_HEARTBEAT = 0x03;
    public static final byte MSG_TYPE_CATCHUP_REQUEST = 0x04;
    public static final byte MSG_TYPE_CATCHUP_RESPONSE = 0x05;

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        if (msg instanceof ReplicationRecord) {
            ReplicationRecord record = (ReplicationRecord) msg;
            byte[] data = record.encode();

            out.writeInt(data.length + 1);  // length including type byte
            out.writeByte(MSG_TYPE_RECORD);
            out.writeBytes(data);
        } else if (msg instanceof Ack) {
            Ack ack = (Ack) msg;
            byte[] data = ack.encode();

            out.writeInt(data.length + 1);
            out.writeByte(MSG_TYPE_ACK);
            out.writeBytes(data);
        } else if (msg instanceof HeartbeatMessage) {
            HeartbeatMessage heartbeat = (HeartbeatMessage) msg;
            byte[] data = heartbeat.encode();

            out.writeInt(data.length + 1);
            out.writeByte(MSG_TYPE_HEARTBEAT);
            out.writeBytes(data);
        } else {
            throw new IllegalArgumentException("Unsupported message type: " + msg.getClass());
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // 等待至少 4 字节（长度字段）
        if (in.readableBytes() < 4) {
            return;
        }

        in.markReaderIndex();
        int length = in.readInt();

        // 验证长度合理性
        if (length < 0 || length > 100 * 1024 * 1024) {  // max 100MB
            throw new IllegalArgumentException("Invalid message length: " + length);
        }

        // 等待完整消息
        if (in.readableBytes() < length) {
            in.resetReaderIndex();
            return;
        }

        // 读取消息类型
        byte msgType = in.readByte();
        int payloadLength = length - 1;

        // 读取 payload
        byte[] payload = new byte[payloadLength];
        in.readBytes(payload);

        // 根据类型解码
        switch (msgType) {
            case MSG_TYPE_RECORD:
                out.add(ReplicationRecord.decode(payload));
                break;
            case MSG_TYPE_ACK:
                out.add(Ack.decode(payload));
                break;
            case MSG_TYPE_HEARTBEAT:
                out.add(HeartbeatMessage.decode(payload));
                break;
            default:
                throw new IllegalArgumentException("Unknown message type: " + msgType);
        }
    }
}

package com.loomq.replication.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * 心跳消息
 *
 * Primary 和 Replica 之间的心跳检测
 *
 * @author loomq
 * @since v0.4.8
 */
public class HeartbeatMessage {

    /**
     * 节点 ID
     */
    private final String nodeId;

    /**
     * 角色：PRIMARY 或 REPLICA
     */
    private final String role;

    /**
     * 当前状态
     */
    private final String state;

    /**
     * 最后应用的 offset
     */
    private final long lastAppliedOffset;

    /**
     * 时间戳
     */
    private final long timestamp;

    public HeartbeatMessage(String nodeId, String role, String state,
                            long lastAppliedOffset, long timestamp) {
        this.nodeId = nodeId;
        this.role = role;
        this.state = state;
        this.lastAppliedOffset = lastAppliedOffset;
        this.timestamp = timestamp;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getRole() {
        return role;
    }

    public String getState() {
        return state;
    }

    public long getLastAppliedOffset() {
        return lastAppliedOffset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    /**
     * 编码为字节数组
     */
    public byte[] encode() {
        byte[] nodeIdBytes = nodeId.getBytes(StandardCharsets.UTF_8);
        byte[] roleBytes = role.getBytes(StandardCharsets.UTF_8);
        byte[] stateBytes = state.getBytes(StandardCharsets.UTF_8);

        int totalSize = 4 + nodeIdBytes.length +
                       4 + roleBytes.length +
                       4 + stateBytes.length +
                       8 + 8;  // offsets

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        // nodeId
        buffer.putInt(nodeIdBytes.length);
        buffer.put(nodeIdBytes);

        // role
        buffer.putInt(roleBytes.length);
        buffer.put(roleBytes);

        // state
        buffer.putInt(stateBytes.length);
        buffer.put(stateBytes);

        // offsets
        buffer.putLong(lastAppliedOffset);
        buffer.putLong(timestamp);

        return buffer.array();
    }

    /**
     * 从字节数组解码
     */
    public static HeartbeatMessage decode(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // nodeId
        int nodeIdLen = buffer.getInt();
        byte[] nodeIdBytes = new byte[nodeIdLen];
        buffer.get(nodeIdBytes);
        String nodeId = new String(nodeIdBytes, StandardCharsets.UTF_8);

        // role
        int roleLen = buffer.getInt();
        byte[] roleBytes = new byte[roleLen];
        buffer.get(roleBytes);
        String role = new String(roleBytes, StandardCharsets.UTF_8);

        // state
        int stateLen = buffer.getInt();
        byte[] stateBytes = new byte[stateLen];
        buffer.get(stateBytes);
        String state = new String(stateBytes, StandardCharsets.UTF_8);

        // offsets
        long lastAppliedOffset = buffer.getLong();
        long timestamp = buffer.getLong();

        return new HeartbeatMessage(nodeId, role, state, lastAppliedOffset, timestamp);
    }

    @Override
    public String toString() {
        return String.format("HeartbeatMessage{nodeId='%s', role='%s', state='%s', offset=%d}",
            nodeId, role, state, lastAppliedOffset);
    }
}

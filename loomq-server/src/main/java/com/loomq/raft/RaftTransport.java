package com.loomq.raft;

import com.loomq.replication.Ack;
import com.loomq.replication.ReplicationRecord;
import com.loomq.replication.ReplicationRecordType;
import com.loomq.replication.client.ReplicaClient;
import com.loomq.replication.server.ReplicaServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** Raft RPC Transport. Wraps existing ReplicaClient/ReplicaServer for Raft messages. */
public class RaftTransport implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RaftTransport.class);
    private final String nodeId;
    private final Map<String, ReplicaClient> clients = new ConcurrentHashMap<>();
    private ReplicaServer server;

    // Server-side handlers: decode payload → call function → encode response
    private Function<RequestVoteMessage, Boolean> onRequestVote;
    private Function<AppendEntriesMessage, AppendEntriesResult> onAppendEntries;
    private Function<InstallSnapshotMessage, Long> onInstallSnapshot;

    public RaftTransport(String nodeId) { this.nodeId = nodeId; }

    /** Encode: term(8) + candidateIdLen(2) + candidateId + lastLogIndex(8) + lastLogTerm(8) */
    static byte[] encodeRequestVote(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
        byte[] idBytes = candidateId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(8 + 2 + idBytes.length + 8 + 8);
        buf.putLong(term);
        buf.putShort((short) idBytes.length);
        buf.put(idBytes);
        buf.putLong(lastLogIndex);
        buf.putLong(lastLogTerm);
        return buf.array();
    }

    /** Decode RequestVote from payload into a message record */
    static RequestVoteMessage decodeRequestVote(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        long term = buf.getLong();
        short idLen = buf.getShort();
        byte[] idBytes = new byte[idLen];
        buf.get(idBytes);
        String candidateId = new String(idBytes, StandardCharsets.UTF_8);
        long lastLogIndex = buf.getLong();
        long lastLogTerm = buf.getLong();
        return new RequestVoteMessage(term, candidateId, lastLogIndex, lastLogTerm);
    }

    /** Encode response: term(8) + voteGranted(1) */
    static byte[] encodeRequestVoteResponse(long term, boolean voteGranted) {
        ByteBuffer buf = ByteBuffer.allocate(9);
        buf.putLong(term);
        buf.put(voteGranted ? (byte) 1 : (byte) 0);
        return buf.array();
    }

    /** Decode response */
    static boolean decodeRequestVoteResponse(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        buf.getLong(); // term
        return buf.get() == 1;
    }

    /** Encode: term(8) + leaderIdLen(2) + leaderId + prevLogIndex(8) + prevLogTerm(8) + entryCount(4) + [entryLen(4)+entry]* + leaderCommit(8) */
    static byte[] encodeAppendEntries(long term, String leaderId, long prevLogIndex,
            long prevLogTerm, byte[][] entries, long leaderCommit) {
        byte[] idBytes = leaderId.getBytes(StandardCharsets.UTF_8);
        int entryCount = entries != null ? entries.length : 0;
        int entriesSize = 0;
        if (entryCount > 0) {
            for (byte[] e : entries) if (e != null) entriesSize += 4 + e.length;
        }
        ByteBuffer buf = ByteBuffer.allocate(8 + 2 + idBytes.length + 8 + 8 + 4 + entriesSize + 8);
        buf.putLong(term);
        buf.putShort((short) idBytes.length);
        buf.put(idBytes);
        buf.putLong(prevLogIndex);
        buf.putLong(prevLogTerm);
        buf.putInt(entryCount);
        if (entryCount > 0) {
            for (byte[] e : entries) {
                if (e == null) { buf.putInt(0); continue; }
                buf.putInt(e.length);
                buf.put(e);
            }
        }
        buf.putLong(leaderCommit);
        return buf.array();
    }

    /** Decode AppendEntries from payload */
    static AppendEntriesMessage decodeAppendEntries(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        long term = buf.getLong();
        short idLen = buf.getShort();
        byte[] idBytes = new byte[idLen];
        buf.get(idBytes);
        String leaderId = new String(idBytes, StandardCharsets.UTF_8);
        long prevLogIndex = buf.getLong();
        long prevLogTerm = buf.getLong();
        int entryCount = buf.getInt();
        byte[][] entries = new byte[entryCount][];
        for (int i = 0; i < entryCount; i++) {
            int len = buf.getInt();
            entries[i] = new byte[len];
            buf.get(entries[i]);
        }
        long leaderCommit = buf.getLong();
        return new AppendEntriesMessage(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    /** Encode response: term(8) + success(1) + matchIndex(8) + conflictIndex(8) */
    static byte[] encodeAppendEntriesResponse(long term, boolean success, long matchIndex, long conflictIndex) {
        ByteBuffer buf = ByteBuffer.allocate(25);
        buf.putLong(term);
        buf.put(success ? (byte) 1 : (byte) 0);
        buf.putLong(matchIndex);
        buf.putLong(conflictIndex);
        return buf.array();
    }

    // ========== RequestVote payload codec ==========

    /** Decode response */
    static AppendEntriesResult decodeAppendEntriesResponse(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        long term = buf.getLong();
        boolean success = buf.get() == 1;
        long matchIndex = buf.getLong();
        long conflictIndex = buf.remaining() >= 8 ? buf.getLong() : -1;
        if (success) {
            return AppendEntriesResult.success(term, matchIndex);
        } else {
            return conflictIndex >= 0 ? AppendEntriesResult.fail(term, conflictIndex) : AppendEntriesResult.fail(term);
        }
    }

    /** Encode: term(8) + leaderIdLen(2) + leaderId + lastIncludedIndex(8) + lastIncludedTerm(8) + dataLen(4) + data */
    static byte[] encodeInstallSnapshot(long term, String leaderId, long lastIncludedIndex,
            long lastIncludedTerm, byte[] snapshotData) {
        byte[] idBytes = leaderId.getBytes(StandardCharsets.UTF_8);
        int dataLen = snapshotData != null ? snapshotData.length : 0;
        ByteBuffer buf = ByteBuffer.allocate(8 + 2 + idBytes.length + 8 + 8 + 4 + dataLen);
        buf.putLong(term);
        buf.putShort((short) idBytes.length);
        buf.put(idBytes);
        buf.putLong(lastIncludedIndex);
        buf.putLong(lastIncludedTerm);
        buf.putInt(dataLen);
        if (dataLen > 0) buf.put(snapshotData);
        return buf.array();
    }

    /** Decode InstallSnapshot from payload */
    static InstallSnapshotMessage decodeInstallSnapshot(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        long term = buf.getLong();
        short idLen = buf.getShort();
        byte[] idBytes = new byte[idLen];
        buf.get(idBytes);
        String leaderId = new String(idBytes, StandardCharsets.UTF_8);
        long lastIncludedIndex = buf.getLong();
        long lastIncludedTerm = buf.getLong();
        int dataLen = buf.getInt();
        byte[] snapshotData = new byte[dataLen];
        buf.get(snapshotData);
        return new InstallSnapshotMessage(term, leaderId, lastIncludedIndex, lastIncludedTerm, snapshotData);
    }

    public void setOnRequestVote(Function<RequestVoteMessage, Boolean> h) { this.onRequestVote = h; }

    // ========== AppendEntries payload codec ==========

    public void setOnAppendEntries(Function<AppendEntriesMessage, AppendEntriesResult> h) { this.onAppendEntries = h; }

    public void setOnInstallSnapshot(Function<InstallSnapshotMessage, Long> h) { this.onInstallSnapshot = h; }

    /** Start as server (listen for incoming Raft RPCs) */
    public void listen(String host, int port) {
        server = new ReplicaServer(nodeId, host, port);
        server.setAckHandler(record -> {
            ReplicationRecordType type = record.getType();
            if (type == ReplicationRecordType.RAFT_REQUEST_VOTE) return handleRequestVote(record);
            else if (type == ReplicationRecordType.RAFT_APPEND_ENTRIES) return handleAppendEntries(record);
            else if (type == ReplicationRecordType.RAFT_INSTALL_SNAPSHOT) return handleInstallSnapshot(record);
            return null;
        });
        server.start();
        log.info("RaftTransport listening on {}:{}", host, port);
    }

    /** Connect to a peer for outgoing Raft RPCs */
    public CompletableFuture<Void> connect(String peerId, String host, int port) {
        ReplicaClient existing = clients.remove(peerId);
        if (existing != null) {
            existing.shutdown();
        }
        ReplicaClient client = new ReplicaClient(nodeId, host, port);
        clients.put(peerId, client);
        return client.connect().whenComplete((ignored, ex) -> {
            if (ex != null) {
                clients.remove(peerId, client);
                client.shutdown();
            }
        });
    }

    // ========== Server-side handlers (called from Ack handler) ==========

    /** Send RequestVote to a peer */
    public CompletableFuture<Boolean> sendRequestVote(String peerId, long term, String candidateId,
            long lastLogIndex, long lastLogTerm) {
        byte[] payload = encodeRequestVote(term, candidateId, lastLogIndex, lastLogTerm);
        ReplicationRecord record = ReplicationRecord.builder()
            .offset(term).type(ReplicationRecordType.RAFT_REQUEST_VOTE)
            .sourceNodeId(candidateId).payload(payload).build();
        ReplicaClient client = clients.get(peerId);
        if (client == null) return CompletableFuture.completedFuture(false);
        return client.send(record).thenApply(ack -> {
            if (!ack.isSuccess()) return false;
            byte[] resp = ack.getRaftResponse();
            if (resp == null || resp.length < 9) return false;
            return decodeRequestVoteResponse(resp);
        });
    }

    /** Send AppendEntries to a peer */
    public CompletableFuture<AppendEntriesResult> sendAppendEntries(String peerId, long term,
            String leaderId, long prevLogIndex, long prevLogTerm, byte[][] entries, long leaderCommit) {
        byte[] payload = encodeAppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
        ReplicationRecord record = ReplicationRecord.builder()
            .offset(term).type(ReplicationRecordType.RAFT_APPEND_ENTRIES)
            .sourceNodeId(leaderId).payload(payload).build();
        ReplicaClient client = clients.get(peerId);
        if (client == null) return CompletableFuture.completedFuture(AppendEntriesResult.fail(term));
        return client.send(record).thenApply(ack -> {
            if (!ack.isSuccess()) return AppendEntriesResult.fail(term);
            byte[] resp = ack.getRaftResponse();
            if (resp == null || resp.length < 9) return AppendEntriesResult.fail(term);
            return decodeAppendEntriesResponse(resp);
        });
    }

    // ========== InstallSnapshot payload codec ==========

    private Ack handleRequestVote(ReplicationRecord record) {
        if (onRequestVote == null) return Ack.success(record.getOffset());
        try {
            RequestVoteMessage msg = decodeRequestVote(record.getPayload());
            boolean voteGranted = onRequestVote.apply(msg);
            byte[] resp = encodeRequestVoteResponse(msg.term(), voteGranted);
            return Ack.raftResponse(record.getOffset(),
                voteGranted ? com.loomq.replication.AckStatus.REPLICATED : com.loomq.replication.AckStatus.REJECTED,
                resp);
        } catch (Exception e) {
            log.error("Failed to handle RequestVote", e);
            return Ack.failed(record.getOffset(), e.getMessage());
        }
    }

    private Ack handleAppendEntries(ReplicationRecord record) {
        if (onAppendEntries == null) return Ack.success(record.getOffset());
        try {
            AppendEntriesMessage msg = decodeAppendEntries(record.getPayload());
            AppendEntriesResult result = onAppendEntries.apply(msg);
            byte[] resp = encodeAppendEntriesResponse(result.term, result.success, result.matchIndex, result.conflictIndex);
            return Ack.raftResponse(record.getOffset(),
                result.success ? com.loomq.replication.AckStatus.REPLICATED : com.loomq.replication.AckStatus.REJECTED,
                resp);
        } catch (Exception e) {
            log.error("Failed to handle AppendEntries", e);
            return Ack.failed(record.getOffset(), e.getMessage());
        }
    }

    /** Send InstallSnapshot to a lagging follower */
    public CompletableFuture<Long> sendInstallSnapshot(String peerId, long term, String leaderId,
            long lastIncludedIndex, long lastIncludedTerm, byte[] snapshotData) {
        byte[] payload = encodeInstallSnapshot(term, leaderId, lastIncludedIndex, lastIncludedTerm, snapshotData);
        ReplicationRecord record = ReplicationRecord.builder()
            .offset(term).type(ReplicationRecordType.RAFT_INSTALL_SNAPSHOT)
            .sourceNodeId(leaderId).payload(payload).build();
        ReplicaClient client = clients.get(peerId);
        if (client == null) return CompletableFuture.completedFuture(-1L);
        return client.send(record).thenApply(ack -> {
            if (!ack.isSuccess()) return -1L;
            byte[] resp = ack.getRaftResponse();
            if (resp == null || resp.length < 8) return -1L;
            ByteBuffer buf = ByteBuffer.wrap(resp);
            return buf.getLong(); // follower's new lastIncludedIndex
        });
    }

    // ========== Server-side handler ==========

    private Ack handleInstallSnapshot(ReplicationRecord record) {
        if (onInstallSnapshot == null) return Ack.success(record.getOffset());
        try {
            InstallSnapshotMessage msg = decodeInstallSnapshot(record.getPayload());
            Long newIndex = onInstallSnapshot.apply(msg);
            byte[] resp = ByteBuffer.allocate(8).putLong(newIndex != null ? newIndex : -1).array();
            return Ack.raftResponse(record.getOffset(),
                newIndex != null && newIndex >= 0 ? com.loomq.replication.AckStatus.REPLICATED : com.loomq.replication.AckStatus.REJECTED,
                resp);
        } catch (Exception e) {
            log.error("Failed to handle InstallSnapshot", e);
            return Ack.failed(record.getOffset(), e.getMessage());
        }
    }

    @Override public void close() { clients.values().forEach(ReplicaClient::shutdown); if (server != null) server.shutdown(); }

    /** Full RequestVote RPC message */
    public record RequestVoteMessage(long term, String candidateId, long lastLogIndex, long lastLogTerm) {}

    /** Full AppendEntries RPC message */
    public record AppendEntriesMessage(long term, String leaderId, long prevLogIndex, long prevLogTerm,
            byte[][] entries, long leaderCommit) {}

    /** Full InstallSnapshot RPC message */
    public record InstallSnapshotMessage(long term, String leaderId, long lastIncludedIndex,
            long lastIncludedTerm, byte[] snapshotData) {}
}

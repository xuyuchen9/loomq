package com.loomq.raft;

import com.loomq.metrics.LoomQMetrics;
import com.loomq.spi.WalAccessor;
import com.loomq.store.IntentStore;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raft 节点 — 组合选举、日志复制、状态机应用。
 *
 * 通过内核 SPI（WalAccessor + IntentStore）与 LoomqEngine 交互。
 */
public class RaftNode implements AutoCloseable, RaftStatusProvider {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);
    private static final LoomQMetrics metrics = LoomQMetrics.getInstance();
    private static final int MAX_ENTRIES_PER_APPEND = 1000;
    private final String nodeId;
    private final WalAccessor wal;
    private final IntentStore store;
    private final LeaderElection election;
    private final LogReplication replication;
    private final RaftLog raftLog;
    private final RaftTransport transport;
    private final RaftRuntimeListener runtimeListener;
    private final List<String> peers;
    private final ScheduledExecutorService heartbeatTimer;
    private final long heartbeatMs;
    private final long readLeaseMs;
    private final ConcurrentMap<String, PeerReplicationState> peerStates;
    private ScheduledFuture<?> heartbeatTask;
    private volatile long readLeaseUntilMs = 0L;
    /**
     * Leader generation counter — incremented each time this node becomes leader.
     * Heartbeat callbacks check this against their captured generation to discard
     * stale responses after stepDown mid-loop (§5.1 safety).
     */
    private volatile long leaderGeneration = 0;

    public RaftNode(RaftConfig config, WalAccessor wal, IntentStore store, RaftTransport transport) {
        this(config, wal, store, transport, null);
    }

    public RaftNode(RaftConfig config, WalAccessor wal, IntentStore store, RaftTransport transport,
                    RaftRuntimeListener runtimeListener) {
        this.nodeId = config.nodeId();
        this.wal = wal;
        this.store = store;
        this.transport = transport;
        this.runtimeListener = runtimeListener;
        this.peers = config.peers();
        this.heartbeatMs = config.heartbeatMs();
        this.readLeaseMs = computeReadLeaseMs(config.electionMinMs(), config.heartbeatMs());
        this.peerStates = new ConcurrentHashMap<>();
        for (String peerId : peers) {
            if (!peerId.equals(nodeId)) {
                peerStates.put(peerId, new PeerReplicationState(peerId));
            }
        }
        this.raftLog = new RaftLog(wal);
        this.election = new LeaderElection(nodeId, wal, config.peers(),
            config.electionMinMs(), config.electionMaxMs());
        this.replication = new LogReplication(nodeId, wal, raftLog, store, election, runtimeListener);
        this.heartbeatTimer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-heartbeat-" + nodeId);
            t.setDaemon(true);
            return t;
        });

        // Server-side RPC handlers (transport may be null for single-node/no-network tests)
        if (transport != null) {
            transport.setOnRequestVote(msg -> {
                boolean granted = election.handleRequestVote(msg.term(), msg.candidateId(),
                    msg.lastLogIndex(), msg.lastLogTerm());
                syncRaftMetrics();
                return granted;
            });

            transport.setOnAppendEntries(msg -> {
                AppendEntriesResult result = replication.handleAppendEntries(msg.term(), msg.leaderId(),
                    msg.prevLogIndex(), msg.prevLogTerm(), msg.entries(), msg.leaderCommit());
                syncRaftMetrics();
                return result;
            });

            transport.setOnInstallSnapshot(msg -> {
                Long appliedIndex = handleInstallSnapshot(msg);
                syncRaftMetrics();
                return appliedIndex;
            });
        }

        // Election lifecycle callbacks
        election.setOnElectionStarted(this::onElectionStarted);
        election.setOnBecomeLeader(this::onBecomeLeader);
        election.setOnBecomeFollower(this::onBecomeFollower);
    }

    public void start() {
        election.start();
        syncRaftMetrics();
        if (runtimeListener != null && role() == RaftRole.FOLLOWER) {
            notifyRuntimeRole();
        }
        log.info("RaftNode started: node={}, term={}, logIndex={}",
            nodeId, election.currentTerm(), raftLog.lastIndex());
    }

    @Override
    public void close() {
        stopHeartbeat();
        heartbeatTimer.shutdown();
        election.stop();
        replication.failPendingWaiters(new IllegalStateException("Raft node closed"));
        notifyRuntimeRole(RaftRole.FOLLOWER);
        metrics.updateRaftRole("OFFLINE");
        metrics.updateRaftLeaderId(null);
        metrics.updateRaftReplicationLag(0);
        metrics.updateRaftConnectedPeers(0);
        metrics.updateRaftTotalPeers(0);
        if (transport != null) {
            transport.close();
        }
        log.info("RaftNode closed: node={}", nodeId);
    }

    public RaftRole role() { return election.role(); }
    public boolean isLeader() { return role() == RaftRole.LEADER; }
    public LeaderElection getElection() { return election; }
    public LogReplication getReplication() { return replication; }
    public WalAccessor getWal() { return wal; }
    public RaftLog getRaftLog() { return raftLog; }

    @Override
    public boolean isRaftEnabled() {
        return true;
    }

    @Override
    public boolean canServeLinearizableRead() {
        if (!isLeader()) {
            return false;
        }
        if (peerStates.isEmpty()) {
            return true;
        }
        return System.currentTimeMillis() <= readLeaseUntilMs;
    }

    @Override
    public String currentLeaderId() {
        return election.currentLeader();
    }

    /** Leader 提交一条 entry 到本地 WAL 并返回 index */
    public long propose(byte[] entry) {
        if (!isLeader()) {
            throw new IllegalStateException("Raft proposals must be issued by the leader");
        }
        long index = raftLog.appendEntry(election.currentTerm(), entry);
        if (peerStates.isEmpty()) {
            replication.advanceCommitIndex(new long[]{index}, election.currentTerm());
            replication.applyCommitted();
        }
        log.debug("Proposed entry at index {}", index);
        return index;
    }

    /** Apply committed entries to state machine */
    public void applyCommitted() {
        replication.applyCommitted();
    }

    // ========== InstallSnapshot (§5.5 / §7) ==========

    /**
     * Leader: 发送快照给落后过多的 follower。
     * 当 follower 的 nextIndex 低于 WAL 最早可用 index 时（段文件已截断），
     * 必须用快照替代 AppendEntries 来同步。
     */
    public void sendInstallSnapshot(String peerId) {
        PeerReplicationState ps = peerStates.get(peerId);
        if (ps == null) {
            return;
        }

        long term = election.currentTerm();
        long snapshotIndex = replication.lastApplied();
        long snapshotTerm = snapshotIndex > 0 ? raftLog.readEntryTerm(snapshotIndex) : 0;
        long requestGeneration = ++ps.requestGeneration;

        // Encode current store state as snapshot payload
        byte[] snapshotData = encodeStoreSnapshot();
        raftLog.compactThrough(snapshotIndex, snapshotTerm);

        transport.sendInstallSnapshot(peerId, term, nodeId, snapshotIndex, snapshotTerm, snapshotData)
            .thenAccept(newIndex -> {
                if (requestGeneration != ps.requestGeneration) {
                    return;
                }
                if (newIndex >= 0) {
                    ps.nextIndex = newIndex + 1;
                    ps.matchIndex = newIndex;
                    log.info("InstallSnapshot accepted by {} (index={})", peerId, newIndex);
                } else {
                    log.warn("InstallSnapshot rejected by {}", peerId);
                }
            });
    }

    /**
     * Follower: 处理 InstallSnapshot RPC。
     * 清空当前 IntentStore，解码并应用快照数据。
     *
     * @return 新的 lastApplied index（失败返回 -1）
     */
    private Long handleInstallSnapshot(RaftTransport.InstallSnapshotMessage msg) {
        if (msg.term() < election.currentTerm()) {
            log.debug("Rejecting InstallSnapshot: stale term {} < {}", msg.term(), election.currentTerm());
            return -1L;
        }
        election.onAppendEntries(msg.term(), msg.leaderId());

        try {
            // Decode snapshot first — if this fails, store is untouched and
            // we reject the snapshot (leader will retry).
            java.util.List<com.loomq.domain.intent.Intent> decoded = decodeStoreSnapshot(msg.snapshotData());

            // Clear existing store state before applying the snapshot
            store.clear();

            // Apply decoded intents
            for (com.loomq.domain.intent.Intent intent : decoded) {
                store.upsert(intent);
            }

            raftLog.compactThrough(msg.lastIncludedIndex(), msg.lastIncludedTerm());
            replication.resetToSnapshot(msg.lastIncludedIndex());
            log.info("InstallSnapshot applied: {} intents, index={}, term={}",
                decoded.size(), msg.lastIncludedIndex(), msg.lastIncludedTerm());
            return msg.lastIncludedIndex();
        } catch (Exception e) {
            log.error("Failed to apply InstallSnapshot from {}", msg.leaderId(), e);
            return -1L;
        }
    }

    /** Encode all intents in store as a binary snapshot blob */
    private byte[] encodeStoreSnapshot() {
        try {
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            java.io.DataOutputStream dos = new java.io.DataOutputStream(bos);
            var allIntents = store.getAllIntents();
            dos.writeInt(allIntents.size());
            for (var intent : allIntents.values()) {
                byte[] encoded = com.loomq.infrastructure.wal.IntentBinaryCodec.encode(intent);
                dos.writeInt(encoded.length);
                dos.write(encoded);
            }
            dos.flush();
            return bos.toByteArray();
        } catch (java.io.IOException e) {
            log.error("Failed to encode store snapshot", e);
            return new byte[0];
        }
    }

    /** Decode a binary snapshot blob into a list of intents (no side effects). */
    private java.util.List<com.loomq.domain.intent.Intent> decodeStoreSnapshot(byte[] data) throws java.io.IOException {
        java.io.DataInputStream dis = new java.io.DataInputStream(
            new java.io.ByteArrayInputStream(data));
        int count = dis.readInt();
        java.util.List<com.loomq.domain.intent.Intent> result = new java.util.ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int len = dis.readInt();
            byte[] encoded = new byte[len];
            dis.readFully(encoded);
            var intent = com.loomq.infrastructure.wal.IntentBinaryCodec.decode(encoded);
            result.add(intent);
        }
        return result;
    }

    // ========== Vote coordination ==========

    private void onElectionStarted(long term) {
        syncRaftMetrics();
        if (peers.isEmpty() || transport == null) return;
        log.info("Sending RequestVote to {} peers for term {}", peers.size(), term);
        long lastIdx = raftLog.lastIndex();
        long lastTerm = raftLog.lastTerm();
        AtomicInteger votesGranted = new AtomicInteger(1); // self-vote
        int majority = (peers.size() / 2) + 1;

        for (String peerId : peers) {
            if (peerId.equals(nodeId)) continue;
            transport.sendRequestVote(peerId, term, nodeId, lastIdx, lastTerm)
                .thenAccept(granted -> {
                    if (granted) {
                        int total = votesGranted.incrementAndGet();
                        log.debug("Vote granted by {} (total={}/{})", peerId, total, majority);
                        if (total >= majority && election.role() == RaftRole.CANDIDATE) {
                            election.becomeLeader(term);
                        }
                    }
                });
        }
    }

    // ========== Heartbeat ==========

    private void onBecomeLeader(long term) {
        leaderGeneration++;
        renewReadLease();
        syncRaftMetrics();
        notifyRuntimeRole();
        log.info("Node {} became LEADER at term {} (gen {})", nodeId, term, leaderGeneration);
        startHeartbeat(term);
    }

    private void onBecomeFollower(long term) {
        readLeaseUntilMs = 0L;
        syncRaftMetrics();
        replication.failPendingWaiters(new IllegalStateException("Leadership lost"));
        notifyRuntimeRole();
        log.info("Node {} became FOLLOWER at term {}", nodeId, term);
        leaderGeneration++;
        stopHeartbeat();
    }

    private void startHeartbeat(long term) {
        stopHeartbeat();
        heartbeatTask = heartbeatTimer.scheduleAtFixedRate(() -> {
            try {
                if (!isLeader() || transport == null) {
                    stopHeartbeat();
                    return;
                }
                syncRaftMetrics();
                final long myGeneration = leaderGeneration;
                long currentTerm = election.currentTerm();
                long commitIdx = replication.commitIndex();
                long lastIdx = raftLog.lastIndex();
                int majority = ((peerStates.size() + 1) / 2) + 1;
                java.util.concurrent.atomic.AtomicInteger quorumAcks = new java.util.concurrent.atomic.AtomicInteger(1);
                java.util.concurrent.atomic.AtomicBoolean leaseRenewed = new java.util.concurrent.atomic.AtomicBoolean(false);

                for (PeerReplicationState ps : peerStates.values()) {
                    // Stop sending if we lost leadership mid-iteration (stepDown via callback)
                    if (!isLeader()) break;

                    try {
                        // If follower is too far behind (log already truncated), send snapshot
                        if (ps.nextIndex < raftLog.firstIndex()) {
                            sendInstallSnapshot(ps.peerId);
                            continue;
                        }

                        if (ps.appendInFlight) {
                            continue;
                        }

                        long requestGeneration = ++ps.requestGeneration;
                        ps.appendInFlight = true;

                        // Read entries the peer is missing (batch-limited)
                        long nextIdx = ps.nextIndex;
                        long prevLogIndex = nextIdx - 1;
                        long prevLogTerm = prevLogIndex > 0 ? raftLog.readEntryTerm(prevLogIndex) : 0;
                        if (prevLogTerm < 0) prevLogTerm = 0; // entry not yet readable

                        int count = (int) Math.min(lastIdx - nextIdx + 1, MAX_ENTRIES_PER_APPEND);
                        byte[][] entries = new byte[count][];
                        for (int i = 0; i < count; i++) {
                            byte[] entry = raftLog.readEntryRaw(nextIdx + i);
                            if (entry == null) {
                                entries[i] = new byte[0];
                            } else {
                                entries[i] = entry;
                            }
                        }

                        transport.sendAppendEntries(ps.peerId, currentTerm, nodeId,
                                prevLogIndex, prevLogTerm, entries, commitIdx)
                            .whenComplete((result, throwable) -> {
                                ps.appendInFlight = false;
                                if (throwable != null) {
                                    log.error("AppendEntries failed for peer {}: {}", ps.peerId,
                                        throwable.getMessage(), throwable);
                                    return;
                                }
                                if (requestGeneration != ps.requestGeneration) return;
                                // Bail out if this response belongs to a past leadership term
                                if (myGeneration != leaderGeneration) return;

                                if (result.success) {
                                    ps.matchIndex = result.matchIndex;
                                    ps.nextIndex = result.matchIndex + 1;
                                    if (quorumAcks.incrementAndGet() >= majority && leaseRenewed.compareAndSet(false, true)) {
                                        renewReadLease();
                                    }
                                    advanceCommitIndexFromAllPeers();
                                } else if (result.term > currentTerm) {
                                    election.stepDown(result.term);
                                } else {
                                    // Log inconsistency: use conflictIndex for fast backtrack
                                    if (result.conflictIndex > 0) {
                                        ps.nextIndex = Math.max(1, result.conflictIndex);
                                    } else {
                                        ps.nextIndex = Math.max(1, ps.nextIndex - 1);
                                    }
                                    log.debug("AppendEntries rejected by {} (conflictIdx={}), nextIndex={}",
                                        ps.peerId, result.conflictIndex, ps.nextIndex);
                                }
                            });
                    } catch (Exception peerEx) {
                        ps.appendInFlight = false;
                        log.error("Heartbeat failed for peer {}: {}", ps.peerId,
                            peerEx.getMessage(), peerEx);
                        // Continue to next peer — don't let one peer failure stop others
                    }
                }
                syncRaftMetrics();
            } catch (Throwable t) {
                log.error("Heartbeat task terminated unexpectedly — this is a bug", t);
                // Do NOT rethrow — ScheduledExecutorService would silently cancel the task
            }
        }, heartbeatMs, heartbeatMs, TimeUnit.MILLISECONDS);
    }

    private void advanceCommitIndexFromAllPeers() {
        long[] matchIndices = new long[peerStates.size() + 1];
        matchIndices[0] = raftLog.lastIndex(); // self always matches
        int i = 1;
        for (PeerReplicationState ps : peerStates.values()) {
            matchIndices[i++] = ps.matchIndex;
        }
        long before = replication.commitIndex();
        replication.advanceCommitIndex(matchIndices, election.currentTerm());
        if (replication.commitIndex() > before) {
            replication.applyCommitted();
        }
        syncRaftMetrics();
    }

    private void stopHeartbeat() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
        }
    }

    private void syncRaftMetrics() {
        RaftStatusSnapshot status = snapshotStatus();
        metrics.updateRaftRole(status.role().name());
        metrics.updateRaftLeaderId(status.leaderId());
        metrics.updateRaftTerm(status.term());
        metrics.updateRaftCommitIndex(status.commitIndex());
        metrics.updateRaftLastApplied(status.lastApplied());
        metrics.updateRaftReplicationLag(status.replicationLag());
        metrics.updateRaftConnectedPeers(status.connectedPeers());
        metrics.updateRaftTotalPeers(status.totalPeers());
    }

    private void notifyRuntimeRole() {
        notifyRuntimeRole(role());
    }

    private void notifyRuntimeRole(RaftRole role) {
        if (runtimeListener != null) {
            runtimeListener.onRoleChanged(role, election.currentTerm());
        }
    }

    @Override
    public RaftStatusSnapshot snapshotStatus() {
        long term = election.currentTerm();
        long commitIndex = replication.commitIndex();
        long lastApplied = replication.lastApplied();
        long commitLag = Math.max(0, commitIndex - lastApplied);
        long replicationLag = 0;
        Map<String, Boolean> reachability = new LinkedHashMap<>();
        int connectedPeers = 0;
        int totalPeers = 0;

        for (String peerId : peers) {
            if (peerId.equals(nodeId)) {
                continue;
            }

            totalPeers++;
            boolean connected = transport != null && transport.isPeerConnected(peerId);
            reachability.put(peerId, connected);
            if (connected) {
                connectedPeers++;
            }

            if (isLeader()) {
                PeerReplicationState ps = peerStates.get(peerId);
                if (ps != null) {
                    replicationLag = Math.max(replicationLag, Math.max(0, raftLog.lastIndex() - ps.matchIndex));
                }
            }
        }

        return new RaftStatusSnapshot(
            nodeId,
            election.role(),
            election.currentLeader(),
            term,
            commitIndex,
            lastApplied,
            commitLag,
            isLeader() ? replicationLag : 0,
            connectedPeers,
            totalPeers,
            reachability
        );
    }

    // ========== Peer replication state ==========

    /** Tracks replication progress for a single follower peer. */
    static class PeerReplicationState {
        final String peerId;
        volatile long nextIndex = 1;
        volatile long matchIndex;
        volatile long requestGeneration = 0;
        volatile boolean appendInFlight = false;

        PeerReplicationState(String peerId) {
            this.peerId = peerId;
        }
    }

    private void renewReadLease() {
        readLeaseUntilMs = System.currentTimeMillis() + readLeaseMs;
    }

    private static long computeReadLeaseMs(long electionMinMs, long heartbeatMs) {
        long heartbeatLease = Math.max(heartbeatMs * 2L, 100L);
        long maxSafeLease = Math.max(1L, electionMinMs - 1L);
        return Math.max(1L, Math.min(maxSafeLease, heartbeatLease));
    }
}

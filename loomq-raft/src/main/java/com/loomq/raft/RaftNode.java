package com.loomq.raft;

import com.loomq.common.RaftRole;
import com.loomq.common.RaftStatusSnapshot;
import com.loomq.metrics.LoomQMetrics;
import com.loomq.spi.RaftStatusProvider;
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
    private static final int SNAPSHOT_CHUNK_SIZE = 256 * 1024; // 256KB per chunk
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
    private final long shutdownTimeoutMs;
    private final ConcurrentMap<String, PeerReplicationState> peerStates;
    private ScheduledFuture<?> heartbeatTask;
    private volatile long readLeaseUntilMs = 0L;
    /** Chunk reassembly buffer for incoming InstallSnapshot chunks. */
    private final ConcurrentMap<String, ChunkReassembly> pendingChunks = new ConcurrentHashMap<>();
    /**
     * Leader generation counter — incremented each time this node becomes leader.
     * Heartbeat callbacks check this against their captured generation to discard
     * stale responses after stepDown mid-loop (§5.1 safety).
     */
    private volatile long leaderGeneration = 0;

    public RaftNode(RaftConfig config, WalAccessor wal, IntentStore store, RaftTransport transport) {
        this(config, wal, store, transport, null, null);
    }

    public RaftNode(RaftConfig config, WalAccessor wal, IntentStore store, RaftTransport transport,
                    RaftRuntimeListener runtimeListener) {
        this(config, wal, store, transport, runtimeListener, null);
    }

    /**
     * 创建 RaftNode，使用外部提供的 LeaderElection 实现。
     *
     * <p>当 externalElection 不为 null 时，使用外部选举实现（如 K8sLeaseElection）；
     * 否则创建默认的 RaftElection。
     */
    public RaftNode(RaftConfig config, WalAccessor wal, IntentStore store, RaftTransport transport,
                    RaftRuntimeListener runtimeListener, LeaderElection externalElection) {
        this.nodeId = config.nodeId();
        this.wal = wal;
        this.store = store;
        this.transport = transport;
        this.runtimeListener = runtimeListener;
        this.peers = config.peers();
        this.heartbeatMs = config.heartbeatMs();
        this.readLeaseMs = computeReadLeaseMs(config.electionMinMs(), config.heartbeatMs());
        this.shutdownTimeoutMs = config.shutdownTimeoutMs();
        this.peerStates = new ConcurrentHashMap<>();
        for (String peerId : peers) {
            if (!peerId.equals(nodeId)) {
                peerStates.put(peerId, new PeerReplicationState(peerId));
            }
        }
        this.raftLog = new RaftLog(wal);
        if (externalElection != null) {
            this.election = externalElection;
        } else {
            RaftElection raftElection = new RaftElection(nodeId, wal, config.peers(),
                config.electionMinMs(), config.electionMaxMs());
            this.election = raftElection;
        }
        this.replication = new LogReplication(nodeId, wal, raftLog, store, election, runtimeListener);
        this.heartbeatTimer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-heartbeat-" + nodeId);
            t.setDaemon(true);
            return t;
        });

        // Server-side RPC handlers (transport may be null for single-node/no-network tests)
        if (transport != null) {
            // RequestVote handler — only for Raft election mode
            if (election instanceof RaftElection raftElection) {
                transport.setOnRequestVote(msg -> {
                    boolean granted = raftElection.handleRequestVote(msg.epoch(), msg.candidateId(),
                        msg.lastLogIndex(), msg.lastLogEpoch());
                    syncRaftMetrics();
                    return granted;
                });
            }

            transport.setOnAppendEntries(msg -> {
                AppendEntriesResult result = replication.handleAppendEntries(msg.epoch(), msg.leaderId(),
                    msg.prevLogIndex(), msg.prevLogEpoch(), msg.entries(), msg.leaderCommit());
                syncRaftMetrics();
                return result;
            });

            transport.setOnInstallSnapshot(msg -> {
                Long appliedIndex = handleInstallSnapshot(msg);
                syncRaftMetrics();
                return appliedIndex;
            });

            transport.setOnInstallSnapshotChunk(msg -> {
                Long appliedIndex = handleInstallSnapshotChunk(msg);
                syncRaftMetrics();
                return appliedIndex;
            });
        }

        // Election lifecycle callbacks
        if (election instanceof RaftElection raftElection) {
            raftElection.setOnElectionStarted(this::onElectionStarted);
            raftElection.setOnBecomeLeader(this::onBecomeLeader);
            raftElection.setOnBecomeFollower(this::onBecomeFollower);
        } else {
            // K8s Lease mode — register listeners via interface
            election.addBecomeLeaderListener(this::onBecomeLeader);
            election.addBecomeFollowerListener(this::onBecomeFollower);
        }
    }

    public void start() {
        election.start();
        syncRaftMetrics();
        if (runtimeListener != null && role() == RaftRole.FOLLOWER) {
            notifyRuntimeRole();
        }
        log.info("RaftNode started: node={}, epoch={}, logIndex={}",
            nodeId, election.currentEpoch(), raftLog.lastIndex());
    }

    @Override
    public void close() {
        stopHeartbeat();  // Stop periodic heartbeat FIRST
        // 优雅关机：如果是 Leader，等待异步复制完成
        if (isLeader() && !peerStates.isEmpty() && shutdownTimeoutMs > 0) {
            gracefulShutdown();  // Now safe to send final heartbeats without racing
        }

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

    /**
     * 优雅关机：等待异步复制完成。
     *
     * <p>收到 SIGTERM 后，Leader 执行以下步骤：
     * <ol>
     *   <li>停止接收新的写入请求（election.stop() 后 isLeader() 返回 false）</li>
     *   <li>持续复制积压日志到 Followers</li>
     *   <li>等待 commitIndex 追上 lastLogIndex（最长 shutdownTimeoutMs）</li>
     *   <li>超时后强制关闭</li>
     * </ol>
     */
    private void gracefulShutdown() {
        log.info("Graceful shutdown: waiting for async replication (max {}ms)", shutdownTimeoutMs);
        long deadline = System.currentTimeMillis() + shutdownTimeoutMs;
        long lastLogIdx = raftLog.lastIndex();
        long commitIdx = replication.commitIndex();

        // 先做一次心跳，触发复制
        sendHeartbeats();

        while (System.currentTimeMillis() < deadline && commitIdx < lastLogIdx) {
            try {
                Thread.sleep(100);
                sendHeartbeats();
                commitIdx = replication.commitIndex();
                lastLogIdx = raftLog.lastIndex();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        if (commitIdx >= lastLogIdx) {
            log.info("Graceful shutdown: all entries replicated (commitIndex={})", commitIdx);
        } else {
            log.warn("Graceful shutdown: timeout, {} entries may not be replicated (commitIndex={}, lastLogIndex={})",
                lastLogIdx - commitIdx, commitIdx, lastLogIdx);
        }
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
        long epoch = election.currentEpoch(); // capture once
        long index = raftLog.appendEntry(epoch, entry);
        if (peerStates.isEmpty()) {
            // Single-node: commit and apply immediately
            replication.advanceCommitIndex(new long[]{index}, epoch);
            replication.applyCommitted();
        } else {
            // Multi-node: trigger immediate replication instead of waiting for next heartbeat tick
            triggerImmediateReplication();
        }
        log.debug("Proposed entry at index {}", index);
        return index;
    }

    /**
     * 异步模式：Leader 提交 entry 到本地 WAL 并立即提交应用。
     *
     * <p>不等待多数节点确认，直接将 commitIndex 推进到此 index。
     * 用于 ASYNC 复制模式，写入延迟最低，但 Leader 崩溃可能丢失未复制的数据。
     */
    public long proposeAsync(byte[] entry) {
        if (!isLeader()) {
            throw new IllegalStateException("Raft proposals must be issued by the leader");
        }
        long epoch = election.currentEpoch(); // capture once
        long index = raftLog.appendEntry(epoch, entry);
        // 立即推进 commitIndex 并应用（不等待 quorum）
        replication.advanceCommitIndex(new long[]{index}, epoch);
        replication.applyCommitted();
        // 后台异步复制到 Followers
        if (!peerStates.isEmpty()) {
            triggerImmediateReplication();
        }
        log.debug("Async proposed entry at index {}", index);
        return index;
    }

    /**
     * Trigger an immediate replication round by scheduling a heartbeat on the executor.
     * This reduces replication latency from avg(heartbeatMs/2) to near-zero.
     */
    private void triggerImmediateReplication() {
        try {
            heartbeatTimer.execute(this::sendHeartbeats);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Timer shut down — will be caught by next heartbeat cycle
            log.debug("Immediate replication trigger rejected (timer shut down)");
        }
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
     *
     * 对于大快照（>256KB），使用分块传输避免单次 RPC 内存爆炸。
     */
    public void sendInstallSnapshot(String peerId) {
        PeerReplicationState ps = peerStates.get(peerId);
        if (ps == null) {
            return;
        }

        long epoch = election.currentEpoch();
        long snapshotIndex = replication.lastApplied();
        long snapshotEpoch = snapshotIndex > 0 ? raftLog.readEntryEpoch(snapshotIndex) : 0;
        long requestGeneration = ++ps.requestGeneration;

        // Encode current store state as snapshot payload
        byte[] snapshotData = encodeStoreSnapshot();
        raftLog.compactThrough(snapshotIndex, snapshotEpoch);

        if (snapshotData.length <= SNAPSHOT_CHUNK_SIZE) {
            // Small snapshot: single RPC (original behavior)
            transport.sendInstallSnapshot(peerId, epoch, nodeId, snapshotIndex, snapshotEpoch, snapshotData)
                .thenAccept(newIndex -> {
                    if (requestGeneration != ps.requestGeneration) return;
                    if (newIndex >= 0) {
                        ps.nextIndex = newIndex + 1;
                        ps.matchIndex = newIndex;
                        log.info("InstallSnapshot accepted by {} (index={})", peerId, newIndex);
                    } else {
                        log.warn("InstallSnapshot rejected by {}", peerId);
                    }
                });
        } else {
            // Large snapshot: chunked transfer
            int totalChunks = (snapshotData.length + SNAPSHOT_CHUNK_SIZE - 1) / SNAPSHOT_CHUNK_SIZE;
            log.info("Sending chunked snapshot to {}: {} bytes in {} chunks", peerId, snapshotData.length, totalChunks);

            java.util.concurrent.CompletableFuture<Boolean> chain =
                java.util.concurrent.CompletableFuture.completedFuture(true);

            for (int i = 0; i < totalChunks; i++) {
                final int chunkIndex = i;
                int start = i * SNAPSHOT_CHUNK_SIZE;
                int end = Math.min(start + SNAPSHOT_CHUNK_SIZE, snapshotData.length);
                byte[] chunk = java.util.Arrays.copyOfRange(snapshotData, start, end);

                chain = chain.thenCompose(ok -> {
                    if (requestGeneration != ps.requestGeneration) {
                        return java.util.concurrent.CompletableFuture.completedFuture(false);
                    }
                    return transport.sendInstallSnapshotChunk(peerId, epoch, nodeId,
                        snapshotIndex, snapshotEpoch, chunkIndex, totalChunks, chunk);
                });
            }

            chain.thenAccept(success -> {
                if (requestGeneration != ps.requestGeneration) return;
                if (success) {
                    ps.nextIndex = snapshotIndex + 1;
                    ps.matchIndex = snapshotIndex;
                    log.info("Chunked snapshot accepted by {} (index={}, chunks={})", peerId, snapshotIndex, totalChunks);
                } else {
                    log.warn("Chunked snapshot rejected by {} (chunks={})", peerId, totalChunks);
                }
            }).exceptionally(ex -> {
                log.error("Chunked snapshot failed for {}: {}", peerId, ex.getMessage(), ex);
                return null;
            });
        }
    }

    /**
     * Follower: 处理 InstallSnapshot RPC。
     * 清空当前 IntentStore，解码并应用快照数据。
     *
     * @return 新的 lastApplied index（失败返回 -1）
     */
    private Long handleInstallSnapshot(RaftTransport.InstallSnapshotMessage msg) {
        if (msg.epoch() < election.currentEpoch()) {
            log.debug("Rejecting InstallSnapshot: stale epoch {} < {}", msg.epoch(), election.currentEpoch());
            return -1L;
        }
        election.onAppendEntries(msg.epoch(), msg.leaderId());

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

            raftLog.compactThrough(msg.lastIncludedIndex(), msg.lastIncludedEpoch());
            replication.resetToSnapshot(msg.lastIncludedIndex());
            log.info("InstallSnapshot applied: {} intents, index={}, epoch={}",
                decoded.size(), msg.lastIncludedIndex(), msg.lastIncludedEpoch());
            return msg.lastIncludedIndex();
        } catch (Exception e) {
            log.error("Failed to apply InstallSnapshot from {}", msg.leaderId(), e);
            return -1L;
        }
    }

    /**
     * Follower: handle an incoming InstallSnapshot chunk.
     * Buffers chunks until all received, then reassembles and applies.
     */
    private Long handleInstallSnapshotChunk(RaftTransport.InstallSnapshotChunkMessage msg) {
        if (msg.epoch() < election.currentEpoch()) {
            log.debug("Rejecting InstallSnapshot chunk: stale epoch {} < {}", msg.epoch(), election.currentEpoch());
            return -1L;
        }
        election.onAppendEntries(msg.epoch(), msg.leaderId());

        String reassemblyKey = msg.leaderId() + ":" + msg.lastIncludedIndex();
        ChunkReassembly reassembly = pendingChunks.computeIfAbsent(reassemblyKey,
            k -> new ChunkReassembly(msg.totalChunks(), msg.lastIncludedIndex(), msg.lastIncludedEpoch()));

        boolean complete = reassembly.addChunk(msg.chunkIndex(), msg.chunkData());
        if (!complete) {
            log.debug("Received chunk {}/{} for snapshot from {}", msg.chunkIndex() + 1, msg.totalChunks(), msg.leaderId());
            return msg.lastIncludedIndex(); // ack chunk but not yet complete
        }

        // All chunks received — reassemble and apply
        pendingChunks.remove(reassemblyKey);
        try {
            byte[] fullSnapshot = reassembly.reassemble();
            java.util.List<com.loomq.domain.intent.Intent> decoded = decodeStoreSnapshot(fullSnapshot);

            store.clear();
            for (com.loomq.domain.intent.Intent intent : decoded) {
                store.upsert(intent);
            }

            raftLog.compactThrough(msg.lastIncludedIndex(), msg.lastIncludedEpoch());
            replication.resetToSnapshot(msg.lastIncludedIndex());
            log.info("Chunked snapshot applied: {} intents, index={}, epoch={}",
                decoded.size(), msg.lastIncludedIndex(), msg.lastIncludedEpoch());
            return msg.lastIncludedIndex();
        } catch (Exception e) {
            log.error("Failed to apply chunked snapshot from {}", msg.leaderId(), e);
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

    private void onElectionStarted(long epoch) {
        syncRaftMetrics();
        if (peers.isEmpty() || transport == null) return;
        if (!(election instanceof RaftElection raftElection)) return; // K8s mode: no RequestVote
        log.info("Sending RequestVote to {} peers for epoch {}", peers.size(), epoch);
        long lastIdx = raftLog.lastIndex();
        long lastEpoch = raftLog.lastEpoch();
        AtomicInteger votesGranted = new AtomicInteger(1); // self-vote
        int majority = (peers.size() / 2) + 1;

        for (String peerId : peers) {
            if (peerId.equals(nodeId)) continue;
            transport.sendRequestVote(peerId, epoch, nodeId, lastIdx, lastEpoch)
                .thenAccept(granted -> {
                    if (granted) {
                        int total = votesGranted.incrementAndGet();
                        log.debug("Vote granted by {} (total={}/{})", peerId, total, majority);
                        if (total >= majority && raftElection.role() == RaftRole.CANDIDATE) {
                            raftElection.becomeLeader(epoch);
                        }
                    }
                });
        }
    }

    // ========== Heartbeat ==========

    private void onBecomeLeader(long epoch) {
        leaderGeneration++;
        renewReadLease();
        syncRaftMetrics();
        notifyRuntimeRole();
        log.info("Node {} became LEADER at epoch {} (gen {})", nodeId, epoch, leaderGeneration);

        // 追加 no-op entry（Raft 安全性保证）
        // 新 Leader 无法直接提交之前 term 的 entry，
        // 通过追加当前 term 的 no-op entry，间接提交所有之前的 entry。
        if (!peerStates.isEmpty()) {
            appendNoOpEntry();
        }

        startHeartbeat(epoch);
    }

    /**
     * 追加 no-op entry 到当前 term。
     * 用于新 Leader 间接提交之前 term 的 entry（Raft §5.4.2）。
     */
    private void appendNoOpEntry() {
        try {
            long index = raftLog.appendEntry(election.currentEpoch(), new byte[0]);
            log.debug("Appended no-op entry at index {} for epoch {}", index, election.currentEpoch());
            // 触发复制以尽快推进 commitIndex
            triggerImmediateReplication();
        } catch (Exception e) {
            log.error("Failed to append no-op entry", e);
        }
    }

    private void onBecomeFollower(long epoch) {
        readLeaseUntilMs = 0L;
        syncRaftMetrics();
        replication.failPendingWaiters(new IllegalStateException("Leadership lost"));
        notifyRuntimeRole();
        log.info("Node {} became FOLLOWER at epoch {}", nodeId, epoch);
        leaderGeneration++;
        stopHeartbeat();
    }

    private void startHeartbeat(long epoch) {
        stopHeartbeat();
        heartbeatTask = heartbeatTimer.scheduleAtFixedRate(this::sendHeartbeats,
            heartbeatMs, heartbeatMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Send AppendEntries to all peers. Called both by the periodic heartbeat timer
     * and by {@link #triggerImmediateReplication()} after a new entry is proposed.
     */
    private void sendHeartbeats() {
        try {
            if (!isLeader() || transport == null) {
                stopHeartbeat();
                return;
            }
            syncRaftMetrics();
            final long myGeneration = leaderGeneration;
            long currentEpoch = election.currentEpoch();
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

                    // Pipeline: skip if all missing entries are already covered by an in-flight RPC
                    if (ps.inflightMaxIndex > 0 && ps.nextIndex <= ps.inflightMaxIndex) {
                        continue;
                    }

                    long requestGeneration = ++ps.requestGeneration;

                    // Read entries the peer is missing (batch-limited)
                    long nextIdx = ps.nextIndex;
                    long prevLogIndex = nextIdx - 1;
                    long prevLogEpoch = prevLogIndex > 0 ? raftLog.readEntryEpoch(prevLogIndex) : 0;
                    if (prevLogEpoch < 0) prevLogEpoch = 0; // entry not yet readable

                    int count = (int) Math.min(lastIdx - nextIdx + 1, MAX_ENTRIES_PER_APPEND);
                    if (count <= 0) continue;

                    byte[][] entries = new byte[count][];
                    for (int i = 0; i < count; i++) {
                        byte[] entry = raftLog.readEntryRaw(nextIdx + i);
                        if (entry == null) {
                            entries[i] = new byte[0];
                        } else {
                            entries[i] = entry;
                        }
                    }

                    // Track the max index covered by this in-flight RPC
                    ps.inflightMaxIndex = nextIdx + count - 1;

                    transport.sendAppendEntries(ps.peerId, currentEpoch, nodeId,
                            prevLogIndex, prevLogEpoch, entries, commitIdx)
                        .whenComplete((result, throwable) -> {
                            if (throwable != null) {
                                log.error("AppendEntries failed for peer {}: {}", ps.peerId,
                                    throwable.getMessage(), throwable);
                                return;
                            }
                            if (requestGeneration != ps.requestGeneration) return;
                            // Bail out if this response belongs to a past leadership epoch
                            if (myGeneration != leaderGeneration) return;

                            if (result.success) {
                                ps.matchIndex = result.matchIndex;
                                ps.nextIndex = result.matchIndex + 1;
                                if (quorumAcks.incrementAndGet() >= majority && leaseRenewed.compareAndSet(false, true)) {
                                    renewReadLease();
                                }
                                advanceCommitIndexFromAllPeers();
                            } else if (result.epoch > currentEpoch) {
                                if (election instanceof RaftElection raftElection) {
                                    raftElection.stepDown(result.epoch);
                                } else if (election instanceof K8sLeaseElection k8sElection) {
                                    log.warn("AppendEntries rejected: follower {} has higher epoch {} > {}, stepping down",
                                        ps.peerId, result.epoch, currentEpoch);
                                    k8sElection.forceStepDown(result.epoch);
                                }
                            } else {
                                // Log inconsistency: use conflictIndex for fast backtrack
                                if (result.conflictIndex > 0) {
                                    ps.nextIndex = Math.max(1, result.conflictIndex);
                                } else {
                                    ps.nextIndex = Math.max(1, ps.nextIndex - 1);
                                }
                                // Reset inflightMaxIndex so next heartbeat can retry
                                ps.inflightMaxIndex = 0;
                                log.debug("AppendEntries rejected by {} (conflictIdx={}), nextIndex={}",
                                    ps.peerId, result.conflictIndex, ps.nextIndex);
                            }
                        });
                } catch (Exception peerEx) {
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
    }

    private void advanceCommitIndexFromAllPeers() {
        long[] matchIndices = new long[peerStates.size() + 1];
        matchIndices[0] = raftLog.lastIndex(); // self always matches
        int i = 1;
        for (PeerReplicationState ps : peerStates.values()) {
            matchIndices[i++] = ps.matchIndex;
        }
        long before = replication.commitIndex();
        replication.advanceCommitIndex(matchIndices, election.currentEpoch());
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
        metrics.updateRaftEpoch(status.epoch());
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
            runtimeListener.onRoleChanged(role, election.currentEpoch());
        }
    }

    @Override
    public RaftStatusSnapshot snapshotStatus() {
        long epoch = election.currentEpoch();
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
            epoch,
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
        /**
         * Highest index included in an in-flight AppendEntries RPC.
         * Used for pipelining: a new AE is sent only if nextIndex > inflightMaxIndex,
         * meaning there are entries not yet covered by any in-flight RPC.
         */
        volatile long inflightMaxIndex = 0;

        PeerReplicationState(String peerId) {
            this.peerId = peerId;
        }
    }

    /**
     * Reassembly buffer for chunked InstallSnapshot transfer.
     * Collects chunks by index and reassembles into a full snapshot blob.
     */
    static class ChunkReassembly {
        private final int totalChunks;
        private final long lastIncludedIndex;
        private final long lastIncludedEpoch;
        private final byte[][] chunks;
        private final java.util.concurrent.atomic.AtomicInteger receivedCount = new java.util.concurrent.atomic.AtomicInteger(0);

        ChunkReassembly(int totalChunks, long lastIncludedIndex, long lastIncludedEpoch) {
            this.totalChunks = totalChunks;
            this.lastIncludedIndex = lastIncludedIndex;
            this.lastIncludedEpoch = lastIncludedEpoch;
            this.chunks = new byte[totalChunks][];
        }

        /**
         * Add a chunk. Returns true when all chunks have been received.
         */
        synchronized boolean addChunk(int chunkIndex, byte[] data) {
            if (chunkIndex < 0 || chunkIndex >= totalChunks) {
                log.warn("Invalid chunk index: {} (total={})", chunkIndex, totalChunks);
                return false;
            }
            if (chunks[chunkIndex] != null) {
                // Duplicate chunk — ignore
                return receivedCount.get() >= totalChunks;
            }
            chunks[chunkIndex] = data;
            receivedCount.incrementAndGet();
            return receivedCount.get() >= totalChunks;
        }

        /**
         * Reassemble all chunks into a single byte array.
         */
        byte[] reassemble() {
            int totalSize = 0;
            for (byte[] chunk : chunks) {
                totalSize += chunk != null ? chunk.length : 0;
            }
            byte[] result = new byte[totalSize];
            int offset = 0;
            for (byte[] chunk : chunks) {
                if (chunk != null) {
                    System.arraycopy(chunk, 0, result, offset, chunk.length);
                    offset += chunk.length;
                }
            }
            return result;
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

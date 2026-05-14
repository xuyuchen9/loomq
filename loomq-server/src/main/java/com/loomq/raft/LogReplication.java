package com.loomq.raft;

import com.loomq.domain.intent.Intent;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.metrics.LoomQMetrics;
import com.loomq.spi.WalAccessor;
import com.loomq.store.IntentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicLong;

/** Raft Log Replication (§5.3). Leader 复制 entries 到 followers，多数确认后 commit。 */
public class LogReplication {
    private static final Logger log = LoggerFactory.getLogger(LogReplication.class);
    private static final LoomQMetrics metrics = LoomQMetrics.getInstance();
    private final String nodeId;
    private final WalAccessor wal;
    private final RaftLog raftLog;
    private final IntentStore store;
    private final LeaderElection election;
    private final AtomicLong commitIndex = new AtomicLong(0);
    private final AtomicLong lastApplied = new AtomicLong(0);

    public LogReplication(String nodeId, WalAccessor wal, RaftLog raftLog, IntentStore store, LeaderElection election) {
        this.nodeId = nodeId; this.wal = wal; this.raftLog = raftLog;
        this.store = store; this.election = election;
    }

    public long commitIndex() { return commitIndex.get(); }
    public long lastApplied() { return lastApplied.get(); }

    /**
     * 重置 commitIndex 和 lastApplied（InstallSnapshot 使用）。
     * Leader 通过 majority 推进，follower 通过此方法直接跳转到快照 index。
     */
    public void resetToSnapshot(long index) {
        commitIndex.set(index);
        lastApplied.set(index);
        metrics.updateRaftCommitIndex(index);
        metrics.updateRaftLastApplied(index);
    }

    /**
     * Leader: 推进 commitIndex（多数 matchIndex >= idx 且 entry 来自当前 term）。
     *
     * 算法：对 matchIndex 排序，取中位数作为 candidate（多数阈值）。
     * 若 candidate > commitIndex 且 candidate entry 的 term == currentTerm，则提交。
     * 复杂度 O(p log p)，支持数十节点集群。
     */
    public void advanceCommitIndex(long[] matchIndices, long currentTerm) {
        java.util.Arrays.sort(matchIndices);
        long candidate = matchIndices[matchIndices.length / 2]; // majority threshold
        if (candidate > commitIndex.get()
            && raftLog.readEntryTerm(candidate) == currentTerm) {
            commitIndex.set(candidate);
            metrics.updateRaftTerm(currentTerm);
            metrics.updateRaftCommitIndex(candidate);
            log.debug("commitIndex -> {}", candidate);
        }
    }

    /** Follower: 处理 AppendEntries */
    public AppendEntriesResult handleAppendEntries(long term, String leaderId,
            long prevLogIndex, long prevLogTerm, byte[][] entries, long leaderCommit) {
        if (term < election.currentTerm()) return AppendEntriesResult.fail(election.currentTerm());
        election.onAppendEntries(term, leaderId);
        metrics.updateRaftTerm(term);

        // §5.3: 验证 prevLogIndex 处的 entry term 与 prevLogTerm 一致
        if (prevLogIndex > 0) {
            long lastIdx = raftLog.lastIndex();
            if (lastIdx < prevLogIndex) return AppendEntriesResult.fail(term, lastIdx);
            long existingTerm = raftLog.readEntryTerm(prevLogIndex);
            if (existingTerm < 0) {
                // Entry not found or read error — treat as log inconsistency
                log.warn("Cannot read term at prevLogIndex={}, rejecting AppendEntries", prevLogIndex);
                return AppendEntriesResult.fail(term, lastIdx);
            }
            if (existingTerm != prevLogTerm) {
                // Term mismatch — conflicting entry (§5.3).
                // Report prevLogIndex as conflict point for leader to skip to.
                return AppendEntriesResult.fail(term, prevLogIndex);
            }
        }

        // Truncate conflicting entries before appending (§5.3).
        // If the follower has entries past prevLogIndex (e.g., from a previous leader),
        // they must be deleted so the new leader's entries replace them contiguously.
        if (prevLogIndex < raftLog.lastIndex()) {
            raftLog.truncateAfter(prevLogIndex);
        }

        // 写入新 entries（每个 entry 前 8 字节为原始 term，由 leader 在 readEntryRaw 中保留）
        for (byte[] entry : entries) {
            if (entry == null || entry.length <= 8) continue;
            java.nio.ByteBuffer entryBuf = java.nio.ByteBuffer.wrap(entry);
            long entryTerm = entryBuf.getLong();
            byte[] payload = new byte[entry.length - 8];
            entryBuf.get(payload);
            raftLog.appendEntry(entryTerm, payload);
        }
        long lastIdx = raftLog.lastIndex();
        boolean commitAdvanced = false;
        long currentCommit = commitIndex.get();
        if (leaderCommit > currentCommit) {
            long newCommit = Math.min(leaderCommit, lastIdx);
            if (newCommit > currentCommit) {
                commitIndex.set(newCommit);
                commitAdvanced = true;
            }
        }
        if (commitAdvanced) {
            applyCommitted();
        }
        metrics.updateRaftCommitIndex(commitIndex.get());
        metrics.updateRaftLastApplied(lastApplied.get());
        return AppendEntriesResult.success(term, lastIdx);
    }

    /** Apply committed entries to state machine */
    public void applyCommitted() {
        long committed = commitIndex.get();
        long applied = lastApplied.get();
        while (applied < committed) {
            applied++;
            lastApplied.set(applied);
            byte[] entryPayload = raftLog.readEntry(applied);
            if (entryPayload == null || entryPayload.length == 0) continue;
            try {
                Intent intent = IntentBinaryCodec.decode(entryPayload);
                store.upsert(intent);
                log.debug("Applied committed entry at index {}: intentId={}", applied, intent.getIntentId());
            } catch (Exception e) {
                log.error("Failed to apply committed entry at index {}", applied, e);
            }
        }
        metrics.updateRaftCommitIndex(commitIndex.get());
        metrics.updateRaftLastApplied(lastApplied.get());
    }
}

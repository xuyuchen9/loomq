package com.loomq.raft;

import com.loomq.common.RaftRole;
import com.loomq.spi.WalAccessor;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raft Leader Election (§5.2).
 *
 * 使用随机选举超时避免分裂投票。
 * epoch 和 votedFor 通过 WalAccessor 持久化。
 */
public class RaftElection implements LeaderElection {
    private static final Logger log = LoggerFactory.getLogger(RaftElection.class);

    private final String nodeId;
    private final WalAccessor wal;
    private final List<String> peers;
    private final long minTimeoutMs;
    private final long maxTimeoutMs;
    private final Random rng = new Random();
    private final ScheduledExecutorService timer;
    private volatile RaftRole role = RaftRole.FOLLOWER;
    private volatile String currentLeader = null;
    private ScheduledFuture<?> electionTask;

    private Consumer<Long> onBecomeLeader;
    private Consumer<Long> onBecomeFollower;
    private Consumer<Long> onElectionStarted;

    public RaftElection(String nodeId, WalAccessor wal, List<String> peers,
                        long minTimeoutMs, long maxTimeoutMs) {
        this.nodeId = nodeId;
        this.wal = wal;
        this.peers = peers;
        this.minTimeoutMs = minTimeoutMs;
        this.maxTimeoutMs = maxTimeoutMs;
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-election-" + nodeId);
            t.setDaemon(true);
            return t;
        });
    }

    public void setOnBecomeLeader(Consumer<Long> cb) { this.onBecomeLeader = cb; }
    public void setOnBecomeFollower(Consumer<Long> cb) { this.onBecomeFollower = cb; }
    public void setOnElectionStarted(Consumer<Long> cb) { this.onElectionStarted = cb; }

    @Override
    public void addBecomeLeaderListener(Consumer<Long> listener) { this.onBecomeLeader = listener; }

    @Override
    public void addBecomeFollowerListener(Consumer<Long> listener) { this.onBecomeFollower = listener; }

    @Override
    public RaftRole role() { return role; }
    @Override
    public boolean isLeader() { return role == RaftRole.LEADER; }
    @Override
    public String currentLeader() { return currentLeader; }
    @Override
    public long currentEpoch() { return wal.getLastLogEpoch(); }

    /**
     * Raft §5.4.1: 判断 candidate 的日志是否至少和本节点一样新。
     * 比较 lastLogEpoch，若相等再比较 lastLogIndex。
     */
    public boolean isUpToDate(long candidateLastIndex, long candidateLastEpoch) {
        long myLastEpoch = wal.getLastLogEntryEpoch();
        long myLastIndex = wal.getLastLogIndex();
        return candidateLastEpoch > myLastEpoch
            || (candidateLastEpoch == myLastEpoch && candidateLastIndex >= myLastIndex);
    }

    @Override
    public void start() {
        resetElectionTimer();
        log.info("RaftElection started: node={}, role=FOLLOWER, timeout=[{},{}]ms",
            nodeId, minTimeoutMs, maxTimeoutMs);
    }

    @Override
    public void stop() {
        if (electionTask != null) electionTask.cancel(false);
        timer.shutdown();
    }

    /**
     * 收到 AppendEntries 或 RequestVote 时重置选举时钟。
     *
     * synchronized 保证 cancel+schedule 原子性，防止选举超时线程
     * 和 RPC 回调线程并发调用导致多个计时器任务同时存在。
     */
    public synchronized void resetElectionTimer() {
        if (electionTask != null) electionTask.cancel(false);
        long delay = minTimeoutMs + rng.nextLong(maxTimeoutMs - minTimeoutMs);
        electionTask = timer.schedule(this::onElectionTimeout, delay, TimeUnit.MILLISECONDS);
    }

    /** 处理 RequestVote RPC */
    public boolean handleRequestVote(long candidateEpoch, String candidateId,
            long candidateLastIndex, long candidateLastEpoch) {
        long myEpoch = currentEpoch();

        if (candidateEpoch > myEpoch) {
            stepDown(candidateEpoch);
        }

        if (candidateEpoch < myEpoch) return false;

        String votedFor = wal.getVotedFor();
        boolean canVote = (votedFor == null || votedFor.equals(candidateId))
            && isUpToDate(candidateLastIndex, candidateLastEpoch);

        if (canVote) {
            wal.setEpochAndVotedFor(candidateEpoch, candidateId);
            wal.persistRaftMeta();  // Raft §5.2: persist before responding to RequestVote
            resetElectionTimer();
            return true;
        }
        return false;
    }

    /** 收到新 leader 的 AppendEntries */
    @Override
    public void onAppendEntries(long leaderEpoch, String leaderId) {
        if (leaderEpoch >= currentEpoch()) {
            if (role != RaftRole.FOLLOWER) {
                stepDown(leaderEpoch);
            }
            currentLeader = leaderId;
            resetElectionTimer();
        }
    }

    private void onElectionTimeout() {
        startElection();
    }

    private synchronized void startElection() {
        long newEpoch = currentEpoch() + 1;
        wal.setEpochAndVotedFor(newEpoch, nodeId);
        wal.persistRaftMeta();  // Raft §5.2: persist before sending RequestVote RPCs
        role = RaftRole.CANDIDATE;
        currentLeader = null;

        log.info("Starting election: node={}, epoch={}", nodeId, newEpoch);

        // Single-node cluster: immediately become leader and return
        // (becomeLeader() cancels the election timer; no need to re-arm it)
        if (peers.isEmpty()) {
            becomeLeader(newEpoch);
            return;
        }

        // Multi-node: notify RaftNode to send vote requests via transport
        if (onElectionStarted != null) {
            onElectionStarted.accept(newEpoch);
        }
        // Re-arm timer in case election fails
        resetElectionTimer();
    }

    public void becomeLeader(long epoch) {
        // Cancel the election timer to prevent self-triggered re-election
        if (electionTask != null) {
            electionTask.cancel(false);
        }
        role = RaftRole.LEADER;
        currentLeader = nodeId;
        log.info("Became LEADER: node={}, epoch={}", nodeId, epoch);
        if (onBecomeLeader != null) onBecomeLeader.accept(epoch);
    }

    public void stepDown(long epoch) {
        role = RaftRole.FOLLOWER;
        currentLeader = null;
        wal.setEpochAndVotedFor(epoch, null);
        wal.persistRaftMeta();  // Raft §5.2: persist before responding to AppendEntries
        resetElectionTimer();
        log.info("Stepped down to FOLLOWER: node={}, epoch={}", nodeId, epoch);
        if (onBecomeFollower != null) onBecomeFollower.accept(epoch);
    }
}

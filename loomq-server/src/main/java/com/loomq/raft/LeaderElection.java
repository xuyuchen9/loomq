package com.loomq.raft;

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
 * term 和 votedFor 通过 WalAccessor 持久化。
 */
public class LeaderElection {
    private static final Logger log = LoggerFactory.getLogger(LeaderElection.class);

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

    public LeaderElection(String nodeId, WalAccessor wal, List<String> peers,
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

    public RaftRole role() { return role; }
    public String currentLeader() { return currentLeader; }
    public long currentTerm() { return wal.getLastLogTerm(); }

    /**
     * Raft §5.4.1: 判断 candidate 的日志是否至少和本节点一样新。
     * 比较 lastLogTerm，若相等再比较 lastLogIndex。
     */
    public boolean isUpToDate(long candidateLastIndex, long candidateLastTerm) {
        long myLastTerm = wal.getLastLogEntryTerm();
        long myLastIndex = wal.getLastLogIndex();
        return candidateLastTerm > myLastTerm
            || (candidateLastTerm == myLastTerm && candidateLastIndex >= myLastIndex);
    }

    public void start() {
        resetElectionTimer();
        log.info("LeaderElection started: node={}, role=FOLLOWER, timeout=[{},{}]ms",
            nodeId, minTimeoutMs, maxTimeoutMs);
    }

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
    public boolean handleRequestVote(long candidateTerm, String candidateId,
            long candidateLastIndex, long candidateLastTerm) {
        long myTerm = currentTerm();

        if (candidateTerm > myTerm) {
            stepDown(candidateTerm);
        }

        if (candidateTerm < myTerm) return false;

        String votedFor = wal.getVotedFor();
        boolean canVote = (votedFor == null || votedFor.equals(candidateId))
            && isUpToDate(candidateLastIndex, candidateLastTerm);

        if (canVote) {
            wal.setTermAndVotedFor(candidateTerm, candidateId);
            wal.persistRaftMeta();  // Raft §5.2: persist before responding to RequestVote
            resetElectionTimer();
            return true;
        }
        return false;
    }

    /** 收到新 leader 的 AppendEntries */
    public void onAppendEntries(long leaderTerm, String leaderId) {
        if (leaderTerm >= currentTerm()) {
            if (role != RaftRole.FOLLOWER) {
                stepDown(leaderTerm);
            }
            currentLeader = leaderId;
            resetElectionTimer();
        }
    }

    private void onElectionTimeout() {
        startElection();
    }

    private synchronized void startElection() {
        long newTerm = currentTerm() + 1;
        wal.setTermAndVotedFor(newTerm, nodeId);
        wal.persistRaftMeta();  // Raft §5.2: persist before sending RequestVote RPCs
        role = RaftRole.CANDIDATE;
        currentLeader = null;

        log.info("Starting election: node={}, term={}", nodeId, newTerm);

        // Single-node cluster: immediately become leader and return
        // (becomeLeader() cancels the election timer; no need to re-arm it)
        if (peers.isEmpty()) {
            becomeLeader(newTerm);
            return;
        }

        // Multi-node: notify RaftNode to send vote requests via transport
        if (onElectionStarted != null) {
            onElectionStarted.accept(newTerm);
        }
        // Re-arm timer in case election fails
        resetElectionTimer();
    }

    public void becomeLeader(long term) {
        // Cancel the election timer to prevent self-triggered re-election
        if (electionTask != null) {
            electionTask.cancel(false);
        }
        role = RaftRole.LEADER;
        currentLeader = nodeId;
        log.info("Became LEADER: node={}, term={}", nodeId, term);
        if (onBecomeLeader != null) onBecomeLeader.accept(term);
    }

    public void stepDown(long term) {
        role = RaftRole.FOLLOWER;
        currentLeader = null;
        wal.setTermAndVotedFor(term, null);
        wal.persistRaftMeta();  // Raft §5.2: persist before responding to AppendEntries
        resetElectionTimer();
        log.info("Stepped down to FOLLOWER: node={}, term={}", nodeId, term);
        if (onBecomeFollower != null) onBecomeFollower.accept(term);
    }
}

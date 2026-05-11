package com.loomq.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * 副本状态
 *
 * @author loomq
 * @since v0.3+
 */
public class ReplicaState {

    private static final Logger logger = LoggerFactory.getLogger(ReplicaState.class);

    // 当前角色
    private final AtomicReference<ReplicaRole> role;

    // 当前任期（用于选举）
    private final AtomicReference<Long> currentTerm;

    // 已知的 Leader ID
    private final AtomicReference<String> knownLeaderId;

    // 已提交的日志索引
    private volatile long commitIndex = 0;

    // 最后应用的日志索引
    private volatile long lastAppliedIndex = 0;

    // 最后收到的心跳时间
    private volatile long lastHeartbeatTime = 0;

    // Leader 心跳超时（毫秒）
    private final long heartbeatTimeoutMs;

    /**
     * 创建副本状态
     *
     * @param initialRole 初始角色
     * @param heartbeatTimeoutMs 心跳超时时间
     */
    public ReplicaState(ReplicaRole initialRole, long heartbeatTimeoutMs) {
        this.role = new AtomicReference<>(initialRole);
        this.currentTerm = new AtomicReference<>(0L);
        this.knownLeaderId = new AtomicReference<>(null);
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    }

    /**
     * 获取当前角色
     */
    public ReplicaRole getRole() {
        return role.get();
    }

    /**
     * 是否是 Leader
     */
    public boolean isLeader() {
        return role.get() == ReplicaRole.LEADER;
    }

    /**
     * 是否是 Follower
     */
    public boolean isFollower() {
        return role.get() == ReplicaRole.FOLLOWER;
    }

    /**
     * 转换为 Leader
     */
    public boolean becomeLeader(long term) {
        ReplicaRole current = role.get();
        if (current == ReplicaRole.LEADER) {
            return true;
        }

        if (role.compareAndSet(current, ReplicaRole.LEADER)) {
            currentTerm.set(term);
            knownLeaderId.set(null);
            logger.info("Became LEADER for term {}", term);
            return true;
        }
        return false;
    }

    /**
     * 转换为 Follower
     */
    public boolean becomeFollower(long term, String leaderId) {
        ReplicaRole current = role.get();

        if (role.compareAndSet(current, ReplicaRole.FOLLOWER)) {
            currentTerm.set(term);
            knownLeaderId.set(leaderId);
            updateHeartbeat();
            logger.info("Became FOLLOWER for term {}, leader={}", term, leaderId);
            return true;
        }
        return false;
    }

    /**
     * 转换为 Candidate
     */
    public boolean becomeCandidate(long term) {
        ReplicaRole current = role.get();

        if (role.compareAndSet(current, ReplicaRole.CANDIDATE)) {
            currentTerm.set(term);
            knownLeaderId.set(null);
            logger.info("Became CANDIDATE for term {}", term);
            return true;
        }
        return false;
    }

    /**
     * 更新心跳时间
     */
    public void updateHeartbeat() {
        this.lastHeartbeatTime = System.currentTimeMillis();
    }

    /**
     * 检查心跳是否超时
     */
    public boolean isHeartbeatTimeout() {
        if (role.get() != ReplicaRole.FOLLOWER) {
            return false;
        }
        return System.currentTimeMillis() - lastHeartbeatTime > heartbeatTimeoutMs;
    }

    /**
     * 获取当前任期
     */
    public long getCurrentTerm() {
        return currentTerm.get();
    }

    /**
     * 获取已知 Leader ID
     */
    public String getKnownLeaderId() {
        return knownLeaderId.get();
    }

    /**
     * 获取已提交索引
     */
    public long getCommitIndex() {
        return commitIndex;
    }

    /**
     * 更新已提交索引
     */
    public void setCommitIndex(long index) {
        this.commitIndex = Math.max(this.commitIndex, index);
    }

    /**
     * 获取最后应用索引
     */
    public long getLastAppliedIndex() {
        return lastAppliedIndex;
    }

    /**
     * 更新最后应用索引
     */
    public void setLastAppliedIndex(long index) {
        this.lastAppliedIndex = Math.max(this.lastAppliedIndex, index);
    }

    @Override
    public String toString() {
        return String.format("ReplicaState{role=%s, term=%d, leader=%s, commitIndex=%d, lastApplied=%d}",
                role.get(), currentTerm.get(), knownLeaderId.get(), commitIndex, lastAppliedIndex);
    }
}

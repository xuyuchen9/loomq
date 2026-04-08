package com.loomq.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Failover 服务（自动故障转移）
 *
 * 设计目标（设计文档 §9）：
 * 1. 检测 Leader 故障
 * 2. 发起选举
 * 3. 提升 Follower 为 Leader
 * 4. RTO < 10s
 *
 * 简化设计：
 * - 基于心跳超时检测故障
 * - 使用租约机制避免脑裂
 * - 单步选举（无预投票）
 *
 * @author loomq
 * @since v0.3+
 */
public class FailoverService {

    private static final Logger logger = LoggerFactory.getLogger(FailoverService.class);

    // 默认选举超时（毫秒）
    public static final long DEFAULT_ELECTION_TIMEOUT_MS = 3000;

    // 默认心跳间隔（毫秒）
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 1000;

    // 副本状态
    private final ReplicaState replicaState;

    // WAL 复制器
    private final WalReplicator walReplicator;

    // 集群协调器
    private final ClusterCoordinator clusterCoordinator;

    // 选举超时
    private final long electionTimeoutMs;

    // 选举执行器
    private ScheduledExecutorService electionExecutor;

    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);

    // Failover 统计
    private final Stats stats = new Stats();

    // Failover 监听器
    private final CopyOnWriteArrayList<FailoverListener> listeners = new CopyOnWriteArrayList<>();

    /**
     * 创建 Failover 服务
     *
     * @param replicaState 副本状态
     * @param walReplicator WAL 复制器
     * @param clusterCoordinator 集群协调器
     */
    public FailoverService(ReplicaState replicaState, WalReplicator walReplicator,
                           ClusterCoordinator clusterCoordinator) {
        this.replicaState = replicaState;
        this.walReplicator = walReplicator;
        this.clusterCoordinator = clusterCoordinator;
        this.electionTimeoutMs = DEFAULT_ELECTION_TIMEOUT_MS;

        logger.info("FailoverService created, electionTimeout={}ms", electionTimeoutMs);
    }

    /**
     * 启动 Failover 服务
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        // 启动选举检测
        electionExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "failover-election");
            t.setDaemon(true);
            return t;
        });

        // 定期检查是否需要发起选举
        electionExecutor.scheduleAtFixedRate(
                this::checkElection,
                electionTimeoutMs,
                electionTimeoutMs / 2,
                TimeUnit.MILLISECONDS
        );

        logger.info("FailoverService started, role={}", replicaState.getRole());
    }

    /**
     * 停止 Failover 服务
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        if (electionExecutor != null) {
            electionExecutor.shutdown();
        }

        logger.info("FailoverService stopped");
    }

    /**
     * 检查是否需要发起选举
     */
    private void checkElection() {
        if (!running.get()) {
            return;
        }

        // 检查心跳超时
        if (walReplicator.shouldStartElection()) {
            logger.warn("Leader heartbeat timeout detected, starting election");
            startElection();
        }
    }

    /**
     * 发起选举
     */
    private void startElection() {
        long startTime = System.currentTimeMillis();

        // 转换为 Candidate
        if (!walReplicator.startElection()) {
            return;
        }

        stats.recordElection();

        // 简化实现：直接提升为 Leader
        // 实际应进行投票请求
        long newTerm = replicaState.getCurrentTerm();

        // 检查是否获得多数票（简化：单节点直接成功）
        int totalNodes = getTotalNodes();
        int votesNeeded = (totalNodes / 2) + 1;

        // 单节点集群直接成功
        if (totalNodes == 1) {
            becomeLeader(newTerm);
            return;
        }

        // 模拟投票（实际应向其他节点发送投票请求）
        int votes = 1;  // 自己的一票

        // TODO: 发送 RequestVote RPC 到其他节点

        if (votes >= votesNeeded) {
            becomeLeader(newTerm);
        } else {
            // 选举失败，退回 Follower
            logger.warn("Election failed: votes={}/{}, reverting to follower", votes, votesNeeded);
            replicaState.becomeFollower(newTerm, null);
            stats.recordElectionFailure();
        }

        long elapsed = System.currentTimeMillis() - startTime;
        stats.recordElectionTime(elapsed);

        logger.info("Election completed in {}ms, role={}", elapsed, replicaState.getRole());
    }

    /**
     * 成为 Leader
     */
    private void becomeLeader(long term) {
        if (replicaState.becomeLeader(term)) {
            logger.info(" Became LEADER for term {}", term);

            // 通知监听器
            notifyLeaderElected();

            // 启动 Leader 心跳
            // walReplicator 已在 start() 中启动心跳
        }
    }

    /**
     * 获取总节点数
     */
    private int getTotalNodes() {
        if (clusterCoordinator != null) {
            return clusterCoordinator.getClusterState().knownNodes();
        }
        return 1;
    }

    /**
     * 添加 Failover 监听器
     */
    public void addListener(FailoverListener listener) {
        listeners.add(listener);
    }

    /**
     * 移除 Failover 监听器
     */
    public void removeListener(FailoverListener listener) {
        listeners.remove(listener);
    }

    private void notifyLeaderElected() {
        for (FailoverListener listener : listeners) {
            try {
                listener.onLeaderElected(replicaState.getCurrentTerm());
            } catch (Exception e) {
                logger.error("Failover listener error", e);
            }
        }
    }

    private void notifyLeaderLost() {
        for (FailoverListener listener : listeners) {
            try {
                listener.onLeaderLost();
            } catch (Exception e) {
                logger.error("Failover listener error", e);
            }
        }
    }

    // ========== 统计接口 ==========

    public Stats getStats() {
        return stats;
    }

    /**
     * 统计信息
     */
    public static class Stats {
        private volatile long electionCount = 0;
        private volatile long electionSuccessCount = 0;
        private volatile long electionFailureCount = 0;
        private volatile long lastElectionTimeMs = 0;
        private volatile long totalElectionTimeMs = 0;

        void recordElection() {
            electionCount++;
        }

        void recordElectionFailure() {
            electionFailureCount++;
        }

        void recordElectionTime(long elapsedMs) {
            electionSuccessCount++;
            lastElectionTimeMs = elapsedMs;
            totalElectionTimeMs += elapsedMs;
        }

        public long getElectionCount() {
            return electionCount;
        }

        public long getElectionSuccessCount() {
            return electionSuccessCount;
        }

        public long getElectionFailureCount() {
            return electionFailureCount;
        }

        public long getLastElectionTimeMs() {
            return lastElectionTimeMs;
        }

        public double getAverageElectionTimeMs() {
            return electionSuccessCount > 0
                    ? (double) totalElectionTimeMs / electionSuccessCount
                    : 0;
        }
    }

    /**
     * Failover 监听器
     */
    public interface FailoverListener {
        /**
         * Leader 选举成功
         */
        default void onLeaderElected(long term) {}

        /**
         * Leader 丢失
         */
        default void onLeaderLost() {}
    }
}

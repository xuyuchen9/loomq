package com.loomq.cluster;

import com.loomq.entity.EventType;
import com.loomq.wal.WalRecord;
import com.loomq.wal.v2.WalWriterV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WAL 复制器（简化版 Raft 日志复制）
 *
 * 设计目标（设计文档 §3）：
 * 1. Leader 接收写入请求
 * 2. 同步 WAL 到 Follower
 * 3. 达到 quorum 后返回成功
 *
 * 简化设计：
 * - 不实现完整 Raft 协议
 * - Leader 负责调度
 * - Follower 仅复制
 * - 同步复制（简化实现）
 *
 * @author loomq
 * @since v0.3+
 */
public class WalReplicator {

    private static final Logger logger = LoggerFactory.getLogger(WalReplicator.class);

    // 默认复制超时（毫秒）
    public static final long DEFAULT_REPLICATION_TIMEOUT_MS = 5000;

    // 默认心跳间隔（毫秒）
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 1000;

    // 副本状态
    private final ReplicaState replicaState;

    // 本地 WAL 写入器
    private final WalWriterV2 walWriter;

    // Follower 节点列表（仅 Leader 使用）
    private final Map<String, FollowerConnection> followers;

    // Quorum 大小
    private final int quorumSize;

    // 复制超时
    private final long replicationTimeoutMs;

    // 心跳间隔
    private final long heartbeatIntervalMs;

    // 心跳执行器
    private ScheduledExecutorService heartbeatExecutor;

    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);

    // 复制统计
    private final Stats stats = new Stats();

    /**
     * 创建 WAL 复制器
     *
     * @param replicaState 副本状态
     * @param walWriter WAL 写入器
     * @param clusterConfig 集群配置
     */
    public WalReplicator(ReplicaState replicaState, WalWriterV2 walWriter, ClusterConfig clusterConfig) {
        this.replicaState = replicaState;
        this.walWriter = walWriter;
        this.followers = new ConcurrentHashMap<>();
        this.quorumSize = (clusterConfig.getTotalShards() / 2) + 1;
        this.replicationTimeoutMs = DEFAULT_REPLICATION_TIMEOUT_MS;
        this.heartbeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MS;

        logger.info("WalReplicator created, quorumSize={}", quorumSize);
    }

    /**
     * 启动复制器
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        // 如果是 Leader，启动心跳
        if (replicaState.isLeader()) {
            startHeartbeat();
        }

        logger.info("WalReplicator started, role={}", replicaState.getRole());
    }

    /**
     * 停止复制器
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
        }

        logger.info("WalReplicator stopped");
    }

    /**
     * 添加 Follower 连接
     */
    public void addFollower(String followerId, String host, int port) {
        followers.put(followerId, new FollowerConnection(followerId, host, port));
        logger.info("Added follower: {}@{}:{}", followerId, host, port);
    }

    /**
     * 移除 Follower 连接
     */
    public void removeFollower(String followerId) {
        followers.remove(followerId);
        logger.info("Removed follower: {}", followerId);
    }

    /**
     * 复制日志记录（Leader 调用）
     *
     * 流程：
     * 1. 本地写入 WAL
     * 2. 并行发送到所有 Follower
     * 3. 等待 quorum 确认
     *
     * @param record WAL 记录
     * @return 是否复制成功
     */
    public boolean replicate(WalRecord record) {
        if (!running.get()) {
            return false;
        }

        if (!replicaState.isLeader()) {
            logger.warn("Not leader, cannot replicate");
            return false;
        }

        long startTime = System.currentTimeMillis();

        try {
            // 1. 本地写入
            walWriter.append(
                    record.getTaskId(),
                    record.getBizKey(),
                    record.getEventType(),
                    record.getEventTime(),
                    record.getPayload()
            );
            stats.recordLocalWrite();

            // 2. 如果没有 Follower，单节点直接成功
            if (followers.isEmpty()) {
                stats.recordReplication(0, System.currentTimeMillis() - startTime);
                return true;
            }

            // 3. 并行发送到 Follower
            List<CompletableFuture<Boolean>> futures = new ArrayList<>();

            for (FollowerConnection follower : followers.values()) {
                futures.add(sendToFollower(follower, record));
            }

            // 4. 等待 quorum 确认
            int successCount = 1;  // 本地已成功
            int needed = quorumSize;

            for (CompletableFuture<Boolean> future : futures) {
                try {
                    if (future.get(replicationTimeoutMs, TimeUnit.MILLISECONDS)) {
                        successCount++;
                    }
                } catch (TimeoutException | ExecutionException | InterruptedException e) {
                    logger.warn("Follower replication failed: {}", e.getMessage());
                }

                if (successCount >= needed) {
                    break;
                }
            }

            long elapsed = System.currentTimeMillis() - startTime;
            stats.recordReplication(successCount, elapsed);

            if (successCount >= needed) {
                logger.debug("Replication succeeded: quorum={}/{}, elapsed={}ms",
                        successCount, needed, elapsed);
                return true;
            } else {
                logger.warn("Replication failed: quorum={}/{}, elapsed={}ms",
                        successCount, needed, elapsed);
                return false;
            }

        } catch (IOException e) {
            logger.error("Local WAL write failed", e);
            stats.recordError();
            return false;
        }
    }

    /**
     * 发送记录到 Follower
     */
    private CompletableFuture<Boolean> sendToFollower(FollowerConnection follower, WalRecord record) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // 简化实现：直接调用 HTTP API 发送
                // 实际生产环境应使用 gRPC 或 Netty
                return follower.sendReplicateRequest(record);
            } catch (Exception e) {
                logger.warn("Send to follower {} failed: {}", follower.getFollowerId(), e.getMessage());
                return false;
            }
        });
    }

    /**
     * 处理来自 Leader 的复制请求（Follower 调用）
     *
     * @param record WAL 记录
     * @return 是否接受
     */
    public boolean acceptReplication(WalRecord record) {
        if (!running.get()) {
            return false;
        }

        if (!replicaState.isFollower()) {
            logger.warn("Not follower, cannot accept replication");
            return false;
        }

        try {
            // 写入本地 WAL
            walWriter.append(
                    record.getTaskId(),
                    record.getBizKey(),
                    record.getEventType(),
                    record.getEventTime(),
                    record.getPayload()
            );

            // 更新心跳
            replicaState.updateHeartbeat();

            stats.recordLocalWrite();
            logger.debug("Accepted replication: taskId={}, seq={}",
                    record.getTaskId(), record.getRecordSeq());

            return true;

        } catch (IOException e) {
            logger.error("Failed to write replicated record", e);
            stats.recordError();
            return false;
        }
    }

    /**
     * 处理来自 Leader 的心跳
     */
    public boolean receiveHeartbeat(String leaderId, long term, long commitIndex) {
        if (!running.get()) {
            return false;
        }

        // 验证 term
        if (term < replicaState.getCurrentTerm()) {
            return false;
        }

        // 更新状态
        if (term > replicaState.getCurrentTerm()) {
            replicaState.becomeFollower(term, leaderId);
        }

        replicaState.updateHeartbeat();
        replicaState.setCommitIndex(commitIndex);

        logger.debug("Received heartbeat from leader={}, term={}, commit={}",
                leaderId, term, commitIndex);

        return true;
    }

    /**
     * 启动心跳（Leader 调用）
     */
    private void startHeartbeat() {
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "replication-heartbeat");
            t.setDaemon(true);
            return t;
        });

        heartbeatExecutor.scheduleAtFixedRate(
                this::sendHeartbeat,
                0,
                heartbeatIntervalMs,
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * 发送心跳到所有 Follower
     */
    private void sendHeartbeat() {
        if (!running.get() || !replicaState.isLeader()) {
            return;
        }

        for (FollowerConnection follower : followers.values()) {
            try {
                follower.sendHeartbeat(
                        replicaState.getCurrentTerm(),
                        replicaState.getCommitIndex()
                );
            } catch (Exception e) {
                logger.debug("Heartbeat to {} failed: {}", follower.getFollowerId(), e.getMessage());
            }
        }
    }

    /**
     * 检查是否应该发起选举
     */
    public boolean shouldStartElection() {
        return replicaState.isHeartbeatTimeout() && replicaState.isFollower();
    }

    /**
     * 发起选举
     */
    public boolean startElection() {
        long newTerm = replicaState.getCurrentTerm() + 1;
        return replicaState.becomeCandidate(newTerm);
    }

    // ========== 统计接口 ==========

    public Stats getStats() {
        return stats;
    }

    /**
     * 统计信息
     */
    public static class Stats {
        private final AtomicLong localWrites = new AtomicLong(0);
        private final AtomicLong replications = new AtomicLong(0);
        private final AtomicLong replicationSuccesses = new AtomicLong(0);
        private final AtomicLong errors = new AtomicLong(0);
        private final AtomicLong totalReplicationTimeMs = new AtomicLong(0);

        void recordLocalWrite() {
            localWrites.incrementAndGet();
        }

        void recordReplication(int successCount, long elapsedMs) {
            replications.incrementAndGet();
            replicationSuccesses.addAndGet(successCount);
            totalReplicationTimeMs.addAndGet(elapsedMs);
        }

        void recordError() {
            errors.incrementAndGet();
        }

        public long getLocalWrites() {
            return localWrites.get();
        }

        public long getReplications() {
            return replications.get();
        }

        public long getErrors() {
            return errors.get();
        }

        public double getAverageReplicationTimeMs() {
            long count = replications.get();
            return count > 0 ? (double) totalReplicationTimeMs.get() / count : 0;
        }
    }

    /**
     * Follower 连接（简化实现）
     */
    private static class FollowerConnection {
        private final String followerId;
        private final String host;
        private final int port;

        FollowerConnection(String followerId, String host, int port) {
            this.followerId = followerId;
            this.host = host;
            this.port = port;
        }

        String getFollowerId() {
            return followerId;
        }

        boolean sendReplicateRequest(WalRecord record) {
            // 简化实现：实际应使用 HTTP/gRPC
            // 这里返回 true 模拟成功
            return true;
        }

        boolean sendHeartbeat(long term, long commitIndex) {
            // 简化实现：实际应使用 HTTP/gRPC
            return true;
        }
    }
}

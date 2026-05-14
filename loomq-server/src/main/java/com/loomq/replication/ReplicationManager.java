package com.loomq.replication;

import com.loomq.cluster.ReplicaRole;
import com.loomq.domain.intent.AckMode;
import com.loomq.replication.client.ReplicaClient;
import com.loomq.replication.server.ReplicaServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Consumer;

/**
 * 复制管理器
 *
 * 这是 leader/follower ACK 流的遗留实现，保留给复制确认路径使用。
 * Raft 启动流程已经接管了集群引导，这个类只负责复制确认和状态维护。
 *
 * 职责：
 * 1. 管理节点角色（LEADER / FOLLOWER）
 * 2. 协调 WAL 写入与复制
 * 3. 维护复制状态（offset、lag）
 * 4. 处理 ACK 等待
 *
 * @author loomq
 * @since v0.4.8
 */
public class ReplicationManager implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationManager.class);

    // 节点信息
    private final String nodeId;

    // 角色状态
    private final AtomicReference<ReplicaRole> role = new AtomicReference<>(ReplicaRole.FOLLOWER);

    // Leader 侧客户端（仅 LEADER 角色时使用）
    private ReplicaClient replicaClient;

    // Follower 侧服务端（仅 FOLLOWER 角色时使用）
    private ReplicaServer replicaServer;
    private Function<ReplicationRecord, Boolean> recordApplier;

    // 复制状态
    private final AtomicLong lastReplicatedOffset = new AtomicLong(-1);
    private final AtomicLong lastAckedOffset = new AtomicLong(-1);

    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);

    // 复制错误回调
    private volatile Consumer<Throwable> onReplicaErrorCallback;

    public ReplicationManager(String nodeId) {
        this.nodeId = nodeId;
    }

    // ==================== 角色管理 ====================

    /**
     * 提升为 Leader
     */
    public synchronized CompletableFuture<Void> promoteToPrimary(String replicaHost, int replicaPort) {
        if (role.get() == ReplicaRole.LEADER) {
            logger.warn("Already leader, ignoring promotion request");
            return CompletableFuture.completedFuture(null);
        }

        logger.info("Promoting {} to LEADER, replica at {}:{}", nodeId, replicaHost, replicaPort);

        // 关闭 replica 服务器（如果有）
        if (replicaServer != null) {
            replicaServer.shutdown();
            replicaServer = null;
        }

        // 创建并启动 replica 客户端
        replicaClient = new ReplicaClient(nodeId, replicaHost, replicaPort);
        replicaClient.setAckCallback(this::onReplicaAck);
        replicaClient.setErrorCallback(this::onReplicaError);

        return replicaClient.connect()
            .thenRun(() -> {
                role.set(ReplicaRole.LEADER);
                running.set(true);
                logger.info("{} successfully promoted to LEADER", nodeId);
            });
    }

    /**
     * 降级为 Follower
     */
    public synchronized CompletableFuture<Void> demoteToReplica(String bindHost, int bindPort) {
        if (role.get() == ReplicaRole.FOLLOWER && replicaServer != null) {
            logger.warn("Already follower, ignoring demotion request");
            return CompletableFuture.completedFuture(null);
        }

        logger.info("Demoting {} to FOLLOWER, binding on {}:{}", nodeId, bindHost, bindPort);

        // 关闭 replica 客户端（如果有）
        if (replicaClient != null) {
            replicaClient.shutdown();
            replicaClient = null;
        }

        // 创建并启动 replica 服务器
        replicaServer = new ReplicaServer(nodeId, bindHost, bindPort);
        replicaServer.setRecordHandler(this::onReplicationRecord);
        replicaServer.setHeartbeatHandler(this::onHeartbeat);

        return replicaServer.start().thenRun(() -> {
            role.set(ReplicaRole.FOLLOWER);
            running.set(true);
            logger.info("{} successfully demoted to FOLLOWER", nodeId);
        });
    }

    /**
     * 获取当前角色
     */
    public ReplicaRole getRole() {
        return role.get();
    }

    /**
     * 检查是否是 Leader
     */
    public boolean isPrimary() {
        return role.get() == ReplicaRole.LEADER;
    }

    /**
     * 检查是否是 Follower
     */
    public boolean isReplica() {
        return role.get() == ReplicaRole.FOLLOWER;
    }

    // ==================== 复制核心 ====================

    /**
     * 复制记录（Leader 调用）
     *
     * 根据 ackLevel 决定等待策略：
     * - ASYNC: 立即返回，不等待
     * - DURABLE: 等待本地 WAL fsync（由调用方保证）
     * - REPLICATED: 等待本地 fsync + replica ACK
     *
     * @param record 复制记录
     * @param ackLevel ACK 级别
     * @return CompletableFuture 在达到指定确认级别后完成
     */
    public CompletableFuture<ReplicationResult> replicate(ReplicationRecord record, AckMode ackLevel) {
        if (!isPrimary()) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Only leader can replicate"));
        }

        if (replicaClient == null || !replicaClient.isConnected()) {
            // Peer 未连接，根据策略降级
            logger.warn("Peer not connected, cannot achieve {} for offset={}",
                ackLevel, record.getOffset());

            if (ackLevel == AckMode.REPLICATED) {
                return CompletableFuture.failedFuture(
                    new IllegalStateException("Peer not connected, cannot achieve REPLICATED"));
            }

            // ASYNC/DURABLE can fall back to local persistence
            return CompletableFuture.completedFuture(
                new ReplicationResult(record.getOffset(), AckStatus.PERSISTED, true));
        }

        // 更新最后复制 offset
        lastReplicatedOffset.set(record.getOffset());

        // 发送给 peer
        CompletableFuture<Ack> replicaFuture = replicaClient.send(record);

        return replicaFuture.thenApply(ack -> {
            if (ack.isSuccess()) {
                lastAckedOffset.set(Math.max(lastAckedOffset.get(), ack.getOffset()));
                return new ReplicationResult(record.getOffset(), AckStatus.REPLICATED, false);
            } else {
                return new ReplicationResult(record.getOffset(), ack.getStatus(), false);
            }
        });
    }

    /**
     * 等待 REPLICATED 确认
     *
     * 组合本地 fsync future 和 replica ack future
     */
    public CompletableFuture<ReplicationResult> awaitReplicated(
            long offset,
            CompletableFuture<Void> localSyncFuture,
            CompletableFuture<Ack> replicaAckFuture) {

        // 组合两个 future
        return CompletableFuture.allOf(localSyncFuture, replicaAckFuture)
            .thenApply(v -> {
                Ack ack = replicaAckFuture.join();
                if (ack.isSuccess()) {
                    lastAckedOffset.set(offset);
                    return new ReplicationResult(offset, AckStatus.REPLICATED, false);
                } else {
                    return new ReplicationResult(offset, ack.getStatus(), false);
                }
            })
            .orTimeout(30, TimeUnit.SECONDS)
            .exceptionally(ex -> {
                logger.error("Timeout waiting for REPLICATED ack, offset={}", offset);
                return new ReplicationResult(offset, AckStatus.TIMEOUT, false);
            });
    }

    // ==================== 回调处理 ====================

    /**
     * 收到 peer 的 ACK（Leader 侧）
     */
    private void onReplicaAck(Ack ack) {
        logger.debug("Received peer ACK: offset={}, status={}",
            ack.getOffset(), ack.getStatus());

        if (ack.isSuccess()) {
            lastAckedOffset.updateAndGet(current ->
                Math.max(current, ack.getOffset()));
        }
    }

    /**
     * Peer 连接错误（Leader 侧）
     */
    private void onReplicaError(Throwable error) {
        logger.error("Peer connection error", error);
        Consumer<Throwable> callback = onReplicaErrorCallback;
        if (callback != null) {
            try {
                callback.accept(error);
            } catch (Exception e) {
                // 用户回调防御：回调异常不应影响 peer 连接管理
                logger.error("Replica error callback failed", e);
            }
        }
    }

    /**
     * 收到复制记录（Follower 侧）
     */
    private void onReplicationRecord(ReplicationRecord record) {
        logger.debug("Received replication record: offset={}, type={}",
            record.getOffset(), record.getType());

        // 硬约束 #4：幂等性检查
        // 实际幂等检查在应用层（IntentStore）处理
        // 这里只负责调用应用处理器

        if (recordApplier != null) {
            try {
                Boolean applied = recordApplier.apply(record);
                if (applied != null && !applied) {
                    logger.warn("Record not applied (possibly duplicate): offset={}",
                        record.getOffset());
                }
            } catch (RuntimeException e) {
                logger.error("Failed to apply record: offset={}", record.getOffset(), e);
                throw e;  // 抛出异常会导致 ACK 失败
            }
        }
    }

    /**
     * 收到心跳（Follower 侧）
     */
    private void onHeartbeat(com.loomq.replication.protocol.HeartbeatMessage heartbeat) {
        logger.debug("Received heartbeat from leader: nodeId={}, offset={}",
            heartbeat.getNodeId(), heartbeat.getLastAppliedOffset());
    }

    /**
     * 设置记录应用器（Follower 侧使用）
     */
    public void setRecordApplier(Function<ReplicationRecord, Boolean> applier) {
        this.recordApplier = applier;
    }

    /**
     * 设置 peer 连接错误回调
     */
    public void setOnReplicaError(Consumer<Throwable> callback) {
        this.onReplicaErrorCallback = callback;
    }

    // ==================== 状态查询 ====================

    /**
     * 获取最后复制的 offset
     */
    public long getLastReplicatedOffset() {
        return lastReplicatedOffset.get();
    }

    /**
     * 获取最后确认的 offset
     */
    public long getLastAckedOffset() {
        return lastAckedOffset.get();
    }

    /**
     * 获取复制延迟（已复制但未确认的 offset 数量）
     */
    public long getReplicationLag() {
        return lastReplicatedOffset.get() - lastAckedOffset.get();
    }

    /**
     * 检查 peer 是否健康
     */
    public boolean isReplicaHealthy() {
        if (!isPrimary() || replicaClient == null) {
            return false;
        }
        return replicaClient.isConnected();
    }

    // ==================== 生命周期 ====================

    @Override
    public void close() throws IOException {
        running.set(false);

        if (replicaClient != null) {
            replicaClient.shutdown();
            replicaClient = null;
        }

        if (replicaServer != null) {
            replicaServer.shutdown();
            replicaServer = null;
        }

        role.set(ReplicaRole.FOLLOWER);
        lastReplicatedOffset.set(-1);
        lastAckedOffset.set(-1);

        logger.info("ReplicationManager closed");
    }

    // ==================== 内部类 ====================

    /**
     * 复制结果
     */
    public record ReplicationResult(
        long offset,
        AckStatus status,
        boolean degraded  // 是否发生降级
    ) {
        public boolean isSuccess() {
            return status.isSuccess();
        }

        public boolean isReplicated() {
            return status == AckStatus.REPLICATED;
        }

        @Override
        public String toString() {
            return String.format("ReplicationResult{offset=%d, status=%s, degraded=%s}",
                offset, status, degraded);
        }
    }
}

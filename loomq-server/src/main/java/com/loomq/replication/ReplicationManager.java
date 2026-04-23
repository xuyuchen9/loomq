package com.loomq.replication;

import com.loomq.cluster.ReplicaRole;
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
 * 管理 primary/replica 角色，协调 WAL 写入与复制
 *
 * 职责：
 * 1. 管理节点角色（PRIMARY / REPLICA）
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

    // Primary 组件（仅 PRIMARY 角色时使用）
    private ReplicaClient replicaClient;

    // Replica 组件（仅 REPLICA 角色时使用）
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
     * 提升为 Primary
     */
    public synchronized CompletableFuture<Void> promoteToPrimary(String replicaHost, int replicaPort) {
        if (role.get() == ReplicaRole.LEADER) {
            logger.warn("Already primary, ignoring promotion request");
            return CompletableFuture.completedFuture(null);
        }

        logger.info("Promoting {} to PRIMARY, replica at {}:{}", nodeId, replicaHost, replicaPort);

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
                logger.info("{} successfully promoted to PRIMARY", nodeId);
            });
    }

    /**
     * 降级为 Replica
     */
    public synchronized CompletableFuture<Void> demoteToReplica(String bindHost, int bindPort) {
        if (role.get() == ReplicaRole.FOLLOWER && replicaServer != null) {
            logger.warn("Already replica, ignoring demotion request");
            return CompletableFuture.completedFuture(null);
        }

        logger.info("Demoting {} to REPLICA, binding on {}:{}", nodeId, bindHost, bindPort);

        // 关闭 replica 客户端（如果有）
        if (replicaClient != null) {
            replicaClient.shutdown();
            replicaClient = null;
        }

        // 创建并启动 replica 服务器
        replicaServer = new ReplicaServer(nodeId, bindHost, bindPort);
        replicaServer.setRecordHandler(this::onReplicationRecord);
        replicaServer.setHeartbeatHandler(this::onHeartbeat);

        CompletableFuture<Void> future = replicaServer.start();
        role.set(ReplicaRole.FOLLOWER);
        running.set(true);

        logger.info("{} successfully demoted to REPLICA", nodeId);
        return future;
    }

    /**
     * 获取当前角色
     */
    public ReplicaRole getRole() {
        return role.get();
    }

    /**
     * 检查是否是 Primary
     */
    public boolean isPrimary() {
        return role.get() == ReplicaRole.LEADER;
    }

    /**
     * 检查是否是 Replica
     */
    public boolean isReplica() {
        return role.get() == ReplicaRole.FOLLOWER;
    }

    // ==================== 复制核心 ====================

    /**
     * 复制记录（Primary 调用）
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
    public CompletableFuture<ReplicationResult> replicate(ReplicationRecord record, AckLevel ackLevel) {
        if (!isPrimary()) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Only primary can replicate"));
        }

        if (replicaClient == null || !replicaClient.isConnected()) {
            // Replica 未连接，根据策略降级
            logger.warn("Replica not connected, cannot achieve {} for offset={}",
                ackLevel, record.getOffset());

            if (ackLevel == AckLevel.REPLICATED) {
                return CompletableFuture.failedFuture(
                    new IllegalStateException("Replica not connected, cannot achieve REPLICATED"));
            }

            // ASYNC/DURABLE 可以降级为本地持久化
            return CompletableFuture.completedFuture(
                new ReplicationResult(record.getOffset(), AckStatus.PERSISTED, true));
        }

        // 更新最后复制 offset
        lastReplicatedOffset.set(record.getOffset());

        // 发送给 replica
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
     * 收到 replica 的 ACK（Primary 侧）
     */
    private void onReplicaAck(Ack ack) {
        logger.debug("Received replica ACK: offset={}, status={}",
            ack.getOffset(), ack.getStatus());

        if (ack.isSuccess()) {
            lastAckedOffset.updateAndGet(current ->
                Math.max(current, ack.getOffset()));
        }
    }

    /**
     * Replica 连接错误（Primary 侧）
     */
    private void onReplicaError(Throwable error) {
        logger.error("Replica connection error", error);
        Consumer<Throwable> callback = onReplicaErrorCallback;
        if (callback != null) {
            try {
                callback.accept(error);
            } catch (Exception e) {
                logger.error("Replica error callback failed", e);
            }
        }
    }

    /**
     * 收到复制记录（Replica 侧）
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
            } catch (Exception e) {
                logger.error("Failed to apply record: offset={}", record.getOffset(), e);
                throw e;  // 抛出异常会导致 ACK 失败
            }
        }
    }

    /**
     * 收到心跳（Replica 侧）
     */
    private void onHeartbeat(com.loomq.replication.protocol.HeartbeatMessage heartbeat) {
        logger.debug("Received heartbeat from primary: nodeId={}, offset={}",
            heartbeat.getNodeId(), heartbeat.getLastAppliedOffset());
    }

    /**
     * 设置记录应用器（Replica 侧使用）
     */
    public void setRecordApplier(Function<ReplicationRecord, Boolean> applier) {
        this.recordApplier = applier;
    }

    /**
     * 设置 replica 连接错误回调
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
     * 检查 replica 是否健康
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
        }

        if (replicaServer != null) {
            replicaServer.shutdown();
        }

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

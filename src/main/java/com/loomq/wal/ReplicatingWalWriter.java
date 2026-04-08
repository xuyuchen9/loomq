package com.loomq.wal;

import com.loomq.config.WalConfig;
import com.loomq.entity.EventType;
import com.loomq.replication.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 支持复制的 WAL 写入器
 *
 * 扩展 AsyncWalWriter，在写入本地 WAL 后触发复制到 replica。
 *
 * 三种 ACK 级别：
 * - ASYNC: 进入内存队列即返回（最快）
 * - DURABLE: 本地 WAL fsync 后返回（默认）
 * - REPLICATED: 本地 fsync + replica 确认后返回（最安全）
 *
 * @author loomq
 * @since v0.4.8
 */
public class ReplicatingWalWriter extends AsyncWalWriter {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatingWalWriter.class);

    // 复制管理器
    private final ReplicationManager replicationManager;

    // 全局 offset 生成器（单调递增）
    private final AtomicLong globalOffset = new AtomicLong(0);

    // 是否启用复制
    private final boolean replicationEnabled;

    public ReplicatingWalWriter(WalConfig config, String nodeId) throws IOException {
        super(config);
        this.replicationManager = new ReplicationManager(nodeId);
        this.replicationEnabled = config.isReplicationEnabled();
    }

    /**
     * 获取复制管理器
     */
    public ReplicationManager getReplicationManager() {
        return replicationManager;
    }

    /**
     * 追加事件（带 ACK 级别选择）
     *
     * @param taskId 任务 ID
     * @param bizKey 业务键
     * @param eventType 事件类型
     * @param eventTime 事件时间
     * @param payload 事件数据
     * @param ackLevel ACK 级别
     * @return CompletableFuture 在达到指定确认级别后完成
     */
    public CompletableFuture<WriteResult> append(
            String taskId,
            String bizKey,
            EventType eventType,
            long eventTime,
            byte[] payload,
            AckLevel ackLevel) {

        // 生成全局 offset
        long offset = globalOffset.incrementAndGet();

        // 1. 本地 WAL 写入
        long walSequence;
        try {
            walSequence = super.append(taskId, bizKey, eventType, eventTime, payload);
        } catch (IOException e) {
            logger.error("Failed to append to local WAL, offset={}", offset, e);
            return CompletableFuture.failedFuture(e);
        }

        // ASYNC 模式：立即返回
        if (ackLevel == AckLevel.ASYNC) {
            return CompletableFuture.completedFuture(
                new WriteResult(offset, walSequence, AckStatus.RECEIVED, true));
        }

        // 创建本地 fsync future
        CompletableFuture<Void> localSyncFuture = waitForLocalSync(walSequence);

        // DURABLE 模式：等待本地 fsync
        if (ackLevel == AckLevel.DURABLE) {
            return localSyncFuture.thenApply(v ->
                new WriteResult(offset, walSequence, AckStatus.PERSISTED, true));
        }

        // REPLICATED 模式：等待本地 fsync + replica ACK
        if (ackLevel == AckLevel.REPLICATED) {
            if (!replicationEnabled || !replicationManager.isPrimary()) {
                // 复制未启用或不是 primary，降级为 DURABLE
                logger.warn("Replication not available for REPLICATED ack, degrading to DURABLE, offset={}",
                    offset);
                return localSyncFuture.thenApply(v ->
                    new WriteResult(offset, walSequence, AckStatus.PERSISTED, true));
            }

            // 创建复制记录
            ReplicationRecord record = createReplicationRecord(
                offset, taskId, eventType, payload);

            // 发送给 replica
            CompletableFuture<ReplicationManager.ReplicationResult> replicaFuture =
                replicationManager.replicate(record, ackLevel);

            // 组合本地 fsync 和 replica ACK
            return localSyncFuture.thenCombine(replicaFuture,
                (local, replica) -> new WriteResult(
                    offset,
                    walSequence,
                    replica.status(),
                    replica.degraded()
                ))
                .orTimeout(30, TimeUnit.SECONDS)
                .exceptionally(ex -> {
                    logger.error("Timeout waiting for REPLICATED ack, offset={}", offset);
                    return new WriteResult(offset, walSequence, AckStatus.TIMEOUT, false);
                });
        }

        return CompletableFuture.failedFuture(
            new IllegalArgumentException("Unknown ack level: " + ackLevel));
    }

    /**
     * 创建复制记录
     */
    private ReplicationRecord createReplicationRecord(
            long offset,
            String taskId,
            EventType eventType,
            byte[] payload) {

        ReplicationRecordType type = mapEventType(eventType);

        // payload 格式：[taskIdLength(2)][taskId][eventPayload]
        byte[] taskIdBytes = taskId.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] recordPayload = new byte[2 + taskIdBytes.length + (payload != null ? payload.length : 0)];

        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(recordPayload);
        buffer.putShort((short) taskIdBytes.length);
        buffer.put(taskIdBytes);
        if (payload != null) {
            buffer.put(payload);
        }

        return ReplicationRecord.builder()
            .offset(offset)
            .type(type)
            .sourceNodeId(replicationManager.getRole().name())
            .payload(recordPayload)
            .build();
    }

    /**
     * 映射 EventType 到 ReplicationRecordType
     */
    private ReplicationRecordType mapEventType(EventType eventType) {
        return switch (eventType) {
            case CREATE -> ReplicationRecordType.TASK_CREATE;
            case CANCEL -> ReplicationRecordType.TASK_CANCEL;
            case FIRE_NOW -> ReplicationRecordType.TASK_TRIGGER_NOW;
            case MODIFY -> ReplicationRecordType.TASK_MODIFY_DELAY;
            case ACK, FAIL -> ReplicationRecordType.STATE_TRANSITION;
            case RETRY -> ReplicationRecordType.STATE_RETRY;
            case EXPIRE -> ReplicationRecordType.STATE_TIMEOUT;
            case DEAD_LETTER -> ReplicationRecordType.DLQ_ENTER;
            default -> ReplicationRecordType.TASK_CREATE;
        };
    }

    /**
     * 等待本地 WAL fsync
     */
    private CompletableFuture<Void> waitForLocalSync(long sequence) {
        return CompletableFuture.runAsync(() -> {
            try {
                boolean synced = awaitDurable(sequence, 5000);
                if (!synced) {
                    throw new IOException("Timeout waiting for local fsync, sequence=" + sequence);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * 获取当前全局 offset
     */
    public long getCurrentOffset() {
        return globalOffset.get();
    }

    /**
     * 获取复制延迟（offset 数量）
     */
    public long getReplicationLag() {
        if (!replicationEnabled) {
            return 0;
        }
        return replicationManager.getReplicationLag();
    }

    @Override
    public void close() {
        try {
            replicationManager.close();
        } catch (IOException e) {
            logger.error("Error closing replication manager", e);
        }
        super.close();
    }

    // ==================== 结果记录 ====================

    /**
     * 写入结果
     */
    public record WriteResult(
        long offset,           // 全局 offset
        long walSequence,      // WAL 序列号
        AckStatus status,      // 确认状态
        boolean degraded       // 是否降级
    ) {
        public boolean isSuccess() {
            return status.isSuccess();
        }

        public boolean isReplicated() {
            return status == AckStatus.REPLICATED;
        }

        @Override
        public String toString() {
            return String.format("WriteResult{offset=%d, walSeq=%d, status=%s, degraded=%s}",
                offset, walSequence, status, degraded);
        }
    }
}

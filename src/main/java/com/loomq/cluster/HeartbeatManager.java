package com.loomq.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * 心跳管理器
 *
 * 硬约束 #1 续：心跳检测与故障发现
 *
 * 职责：
 * 1. 定期发送心跳到 coordinator
 * 2. 接收和处理其他节点的心跳
 * 3. 检测心跳超时，触发故障通知
 * 4. 维护节点的健康状态
 *
 * 设计原则：
 * - 心跳间隔：默认 1 秒
 * - 心跳超时：默认 5 秒
 * - 双重确认：网络探测 + 心跳超时
 *
 * @author loomq
 * @since v0.4.8
 */
public class HeartbeatManager implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(HeartbeatManager.class);

    // 默认心跳间隔（毫秒）
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 1000;

    // 默认心跳超时（毫秒）
    public static final long DEFAULT_HEARTBEAT_TIMEOUT_MS = 5000;

    // 节点信息
    private final String nodeId;
    private final String shardId;

    // 心跳间隔和超时
    private final long heartbeatIntervalMs;
    private final long heartbeatTimeoutMs;

    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);

    // 调度器
    private final ScheduledExecutorService heartbeatExecutor;

    // 当前角色
    private final AtomicReference<ReplicaRole> currentRole;

    // 最后应用 offset
    private final AtomicLong lastAppliedOffset;

    // 最后发送心跳时间
    private final AtomicLong lastHeartbeatSent;

    // 节点健康状态：nodeId -> NodeHealth
    private final ConcurrentHashMap<String, NodeHealth> nodeHealthMap;

    // 故障监听器
    private volatile Consumer<NodeFailureEvent> failureListener;

    // 心跳发送回调
    private volatile Consumer<HeartbeatMessage> heartbeatSender;

    // 租约引用
    private final AtomicReference<CoordinatorLease> currentLease;

    /**
     * 创建心跳管理器
     *
     * @param nodeId 节点 ID
     * @param shardId 分片 ID
     * @param initialRole 初始角色
     */
    public HeartbeatManager(String nodeId, String shardId, ReplicaRole initialRole) {
        this(nodeId, shardId, initialRole,
            DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_HEARTBEAT_TIMEOUT_MS);
    }

    /**
     * 创建心跳管理器
     *
     * @param nodeId 节点 ID
     * @param shardId 分片 ID
     * @param initialRole 初始角色
     * @param heartbeatIntervalMs 心跳间隔（毫秒）
     * @param heartbeatTimeoutMs 心跳超时（毫秒）
     */
    public HeartbeatManager(String nodeId, String shardId, ReplicaRole initialRole,
                            long heartbeatIntervalMs, long heartbeatTimeoutMs) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId cannot be null");
        this.shardId = Objects.requireNonNull(shardId, "shardId cannot be null");
        this.currentRole = new AtomicReference<>(initialRole);
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.lastAppliedOffset = new AtomicLong(-1);
        this.lastHeartbeatSent = new AtomicLong(0);
        this.nodeHealthMap = new ConcurrentHashMap<>();
        this.currentLease = new AtomicReference<>(null);

        this.heartbeatExecutor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "heartbeat-" + nodeId);
            t.setDaemon(true);
            return t;
        });

        logger.info("HeartbeatManager created: node={}, shard={}, interval={}ms, timeout={}ms",
            nodeId, shardId, heartbeatIntervalMs, heartbeatTimeoutMs);
    }

    // ==================== 生命周期 ====================

    /**
     * 启动心跳管理器
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        logger.info("Starting HeartbeatManager for node {}...", nodeId);

        // 启动心跳发送任务
        heartbeatExecutor.scheduleAtFixedRate(
            this::sendHeartbeat,
            0,
            heartbeatIntervalMs,
            TimeUnit.MILLISECONDS
        );

        // 启动故障检测任务
        heartbeatExecutor.scheduleAtFixedRate(
            this::checkNodeHealth,
            heartbeatTimeoutMs,
            heartbeatIntervalMs,
            TimeUnit.MILLISECONDS
        );

        logger.info("HeartbeatManager started");
    }

    /**
     * 停止心跳管理器
     */
    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        logger.info("Stopping HeartbeatManager...");

        heartbeatExecutor.shutdown();
        try {
            if (!heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                heartbeatExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            heartbeatExecutor.shutdownNow();
        }

        logger.info("HeartbeatManager stopped");
    }

    // ==================== 心跳发送 ====================

    /**
     * 发送心跳
     */
    private void sendHeartbeat() {
        if (!running.get()) {
            return;
        }

        HeartbeatMessage heartbeat = new HeartbeatMessage(
            nodeId,
            shardId,
            currentRole.get(),
            lastAppliedOffset.get(),
            currentLease.get() != null ? currentLease.get().getLeaseId() : null,
            currentLease.get() != null ? currentLease.get().getFencingSequence() : -1,
            Instant.now()
        );

        lastHeartbeatSent.set(System.currentTimeMillis());

        // 通过回调发送心跳
        if (heartbeatSender != null) {
            try {
                heartbeatSender.accept(heartbeat);
            } catch (Exception e) {
                logger.error("Failed to send heartbeat", e);
            }
        }

        logger.debug("Heartbeat sent: node={}, offset={}", nodeId, lastAppliedOffset.get());
    }

    /**
     * 设置心跳发送回调
     */
    public void setHeartbeatSender(Consumer<HeartbeatMessage> sender) {
        this.heartbeatSender = sender;
    }

    // ==================== 心跳接收 ====================

    /**
     * 接收心跳
     *
     * @param heartbeat 心跳消息
     */
    public void receiveHeartbeat(HeartbeatMessage heartbeat) {
        if (heartbeat == null || !running.get()) {
            return;
        }

        // 忽略自己的心跳
        if (heartbeat.nodeId().equals(nodeId)) {
            return;
        }

        String sourceNodeId = heartbeat.nodeId();
        Instant now = Instant.now();

        NodeHealth health = nodeHealthMap.compute(sourceNodeId, (id, existing) -> {
            if (existing == null) {
                return new NodeHealth(sourceNodeId, heartbeat.shardId(),
                    heartbeat.role(), now, heartbeat.lastAppliedOffset(), true);
            } else {
                return existing.update(now, heartbeat.lastAppliedOffset(),
                    heartbeat.role(), heartbeat.leaseId());
            }
        });

        logger.debug("Heartbeat received from {}: offset={}, role={}",
            sourceNodeId, heartbeat.lastAppliedOffset(), heartbeat.role());

        // 检查是否是故障恢复
        if (!health.isHealthy()) {
            health.markHealthy();
            logger.info("Node {} recovered, marked as healthy", sourceNodeId);
        }
    }

    // ==================== 故障检测 ====================

    /**
     * 检查节点健康状态
     */
    private void checkNodeHealth() {
        if (!running.get()) {
            return;
        }

        Instant now = Instant.now();
        long timeoutThreshold = heartbeatTimeoutMs;

        for (NodeHealth health : nodeHealthMap.values()) {
            long lastSeenMs = now.toEpochMilli() - health.lastHeartbeat().toEpochMilli();

            if (lastSeenMs > timeoutThreshold) {
                if (health.isHealthy()) {
                    // 检测到故障
                    health.markUnhealthy();
                    logger.warn("Node {} heartbeat timeout (last seen {}ms ago), marking as FAILED",
                        health.nodeId(), lastSeenMs);

                    // 通知故障监听器
                    if (failureListener != null) {
                        try {
                            failureListener.accept(new NodeFailureEvent(
                                health.nodeId(),
                                health.shardId(),
                                health.role(),
                                NodeFailureEvent.FailureType.HEARTBEAT_TIMEOUT,
                                "Heartbeat timeout after " + lastSeenMs + "ms"
                            ));
                        } catch (Exception e) {
                            logger.error("Failure listener error", e);
                        }
                    }
                }
            }
        }
    }

    /**
     * 设置故障监听器
     */
    public void setFailureListener(Consumer<NodeFailureEvent> listener) {
        this.failureListener = listener;
    }

    // ==================== 状态更新 ====================

    /**
     * 更新角色
     */
    public void updateRole(ReplicaRole role) {
        this.currentRole.set(role);
        logger.info("HeartbeatManager role updated to {}", role);
    }

    /**
     * 更新最后应用 offset
     */
    public void updateLastAppliedOffset(long offset) {
        this.lastAppliedOffset.set(offset);
    }

    /**
     * 更新当前租约
     */
    public void updateLease(CoordinatorLease lease) {
        this.currentLease.set(lease);
    }

    // ==================== 查询方法 ====================

    /**
     * 获取节点健康状态
     */
    public NodeHealth getNodeHealth(String nodeId) {
        return nodeHealthMap.get(nodeId);
    }

    /**
     * 检查节点是否健康
     */
    public boolean isNodeHealthy(String nodeId) {
        NodeHealth health = nodeHealthMap.get(nodeId);
        return health != null && health.isHealthy();
    }

    /**
     * 获取所有健康节点
     */
    public java.util.Collection<NodeHealth> getAllNodeHealth() {
        return java.util.Collections.unmodifiableCollection(nodeHealthMap.values());
    }

    // ==================== 内部类 ====================

    /**
     * 心跳消息
     */
    public record HeartbeatMessage(
        String nodeId,
        String shardId,
        ReplicaRole role,
        long lastAppliedOffset,
        String leaseId,
        long fencingSequence,
        Instant timestamp
    ) {
        @Override
        public String toString() {
            return String.format("Heartbeat{node=%s, shard=%s, role=%s, offset=%d, lease=%s}",
                nodeId, shardId, role, lastAppliedOffset,
                leaseId != null ? leaseId.substring(0, 8) : "none");
        }
    }

    /**
     * 节点健康状态
     */
    public static class NodeHealth {
        private final String nodeId;
        private final String shardId;
        private volatile ReplicaRole role;
        private volatile Instant lastHeartbeat;
        private volatile long lastAppliedOffset;
        private volatile boolean healthy;
        private volatile String currentLeaseId;
        private final java.util.concurrent.atomic.AtomicInteger consecutiveMisses;

        public NodeHealth(String nodeId, String shardId, ReplicaRole role,
                          Instant lastHeartbeat, long lastAppliedOffset, boolean healthy) {
            this.nodeId = nodeId;
            this.shardId = shardId;
            this.role = role;
            this.lastHeartbeat = lastHeartbeat;
            this.lastAppliedOffset = lastAppliedOffset;
            this.healthy = healthy;
            this.currentLeaseId = null;
            this.consecutiveMisses = new java.util.concurrent.atomic.AtomicInteger(0);
        }

        public NodeHealth update(Instant timestamp, long offset, ReplicaRole role, String leaseId) {
            this.lastHeartbeat = timestamp;
            this.lastAppliedOffset = offset;
            this.role = role;
            this.currentLeaseId = leaseId;
            this.consecutiveMisses.set(0);
            return this;
        }

        public void markHealthy() {
            this.healthy = true;
            this.consecutiveMisses.set(0);
        }

        public void markUnhealthy() {
            this.healthy = false;
            this.consecutiveMisses.incrementAndGet();
        }

        // Getters
        public String nodeId() { return nodeId; }
        public String shardId() { return shardId; }
        public ReplicaRole role() { return role; }
        public Instant lastHeartbeat() { return lastHeartbeat; }
        public long lastAppliedOffset() { return lastAppliedOffset; }
        public boolean isHealthy() { return healthy; }
        public String currentLeaseId() { return currentLeaseId; }
        public int consecutiveMisses() { return consecutiveMisses.get(); }

        @Override
        public String toString() {
            return String.format("NodeHealth{node=%s, healthy=%s, offset=%d, misses=%d}",
                nodeId, healthy, lastAppliedOffset, consecutiveMisses.get());
        }
    }

    /**
     * 节点故障事件
     */
    public record NodeFailureEvent(
        String nodeId,
        String shardId,
        ReplicaRole role,
        FailureType failureType,
        String reason
    ) {
        public enum FailureType {
            HEARTBEAT_TIMEOUT,   // 心跳超时
            NETWORK_PARTITION,   // 网络分区
            PROCESS_CRASH,       // 进程崩溃
            MANUAL_SHUTDOWN      // 手动关闭
        }
    }

    @Override
    public String toString() {
        return String.format("HeartbeatManager{node=%s, shard=%s, running=%s, nodes=%d}",
            nodeId, shardId, running.get(), nodeHealthMap.size());
    }
}

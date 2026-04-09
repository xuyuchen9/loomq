package com.loomq.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 租约协调器（测试专用简化版）
 *
 * 用于测试租约管理功能，不依赖完整的集群配置。
 *
 * @author loomq
 * @since v0.4.8
 */
class TestLeaseCoordinator implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(LeaseCoordinator.class);

    // Fencing Token 序列号生成器
    private final AtomicLong fencingSequenceGenerator;

    // 租约存储：shardId -> CoordinatorLease
    private final ConcurrentHashMap<String, CoordinatorLease> activeLeases;

    // 路由表版本：shardId -> version
    private final ConcurrentHashMap<String, AtomicLong> shardRoutingVersions;

    // 租约配置
    private final long leaseDurationMs;
    private final double renewalWindowRatio;

    // 运行状态
    private volatile boolean running = false;

    // 租约事件监听器
    private volatile Consumer<LeaseEvent> leaseEventListener;

    /**
     * 创建租约协调器
     *
     * @param leaseDurationMs 租约有效期（毫秒）
     * @param renewalWindowRatio 续约窗口比例
     */
    public TestLeaseCoordinator(long leaseDurationMs, double renewalWindowRatio) {
        this.leaseDurationMs = leaseDurationMs;
        this.renewalWindowRatio = renewalWindowRatio;
        this.fencingSequenceGenerator = new AtomicLong(0);
        this.activeLeases = new ConcurrentHashMap<>();
        this.shardRoutingVersions = new ConcurrentHashMap<>();
    }

    public void start() {
        running = true;
        logger.info("LeaseCoordinator started");
    }

    @Override
    public void close() {
        running = false;
        activeLeases.clear();
        logger.info("LeaseCoordinator stopped");
    }

    // ========== 租约管理（硬约束 #1）==========

    public synchronized Optional<CoordinatorLease> grantLease(String shardId, String nodeId) {
        if (!running) {
            return Optional.empty();
        }

        Objects.requireNonNull(shardId, "shardId cannot be null");
        Objects.requireNonNull(nodeId, "nodeId cannot be null");

        CoordinatorLease currentLease = activeLeases.get(shardId);

        if (currentLease != null && currentLease.isValid()) {
            if (!currentLease.isHeldBy(nodeId)) {
                logger.warn("Lease for shard {} is held by {}, rejecting request from {}",
                    shardId, currentLease.getHolderNodeId(), nodeId);
                return Optional.empty();
            }
            return renewLeaseInternal(shardId, currentLease);
        }

        if (currentLease != null) {
            revokeLeaseInternal(shardId, currentLease.getLeaseId(), "Lease expired");
        }

        return issueNewLease(shardId, nodeId);
    }

    public synchronized Optional<CoordinatorLease> renewLease(String shardId, String leaseId) {
        if (!running) {
            return Optional.empty();
        }

        CoordinatorLease currentLease = activeLeases.get(shardId);
        if (currentLease == null || !currentLease.getLeaseId().equals(leaseId)) {
            return Optional.empty();
        }
        return renewLeaseInternal(shardId, currentLease);
    }

    public synchronized void revokeLease(String shardId, String leaseId, String reason) {
        if (!running) {
            return;
        }
        CoordinatorLease currentLease = activeLeases.get(shardId);
        if (currentLease != null && currentLease.getLeaseId().equals(leaseId)) {
            revokeLeaseInternal(shardId, leaseId, reason);
        }
    }

    public synchronized void forceRevokeLease(String shardId, String reason) {
        CoordinatorLease currentLease = activeLeases.get(shardId);
        if (currentLease != null) {
            revokeLeaseInternal(shardId, currentLease.getLeaseId(), reason);
        }
    }

    // ========== 内部操作 ==========

    private Optional<CoordinatorLease> issueNewLease(String shardId, String nodeId) {
        long newRoutingVersion = shardRoutingVersions
            .computeIfAbsent(shardId, k -> new AtomicLong(0))
            .incrementAndGet();

        long fencingSequence = fencingSequenceGenerator.incrementAndGet();

        CoordinatorLease lease = new CoordinatorLease(
            nodeId, shardId, leaseDurationMs, newRoutingVersion, fencingSequence);

        activeLeases.put(shardId, lease);
        notifyLeaseEvent(new LeaseEvent(LeaseEvent.EventType.GRANTED, shardId, lease, null));

        logger.info("New lease issued: {} for shard {} to node {} (routingVer={}, fencingSeq={})",
            lease.getLeaseId(), shardId, nodeId, newRoutingVersion, fencingSequence);

        return Optional.of(lease);
    }

    private Optional<CoordinatorLease> renewLeaseInternal(String shardId,
                                                           CoordinatorLease currentLease) {
        if (!currentLease.canRenew(renewalWindowRatio)) {
            return Optional.of(currentLease);
        }

        CoordinatorLease newLease = new CoordinatorLease(
            currentLease.getHolderNodeId(), shardId, leaseDurationMs,
            currentLease.getRoutingVersion(), currentLease.getFencingSequence());

        activeLeases.put(shardId, newLease);
        notifyLeaseEvent(new LeaseEvent(LeaseEvent.EventType.RENEWED, shardId, newLease, currentLease));

        return Optional.of(newLease);
    }

    private void revokeLeaseInternal(String shardId, String leaseId, String reason) {
        CoordinatorLease lease = activeLeases.remove(shardId);
        if (lease != null) {
            shardRoutingVersions.computeIfAbsent(shardId, k -> new AtomicLong(0)).incrementAndGet();
            notifyLeaseEvent(new LeaseEvent(LeaseEvent.EventType.REVOKED, shardId, null, lease));
            logger.info("Lease {} for shard {} revoked: {}", leaseId, shardId, reason);
        }
    }

    // ========== Fencing Token ==========

    public boolean validateFencingToken(String shardId, FencingToken token) {
        if (token == null) {
            return false;
        }
        CoordinatorLease currentLease = activeLeases.get(shardId);
        return currentLease != null && token.isValidFor(currentLease);
    }

    public long getCurrentFencingSequence(String shardId) {
        CoordinatorLease lease = activeLeases.get(shardId);
        return lease != null && lease.isValid() ? lease.getFencingSequence() : -1;
    }

    // ========== 查询 ==========

    public Optional<CoordinatorLease> getCurrentLease(String shardId) {
        CoordinatorLease lease = activeLeases.get(shardId);
        return lease != null && lease.isValid() ? Optional.of(lease) : Optional.empty();
    }

    public boolean hasValidLease(String shardId) {
        CoordinatorLease lease = activeLeases.get(shardId);
        return lease != null && lease.isValid();
    }

    public Optional<String> getCurrentPrimary(String shardId) {
        return getCurrentLease(shardId).map(CoordinatorLease::getHolderNodeId);
    }

    public long getShardRoutingVersion(String shardId) {
        AtomicLong version = shardRoutingVersions.get(shardId);
        return version != null ? version.get() : 0;
    }

    public Map<String, CoordinatorLease> getAllLeases() {
        return Map.copyOf(activeLeases);
    }

    public ClusterStatus getClusterStatus() {
        return new ClusterStatus(activeLeases.size(), shardRoutingVersions.size(),
            fencingSequenceGenerator.get());
    }

    // ========== 事件 ==========

    public void setLeaseEventListener(Consumer<LeaseEvent> listener) {
        this.leaseEventListener = listener;
    }

    private void notifyLeaseEvent(LeaseEvent event) {
        if (leaseEventListener != null) {
            try {
                leaseEventListener.accept(event);
            } catch (Exception e) {
                logger.error("Lease event listener error", e);
            }
        }
    }

    // ========== 记录 ==========

    public record LeaseEvent(EventType type, String shardId,
                              CoordinatorLease newLease, CoordinatorLease oldLease) {
        public enum EventType { GRANTED, RENEWED, REVOKED, EXPIRED }
    }

    public record ClusterStatus(int activeLeaseCount, int trackedShardCount, long fencingSequence) {}
}

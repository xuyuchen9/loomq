package com.loomq.cluster;

import com.loomq.domain.cluster.FencingToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Coordinator 租约注册表。
 *
 * 负责租约发放、续约、撤销、fencing token 校验和分片版本号维护。
 */
final class CoordinatorLeaseRegistry implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(CoordinatorLeaseRegistry.class);

    private final AtomicLong fencingSequenceGenerator = new AtomicLong(0);
    private final ConcurrentHashMap<String, CoordinatorLease> activeLeases = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> shardRoutingVersions = new ConcurrentHashMap<>();
    private final long leaseDurationMs;
    private final double renewalWindowRatio;
    private volatile Consumer<ClusterCoordinator.LeaseEvent> leaseEventListener;

    CoordinatorLeaseRegistry(long leaseDurationMs, double renewalWindowRatio) {
        this.leaseDurationMs = leaseDurationMs;
        this.renewalWindowRatio = renewalWindowRatio;
    }

    synchronized Optional<CoordinatorLease> grantLease(boolean running, String shardId, String nodeId) {
        if (!running) {
            logger.warn("Coordinator not running, cannot grant lease");
            return Optional.empty();
        }

        Objects.requireNonNull(shardId, "shardId cannot be null");
        Objects.requireNonNull(nodeId, "nodeId cannot be null");

        CoordinatorLease currentLease = activeLeases.get(shardId);

        if (currentLease != null && currentLease.isValid()) {
            if (!currentLease.isHeldBy(nodeId)) {
                logger.warn("Lease for shard {} is held by {}, rejecting request from {}",
                    shardId, currentLease.getNodeId(), nodeId);
                return Optional.empty();
            }

            return renewLeaseInternal(shardId, currentLease);
        }

        if (currentLease != null) {
            logger.info("Lease for shard {} expired (holder={}), revoking and issuing new",
                shardId, currentLease.getNodeId());
            revokeLeaseInternal(shardId, currentLease.getLeaseId(), "Lease expired");
        }

        return issueNewLease(shardId, nodeId);
    }

    synchronized Optional<CoordinatorLease> renewLease(boolean running, String shardId, String leaseId) {
        if (!running) {
            return Optional.empty();
        }

        CoordinatorLease currentLease = activeLeases.get(shardId);
        if (currentLease == null) {
            logger.warn("No active lease found for shard {} to renew", shardId);
            return Optional.empty();
        }

        if (!currentLease.getLeaseId().equals(leaseId)) {
            logger.warn("Lease ID mismatch for shard {}: expected {}, got {}",
                shardId, currentLease.getLeaseId(), leaseId);
            return Optional.empty();
        }

        return renewLeaseInternal(shardId, currentLease);
    }

    synchronized void revokeLease(boolean running, String shardId, String leaseId, String reason) {
        if (!running) {
            return;
        }

        CoordinatorLease currentLease = activeLeases.get(shardId);
        if (currentLease != null && currentLease.getLeaseId().equals(leaseId)) {
            revokeLeaseInternal(shardId, leaseId, reason);
        } else {
            logger.warn("Cannot revoke lease {} for shard {}: not found or mismatch",
                leaseId, shardId);
        }
    }

    synchronized void forceRevokeLease(String shardId, String reason) {
        CoordinatorLease currentLease = activeLeases.get(shardId);
        if (currentLease != null) {
            revokeLeaseInternal(shardId, currentLease.getLeaseId(), reason);
        }
    }

    boolean validateFencingToken(String shardId, FencingToken token) {
        if (token == null) {
            return false;
        }

        CoordinatorLease currentLease = activeLeases.get(shardId);
        if (currentLease == null) {
            return false;
        }

        return token.isValidAgainst(currentLease.toFencingToken());
    }

    long getCurrentFencingSequence(String shardId) {
        CoordinatorLease lease = activeLeases.get(shardId);
        return lease != null && lease.isValid() ? lease.currentFencingToken() : -1;
    }

    Optional<CoordinatorLease> getCurrentLease(String shardId) {
        CoordinatorLease lease = activeLeases.get(shardId);
        if (lease != null && lease.isValid()) {
            return Optional.of(lease);
        }
        return Optional.empty();
    }

    boolean hasValidLease(String shardId) {
        CoordinatorLease lease = activeLeases.get(shardId);
        return lease != null && lease.isValid();
    }

    Optional<String> getCurrentPrimary(String shardId) {
        return getCurrentLease(shardId).map(CoordinatorLease::getNodeId);
    }

    long getShardRoutingVersion(String shardId) {
        AtomicLong version = shardRoutingVersions.get(shardId);
        return version != null ? version.get() : 0;
    }

    void setLeaseEventListener(Consumer<ClusterCoordinator.LeaseEvent> listener) {
        this.leaseEventListener = listener;
    }

    @Override
    public synchronized void close() {
        for (Map.Entry<String, CoordinatorLease> entry : Map.copyOf(activeLeases).entrySet()) {
            revokeLeaseInternal(entry.getKey(), entry.getValue().getLeaseId(), "Coordinator shutdown");
        }
    }

    private Optional<CoordinatorLease> issueNewLease(String shardId, String nodeId) {
        long newRoutingVersion = shardRoutingVersions
            .computeIfAbsent(shardId, k -> new AtomicLong(0))
            .incrementAndGet();

        long fencingSequence = fencingSequenceGenerator.incrementAndGet();

        CoordinatorLease lease = new CoordinatorLease(
            nodeId,
            shardId,
            leaseDurationMs,
            newRoutingVersion,
            fencingSequence
        );

        activeLeases.put(shardId, lease);

        notifyLeaseEvent(new ClusterCoordinator.LeaseEvent(
            ClusterCoordinator.LeaseEvent.EventType.GRANTED,
            shardId,
            lease,
            null
        ));

        logger.info("New lease issued: {} for shard {} to node {} (routingVer={}, fencingSeq={})",
            lease.getLeaseId(), shardId, nodeId, newRoutingVersion, fencingSequence);

        return Optional.of(lease);
    }

    private Optional<CoordinatorLease> renewLeaseInternal(String shardId, CoordinatorLease currentLease) {
        if (!currentLease.canRenew(renewalWindowRatio)) {
            logger.debug("Lease {} not yet due for renewal", currentLease.getLeaseId());
            return Optional.of(currentLease);
        }

        CoordinatorLease newLease = new CoordinatorLease(
            currentLease.getNodeId(),
            shardId,
            leaseDurationMs,
            currentLease.getRoutingVersion(),
            currentLease.currentFencingToken()
        );

        activeLeases.put(shardId, newLease);

        notifyLeaseEvent(new ClusterCoordinator.LeaseEvent(
            ClusterCoordinator.LeaseEvent.EventType.RENEWED,
            shardId,
            newLease,
            currentLease
        ));

        logger.info("Lease renewed: {} for shard {} (expires at {})",
            newLease.getLeaseId(), shardId, newLease.getExpiresAt());

        return Optional.of(newLease);
    }

    private void revokeLeaseInternal(String shardId, String leaseId, String reason) {
        CoordinatorLease lease = activeLeases.remove(shardId);

        if (lease != null) {
            shardRoutingVersions
                .computeIfAbsent(shardId, k -> new AtomicLong(0))
                .incrementAndGet();

            notifyLeaseEvent(new ClusterCoordinator.LeaseEvent(
                ClusterCoordinator.LeaseEvent.EventType.REVOKED,
                shardId,
                null,
                lease
            ));

            logger.info("Lease {} for shard {} revoked: {}", leaseId, shardId, reason);
        }
    }

    private void notifyLeaseEvent(ClusterCoordinator.LeaseEvent event) {
        if (leaseEventListener != null) {
            try {
                leaseEventListener.accept(event);
            } catch (Exception e) {
                logger.error("Lease event listener error", e);
            }
        }
    }
}

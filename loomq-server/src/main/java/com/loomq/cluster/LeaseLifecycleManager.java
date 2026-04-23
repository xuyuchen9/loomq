package com.loomq.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 租约生命周期管理器。
 *
 * 负责租约获取、续约调度、过期回调和路由版本递增，不直接处理状态机和复制角色切换。
 */
final class LeaseLifecycleManager implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(LeaseLifecycleManager.class);

    private final String shardId;
    private final long leaseDurationMs;
    private final double renewalWindowRatio;
    private final AtomicLong routingVersionGenerator;
    private final Supplier<CoordinatorLease> currentLeaseSupplier;
    private final Consumer<CoordinatorLease> leaseUpdater;
    private final Runnable leaseExpiredHandler;
    private final ScheduledExecutorService renewalExecutor;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private volatile Function<String, CompletableFuture<CoordinatorLease>> leaseProvider;
    private volatile BiFunction<String, String, CompletableFuture<CoordinatorLease>> leaseRenewer;
    private volatile ScheduledFuture<?> leaseRenewalFuture;

    LeaseLifecycleManager(String shardId,
                          long leaseDurationMs,
                          double renewalWindowRatio,
                          AtomicLong routingVersionGenerator,
                          Supplier<CoordinatorLease> currentLeaseSupplier,
                          Consumer<CoordinatorLease> leaseUpdater,
                          Runnable leaseExpiredHandler) {
        this.shardId = Objects.requireNonNull(shardId, "shardId cannot be null");
        this.leaseDurationMs = leaseDurationMs;
        this.renewalWindowRatio = renewalWindowRatio;
        this.routingVersionGenerator = Objects.requireNonNull(routingVersionGenerator, "routingVersionGenerator cannot be null");
        this.currentLeaseSupplier = Objects.requireNonNull(currentLeaseSupplier, "currentLeaseSupplier cannot be null");
        this.leaseUpdater = Objects.requireNonNull(leaseUpdater, "leaseUpdater cannot be null");
        this.leaseExpiredHandler = Objects.requireNonNull(leaseExpiredHandler, "leaseExpiredHandler cannot be null");
        this.renewalExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "lease-renewal-" + shardId);
            t.setDaemon(true);
            return t;
        });
    }

    public CompletableFuture<CoordinatorLease> acquireLeaseAsync() {
        if (leaseProvider == null) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Lease provider not set"));
        }

        long newRoutingVersion = routingVersionGenerator.incrementAndGet();
        logger.info("Requesting lease for shard {} with routing version {}",
            shardId, newRoutingVersion);

        return leaseProvider.apply(shardId).thenApply(lease -> {
            if (lease != null) {
                if (!lease.isValid()) {
                    throw new IllegalStateException("Acquired lease is already expired");
                }
                logger.info("Lease acquired successfully: {} (expires at {})",
                    lease.getLeaseId(), lease.getExpiresAt());
            }
            return lease;
        });
    }

    public void startLeaseRenewal(CoordinatorLease lease) {
        stopLeaseRenewal();

        long renewalInterval = (long) (leaseDurationMs * (1 - renewalWindowRatio));
        if (renewalInterval <= 0) {
            renewalInterval = 1;
        }

        leaseRenewalFuture = renewalExecutor.scheduleAtFixedRate(
            this::renewLease,
            renewalInterval,
            renewalInterval,
            TimeUnit.MILLISECONDS
        );

        logger.info("Lease renewal scheduled every {}ms for shard {}", renewalInterval, shardId);
    }

    public void stopLeaseRenewal() {
        ScheduledFuture<?> currentFuture = leaseRenewalFuture;
        if (currentFuture != null) {
            currentFuture.cancel(false);
            leaseRenewalFuture = null;
        }
    }

    public void setLeaseProvider(Function<String, CompletableFuture<CoordinatorLease>> provider) {
        this.leaseProvider = provider;
    }

    public void setLeaseRenewer(BiFunction<String, String, CompletableFuture<CoordinatorLease>> renewer) {
        this.leaseRenewer = renewer;
    }

    public long getRoutingVersion() {
        return routingVersionGenerator.get();
    }

    private void renewLease() {
        CoordinatorLease currentLease = currentLeaseSupplier.get();
        if (currentLease == null) {
            logger.warn("No active lease to renew");
            return;
        }

        if (!currentLease.canRenew(renewalWindowRatio)) {
            logger.debug("Lease not yet due for renewal");
            return;
        }

        logger.debug("Renewing lease {}...", currentLease.getLeaseId());

        if (leaseRenewer == null) {
            return;
        }

        leaseRenewer.apply(shardId, currentLease.getLeaseId())
            .whenComplete((newLease, error) -> {
                if (error != null || newLease == null) {
                    logger.error("Failed to renew lease", error);
                    if (currentLease.isExpired()) {
                        expireLease();
                    }
                    return;
                }

                logger.info("Lease renewed: {} (new expiry: {})",
                    newLease.getLeaseId(), newLease.getExpiresAt());
                leaseUpdater.accept(newLease);
            });
    }

    private void expireLease() {
        stopLeaseRenewal();
        leaseExpiredHandler.run();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        stopLeaseRenewal();
        renewalExecutor.shutdownNow();
    }
}

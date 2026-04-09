package com.loomq.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 内存实现的 Lease 协调器
 *
 * 用于单节点测试环境，生产环境应使用基于 etcd/Consul 的实现
 *
 * @author loomq
 * @since v0.5.0
 */
public class InMemoryLeaseCoordinator implements LeaseCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryLeaseCoordinator.class);

    private final Map<String, LeaseHolder> leases = new ConcurrentHashMap<>();
    private final Map<String, Set<LeaseWatcherImpl>> watchers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService renewExecutor;
    private final AtomicLong epochGenerator = new AtomicLong(0);

    private volatile boolean running = false;

    public InMemoryLeaseCoordinator() {
        this.renewExecutor = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "lease-renewer");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public Optional<CoordinatorLease> tryAcquire(String shardId, String nodeId, long ttlMs) {
        LeaseHolder holder = leases.compute(shardId, (k, existing) -> {
            if (existing == null || existing.isExpired()) {
                // 可以获取租约
                long epoch = epochGenerator.incrementAndGet();
                CoordinatorLease lease = new CoordinatorLease(
                    shardId, nodeId, epoch, ttlMs, 0L
                );
                notifyWatchers(shardId, LeaseEvent.ACQUIRED, lease);
                logger.info("Lease acquired: shard={}, node={}, epoch={}", shardId, nodeId, epoch);
                return new LeaseHolder(lease);
            }
            // 租约仍有效，无法获取
            if (existing.lease.getNodeId().equals(nodeId)) {
                // 同一节点，返回现有租约
                return existing;
            }
            return existing;
        });

        if (holder.lease.getNodeId().equals(nodeId) && !holder.isExpired()) {
            // 验证是否真的获取到
            if (holder.lease.getEpoch() == epochGenerator.get() ||
                leases.get(shardId) == holder) {
                return Optional.of(holder.lease);
            }
        }
        return Optional.empty();
    }

    @Override
    public CompletableFuture<Optional<CoordinatorLease>> tryAcquireAsync(String shardId, String nodeId, long ttlMs) {
        return CompletableFuture.supplyAsync(() -> tryAcquire(shardId, nodeId, ttlMs));
    }

    @Override
    public Optional<CoordinatorLease> renew(CoordinatorLease lease) {
        LeaseHolder holder = leases.get(lease.getShardId());
        if (holder == null || holder.isExpired()) {
            lease.invalidate();
            notifyWatchers(lease.getShardId(), LeaseEvent.EXPIRED, lease);
            return Optional.empty();
        }

        if (!holder.lease.getNodeId().equals(lease.getNodeId()) ||
            holder.lease.getEpoch() != lease.getEpoch()) {
            // 租约已被其他节点获取
            lease.invalidate();
            notifyWatchers(lease.getShardId(), LeaseEvent.LOST, lease);
            return Optional.empty();
        }

        CoordinatorLease renewed = lease.renew(lease.getTtlMs());
        leases.put(lease.getShardId(), new LeaseHolder(renewed));
        notifyWatchers(lease.getShardId(), LeaseEvent.RENEWED, renewed);

        logger.debug("Lease renewed: shard={}, node={}, remaining={}ms",
            renewed.getShardId(), renewed.getNodeId(), renewed.getRemainingMs());

        return Optional.of(renewed);
    }

    @Override
    public boolean release(CoordinatorLease lease) {
        LeaseHolder holder = leases.get(lease.getShardId());
        if (holder != null &&
            holder.lease.getNodeId().equals(lease.getNodeId()) &&
            holder.lease.getEpoch() == lease.getEpoch()) {

            leases.remove(lease.getShardId());
            lease.invalidate();
            notifyWatchers(lease.getShardId(), LeaseEvent.RELEASED, lease);

            logger.info("Lease released: shard={}, node={}", lease.getShardId(), lease.getNodeId());
            return true;
        }
        return false;
    }

    @Override
    public LeaseWatcher watch(String shardId, LeaseChangeCallback callback) {
        LeaseWatcherImpl watcher = new LeaseWatcherImpl(shardId, callback);
        watchers.computeIfAbsent(shardId, k -> ConcurrentHashMap.newKeySet()).add(watcher);
        return watcher;
    }

    @Override
    public Optional<String> getCurrentHolder(String shardId) {
        LeaseHolder holder = leases.get(shardId);
        if (holder != null && !holder.isExpired()) {
            return Optional.of(holder.lease.getNodeId());
        }
        return Optional.empty();
    }

    @Override
    public boolean isHolder(String shardId, String nodeId) {
        LeaseHolder holder = leases.get(shardId);
        return holder != null &&
               !holder.isExpired() &&
               holder.lease.getNodeId().equals(nodeId);
    }

    @Override
    public long getRemainingTime(String shardId) {
        LeaseHolder holder = leases.get(shardId);
        if (holder != null && !holder.isExpired()) {
            return holder.lease.getRemainingMs();
        }
        return 0;
    }

    private void notifyWatchers(String shardId, LeaseEvent event, CoordinatorLease lease) {
        Set<LeaseWatcherImpl> shardWatchers = watchers.get(shardId);
        if (shardWatchers != null) {
            for (LeaseWatcherImpl watcher : shardWatchers) {
                try {
                    watcher.callback.onLeaseChanged(shardId, event, lease);
                } catch (Exception e) {
                    logger.error("Error notifying lease watcher", e);
                }
            }
        }
    }

    public void start() {
        if (running) return;
        running = true;

        // 启动清理任务，定期检查过期租约
        renewExecutor.scheduleAtFixedRate(this::cleanupExpiredLeases, 1, 1, TimeUnit.SECONDS);
        logger.info("InMemoryLeaseCoordinator started");
    }

    public void stop() {
        running = false;
        renewExecutor.shutdown();
        leases.clear();
        watchers.clear();
        logger.info("InMemoryLeaseCoordinator stopped");
    }

    private void cleanupExpiredLeases() {
        for (Map.Entry<String, LeaseHolder> entry : leases.entrySet()) {
            if (entry.getValue().isExpired()) {
                LeaseHolder holder = leases.remove(entry.getKey());
                if (holder != null) {
                    holder.lease.invalidate();
                    notifyWatchers(entry.getKey(), LeaseEvent.EXPIRED, holder.lease);
                    logger.info("Lease expired: shard={}, node={}",
                        entry.getKey(), holder.lease.getNodeId());
                }
            }
        }
    }

    private static class LeaseHolder {
        final CoordinatorLease lease;

        LeaseHolder(CoordinatorLease lease) {
            this.lease = lease;
        }

        boolean isExpired() {
            return lease.isExpired();
        }
    }

    private class LeaseWatcherImpl implements LeaseWatcher {
        final String shardId;
        final LeaseChangeCallback callback;

        LeaseWatcherImpl(String shardId, LeaseChangeCallback callback) {
            this.shardId = shardId;
            this.callback = callback;
        }

        @Override
        public void stop() {
            Set<LeaseWatcherImpl> shardWatchers = watchers.get(shardId);
            if (shardWatchers != null) {
                shardWatchers.remove(this);
            }
        }
    }
}

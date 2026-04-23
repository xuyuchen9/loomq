package com.loomq.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * 集群心跳监控器
 *
 * 负责维护本地和远程节点的心跳状态、超时判定和离线/恢复事件。
 */
final class ClusterHeartbeatMonitor implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ClusterHeartbeatMonitor.class);

    private final LocalShardNode localNode;
    private final long heartbeatIntervalMs;
    private final long heartbeatTimeoutMs;
    private final int flappingThreshold;
    private final ConcurrentHashMap<String, ClusterCoordinator.NodeHealth> nodeHealthMap;
    private final CopyOnWriteArrayList<Consumer<String>> offlineListeners;
    private final CopyOnWriteArrayList<Consumer<String>> recoveredListeners;
    private final ScheduledExecutorService heartbeatExecutor;
    private final AtomicBoolean running;

    private volatile Predicate<String> offlineAdmission;

    ClusterHeartbeatMonitor(LocalShardNode localNode,
                             long heartbeatIntervalMs,
                             long heartbeatTimeoutMs,
                             int flappingThreshold) {
        this.localNode = Objects.requireNonNull(localNode, "localNode cannot be null");
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.flappingThreshold = flappingThreshold;
        this.nodeHealthMap = new ConcurrentHashMap<>();
        this.offlineListeners = new CopyOnWriteArrayList<>();
        this.recoveredListeners = new CopyOnWriteArrayList<>();
        this.running = new AtomicBoolean(false);
        this.offlineAdmission = shardId -> true;
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cluster-heartbeat-monitor");
            t.setDaemon(true);
            return t;
        });

        Instant now = Instant.now();
        nodeHealthMap.put(localNode.getShardId(), new ClusterCoordinator.NodeHealth(
                localNode.getShardId(),
                ClusterCoordinator.NodeHealth.Status.HEALTHY,
                0,
                now,
                now
        ));

        logger.info("ClusterHeartbeatMonitor created for {}", localNode.getShardId());
    }

    void setOfflineAdmission(Predicate<String> offlineAdmission) {
        this.offlineAdmission = Objects.requireNonNull(offlineAdmission, "offlineAdmission cannot be null");
    }

    void addOfflineListener(Consumer<String> listener) {
        offlineListeners.add(Objects.requireNonNull(listener, "listener cannot be null"));
    }

    void addRecoveredListener(Consumer<String> listener) {
        recoveredListeners.add(Objects.requireNonNull(listener, "listener cannot be null"));
    }

    void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        heartbeatExecutor.scheduleAtFixedRate(
                this::checkNodeHealth,
                heartbeatIntervalMs,
                heartbeatIntervalMs,
                TimeUnit.MILLISECONDS
        );

        logger.info("Cluster heartbeat monitor started, interval={}ms, timeout={}ms",
                heartbeatIntervalMs, heartbeatTimeoutMs);
    }

    void receiveHeartbeat(String shardId) {
        if (!running.get()) {
            return;
        }

        ClusterCoordinator.NodeHealth currentHealth = nodeHealthMap.get(shardId);
        if (currentHealth != null && currentHealth.status() == ClusterCoordinator.NodeHealth.Status.OFFLINE) {
            logger.info("Node {} recovered, marking as HEALTHY", shardId);
            updateNodeHealth(shardId, ClusterCoordinator.NodeHealth.Status.HEALTHY, 0);
            notifyRecovered(shardId);
            return;
        }

        updateNodeHealth(shardId, ClusterCoordinator.NodeHealth.Status.HEALTHY, 0);
    }

    Optional<ClusterCoordinator.NodeHealth> getNodeHealth(String shardId) {
        return Optional.ofNullable(nodeHealthMap.get(shardId));
    }

    Map<String, ClusterCoordinator.NodeHealth> getAllNodeHealth() {
        return Map.copyOf(nodeHealthMap);
    }

    @Override
    public void close() {
        running.set(false);
        heartbeatExecutor.shutdown();
        try {
            if (!heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                heartbeatExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            heartbeatExecutor.shutdownNow();
        }

        logger.info("Cluster heartbeat monitor stopped");
    }

    private void checkNodeHealth() {
        if (!running.get()) {
            return;
        }

        localNode.heartbeat();
        updateNodeHealth(localNode.getShardId(), ClusterCoordinator.NodeHealth.Status.HEALTHY, 0);

        Instant now = Instant.now();

        for (Map.Entry<String, ClusterCoordinator.NodeHealth> entry : nodeHealthMap.entrySet()) {
            String shardId = entry.getKey();
            ClusterCoordinator.NodeHealth health = entry.getValue();

            if (shardId.equals(localNode.getShardId())) {
                continue;
            }

            long lastSeenMs = Duration.between(health.lastHeartbeat(), now).toMillis();
            if (lastSeenMs <= heartbeatTimeoutMs) {
                continue;
            }

            int newConsecutiveFailures = health.consecutiveFailures() + 1;
            updateNodeHealth(shardId, ClusterCoordinator.NodeHealth.Status.SUSPECT, newConsecutiveFailures);

            logger.warn("Node {} heartbeat timeout (last seen {}ms ago), consecutive failures: {}",
                    shardId, lastSeenMs, newConsecutiveFailures);

            if (newConsecutiveFailures >= flappingThreshold) {
                if (offlineAdmission.test(shardId)) {
                    markNodeOffline(shardId);
                } else {
                    logger.warn("Node {} is flapping, delaying offline marking", shardId);
                }
            }
        }
    }

    private void markNodeOffline(String shardId) {
        ClusterCoordinator.NodeHealth currentHealth = nodeHealthMap.get(shardId);
        if (currentHealth == null || currentHealth.status() == ClusterCoordinator.NodeHealth.Status.OFFLINE) {
            return;
        }

        updateNodeHealth(shardId, ClusterCoordinator.NodeHealth.Status.OFFLINE, currentHealth.consecutiveFailures());
        notifyOffline(shardId);

        logger.info("Node {} marked as OFFLINE", shardId);
    }

    private void updateNodeHealth(String shardId,
                                 ClusterCoordinator.NodeHealth.Status status,
                                 int consecutiveFailures) {
        Instant now = Instant.now();
        ClusterCoordinator.NodeHealth previous = nodeHealthMap.get(shardId);
        ClusterCoordinator.NodeHealth fallback = previous != null
                ? previous
                : new ClusterCoordinator.NodeHealth(shardId, status, 0, now, now);

        ClusterCoordinator.NodeHealth newHealth = new ClusterCoordinator.NodeHealth(
                shardId,
                status,
                consecutiveFailures,
                fallback.firstFailureTime(),
                status == ClusterCoordinator.NodeHealth.Status.HEALTHY ? now : fallback.lastHeartbeat()
        );

        nodeHealthMap.put(shardId, newHealth);
    }

    private void notifyOffline(String shardId) {
        for (Consumer<String> listener : offlineListeners) {
            try {
                listener.accept(shardId);
            } catch (Exception e) {
                logger.error("Offline listener error", e);
            }
        }
    }

    private void notifyRecovered(String shardId) {
        for (Consumer<String> listener : recoveredListeners) {
            try {
                listener.accept(shardId);
            } catch (Exception e) {
                logger.error("Recovered listener error", e);
            }
        }
    }
}

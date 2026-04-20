package com.loomq.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 路由表 - 支持版本控制的原子更新 (v0.4.4)
 *
 * 设计目标：
 * 1. 路由表版本单调递增，支持 CAS 语义更新
 * 2. 旧版本路由请求被拒绝，防止路由混乱
 * 3. 路由切换期间保持幂等键到 shard 的映射固定
 * 4. 节点抖动期间禁止频繁切换路由
 *
 * @author loomq
 * @since v0.4.4
 */
public class RoutingTable {

    private static final Logger logger = LoggerFactory.getLogger(RoutingTable.class);

    // 路由表版本号（单调递增）
    private final AtomicLong version;

    // 当前激活的路由表快照
    private final AtomicReference<TableSnapshot> currentSnapshot;

    // 路由表更新监听器
    private final List<RoutingTableListener> listeners;

    // 节点抖动检测器
    private final FlappingDetector flappingDetector;

    // 路由表配置
    private final RoutingTableConfig config;

    /**
     * 创建路由表
     */
    public RoutingTable() {
        this(RoutingTableConfig.defaultConfig());
    }

    /**
     * 创建路由表（指定配置）
     */
    public RoutingTable(RoutingTableConfig config) {
        this.config = config;
        this.version = new AtomicLong(0);
        this.currentSnapshot = new AtomicReference<>(
                new TableSnapshot(0, null, Map.of(), Instant.now())
        );
        this.listeners = new ArrayList<>();
        this.flappingDetector = new FlappingDetector(config.flappingThreshold(), config.flappingWindowMs());

        logger.info("RoutingTable created with config: {}", config);
    }

    // ========== 路由表更新 API ==========

    /**
     * CAS 更新路由表
     *
     * @param expectedVersion 期望的当前版本号
     * @param newRouter       新的路由器
     * @param nodeStates      节点状态映射
     * @return true 如果更新成功
     */
    public boolean compareAndSwap(long expectedVersion, ShardRouter newRouter,
                                   Map<String, ShardNode.State> nodeStates) {
        // 检查节点是否处于抖动状态
        for (Map.Entry<String, ShardNode.State> entry : nodeStates.entrySet()) {
            String shardId = entry.getKey();
            ShardNode.State newState = entry.getValue();

            if (newState == ShardNode.State.OFFLINE) {
                flappingDetector.recordFailure(shardId);
                if (flappingDetector.isFlapping(shardId)) {
                    logger.warn("Node {} is flapping, delaying route table update", shardId);
                    return false;
                }
            } else if (newState == ShardNode.State.ACTIVE) {
                flappingDetector.recordSuccess(shardId);
            }
        }

        long currentVersion = version.get();
        if (currentVersion != expectedVersion) {
            logger.warn("CAS failed: expected version {}, actual {}", expectedVersion, currentVersion);
            return false;
        }

        long newVersion = currentVersion + 1;
        TableSnapshot newSnapshot = new TableSnapshot(
                newVersion,
                newRouter,
                Map.copyOf(nodeStates),
                Instant.now()
        );

        if (currentSnapshot.compareAndSet(currentSnapshot.get(), newSnapshot)) {
            version.incrementAndGet();
            logger.info("Routing table updated: version {} -> {}, nodes={}",
                    currentVersion, newVersion, nodeStates.size());
            notifyListeners(newSnapshot);
            return true;
        }

        return false;
    }

    /**
     * 强制更新路由表（用于初始化）
     */
    public void forceUpdate(ShardRouter router, Map<String, ShardNode.State> nodeStates) {
        long newVersion = version.incrementAndGet();
        TableSnapshot snapshot = new TableSnapshot(
                newVersion,
                router,
                Map.copyOf(nodeStates),
                Instant.now()
        );
        currentSnapshot.set(snapshot);
        logger.info("Routing table force updated: version {}, nodes={}",
                newVersion, nodeStates.size());
        notifyListeners(snapshot);
    }

    /**
     * 检查版本是否有效
     */
    public boolean isVersionValid(long checkVersion) {
        return checkVersion == version.get();
    }

    /**
     * 获取当前版本号
     */
    public long getVersion() {
        return version.get();
    }

    /**
     * 获取当前路由表快照
     */
    public TableSnapshot getSnapshot() {
        return currentSnapshot.get();
    }

    /**
     * 获取当前路由器
     */
    public Optional<ShardRouter> getRouter() {
        return Optional.ofNullable(currentSnapshot.get().router());
    }

    /**
     * 根据 intentId 路由到分片节点（带版本检查）
     *
     * @param intentId Intent ID
     * @param reqVersion 请求的版本号（可选，用于一致性检查）
     * @return 路由结果
     */
    public RoutingResult route(String intentId, Optional<Long> reqVersion) {
        TableSnapshot snapshot = currentSnapshot.get();

        // 如果请求指定了版本号，检查是否匹配
        if (reqVersion.isPresent() && reqVersion.get() != snapshot.version()) {
            return RoutingResult.versionMismatch(snapshot.version(), reqVersion.get());
        }

        if (snapshot.router() == null) {
            return RoutingResult.noRouterAvailable();
        }

        ShardNode node = snapshot.router().route(intentId);
        if (node == null) {
            return RoutingResult.noNodeAvailable();
        }

        ShardNode.State state = snapshot.nodeStates().getOrDefault(
                node.getShardId(), ShardNode.State.OFFLINE);

        return RoutingResult.success(node, state, snapshot.version());
    }

    /**
     * 简单路由（不带版本检查）
     */
    public Optional<ShardNode> routeSimple(String intentId) {
        return getRouter().map(r -> r.route(intentId));
    }

    /**
     * 检查节点是否处于抖动状态
     */
    public boolean isNodeFlapping(String shardId) {
        return flappingDetector.isFlapping(shardId);
    }

    /**
     * 获取节点抖动统计
     */
    public FlappingStats getFlappingStats(String shardId) {
        return flappingDetector.getStats(shardId);
    }

    // ========== 监听器 ==========

    /**
     * 添加路由表更新监听器
     */
    public void addListener(RoutingTableListener listener) {
        listeners.add(listener);
    }

    /**
     * 移除路由表更新监听器
     */
    public void removeListener(RoutingTableListener listener) {
        listeners.remove(listener);
    }

    private void notifyListeners(TableSnapshot snapshot) {
        for (RoutingTableListener listener : listeners) {
            try {
                listener.onRoutingTableChanged(snapshot);
            } catch (Exception e) {
                logger.error("Routing table listener error", e);
            }
        }
    }

    // ========== 内部类 ==========

    /**
     * 路由表快照
     */
    public record TableSnapshot(
            long version,
            ShardRouter router,
            Map<String, ShardNode.State> nodeStates,
            Instant updateTime
    ) {
        /**
         * 获取指定节点的状态
         */
        public Optional<ShardNode.State> getNodeState(String shardId) {
            return Optional.ofNullable(nodeStates.get(shardId));
        }

        /**
         * 获取活跃节点数
         */
        public long getActiveNodeCount() {
            return nodeStates.values().stream()
                    .filter(s -> s == ShardNode.State.ACTIVE)
                    .count();
        }

        /**
         * 获取离线节点数
         */
        public long getOfflineNodeCount() {
            return nodeStates.values().stream()
                    .filter(s -> s == ShardNode.State.OFFLINE)
                    .count();
        }
    }

    /**
     * 路由结果
     */
    public record RoutingResult(
            boolean success,
            ShardNode node,
            ShardNode.State nodeState,
            long currentVersion,
            Optional<Long> requestedVersion,
            RoutingStatus status
    ) {
        public enum RoutingStatus {
            SUCCESS,
            VERSION_MISMATCH,
            NO_ROUTER,
            NO_NODE,
            NODE_OFFLINE
        }

        public static RoutingResult success(ShardNode node, ShardNode.State state, long version) {
            return new RoutingResult(true, node, state, version, Optional.empty(),
                    RoutingStatus.SUCCESS);
        }

        public static RoutingResult versionMismatch(long current, long requested) {
            return new RoutingResult(false, null, null, current, Optional.of(requested),
                    RoutingStatus.VERSION_MISMATCH);
        }

        public static RoutingResult noRouterAvailable() {
            return new RoutingResult(false, null, null, 0, Optional.empty(),
                    RoutingStatus.NO_ROUTER);
        }

        public static RoutingResult noNodeAvailable() {
            return new RoutingResult(false, null, null, 0, Optional.empty(),
                    RoutingStatus.NO_NODE);
        }

        public static RoutingResult nodeOffline(ShardNode node) {
            return new RoutingResult(false, node, ShardNode.State.OFFLINE, 0,
                    Optional.empty(), RoutingStatus.NODE_OFFLINE);
        }

        public boolean isVersionMismatch() {
            return status == RoutingStatus.VERSION_MISMATCH;
        }
    }

    /**
     * 路由表更新监听器
     */
    @FunctionalInterface
    public interface RoutingTableListener {
        void onRoutingTableChanged(TableSnapshot snapshot);
    }

    /**
     * 路由表配置
     */
    public record RoutingTableConfig(
            int flappingThreshold,      // 抖动阈值（连续失败次数）
            long flappingWindowMs,      // 抖动检测窗口（毫秒）
            long updateTimeoutMs        // 更新超时（毫秒）
    ) {
        public static RoutingTableConfig defaultConfig() {
            return new RoutingTableConfig(
                    3,           // 连续 3 次失败判定为抖动
                    30000,       // 30 秒窗口
                    5000         // 5 秒更新超时
            );
        }

        public RoutingTableConfig withFlappingThreshold(int threshold) {
            return new RoutingTableConfig(threshold, flappingWindowMs, updateTimeoutMs);
        }

        public RoutingTableConfig withFlappingWindowMs(long windowMs) {
            return new RoutingTableConfig(flappingThreshold, windowMs, updateTimeoutMs);
        }
    }

    /**
     * 节点抖动检测器
     */
    private static class FlappingDetector {
        private final int threshold;
        private final long windowMs;
        private final Map<String, NodeFlapRecord> records;

        FlappingDetector(int threshold, long windowMs) {
            this.threshold = threshold;
            this.windowMs = windowMs;
            this.records = new ConcurrentHashMap<>();
        }

        void recordFailure(String shardId) {
            records.computeIfAbsent(shardId, k -> new NodeFlapRecord())
                    .recordFailure();
        }

        void recordSuccess(String shardId) {
            NodeFlapRecord record = records.get(shardId);
            if (record != null) {
                record.recordSuccess();
            }
        }

        boolean isFlapping(String shardId) {
            NodeFlapRecord record = records.get(shardId);
            return record != null && record.isFlapping(threshold, windowMs);
        }

        FlappingStats getStats(String shardId) {
            NodeFlapRecord record = records.get(shardId);
            return record != null ? record.toStats() : FlappingStats.empty();
        }
    }

    /**
     * 节点抖动记录
     */
    private static class NodeFlapRecord {
        private final List<FlapEvent> events = new ArrayList<>();
        private int consecutiveFailures = 0;
        private long lastSuccessTime = 0;

        synchronized void recordFailure() {
            events.add(new FlapEvent(FlapEvent.Type.FAILURE, Instant.now()));
            consecutiveFailures++;
            cleanupOldEvents();
        }

        synchronized void recordSuccess() {
            events.add(new FlapEvent(FlapEvent.Type.SUCCESS, Instant.now()));
            consecutiveFailures = 0;
            lastSuccessTime = System.currentTimeMillis();
            cleanupOldEvents();
        }

        synchronized boolean isFlapping(int threshold, long windowMs) {
            cleanupOldEvents();
            return consecutiveFailures >= threshold;
        }

        synchronized FlappingStats toStats() {
            return new FlappingStats(
                    consecutiveFailures,
                    events.size(),
                    lastSuccessTime
            );
        }

        private void cleanupOldEvents() {
            // 保留最近的事件用于调试
            if (events.size() > 100) {
                events.subList(0, events.size() - 100).clear();
            }
        }
    }

    private record FlapEvent(Type type, Instant timestamp) {
        enum Type { SUCCESS, FAILURE }
    }

    /**
     * 抖动统计
     */
    public record FlappingStats(
            int consecutiveFailures,
            int totalEvents,
            long lastSuccessTimeMs
    ) {
        public static FlappingStats empty() {
            return new FlappingStats(0, 0, 0);
        }

        public boolean isCurrentlyFlapping(int threshold) {
            return consecutiveFailures >= threshold;
        }
    }
}

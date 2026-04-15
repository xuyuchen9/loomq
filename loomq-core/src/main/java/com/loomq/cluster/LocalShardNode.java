package com.loomq.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 本地分片节点实现
 *
 * 表示当前 JVM 实例承载的分片。
 * 管理分片的生命周期状态，并提供节点元数据。
 *
 * @author loomq
 * @since v0.3
 */
public class LocalShardNode implements ShardNode {

    private static final Logger logger = LoggerFactory.getLogger(LocalShardNode.class);

    // 分片索引
    private final int shardIndex;

    // 总分片数
    private final int totalShards;

    // 节点地址
    private final InetSocketAddress address;

    // 当前状态
    private final AtomicReference<State> state;

    // 加入时间
    private final Instant joinTime;

    // 最后心跳时间
    private final AtomicReference<Instant> lastHeartbeat;

    // 权重
    private final int weight;

    // 状态变更监听器
    private final CopyOnWriteArrayList<StateChangeListener> listeners;

    /**
     * 创建本地分片节点
     *
     * @param shardIndex  分片索引（0-based）
     * @param totalShards 总分片数
     * @param host        主机地址
     * @param port        端口号
     */
    public LocalShardNode(int shardIndex, int totalShards, String host, int port) {
        this(shardIndex, totalShards, new InetSocketAddress(host, port), 100);
    }

    /**
     * 创建本地分片节点（指定权重）
     *
     * @param shardIndex  分片索引（0-based）
     * @param totalShards 总分片数
     * @param host        主机地址
     * @param port        端口号
     * @param weight      权重
     */
    public LocalShardNode(int shardIndex, int totalShards, String host, int port, int weight) {
        this(shardIndex, totalShards, new InetSocketAddress(host, port), weight);
    }

    /**
     * 创建本地分片节点（指定权重）
     *
     * @param shardIndex  分片索引
     * @param totalShards 总分片数
     * @param address     网络地址
     * @param weight      权重
     */
    public LocalShardNode(int shardIndex, int totalShards, InetSocketAddress address, int weight) {
        if (shardIndex < 0 || shardIndex >= totalShards) {
            throw new IllegalArgumentException(
                    "shardIndex " + shardIndex + " out of range [0, " + totalShards + ")");
        }
        if (totalShards <= 0) {
            throw new IllegalArgumentException("totalShards must be positive");
        }
        if (address == null) {
            throw new IllegalArgumentException("address cannot be null");
        }
        if (weight <= 0) {
            throw new IllegalArgumentException("weight must be positive");
        }

        this.shardIndex = shardIndex;
        this.totalShards = totalShards;
        this.address = address;
        this.weight = weight;
        this.state = new AtomicReference<>(State.JOINING);
        this.joinTime = Instant.now();
        this.lastHeartbeat = new AtomicReference<>(joinTime);
        this.listeners = new CopyOnWriteArrayList<>();

        logger.info("Local shard node created: {} @ {}:{}, totalShards={}",
                getShardId(), host(), port(), totalShards);
    }

    @Override
    public String getShardId() {
        return "shard-" + shardIndex;
    }

    @Override
    public int getShardIndex() {
        return shardIndex;
    }

    @Override
    public int getTotalShards() {
        return totalShards;
    }

    @Override
    public InetSocketAddress getAddress() {
        return address;
    }

    @Override
    public State getState() {
        return state.get();
    }

    @Override
    public int getWeight() {
        return weight;
    }

    @Override
    public Instant getJoinTime() {
        return joinTime;
    }

    @Override
    public Instant getLastHeartbeat() {
        return lastHeartbeat.get();
    }

    @Override
    public void heartbeat() {
        lastHeartbeat.set(Instant.now());
    }

    /**
     * 启动节点（状态：JOINING -> ACTIVE）
     */
    public void start() {
        transitionTo(State.ACTIVE);
        logger.info("Shard {} started and active", getShardId());
    }

    /**
     * 准备下线（状态：ACTIVE -> LEAVING）
     */
    public void prepareShutdown() {
        transitionTo(State.LEAVING);
        logger.info("Shard {} preparing for shutdown", getShardId());
    }

    /**
     * 标记为离线（状态 -> OFFLINE）
     */
    public void markOffline() {
        transitionTo(State.OFFLINE);
        logger.warn("Shard {} marked as offline", getShardId());
    }

    /**
     * 关闭节点（状态：LEAVING -> REMOVED）
     */
    public void shutdown() {
        if (state.get() != State.LEAVING && state.get() != State.JOINING) {
            prepareShutdown();
        }
        transitionTo(State.REMOVED);
        logger.info("Shard {} shutdown complete", getShardId());
    }

    /**
     * 状态转换
     */
    private void transitionTo(State newState) {
        State oldState = state.getAndSet(newState);
        if (oldState != newState) {
            logger.info("Shard {} state changed: {} -> {}", getShardId(), oldState, newState);
            notifyListeners(oldState, newState);
        }
    }

    /**
     * 添加状态变更监听器
     */
    public void addStateChangeListener(StateChangeListener listener) {
        listeners.add(listener);
    }

    /**
     * 移除状态变更监听器
     */
    public void removeStateChangeListener(StateChangeListener listener) {
        listeners.remove(listener);
    }

    private void notifyListeners(State oldState, State newState) {
        for (StateChangeListener listener : listeners) {
            try {
                listener.onStateChange(this, oldState, newState);
            } catch (Exception e) {
                logger.error("State change listener error", e);
            }
        }
    }

    /**
     * 获取主机名
     */
    public String host() {
        return address.getHostName();
    }

    /**
     * 获取端口
     */
    public int port() {
        return address.getPort();
    }

    /**
     * 检查是否由当前分片负责
     * 基于一致性 Hash 的判断
     *
     * @param taskId 任务 ID
     * @param router 路由器（用于计算 hash）
     * @return true 如果该任务应由本分片处理
     */
    public boolean isResponsibleFor(String taskId, ShardRouter router) {
        if (!isAvailable()) {
            return false;
        }
        ShardNode responsibleNode = router.route(taskId);
        return responsibleNode != null && responsibleNode.getShardId().equals(getShardId());
    }

    @Override
    public String toString() {
        return toNodeString();
    }
}

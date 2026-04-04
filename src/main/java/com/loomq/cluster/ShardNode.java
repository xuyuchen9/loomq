package com.loomq.cluster;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 分片节点抽象
 *
 * 表示集群中的一个逻辑分片，负责特定范围内的任务。
 * 每个分片有唯一的 shardId，对应一个物理节点。
 *
 * 状态流转：
 * JOINING -> ACTIVE -> LEAVING -> REMOVED
 *     |         |         |
 *     +---------+---------+ (任何状态都可能直接进入 OFFLINE)
 *
 * @author loomq
 * @since v0.3
 */
public interface ShardNode {

    /**
     * 获取分片 ID（全局唯一）
     * 格式：shard-{index}，如 shard-0, shard-1
     *
     * @return 分片 ID
     */
    String getShardId();

    /**
     * 获取分片索引（0-based）
     *
     * @return 分片索引
     */
    int getShardIndex();

    /**
     * 获取分片总数
     *
     * @return 总分片数
     */
    int getTotalShards();

    /**
     * 获取节点地址
     *
     * @return 网络地址
     */
    InetSocketAddress getAddress();

    /**
     * 获取当前状态
     *
     * @return 节点状态
     */
    State getState();

    /**
     * 获取节点权重（用于负载均衡）
     * 默认值为 100
     *
     * @return 权重
     */
    default int getWeight() {
        return 100;
    }

    /**
     * 检查节点是否可用（ACTIVE 状态）
     *
     * @return true 如果节点可以接受请求
     */
    default boolean isAvailable() {
        return getState() == State.ACTIVE;
    }

    /**
     * 获取节点加入时间
     *
     * @return 加入时间
     */
    Instant getJoinTime();

    /**
     * 获取最后心跳时间
     *
     * @return 最后心跳时间
     */
    Instant getLastHeartbeat();

    /**
     * 更新心跳时间
     */
    void heartbeat();

    /**
     * 转换为字符串标识（用于日志）
     *
     * @return 节点标识
     */
    default String toNodeString() {
        return String.format("%s@%s:%d[%s]",
                getShardId(),
                getAddress().getHostName(),
                getAddress().getPort(),
                getState());
    }

    /**
     * 节点状态枚举
     */
    enum State {
        /**
         * 加入中 - 节点刚加入集群，正在进行数据同步
         */
        JOINING("加入中，正在同步数据"),

        /**
         * 活跃 - 节点正常工作，可以处理请求
         */
        ACTIVE("正常运行"),

        /**
         * 离开中 - 节点即将下线，正在迁移数据
         */
        LEAVING("离开中，正在迁移数据"),

        /**
         * 已移除 - 节点已从集群中移除
         */
        REMOVED("已移除"),

        /**
         * 离线 - 节点不可达（心跳超时）
         */
        OFFLINE("离线");

        private final String description;

        State(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }

        /**
         * 检查是否可以接受写请求
         */
        public boolean canAcceptWrite() {
            return this == ACTIVE || this == JOINING;
        }

        /**
         * 检查是否可以接受读请求
         */
        public boolean canAcceptRead() {
            return this == ACTIVE;
        }
    }

    /**
     * 节点状态变更监听器
     */
    @FunctionalInterface
    interface StateChangeListener {
        void onStateChange(ShardNode node, State oldState, State newState);
    }
}

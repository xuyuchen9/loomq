package com.loomq.cluster;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Lease 协调器接口
 *
 * 定义了租约仲裁的核心操作，用于 Primary-Replica 架构中的领导者选举
 *
 * @author loomq
 * @since v0.5.0
 */
public interface LeaseCoordinator {

    /**
     * 尝试获取租约
     *
     * @param shardId 分片ID
     * @param nodeId  当前节点ID
     * @param ttlMs   租约有效期（毫秒）
     * @return 租约对象，如果获取失败返回 empty
     */
    Optional<CoordinatorLease> tryAcquire(String shardId, String nodeId, long ttlMs);

    /**
     * 异步尝试获取租约
     *
     * @param shardId 分片ID
     * @param nodeId  当前节点ID
     * @param ttlMs   租约有效期（毫秒）
     * @return CompletableFuture<Optional<CoordinatorLease>>
     */
    CompletableFuture<Optional<CoordinatorLease>> tryAcquireAsync(String shardId, String nodeId, long ttlMs);

    /**
     * 续期租约
     *
     * @param lease 当前租约
     * @return 新的租约对象，如果续期失败返回 empty
     */
    Optional<CoordinatorLease> renew(CoordinatorLease lease);

    /**
     * 释放租约
     *
     * @param lease 要释放的租约
     * @return 是否成功释放
     */
    boolean release(CoordinatorLease lease);

    /**
     * 观察租约变化
     *
     * @param shardId  分片ID
     * @param callback 变化回调
     * @return 观察句柄，用于取消观察
     */
    LeaseWatcher watch(String shardId, LeaseChangeCallback callback);

    /**
     * 获取当前租约持有者
     *
     * @param shardId 分片ID
     * @return 当前持有租约的节点ID
     */
    Optional<String> getCurrentHolder(String shardId);

    /**
     * 检查当前节点是否持有租约
     *
     * @param shardId 分片ID
     * @param nodeId  节点ID
     * @return 是否持有租约
     */
    boolean isHolder(String shardId, String nodeId);

    /**
     * 获取租约剩余时间
     *
     * @param shardId 分片ID
     * @return 剩余毫秒数，如果没有租约返回 0
     */
    long getRemainingTime(String shardId);

    /**
     * 租约变化回调接口
     */
    @FunctionalInterface
    interface LeaseChangeCallback {
        void onLeaseChanged(String shardId, LeaseEvent event, CoordinatorLease lease);
    }

    /**
     * 租约事件类型
     */
    enum LeaseEvent {
        ACQUIRED,    // 获得租约
        LOST,        // 失去租约
        RENEWED,     // 续期成功
        EXPIRED,     // 租约过期
        RELEASED     // 租约释放
    }

    /**
     * 观察句柄
     */
    interface LeaseWatcher {
        void stop();
    }
}

package com.loomq.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 分片路由器 - 基于一致性 Hash 实现
 *
 * 设计要点：
 * 1. 使用一致性 Hash 实现平滑扩容
 * 2. 虚拟节点解决数据倾斜问题
 * 3. 读写锁保证线程安全
 * 4. 支持节点动态添加/删除
 *
 * @author loomq
 * @since v0.3
 */
public class ShardRouter {

    private static final Logger logger = LoggerFactory.getLogger(ShardRouter.class);

    // 默认虚拟节点数 - 经验值：150-200 能较好平衡均匀性和内存占用
    private static final int DEFAULT_VIRTUAL_NODES = 150;

    // 一致性 Hash 环：hash -> ShardNode
    // 使用 ConcurrentSkipListMap 支持并发读写和范围查询
    private final NavigableMap<Long, ShardNode> hashRing;

    // 虚拟节点数
    private final int virtualNodes;

    // 读写锁（主要保护 hashRing 的结构变更）
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // MD5 哈希实例（线程不安全，每次新建）
    private final ThreadLocal<MessageDigest> md5Holder;

    /**
     * 创建默认配置的 ShardRouter
     */
    public ShardRouter() {
        this(DEFAULT_VIRTUAL_NODES);
    }

    /**
     * 创建指定虚拟节点数的 ShardRouter
     *
     * @param virtualNodes 每个物理节点的虚拟节点数
     */
    public ShardRouter(int virtualNodes) {
        this.virtualNodes = virtualNodes;
        this.hashRing = new ConcurrentSkipListMap<>();
        this.md5Holder = ThreadLocal.withInitial(() -> {
            try {
                return MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("MD5 not available", e);
            }
        });
        logger.info("ShardRouter initialized with {} virtual nodes per shard", virtualNodes);
    }

    /**
     * 根据 intentId 路由到对应的分片节点
     *
     * @param intentId Intent ID
     * @return 分片节点，如果没有可用节点返回 null
     */
    public ShardNode route(String intentId) {
        if (intentId == null || intentId.isEmpty()) {
            throw new IllegalArgumentException("intentId cannot be null or empty");
        }

        lock.readLock().lock();
        try {
            if (hashRing.isEmpty()) {
                logger.warn("Hash ring is empty, no shard available for intent: {}", intentId);
                return null;
            }

            long hash = hash(intentId);

            // 找到第一个 >= hash 的节点（顺时针查找）
            Map.Entry<Long, ShardNode> entry = hashRing.ceilingEntry(hash);

            // 如果没有找到（hash 大于环上所有值），取第一个节点
            if (entry == null) {
                entry = hashRing.firstEntry();
            }

            ShardNode node = entry.getValue();
            logger.debug("Routed intent {} to shard {} (hash: {}, nodeHash: {})",
                    intentId, node.getShardId(), hash, entry.getKey());

            return node;

        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 添加节点到 Hash 环
     *
     * @param node 分片节点
     */
    public void addNode(ShardNode node) {
        if (node == null) {
            throw new IllegalArgumentException("node cannot be null");
        }

        lock.writeLock().lock();
        try {
            String nodeKey = node.getShardId();

            // 为每个物理节点创建虚拟节点
            for (int i = 0; i < virtualNodes; i++) {
                String virtualKey = nodeKey + "#" + i;
                long hash = hash(virtualKey);
                hashRing.put(hash, node);
            }

            logger.info("Added shard {} to hash ring with {} virtual nodes, total virtual nodes: {}",
                    nodeKey, virtualNodes, hashRing.size());

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 从 Hash 环移除节点
     *
     * @param node 分片节点
     */
    public void removeNode(ShardNode node) {
        if (node == null) {
            throw new IllegalArgumentException("node cannot be null");
        }

        lock.writeLock().lock();
        try {
            String nodeKey = node.getShardId();

            // 移除该节点的所有虚拟节点
            for (int i = 0; i < virtualNodes; i++) {
                String virtualKey = nodeKey + "#" + i;
                long hash = hash(virtualKey);
                hashRing.remove(hash);
            }

            logger.info("Removed shard {} from hash ring, remaining virtual nodes: {}",
                    nodeKey, hashRing.size());

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取需要迁移的 key 范围（用于扩容时数据迁移）
     *
     * @param newNode 新加入的节点
     * @return 该节点负责的 hash 范围列表（用于从其他节点查询数据）
     */
    public Range getResponsibilityRange(ShardNode newNode) {
        lock.readLock().lock();
        try {
            if (hashRing.isEmpty()) {
                // 第一个节点，负责全部范围
                return new Range(Long.MIN_VALUE, Long.MAX_VALUE);
            }

            // 找到该节点的所有虚拟节点位置
            long minHash = Long.MAX_VALUE;
            long maxHash = Long.MIN_VALUE;

            for (int i = 0; i < virtualNodes; i++) {
                String virtualKey = newNode.getShardId() + "#" + i;
                long hash = hash(virtualKey);
                minHash = Math.min(minHash, hash);
                maxHash = Math.max(maxHash, hash);
            }

            // 返回该节点负责的 hash 范围（前驱节点到该节点之间）
            return new Range(minHash, maxHash);

        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取当前所有物理节点（去重）
     *
     * @return 节点集合
     */
    public Collection<ShardNode> getAllNodes() {
        lock.readLock().lock();
        try {
            return hashRing.values().stream()
                    .distinct()
                    .toList();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取 Hash 环大小（虚拟节点数）
     *
     * @return 虚拟节点数量
     */
    public int getVirtualNodeCount() {
        lock.readLock().lock();
        try {
            return hashRing.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 计算 hash 值 - 使用 MD5 取前 8 字节作为 long
     *
     * @param key 输入 key
     * @return hash 值
     */
    public long hash(String key) {
        MessageDigest md5 = md5Holder.get();
        md5.reset();
        md5.update(key.getBytes(StandardCharsets.UTF_8));
        byte[] digest = md5.digest();

        // 取前 8 字节转为 long（FNV-1a 风格混合）
        long hash = ((long) (digest[0] & 0xFF) << 56)
                | ((long) (digest[1] & 0xFF) << 48)
                | ((long) (digest[2] & 0xFF) << 40)
                | ((long) (digest[3] & 0xFF) << 32)
                | ((long) (digest[4] & 0xFF) << 24)
                | ((long) (digest[5] & 0xFF) << 16)
                | ((long) (digest[6] & 0xFF) << 8)
                | ((long) (digest[7] & 0xFF));

        // 处理负数情况，保证一致性
        return hash == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(hash);
    }

    /**
     * 批量添加节点
     *
     * @param nodes 节点列表
     */
    public void addNodes(Collection<ShardNode> nodes) {
        nodes.forEach(this::addNode);
    }

    /**
     * 清空所有节点
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            hashRing.clear();
            logger.info("Hash ring cleared");
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Hash 范围（用于数据迁移）
     */
    public record Range(long start, long end) {

        /**
         * 检查 hash 是否在该范围内
         */
        public boolean contains(long hash) {
            return hash >= start && hash <= end;
        }

        @Override
        public String toString() {
            return String.format("Range[%d, %d]", start, end);
        }
    }
}

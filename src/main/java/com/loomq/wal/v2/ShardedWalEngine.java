package com.loomq.wal.v2;

import com.loomq.config.WalConfig;
import com.loomq.entity.EventType;
import com.loomq.wal.WalRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 分片 WAL 引擎
 *
 * 管理多个分片的 WAL 写入器，每个分片独立：
 * 1. 独立的数据目录
 * 2. 独立的 RingBuffer
 * 3. 独立的刷盘线程
 *
 * @author loomq
 * @since v0.3
 */
public class ShardedWalEngine implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ShardedWalEngine.class);

    // WAL 配置
    private final WalConfig config;

    // 分片写入器映射：shardId -> WalWriterV2
    private final Map<String, WalWriterV2> shardWriters;

    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * 创建分片 WAL 引擎
     *
     * @param config WAL 配置
     */
    public ShardedWalEngine(WalConfig config) {
        this.config = config;
        this.shardWriters = new ConcurrentHashMap<>();
    }

    /**
     * 启动引擎
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        logger.info("ShardedWalEngine started");
    }

    /**
     * 停止引擎
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        logger.info("Stopping ShardedWalEngine...");

        // 停止所有分片写入器
        for (Map.Entry<String, WalWriterV2> entry : shardWriters.entrySet()) {
            try {
                entry.getValue().stop();
                logger.info("Stopped WAL writer for shard: {}", entry.getKey());
            } catch (Exception e) {
                logger.error("Error stopping WAL writer for shard: {}", entry.getKey(), e);
            }
        }

        logger.info("ShardedWalEngine stopped");
    }

    /**
     * 获取或创建分片写入器
     *
     * @param shardId 分片 ID
     * @return WalWriterV2
     */
    private WalWriterV2 getOrCreateWriter(String shardId) throws IOException {
        return shardWriters.computeIfAbsent(shardId, id -> {
            try {
                WalWriterV2 writer = new WalWriterV2(config, id);
                writer.start();
                logger.info("Created WAL writer for shard: {}", id);
                return writer;
            } catch (IOException e) {
                throw new RuntimeException("Failed to create WAL writer for shard: " + id, e);
            }
        });
    }

    /**
     * 异步写入记录
     *
     * @param shardId   分片 ID
     * @param taskId    任务 ID
     * @param bizKey    业务 Key
     * @param eventType 事件类型
     * @param eventTime 事件时间
     * @param payload   负载数据
     * @return CompletableFuture
     */
    public CompletableFuture<Long> appendAsync(String shardId, String taskId, String bizKey,
                                                EventType eventType, long eventTime,
                                                byte[] payload) {
        if (!running.get()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("ShardedWalEngine is not running"));
        }

        try {
            WalWriterV2 writer = getOrCreateWriter(shardId);
            return writer.appendAsync(taskId, bizKey, eventType, eventTime, payload);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * 同步写入记录
     *
     * @param shardId   分片 ID
     * @param taskId    任务 ID
     * @param bizKey    业务 Key
     * @param eventType 事件类型
     * @param eventTime 事件时间
     * @param payload   负载数据
     * @return 文件位置
     * @throws IOException 如果写入失败
     */
    public long append(String shardId, String taskId, String bizKey,
                       EventType eventType, long eventTime,
                       byte[] payload) throws IOException {
        WalWriterV2 writer = getOrCreateWriter(shardId);
        return writer.append(taskId, bizKey, eventType, eventTime, payload);
    }

    /**
     * 获取分片统计信息
     *
     * @param shardId 分片 ID
     * @return 统计信息
     */
    public WalWriterV2.Stats getStats(String shardId) {
        WalWriterV2 writer = shardWriters.get(shardId);
        return writer != null ? writer.getStats() : null;
    }

    /**
     * 获取总分片数
     *
     * @return 分片数
     */
    public int getShardCount() {
        return shardWriters.size();
    }

    /**
     * 获取所有分片 ID
     *
     * @return 分片 ID 集合
     */
    public java.util.Set<String> getShardIds() {
        return shardWriters.keySet();
    }

    @Override
    public void close() throws IOException {
        stop();

        // 关闭所有写入器
        for (WalWriterV2 writer : shardWriters.values()) {
            try {
                writer.close();
            } catch (IOException e) {
                logger.error("Error closing WAL writer", e);
            }
        }
        shardWriters.clear();

        logger.info("ShardedWalEngine closed");
    }

    public boolean isRunning() {
        return running.get();
    }
}

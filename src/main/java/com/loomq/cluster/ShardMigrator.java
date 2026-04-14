package com.loomq.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 分片迁移管理器
 *
 * 负责处理一致性扩容时的数据迁移：
 * 1. 双写阶段：新数据同时写入新旧节点
 * 2. 数据同步：异步同步历史数据
 * 3. 路由切换：原子更新路由表
 * 4. 清理旧数据：迁移完成后清理
 *
 * 迁移状态机：
 * IDLE -> PREPARING -> DUAL_WRITE -> SYNCING -> SWITCHING -> COMPLETED
 *
 * @author loomq
 * @since v0.3
 */
public class ShardMigrator {

    private static final Logger logger = LoggerFactory.getLogger(ShardMigrator.class);

    // 迁移状态
    public enum MigrationState {
        IDLE,           // 空闲
        PREPARING,      // 准备中
        DUAL_WRITE,     // 双写阶段
        SYNCING,        // 数据同步中
        SWITCHING,      // 路由切换中
        COMPLETED,      // 完成
        FAILED          // 失败
    }

    // 迁移任务
    public static class MigrationTask {
        private final String shardId;
        private final String sourceNode;
        private final String targetNode;
        private final long createTime;
        private volatile MigrationState state;
        private volatile long progress;  // 0-100
        private volatile String error;

        MigrationTask(String shardId, String sourceNode, String targetNode) {
            this.shardId = shardId;
            this.sourceNode = sourceNode;
            this.targetNode = targetNode;
            this.createTime = System.currentTimeMillis();
            this.state = MigrationState.PREPARING;
            this.progress = 0;
        }

        public String getShardId() { return shardId; }
        public String getSourceNode() { return sourceNode; }
        public String getTargetNode() { return targetNode; }
        public MigrationState getState() { return state; }
        public long getProgress() { return progress; }
        public String getError() { return error; }

        void setState(MigrationState state) {
            this.state = state;
        }

        void setProgress(long progress) {
            this.progress = Math.min(100, Math.max(0, progress));
        }

        void setError(String error) {
            this.error = error;
            this.state = MigrationState.FAILED;
        }
    }

    // 路由器
    private final ShardRouter router;

    // 当前迁移任务：shardId -> MigrationTask
    private final ConcurrentHashMap<String, MigrationTask> activeMigrations;

    // 构造函数
    public ShardMigrator(ShardRouter router) {
        this.router = router;
        this.activeMigrations = new ConcurrentHashMap<>();
        logger.info("ShardMigrator created");
    }

    /**
     * 启动迁移任务
     *
     * @param shardId    分片 ID
     * @param sourceNode 源节点 ID
     * @param targetNode 目标节点 ID
     * @return 迁移任务
     */
    public MigrationTask startMigration(String shardId, String sourceNode, String targetNode) {
        // 检查是否已有迁移任务
        if (activeMigrations.containsKey(shardId)) {
            throw new IllegalStateException("Migration already in progress for shard: " + shardId);
        }

        MigrationTask task = new MigrationTask(shardId, sourceNode, targetNode);
        activeMigrations.put(shardId, task);

        logger.info("Migration started: {} from {} to {}", shardId, sourceNode, targetNode);

        // 异步执行迁移
        executeMigration(task);

        return task;
    }

    /**
     * 执行迁移（简化版 - V0.3 不实现真实数据迁移）
     */
    private void executeMigration(MigrationTask task) {
        // V0.3 简化：只更新路由，不做实际数据迁移
        // 实际生产环境需要：
        // 1. 双写阶段
        // 2. 数据同步
        // 3. 路由切换

        try {
            // 准备阶段
            task.setState(MigrationState.PREPARING);
            task.setProgress(10);
            Thread.sleep(100);

            // 双写阶段（V0.3 跳过）
            task.setState(MigrationState.DUAL_WRITE);
            task.setProgress(30);
            Thread.sleep(100);

            // 数据同步（V0.3 跳过）
            task.setState(MigrationState.SYNCING);
            task.setProgress(60);
            Thread.sleep(100);

            // 路由切换
            task.setState(MigrationState.SWITCHING);
            task.setProgress(80);

            // 路由器会自动更新（一致性 Hash）
            Thread.sleep(100);

            // 完成
            task.setState(MigrationState.COMPLETED);
            task.setProgress(100);

            logger.info("Migration completed: {}", task.getShardId());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            task.setError("Migration interrupted");
            logger.error("Migration interrupted: {}", task.getShardId());
        } catch (Exception e) {
            task.setError(e.getMessage());
            logger.error("Migration failed: {}", task.getShardId(), e);
        } finally {
            // 保留任务记录一段时间
            // 实际应该延迟清理
        }
    }

    /**
     * 获取迁移任务状态
     */
    public MigrationTask getMigrationTask(String shardId) {
        return activeMigrations.get(shardId);
    }

    /**
     * 取消迁移任务
     */
    public boolean cancelMigration(String shardId) {
        MigrationTask task = activeMigrations.get(shardId);
        if (task == null) {
            return false;
        }

        if (task.getState() == MigrationState.COMPLETED ||
            task.getState() == MigrationState.FAILED) {
            return false;
        }

        task.setError("Cancelled by user");
        logger.info("Migration cancelled: {}", shardId);
        return true;
    }

    /**
     * 检查是否有进行中的迁移
     */
    public boolean hasActiveMigration(String shardId) {
        MigrationTask task = activeMigrations.get(shardId);
        if (task == null) {
            return false;
        }
        MigrationState state = task.getState();
        return state != MigrationState.COMPLETED && state != MigrationState.FAILED;
    }

    /**
     * 获取所有迁移任务
     */
    public int getActiveMigrationCount() {
        return (int) activeMigrations.values().stream()
                .filter(t -> t.getState() != MigrationState.COMPLETED &&
                             t.getState() != MigrationState.FAILED)
                .count();
    }

    /**
     * 清理已完成的迁移任务
     */
    public void cleanupCompletedMigrations() {
        activeMigrations.entrySet().removeIf(entry -> {
            MigrationState state = entry.getValue().getState();
            return state == MigrationState.COMPLETED || state == MigrationState.FAILED;
        });
    }
}

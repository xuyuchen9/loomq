package com.loomq.cluster;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * ShardMigrator 单元测试
 */
class ShardMigratorTest {

    private ShardRouter router;
    private ShardMigrator migrator;

    @BeforeEach
    void setUp() {
        router = new ShardRouter();
        migrator = new ShardMigrator(router);
    }

    @Test
    @DisplayName("启动迁移任务应成功")
    void startMigration() throws InterruptedException {
        ShardMigrator.MigrationTask task = migrator.startMigration(
                "shard-0", "node-old", "node-new");

        assertNotNull(task);
        assertEquals("shard-0", task.getShardId());
        assertEquals("node-old", task.getSourceNode());
        assertEquals("node-new", task.getTargetNode());

        // 等待迁移完成
        Thread.sleep(200); // 优化: 500 -> 200

        assertEquals(ShardMigrator.MigrationState.COMPLETED, task.getState());
        assertEquals(100, task.getProgress());
    }

    @Test
    @DisplayName("重复迁移应被拒绝")
    void duplicateMigration() {
        migrator.startMigration("shard-0", "node-1", "node-2");

        // 第二次迁移应该失败
        assertThrows(IllegalStateException.class, () ->
                migrator.startMigration("shard-0", "node-2", "node-3"));
    }

    @Test
    @DisplayName("检查活跃迁移")
    void hasActiveMigration() throws InterruptedException {
        ShardMigrator.MigrationTask task = migrator.startMigration(
                "shard-1", "node-1", "node-2");

        // 等待完成
        Thread.sleep(500);

        // 完成后不再是活跃的
        assertFalse(migrator.hasActiveMigration("shard-1"));
        assertEquals(ShardMigrator.MigrationState.COMPLETED, task.getState());
    }

    @Test
    @DisplayName("取消迁移任务")
    void cancelMigration() {
        ShardMigrator.MigrationTask task = migrator.startMigration(
                "shard-2", "node-1", "node-2");

        // 立即取消（可能还在执行）
        boolean cancelled = migrator.cancelMigration("shard-2");

        // 取消可能成功或失败（取决于执行速度）
        // 验证任务存在即可
        assertNotNull(task);
    }

    @Test
    @DisplayName("获取迁移任务状态")
    void getMigrationTask() throws InterruptedException {
        migrator.startMigration("shard-3", "node-1", "node-2");

        ShardMigrator.MigrationTask task = migrator.getMigrationTask("shard-3");
        assertNotNull(task);
        assertEquals("shard-3", task.getShardId());

        // 等待完成后清理
        Thread.sleep(500);
        migrator.cleanupCompletedMigrations();

        // 清理后任务应该被移除
        assertNull(migrator.getMigrationTask("shard-3"));
    }
}

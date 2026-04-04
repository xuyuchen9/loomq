package com.loomq.cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ClusterManager 单元测试
 */
class ClusterManagerTest {

    @Test
    @DisplayName("单节点模式应正确启动")
    void singleNodeMode() throws IOException {
        ClusterManager manager = ClusterManager.singleNode(18080);

        assertNotNull(manager);
        assertFalse(manager.isSharded());
        assertEquals(1, manager.getConfig().getTotalShards());
        assertEquals(0, manager.getConfig().getLocalShardIndex());

        manager.start();

        assertTrue(manager.isRunning());
        assertNotNull(manager.getLocalNode());
        assertEquals("shard-0", manager.getLocalShardId());
        assertTrue(manager.getLocalNode().isAvailable());

        // 路由测试
        ShardNode node = manager.route("any-task");
        assertNotNull(node);
        assertEquals("shard-0", node.getShardId());

        // 本地任务检查
        assertTrue(manager.isLocalTask("test-task"));

        manager.stop();
    }

    @Test
    @DisplayName("分片模式应正确启动")
    void shardedMode() throws IOException {
        ClusterConfig config = ClusterConfig.localMultiShard(4, 18090);
        ClusterManager manager = new ClusterManager(config);

        assertTrue(manager.isSharded());
        assertEquals(4, manager.getConfig().getTotalShards());

        manager.start();

        assertTrue(manager.isRunning());
        assertEquals(4, manager.getNodeCount());

        // 路由应均匀分布
        int[] distribution = new int[4];
        for (int i = 0; i < 10000; i++) {
            ShardNode node = manager.route("task-" + i);
            assertNotNull(node);
            int idx = Integer.parseInt(node.getShardId().substring(6));
            distribution[idx]++;
        }

        // 每个分片都应该有任务
        for (int i = 0; i < 4; i++) {
            assertTrue(distribution[i] > 1000, "Shard-" + i + " should have tasks");
        }

        manager.stop();
    }

    @Test
    @DisplayName("本地任务识别应正确")
    void localTaskIdentification() throws IOException {
        // shard-0 的节点
        ClusterConfig config = new ClusterConfig(
                "test",
                4,
                0,
                List.of(
                        new ClusterConfig.NodeConfig("shard-0", "localhost", 18100, 100),
                        new ClusterConfig.NodeConfig("shard-1", "localhost", 18101, 100),
                        new ClusterConfig.NodeConfig("shard-2", "localhost", 18102, 100),
                        new ClusterConfig.NodeConfig("shard-3", "localhost", 18103, 100)
                ),
                false,
                60000
        );

        ClusterManager manager = new ClusterManager(config);
        manager.start();

        // 找到实际路由到 shard-0 的任务
        String localTask = null;
        String remoteTask = null;
        for (int i = 0; i < 10000; i++) {
            String taskId = "task-" + i;
            ShardNode node = manager.route(taskId);
            if (node.getShardId().equals("shard-0")) {
                if (localTask == null) localTask = taskId;
            } else {
                if (remoteTask == null) remoteTask = taskId;
            }
            if (localTask != null && remoteTask != null) break;
        }

        assertNotNull(localTask);
        assertNotNull(remoteTask);

        assertTrue(manager.isLocalTask(localTask));
        assertFalse(manager.isLocalTask(remoteTask));

        manager.stop();
    }

    @Test
    @DisplayName("状态摘要应正确")
    void clusterStatus() throws IOException {
        ClusterManager manager = ClusterManager.singleNode(18110);
        manager.start();

        ClusterManager.ClusterStatus status = manager.getStatus();

        assertNotNull(status);
        assertEquals("loomq-single", status.clusterName());
        assertTrue(status.running());
        assertFalse(status.sharded());
        assertEquals(1, status.totalShards());
        assertEquals("shard-0", status.localShardId());
        assertEquals(ShardNode.State.ACTIVE, status.localState());

        String statusStr = status.toString();
        assertTrue(statusStr.contains("loomq-single"));
        assertTrue(statusStr.contains("shard-0"));

        manager.stop();
    }

    @Test
    @DisplayName("重复启动应安全")
    void duplicateStart() throws IOException {
        ClusterManager manager = ClusterManager.singleNode(18120);

        manager.start();
        assertTrue(manager.isRunning());

        // 重复启动应该安全
        manager.start();
        assertTrue(manager.isRunning());

        manager.stop();
    }

    @Test
    @DisplayName("重复停止应安全")
    void duplicateStop() throws IOException {
        ClusterManager manager = ClusterManager.singleNode(18130);
        manager.start();
        manager.stop();

        assertFalse(manager.isRunning());

        // 重复停止应该安全
        manager.stop();
        assertFalse(manager.isRunning());
    }
}

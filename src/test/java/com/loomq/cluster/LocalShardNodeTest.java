package com.loomq.cluster;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LocalShardNode 单元测试
 */
class LocalShardNodeTest {

    @Test
    @DisplayName("创建节点应正确初始化属性")
    void createNode() {
        LocalShardNode node = new LocalShardNode(0, 4, "localhost", 8080);

        assertEquals("shard-0", node.getShardId());
        assertEquals(0, node.getShardIndex());
        assertEquals(4, node.getTotalShards());
        assertEquals("localhost", node.host());
        assertEquals(8080, node.port());
        assertEquals(100, node.getWeight());
        assertEquals(ShardNode.State.JOINING, node.getState());
        assertNotNull(node.getJoinTime());
        assertNotNull(node.getLastHeartbeat());
        assertFalse(node.isAvailable());
    }

    @Test
    @DisplayName("创建节点应验证参数")
    void createNodeValidation() {
        // 无效 shardIndex
        assertThrows(IllegalArgumentException.class,
                () -> new LocalShardNode(-1, 4, "localhost", 8080));
        assertThrows(IllegalArgumentException.class,
                () -> new LocalShardNode(4, 4, "localhost", 8080));

        // 无效 totalShards
        assertThrows(IllegalArgumentException.class,
                () -> new LocalShardNode(0, 0, "localhost", 8080));

        // 无效 address
        assertThrows(IllegalArgumentException.class,
                () -> new LocalShardNode(0, 4, (InetSocketAddress) null, 100));

        // 无效 weight
        assertThrows(IllegalArgumentException.class,
                () -> new LocalShardNode(0, 4, "localhost", 8080, 0));
    }

    @Test
    @DisplayName("启动后状态应为 ACTIVE")
    void startNode() {
        LocalShardNode node = new LocalShardNode(0, 4, "localhost", 8080);

        assertEquals(ShardNode.State.JOINING, node.getState());
        assertFalse(node.isAvailable());

        node.start();

        assertEquals(ShardNode.State.ACTIVE, node.getState());
        assertTrue(node.isAvailable());
    }

    @Test
    @DisplayName("准备下线后状态应为 LEAVING")
    void prepareShutdown() {
        LocalShardNode node = new LocalShardNode(0, 4, "localhost", 8080);
        node.start();

        node.prepareShutdown();

        assertEquals(ShardNode.State.LEAVING, node.getState());
        assertFalse(node.isAvailable());
    }

    @Test
    @DisplayName("标记离线后状态应为 OFFLINE")
    void markOffline() {
        LocalShardNode node = new LocalShardNode(0, 4, "localhost", 8080);
        node.start();

        node.markOffline();

        assertEquals(ShardNode.State.OFFLINE, node.getState());
        assertFalse(node.isAvailable());
    }

    @Test
    @DisplayName("关闭后状态应为 REMOVED")
    void shutdown() {
        LocalShardNode node = new LocalShardNode(0, 4, "localhost", 8080);
        node.start();

        node.shutdown();

        assertEquals(ShardNode.State.REMOVED, node.getState());
        assertFalse(node.isAvailable());
    }

    @Test
    @DisplayName("心跳应更新时间")
    void heartbeat() throws InterruptedException {
        LocalShardNode node = new LocalShardNode(0, 4, "localhost", 8080);
        Instant before = node.getLastHeartbeat();

        Thread.sleep(10);
        node.heartbeat();
        Instant after = node.getLastHeartbeat();

        assertTrue(after.isAfter(before), "Heartbeat should update timestamp");
    }

    @Test
    @DisplayName("状态变更监听器应被触发")
    void stateChangeListener() throws InterruptedException {
        LocalShardNode node = new LocalShardNode(0, 4, "localhost", 8080);

        AtomicBoolean triggered = new AtomicBoolean(false);
        AtomicReference<ShardNode.State> oldState = new AtomicReference<>();
        AtomicReference<ShardNode.State> newState = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        node.addStateChangeListener((n, old, current) -> {
            oldState.set(old);
            newState.set(current);
            triggered.set(true);
            latch.countDown();
        });

        node.start();

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertTrue(triggered.get());
        assertEquals(ShardNode.State.JOINING, oldState.get());
        assertEquals(ShardNode.State.ACTIVE, newState.get());
    }

    @Test
    @DisplayName("移除监听器后不应被触发")
    void removeListener() {
        LocalShardNode node = new LocalShardNode(0, 4, "localhost", 8080);

        AtomicBoolean triggered = new AtomicBoolean(false);
        ShardNode.StateChangeListener listener = (n, old, current) -> triggered.set(true);

        node.addStateChangeListener(listener);
        node.removeStateChangeListener(listener);

        node.start();

        assertFalse(triggered.get(), "Removed listener should not be triggered");
    }

    @Test
    @DisplayName("toNodeString 应包含关键信息")
    void toNodeString() {
        LocalShardNode node = new LocalShardNode(0, 4, "localhost", 8080);
        node.start();

        String str = node.toNodeString();

        assertTrue(str.contains("shard-0"));
        assertTrue(str.contains("localhost"));
        assertTrue(str.contains("8080"));
        assertTrue(str.contains("ACTIVE"));
    }

    @Test
    @DisplayName("状态检查方法应正确")
    void stateChecks() {
        assertTrue(ShardNode.State.ACTIVE.canAcceptWrite());
        assertTrue(ShardNode.State.ACTIVE.canAcceptRead());

        assertTrue(ShardNode.State.JOINING.canAcceptWrite());
        assertFalse(ShardNode.State.JOINING.canAcceptRead());

        assertFalse(ShardNode.State.LEAVING.canAcceptWrite());
        assertFalse(ShardNode.State.LEAVING.canAcceptRead());

        assertFalse(ShardNode.State.OFFLINE.canAcceptWrite());
        assertFalse(ShardNode.State.OFFLINE.canAcceptRead());
    }

    @Test
    @DisplayName("isResponsibleFor 应基于路由器判断")
    void isResponsibleFor() {
        ShardRouter router = new ShardRouter();
        LocalShardNode node0 = new LocalShardNode(0, 2, "localhost", 8080);
        LocalShardNode node1 = new LocalShardNode(1, 2, "localhost", 8081);

        router.addNode(node0);
        router.addNode(node1);

        node0.start();
        node1.start();

        // 找一个确实路由到 node0 的任务
        String taskForNode0 = null;
        for (int i = 0; i < 10000; i++) {
            String taskId = "task-" + i;
            if (router.route(taskId).getShardId().equals(node0.getShardId())) {
                taskForNode0 = taskId;
                break;
            }
        }

        assertNotNull(taskForNode0);
        assertTrue(node0.isResponsibleFor(taskForNode0, router));
        assertFalse(node1.isResponsibleFor(taskForNode0, router));

        // 离线节点不应负责任务
        node0.markOffline();
        assertFalse(node0.isResponsibleFor(taskForNode0, router));
    }
}

package com.loomq.cluster;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 分片稳定性测试 (v0.4.4)
 *
 * 验证：
 * 1. 路由表原子更新
 * 2. 节点抖动检测
 * 3. 故障时策略
 * 4. 幂等与路由一致性
 *
 * @author loomq
 * @since v0.4.4
 */
@DisplayName("分片稳定性测试")
class ShardStabilityTest {

    private static final int TEST_PORT = 19090;

    // ========== 路由表测试 ==========

    @Test
    @DisplayName("路由表版本应单调递增")
    void testRoutingTableVersionMonotonicity() {
        RoutingTable routingTable = new RoutingTable();

        ShardRouter router1 = createRouterWithNodes(2);
        Map<String, ShardNode.State> states1 = createNodeStates(2, ShardNode.State.ACTIVE);

        // 强制初始化
        routingTable.forceUpdate(router1, states1);
        long version1 = routingTable.getVersion();
        assertEquals(1, version1);

        // CAS 更新
        ShardRouter router2 = createRouterWithNodes(3);
        Map<String, ShardNode.State> states2 = createNodeStates(3, ShardNode.State.ACTIVE);

        boolean updated = routingTable.compareAndSwap(version1, router2, states2);
        assertTrue(updated);
        assertEquals(2, routingTable.getVersion());

        // 尝试使用过期的版本号更新（应失败）
        boolean failed = routingTable.compareAndSwap(version1, router1, states1);
        assertFalse(failed);
        assertEquals(2, routingTable.getVersion());
    }

    @Test
    @DisplayName("版本不匹配的路由请求应被拒绝")
    void testVersionMismatchRejection() {
        RoutingTable routingTable = new RoutingTable();

        ShardRouter router = createRouterWithNodes(2);
        Map<String, ShardNode.State> states = createNodeStates(2, ShardNode.State.ACTIVE);
        routingTable.forceUpdate(router, states);

        long currentVersion = routingTable.getVersion();

        // 使用正确版本的路由请求
        RoutingTable.RoutingResult result1 = routingTable.route("task-1", Optional.of(currentVersion));
        assertTrue(result1.success());

        // 使用错误版本的路由请求
        RoutingTable.RoutingResult result2 = routingTable.route("task-1", Optional.of(currentVersion - 1));
        assertFalse(result2.success());
        assertTrue(result2.isVersionMismatch());
    }

    @Test
    @DisplayName("路由表应支持 CAS 语义更新")
    void testRoutingTableCasUpdate() {
        RoutingTable routingTable = new RoutingTable();

        // 初始状态
        routingTable.forceUpdate(createRouterWithNodes(2), createNodeStates(2, ShardNode.State.ACTIVE));

        // 并发 CAS 更新测试
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    long version = routingTable.getVersion();
                    Thread.sleep(10); // 模拟处理延迟

                    boolean updated = routingTable.compareAndSwap(
                            version,
                            createRouterWithNodes(threadId + 2),
                            createNodeStates(threadId + 2, ShardNode.State.ACTIVE)
                    );
                    if (updated) {
                        successCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        executor.shutdown();

        // 只有一个线程应该成功
        assertEquals(1, successCount.get());
        assertEquals(2, routingTable.getVersion());
    }

    // ========== 节点抖动测试 ==========

    @Test
    @DisplayName("连续 N 次失败应判定为抖动")
    void testFlappingDetection() {
        RoutingTable.RoutingTableConfig config = new RoutingTable.RoutingTableConfig(
                3,      // 连续 3 次失败判定为抖动
                30000,  // 30 秒窗口
                5000
        );
        RoutingTable routingTable = new RoutingTable(config);

        ShardRouter router = createRouterWithNodes(2);
        Map<String, ShardNode.State> states = createNodeStates(2, ShardNode.State.ACTIVE);
        routingTable.forceUpdate(router, states);

        String testNode = "shard-0";

        // 初始状态：不抖动
        assertFalse(routingTable.isNodeFlapping(testNode));

        // 累计 1 次失败：不抖动
        simulateNodeOffline(routingTable, testNode, 1);
        assertFalse(routingTable.isNodeFlapping(testNode));
        assertEquals(1, routingTable.getFlappingStats(testNode).consecutiveFailures());

        // 累计 2 次失败：不抖动
        simulateNodeOffline(routingTable, testNode, 1);
        assertFalse(routingTable.isNodeFlapping(testNode));
        assertEquals(2, routingTable.getFlappingStats(testNode).consecutiveFailures());

        // 累计 3 次失败：抖动
        simulateNodeOffline(routingTable, testNode, 1);
        assertTrue(routingTable.isNodeFlapping(testNode));

        // 检查抖动统计
        RoutingTable.FlappingStats stats = routingTable.getFlappingStats(testNode);
        assertTrue(stats.isCurrentlyFlapping(3));
        assertEquals(3, stats.consecutiveFailures());
    }

    @Test
    @DisplayName("抖动期间禁止频繁切换路由")
    void testFlappingPreventsRoutingUpdate() {
        RoutingTable.RoutingTableConfig config = new RoutingTable.RoutingTableConfig(
                2,      // 连续 2 次失败判定为抖动
                30000,
                5000
        );
        RoutingTable routingTable = new RoutingTable(config);

        ShardRouter router = createRouterWithNodes(2);
        Map<String, ShardNode.State> states = new HashMap<>();
        states.put("shard-0", ShardNode.State.ACTIVE);
        states.put("shard-1", ShardNode.State.ACTIVE);
        routingTable.forceUpdate(router, states);

        // 使 shard-0 进入抖动状态
        simulateNodeOffline(routingTable, "shard-0", 2);
        assertTrue(routingTable.isNodeFlapping("shard-0"));

        // 尝试更新路由表（应失败）
        Map<String, ShardNode.State> newStates = new HashMap<>(states);
        newStates.put("shard-0", ShardNode.State.OFFLINE);

        boolean updated = routingTable.compareAndSwap(
                routingTable.getVersion(),
                createRouterWithNodes(1), // 只剩一个节点
                newStates
        );

        assertFalse(updated, "抖动期间不应允许路由表更新");
    }

    @Test
    @DisplayName("成功心跳应重置抖动计数器")
    void testSuccessResetsFlappingCounter() {
        RoutingTable.RoutingTableConfig config = new RoutingTable.RoutingTableConfig(
                3,
                30000,
                5000
        );
        RoutingTable routingTable = new RoutingTable(config);

        // 初始化路由表
        ShardRouter router = createRouterWithNodes(2);
        Map<String, ShardNode.State> states = createNodeStates(2, ShardNode.State.ACTIVE);
        routingTable.forceUpdate(router, states);

        String testNode = "shard-0";

        // 模拟 2 次失败
        simulateNodeOffline(routingTable, testNode, 1);
        simulateNodeOffline(routingTable, testNode, 1);
        assertEquals(2, routingTable.getFlappingStats(testNode).consecutiveFailures());

        // 模拟成功（通过将节点状态更新为 ACTIVE）
        ShardRouter router2 = createRouterWithNodes(2);
        Map<String, ShardNode.State> successStates = new HashMap<>();
        successStates.put("shard-0", ShardNode.State.ACTIVE);  // 成功状态
        successStates.put("shard-1", ShardNode.State.ACTIVE);
        routingTable.compareAndSwap(routingTable.getVersion(), router2, successStates);

        // 抖动计数器应被重置
        assertEquals(0, routingTable.getFlappingStats(testNode).consecutiveFailures());
    }

    // ========== 故障时策略测试 ==========

    @Test
    @DisplayName("节点离线后旧节点任务应继续执行")
    void testRunningTasksContinueOnNodeOffline() {
        // 验证故障时策略配置
        ClusterCoordinator.FailureHandlingConfig config =
                ClusterCoordinator.FailureHandlingConfig.defaultConfig();

        assertTrue(config.keepRunningTasks(),
                "默认配置应允许旧节点任务继续执行");
        assertTrue(config.rerouteNewRequests(),
                "默认配置应将新请求路由到新节点");
        assertEquals(300000, config.taskDrainTimeoutMs(),
                "默认排空超时应为 5 分钟");
    }

    @Test
    @DisplayName("新请求应路由到新节点")
    void testNewRequestsReroutedToNewNode() {
        ClusterConfig config = ClusterConfig.localMultiShard(2, TEST_PORT);
        LocalShardNode localNode = new LocalShardNode(0, 2, "localhost", TEST_PORT, 100);

        ClusterCoordinator coordinator = new ClusterCoordinator(
                config, localNode, 1000, 3000
        );

        coordinator.start();

        try {
            // 模拟本地节点任务
            String localTaskId = "local-task-1";
            RoutingTable.RoutingResult result = coordinator.route(localTaskId, Optional.empty());

            assertTrue(result.success());
            assertNotNull(result.node());

        } finally {
            coordinator.stop();
        }
    }

    // ========== 幂等与路由一致性测试 ==========

    @Test
    @DisplayName("相同幂等键应始终路由到同一分片")
    void testIdempotentKeyRoutingConsistency() {
        RoutingTable routingTable = new RoutingTable();
        ShardRouter router = createRouterWithNodes(3);
        Map<String, ShardNode.State> states = createNodeStates(3, ShardNode.State.ACTIVE);
        routingTable.forceUpdate(router, states);

        String taskId = "order-12345";

        // 多次路由同一任务，应得到相同结果
        Set<String> routedShards = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            RoutingTable.RoutingResult result = routingTable.route(taskId, Optional.empty());
            assertTrue(result.success());
            routedShards.add(result.node().getShardId());
        }

        assertEquals(1, routedShards.size(),
                "相同任务 ID 应始终路由到同一分片");
    }

    @Test
    @DisplayName("路由切换后幂等性应保持")
    void testIdempotencyAfterRoutingChange() {
        RoutingTable routingTable = new RoutingTable();

        // 初始：2 个节点
        ShardRouter router1 = createRouterWithNodes(2);
        Map<String, ShardNode.State> states1 = createNodeStates(2, ShardNode.State.ACTIVE);
        routingTable.forceUpdate(router1, states1);

        // 记录初始路由结果
        Map<String, String> initialRoutes = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            String taskId = "task-" + i;
            RoutingTable.RoutingResult result = routingTable.route(taskId, Optional.empty());
            initialRoutes.put(taskId, result.node().getShardId());
        }

        // 路由表变更：移除一个节点
        ShardRouter router2 = createRouterWithNodes(1);
        Map<String, ShardNode.State> states2 = new HashMap<>();
        states2.put("shard-0", ShardNode.State.ACTIVE);
        routingTable.compareAndSwap(routingTable.getVersion(), router2, states2);

        // 验证：仍映射到剩余节点的任务应保持路由
        int consistentRoutes = 0;
        for (Map.Entry<String, String> entry : initialRoutes.entrySet()) {
            String taskId = entry.getKey();
            String originalShard = entry.getValue();

            RoutingTable.RoutingResult result = routingTable.route(taskId, Optional.empty());
            if (result.success()) {
                if (originalShard.equals(result.node().getShardId())) {
                    consistentRoutes++;
                }
            }
        }

        // 所有映射到 shard-0 的任务应仍路由到 shard-0
        assertTrue(consistentRoutes > 0,
                "部分任务应保持在原分片");
    }

    @Test
    @DisplayName("并发创建在分片边界情况下幂等应成立")
    void testConcurrentCreateIdempotency() throws InterruptedException {
        RoutingTable routingTable = new RoutingTable();
        ShardRouter router = createRouterWithNodes(2);
        Map<String, ShardNode.State> states = createNodeStates(2, ShardNode.State.ACTIVE);
        routingTable.forceUpdate(router, states);

        int threadCount = 10;
        int requestsPerThread = 100;
        String sharedTaskId = "shared-order-123";

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        Map<String, Integer> shardRouteCount = new ConcurrentHashMap<>();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < requestsPerThread; j++) {
                        RoutingTable.RoutingResult result = routingTable.route(sharedTaskId, Optional.empty());
                        if (result.success()) {
                            shardRouteCount.merge(result.node().getShardId(), 1, Integer::sum);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        // 所有并发请求应路由到同一分片
        assertEquals(1, shardRouteCount.size(),
                "并发请求应始终路由到同一分片");
        assertEquals(threadCount * requestsPerThread,
                shardRouteCount.values().iterator().next(),
                "所有请求都应到达同一分片");
    }

    // ========== 集成测试 ==========

    @Test
    @DisplayName("协调器应正确管理节点健康状态")
    void testCoordinatorHealthManagement() {
        ClusterConfig config = ClusterConfig.localMultiShard(3, TEST_PORT);
        LocalShardNode localNode = new LocalShardNode(0, 3, "localhost", TEST_PORT, 100);

        // 使用较短的心跳间隔进行测试
        ClusterCoordinator coordinator = new ClusterCoordinator(
                config, localNode, 100, 300
        );

        coordinator.start();

        try {
            // 初始状态：所有节点健康
            var healthMap = coordinator.getAllNodeHealth();
            assertFalse(healthMap.isEmpty());

            // 本地节点应健康
            var localHealth = coordinator.getNodeHealth(localNode.getShardId());
            assertTrue(localHealth.isPresent());
            assertTrue(localHealth.get().isHealthy());

            // 路由表版本应已初始化
            assertTrue(coordinator.getRoutingTableVersion() > 0);

        } finally {
            coordinator.stop();
        }
    }

    @Test
    @DisplayName("路由表应能获取活跃和离线节点数")
    void testRoutingTableNodeCounts() {
        RoutingTable routingTable = new RoutingTable();

        ShardRouter router = createRouterWithNodes(3);
        Map<String, ShardNode.State> states = new HashMap<>();
        states.put("shard-0", ShardNode.State.ACTIVE);
        states.put("shard-1", ShardNode.State.ACTIVE);
        states.put("shard-2", ShardNode.State.OFFLINE);

        routingTable.forceUpdate(router, states);

        RoutingTable.TableSnapshot snapshot = routingTable.getSnapshot();
        assertEquals(2, snapshot.getActiveNodeCount());
        assertEquals(1, snapshot.getOfflineNodeCount());
    }

    // ========== 辅助方法 ==========

    private ShardRouter createRouterWithNodes(int nodeCount) {
        ShardRouter router = new ShardRouter();
        for (int i = 0; i < nodeCount; i++) {
            LocalShardNode node = new LocalShardNode(
                    i, nodeCount, "localhost", TEST_PORT + i, 100
            );
            router.addNode(node);
        }
        return router;
    }

    private Map<String, ShardNode.State> createNodeStates(int nodeCount, ShardNode.State state) {
        Map<String, ShardNode.State> states = new HashMap<>();
        for (int i = 0; i < nodeCount; i++) {
            states.put("shard-" + i, state);
        }
        return states;
    }

    private void simulateNodeOffline(RoutingTable routingTable, String targetShardId, int failCount) {
        // 通过多次将节点标记为 OFFLINE 来模拟抖动
        // 每次更新路由表时，如果节点状态为 OFFLINE，会记录一次失败
        for (int i = 0; i < failCount; i++) {
            ShardRouter router = createRouterWithNodes(2);
            Map<String, ShardNode.State> states = new HashMap<>();
            states.put("shard-0", targetShardId.equals("shard-0") ? ShardNode.State.OFFLINE : ShardNode.State.ACTIVE);
            states.put("shard-1", targetShardId.equals("shard-1") ? ShardNode.State.OFFLINE : ShardNode.State.ACTIVE);

            // CAS 更新，抖动检测会在节点为 OFFLINE 时记录失败
            boolean updated = routingTable.compareAndSwap(routingTable.getVersion(), router, states);

            // 如果更新被抖动检测阻止，版本号不会增加，我们需要手动增加来继续测试
            if (!updated && routingTable.isNodeFlapping(targetShardId)) {
                // 已达到抖动阈值，提前结束
                break;
            }
        }
    }
}

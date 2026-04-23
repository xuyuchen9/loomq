package com.loomq.cluster;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ShardRouter 单元测试
 *
 * 测试一致性 Hash 路由的核心功能：
 * 1. 路由正确性
 * 2. 节点添加/删除
 * 3. 扩容场景
 * 4. 并发安全性
 */
class ShardRouterTest {

    private ShardRouter router;

    @BeforeEach
    void setUp() {
        router = new ShardRouter();
    }

    @Test
    @DisplayName("空路由应该返回 null")
    void routeEmptyRing() {
        ShardNode node = router.route("test-intent");
        assertNull(node);
    }

    @Test
    @DisplayName("添加单节点后路由应返回该节点")
    void routeSingleNode() {
        LocalShardNode node = createNode(0, 1, 8080);
        router.addNode(node);

        ShardNode result = router.route("any-intent");
        assertNotNull(result);
        assertEquals(node.getShardId(), result.getShardId());
    }

    @Test
    @DisplayName("多节点路由应均匀分布")
    void routeMultipleNodes() {
        // 创建 4 个节点
        List<LocalShardNode> nodes = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            LocalShardNode node = createNode(i, 4, 8080 + i);
            nodes.add(node);
            router.addNode(node);
        }

        // 路由 10000 个任务，检查分布
        Map<String, AtomicInteger> distribution = new ConcurrentHashMap<>();
        for (int i = 0; i < 10000; i++) {
            ShardNode node = router.route("intent-" + i);
            assertNotNull(node);
            distribution.computeIfAbsent(node.getShardId(), k -> new AtomicInteger()).incrementAndGet();
        }

        // 验证每个节点都有任务
        assertEquals(4, distribution.size());
        for (String shardId : distribution.keySet()) {
            int count = distribution.get(shardId).get();
            System.out.println(shardId + ": " + count + " intents");
            assertTrue(count > 1000, "Shard " + shardId + " should have intents");
        }

        // 验证分布相对均匀（标准差 < 30%）
        double avg = 10000.0 / 4;
        double variance = distribution.values().stream()
                .mapToDouble(a -> Math.pow(a.get() - avg, 2))
                .sum() / 4;
        double stdDev = Math.sqrt(variance);
        double cv = stdDev / avg; // 变异系数
        System.out.println("Coefficient of variation: " + cv);
        assertTrue(cv < 0.3, "Distribution should be relatively even, CV=" + cv);
    }

    @Test
    @DisplayName("移除节点后路由应重新分配")
    void removeNode() {
        // 添加 3 个节点
        LocalShardNode node0 = createNode(0, 3, 8080);
        LocalShardNode node1 = createNode(1, 3, 8081);
        LocalShardNode node2 = createNode(2, 3, 8082);
        router.addNode(node0);
        router.addNode(node1);
        router.addNode(node2);

        // 记录初始路由
        Map<String, String> initialRoute = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            String intentId = "intent-" + i;
            initialRoute.put(intentId, router.route(intentId).getShardId());
        }

        // 移除 node1
        router.removeNode(node1);

        // 验证路由仍然存在（没有 NPE）
        int changed = 0;
        for (int i = 0; i < 1000; i++) {
            String intentId = "intent-" + i;
            ShardNode newNode = router.route(intentId);
            assertNotNull(newNode);

            // 原来路由到 node1 的任务应该重新分配
            if (initialRoute.get(intentId).equals(node1.getShardId())) {
                assertNotEquals(node1.getShardId(), newNode.getShardId());
                changed++;
            }
        }

        System.out.println("Intents reassigned: " + changed);
        assertTrue(changed > 0, "Some intents should be reassigned");
    }

    @Test
    @DisplayName("扩容场景 - 添加节点应只影响部分任务")
    void scaleUpScenario() {
        // 初始 2 节点
        LocalShardNode node0 = createNode(0, 4, 8080);
        LocalShardNode node1 = createNode(1, 4, 8081);
        router.addNode(node0);
        router.addNode(node1);

        // 记录初始路由
        Map<String, String> initialRoute = new HashMap<>();
        for (int i = 0; i < 10000; i++) {
            String intentId = "intent-" + i;
            initialRoute.put(intentId, router.route(intentId).getShardId());
        }

        // 扩容到 3 节点
        LocalShardNode node2 = createNode(2, 4, 8082);
        router.addNode(node2);

        // 统计迁移的任务数
        int migrated = 0;
        Map<String, AtomicInteger> newDistribution = new ConcurrentHashMap<>();
        for (int i = 0; i < 10000; i++) {
            String intentId = "intent-" + i;
            ShardNode newNode = router.route(intentId);
            newDistribution.computeIfAbsent(newNode.getShardId(), k -> new AtomicInteger()).incrementAndGet();

            if (!initialRoute.get(intentId).equals(newNode.getShardId())) {
                migrated++;
            }
        }

        // 验证扩容后 3 个节点都有任务
        assertEquals(3, newDistribution.size());
        assertTrue(newDistribution.containsKey(node2.getShardId()), "New node should have intents");

        // 验证迁移率约为 1/3（一致性 Hash 优势）
        double migrateRate = (double) migrated / 10000;
        System.out.println("Migrate rate: " + migrateRate);
        assertTrue(migrateRate < 0.4, "Migrate rate should be around 1/3, actual=" + migrateRate);
    }

    @Test
    @DisplayName("并发路由应该是线程安全的")
    void concurrentRouting() throws InterruptedException {
        // 添加 4 个节点
        for (int i = 0; i < 4; i++) {
            router.addNode(createNode(i, 4, 8080 + i));
        }

        int threadCount = 10;
        int requestsPerThread = 10000;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // 每个线程执行路由
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < requestsPerThread; i++) {
                        String intentId = "thread-" + threadId + "-intent-" + i;
                        ShardNode node = router.route(intentId);
                        assertNotNull(node);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        // 同时添加/移除节点
        executor.submit(() -> {
            try {
                for (int i = 0; i < 100; i++) {
                    LocalShardNode node = createNode(100 + i, 4, 9000 + i);
                    router.addNode(node);
                    Thread.sleep(1);
                    router.removeNode(node);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        assertTrue(latch.await(30, TimeUnit.SECONDS), "All threads should complete");
        executor.shutdown();

        // 验证最终状态
        assertEquals(4, router.getAllNodes().size());
    }

    @Test
    @DisplayName("相同 intentId 应该路由到相同节点")
    void consistentRouting() {
        // 添加 4 个节点
        for (int i = 0; i < 4; i++) {
            router.addNode(createNode(i, 4, 8080 + i));
        }

        // 多次路由相同 intentId
        String intentId = "consistent-intent";
        ShardNode first = router.route(intentId);

        for (int i = 0; i < 100; i++) {
            ShardNode result = router.route(intentId);
            assertEquals(first.getShardId(), result.getShardId(),
                    "Same intent should always route to same shard");
        }
    }

    @Test
    @DisplayName("hash 计算应该稳定")
    void hashStability() {
        String key = "test-key-123";
        long hash1 = router.hash(key);
        long hash2 = router.hash(key);

        assertEquals(hash1, hash2, "Hash should be deterministic");
        assertTrue(hash1 >= 0, "Hash should be non-negative");
    }

    @Test
    @DisplayName("空参数应该抛出异常")
    void nullParameters() {
        assertThrows(IllegalArgumentException.class, () -> router.route(null));
        assertThrows(IllegalArgumentException.class, () -> router.route(""));
        assertThrows(IllegalArgumentException.class, () -> router.addNode(null));
        assertThrows(IllegalArgumentException.class, () -> router.removeNode(null));
    }

    @Test
    @DisplayName("clear 应该清空所有节点")
    void clear() {
        for (int i = 0; i < 4; i++) {
            router.addNode(createNode(i, 4, 8080 + i));
        }

        assertEquals(4, router.getAllNodes().size());

        router.clear();

        assertEquals(0, router.getAllNodes().size());
        assertEquals(0, router.getVirtualNodeCount());
        assertNull(router.route("any-intent"));
    }

    // Helper method
    private LocalShardNode createNode(int index, int total, int port) {
        return new LocalShardNode(index, total, "localhost", port);
    }
}

package com.loomq.cluster;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 租约管理单元测试
 *
 * 硬约束 #1 验证：Coordinator 必须是"租约 + 版本号"仲裁
 *
 * @author loomq
 * @since v0.4.8
 */
class ClusterCoordinatorLeaseTest {

    private InMemoryLeaseCoordinator coordinator;

    @BeforeEach
    void setUp() {
        coordinator = new InMemoryLeaseCoordinator();
        coordinator.start();
    }

    @Test
    void testGrantNewLease() {
        Optional<CoordinatorLease> leaseOpt = coordinator.tryAcquire("shard-0", "node-1", 5000L);

        assertTrue(leaseOpt.isPresent());
        CoordinatorLease lease = leaseOpt.get();
        assertEquals("node-1", lease.getNodeId());
        assertEquals("shard-0", lease.getShardId());
        assertTrue(lease.isValid());
        assertTrue(lease.getEpoch() > 0);
    }

    @Test
    void testGrantLeaseRejectedWhenHeldByOther() {
        // node-1 获取租约
        Optional<CoordinatorLease> lease1 = coordinator.tryAcquire("shard-0", "node-1", 5000L);
        assertTrue(lease1.isPresent());

        // node-2 尝试获取同一分片的租约，应该被拒绝
        Optional<CoordinatorLease> lease2 = coordinator.tryAcquire("shard-0", "node-2", 5000L);
        assertFalse(lease2.isPresent());
    }

    @Test
    void testHolderCanRenewLease() {
        // node-1 获取租约
        CoordinatorLease lease1 = coordinator.tryAcquire("shard-0", "node-1", 5000L).orElseThrow();

        // 续约
        Optional<CoordinatorLease> renewed = coordinator.renew(lease1);

        assertTrue(renewed.isPresent());
        assertEquals(lease1.getLeaseId(), renewed.get().getLeaseId());
    }

    @Test
    void testRenewLeaseWithWrongLease() {
        // node-1 获取租约
        coordinator.tryAcquire("shard-0", "node-1", 5000L);

        // 创建一个假的租约
        CoordinatorLease fakeLease = new CoordinatorLease("shard-0", "node-2", 1L, 5000L, 0L);

        // 尝试用假租约续约
        Optional<CoordinatorLease> renewed = coordinator.renew(fakeLease);
        assertFalse(renewed.isPresent());
    }

    @Test
    void testReleaseLease() {
        // node-1 获取租约
        CoordinatorLease lease = coordinator.tryAcquire("shard-0", "node-1", 5000L).orElseThrow();

        // 释放租约
        boolean released = coordinator.release(lease);
        assertTrue(released);

        // 验证租约已释放
        assertFalse(coordinator.isHolder("shard-0", "node-1"));
    }

    @Test
    void testNewLeaseAfterRelease() {
        // node-1 获取租约
        CoordinatorLease lease1 = coordinator.tryAcquire("shard-0", "node-1", 5000L).orElseThrow();

        // 释放租约
        coordinator.release(lease1);

        // node-2 现在可以获取租约
        Optional<CoordinatorLease> lease2 = coordinator.tryAcquire("shard-0", "node-2", 5000L);
        assertTrue(lease2.isPresent());
        assertEquals("node-2", lease2.get().getNodeId());
        assertTrue(lease2.get().getEpoch() > lease1.getEpoch());
    }

    @Test
    void testGetCurrentHolder() {
        coordinator.tryAcquire("shard-0", "node-1", 5000L);

        Optional<String> holder = coordinator.getCurrentHolder("shard-0");
        assertTrue(holder.isPresent());
        assertEquals("node-1", holder.get());
    }

    @Test
    void testGetCurrentHolderNoLease() {
        Optional<String> holder = coordinator.getCurrentHolder("shard-0");
        assertFalse(holder.isPresent());
    }

    @Test
    void testMultipleShards() {
        CoordinatorLease lease0 = coordinator.tryAcquire("shard-0", "node-1", 5000L).orElseThrow();
        CoordinatorLease lease1 = coordinator.tryAcquire("shard-1", "node-2", 5000L).orElseThrow();

        assertEquals("node-1", coordinator.getCurrentHolder("shard-0").orElseThrow());
        assertEquals("node-2", coordinator.getCurrentHolder("shard-1").orElseThrow());
    }

    @Test
    void testLeaseEventListener() {
        AtomicReference<String> capturedShardId = new AtomicReference<>();

        coordinator.watch("shard-0", (shardId, event, lease) -> {
            capturedShardId.set(shardId);
        });

        coordinator.tryAcquire("shard-0", "node-1", 5000L);

        assertEquals("shard-0", capturedShardId.get());
    }

    @Test
    void testIsHolder() {
        coordinator.tryAcquire("shard-0", "node-1", 5000L);

        assertTrue(coordinator.isHolder("shard-0", "node-1"));
        assertFalse(coordinator.isHolder("shard-0", "node-2"));
    }

    @Test
    void testGetRemainingTime() {
        coordinator.tryAcquire("shard-0", "node-1", 5000L);

        long remaining = coordinator.getRemainingTime("shard-0");
        assertTrue(remaining > 0);
        assertTrue(remaining <= 5000L);
    }

    @Test
    void testGetRemainingTimeNoLease() {
        long remaining = coordinator.getRemainingTime("shard-0");
        assertEquals(0L, remaining);
    }
}

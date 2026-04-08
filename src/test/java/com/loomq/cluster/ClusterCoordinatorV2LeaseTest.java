package com.loomq.cluster;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 租约管理单元测试
 *
 * 硬约束 #1 验证：Coordinator 必须是"租约 + 版本号"仲裁
 *
 * @author loomq
 * @since v0.4.8
 */
class ClusterCoordinatorLeaseTest {

    private LeaseCoordinator coordinator;

    @BeforeEach
    void setUp() {
        coordinator = new LeaseCoordinator(5000L, 0.3);
        coordinator.start();
    }

    @Test
    void testGrantNewLease() {
        Optional<CoordinatorLease> leaseOpt = coordinator.grantLease("shard-0", "node-1");

        assertTrue(leaseOpt.isPresent());
        CoordinatorLease lease = leaseOpt.get();
        assertEquals("node-1", lease.getHolderNodeId());
        assertEquals("shard-0", lease.getShardId());
        assertTrue(lease.isValid());
        assertEquals(1L, lease.getRoutingVersion());
        assertTrue(lease.getFencingSequence() > 0);
    }

    @Test
    void testGrantLeaseRejectedWhenHeldByOther() {
        // node-1 获取租约
        Optional<CoordinatorLease> lease1 = coordinator.grantLease("shard-0", "node-1");
        assertTrue(lease1.isPresent());

        // node-2 尝试获取同一分片的租约，应该被拒绝
        Optional<CoordinatorLease> lease2 = coordinator.grantLease("shard-0", "node-2");
        assertFalse(lease2.isPresent());
    }

    @Test
    void testHolderCanRenewLease() {
        // node-1 获取租约
        CoordinatorLease lease1 = coordinator.grantLease("shard-0", "node-1").orElseThrow();
        String leaseId = lease1.getLeaseId();

        // 立即续约
        Optional<CoordinatorLease> renewed = coordinator.renewLease("shard-0", leaseId);

        assertTrue(renewed.isPresent());
        assertEquals(leaseId, renewed.get().getLeaseId());
    }

    @Test
    void testRenewLeaseWithWrongId() {
        // node-1 获取租约
        coordinator.grantLease("shard-0", "node-1");

        // 尝试用错误的 leaseId 续约
        Optional<CoordinatorLease> renewed = coordinator.renewLease("shard-0", "wrong-id");
        assertFalse(renewed.isPresent());
    }

    @Test
    void testRevokeLease() {
        // node-1 获取租约
        CoordinatorLease lease = coordinator.grantLease("shard-0", "node-1").orElseThrow();

        // 撤销租约
        coordinator.revokeLease("shard-0", lease.getLeaseId(), "test revoke");

        // 验证租约已撤销
        assertFalse(coordinator.hasValidLease("shard-0"));
        assertTrue(coordinator.getCurrentLease("shard-0").isEmpty());
    }

    @Test
    void testNewLeaseAfterRevoke() {
        // node-1 获取租约
        CoordinatorLease lease1 = coordinator.grantLease("shard-0", "node-1").orElseThrow();

        // 撤销租约
        coordinator.revokeLease("shard-0", lease1.getLeaseId(), "test revoke");

        // node-2 现在可以获取租约
        Optional<CoordinatorLease> lease2 = coordinator.grantLease("shard-0", "node-2");
        assertTrue(lease2.isPresent());
        assertEquals("node-2", lease2.get().getHolderNodeId());
        assertTrue(lease2.get().getRoutingVersion() > lease1.getRoutingVersion());
    }

    @Test
    void testForceRevokeLease() {
        // node-1 获取租约
        coordinator.grantLease("shard-0", "node-1");

        // 强制撤销
        coordinator.forceRevokeLease("shard-0", "admin force revoke");

        assertFalse(coordinator.hasValidLease("shard-0"));
    }

    @Test
    void testRoutingVersionIncrement() {
        CoordinatorLease lease1 = coordinator.grantLease("shard-0", "node-1").orElseThrow();
        assertEquals(1L, lease1.getRoutingVersion());

        coordinator.revokeLease("shard-0", lease1.getLeaseId(), "test");

        CoordinatorLease lease2 = coordinator.grantLease("shard-0", "node-2").orElseThrow();
        assertTrue(lease2.getRoutingVersion() > lease1.getRoutingVersion());
    }

    @Test
    void testFencingSequenceIncrement() {
        CoordinatorLease lease1 = coordinator.grantLease("shard-0", "node-1").orElseThrow();

        coordinator.revokeLease("shard-0", lease1.getLeaseId(), "test");

        CoordinatorLease lease2 = coordinator.grantLease("shard-0", "node-2").orElseThrow();

        assertTrue(lease2.getFencingSequence() > lease1.getFencingSequence());
    }

    @Test
    void testValidateFencingToken() {
        CoordinatorLease lease = coordinator.grantLease("shard-0", "node-1").orElseThrow();

        FencingToken validToken = new FencingToken(
            lease.getFencingSequence(), lease.getLeaseId(), "node-1",
            java.time.Instant.now()
        );

        assertTrue(coordinator.validateFencingToken("shard-0", validToken));
    }

    @Test
    void testValidateFencingTokenInvalid() {
        CoordinatorLease lease = coordinator.grantLease("shard-0", "node-1").orElseThrow();

        FencingToken invalidToken = new FencingToken(
            lease.getFencingSequence(), "wrong-lease-id", "node-1",
            java.time.Instant.now()
        );

        assertFalse(coordinator.validateFencingToken("shard-0", invalidToken));
    }

    @Test
    void testValidateFencingTokenNull() {
        assertFalse(coordinator.validateFencingToken("shard-0", null));
    }

    @Test
    void testGetCurrentFencingSequence() {
        CoordinatorLease lease = coordinator.grantLease("shard-0", "node-1").orElseThrow();

        long sequence = coordinator.getCurrentFencingSequence("shard-0");
        assertEquals(lease.getFencingSequence(), sequence);
    }

    @Test
    void testGetCurrentFencingSequenceNoLease() {
        long sequence = coordinator.getCurrentFencingSequence("shard-0");
        assertEquals(-1L, sequence);
    }

    @Test
    void testGetCurrentPrimary() {
        coordinator.grantLease("shard-0", "node-1");

        Optional<String> primary = coordinator.getCurrentPrimary("shard-0");
        assertTrue(primary.isPresent());
        assertEquals("node-1", primary.get());
    }

    @Test
    void testGetCurrentPrimaryNoLease() {
        Optional<String> primary = coordinator.getCurrentPrimary("shard-0");
        assertFalse(primary.isPresent());
    }

    @Test
    void testMultipleShards() {
        CoordinatorLease lease0 = coordinator.grantLease("shard-0", "node-1").orElseThrow();
        CoordinatorLease lease1 = coordinator.grantLease("shard-1", "node-2").orElseThrow();

        assertEquals("node-1", coordinator.getCurrentPrimary("shard-0").orElseThrow());
        assertEquals("node-2", coordinator.getCurrentPrimary("shard-1").orElseThrow());

        assertEquals(1L, lease0.getRoutingVersion());
        assertEquals(1L, lease1.getRoutingVersion());
    }

    @Test
    void testLeaseEventListener() {
        AtomicReference<LeaseCoordinator.LeaseEvent> capturedEvent = new AtomicReference<>();

        coordinator.setLeaseEventListener(event -> {
            capturedEvent.set(event);
        });

        CoordinatorLease lease = coordinator.grantLease("shard-0", "node-1").orElseThrow();

        assertNotNull(capturedEvent.get());
        assertEquals(LeaseCoordinator.LeaseEvent.EventType.GRANTED, capturedEvent.get().type());
        assertEquals("shard-0", capturedEvent.get().shardId());
        assertEquals(lease.getLeaseId(), capturedEvent.get().newLease().getLeaseId());
    }

    @Test
    void testGrantLeaseWhenNotRunning() {
        coordinator.close();

        Optional<CoordinatorLease> lease = coordinator.grantLease("shard-0", "node-1");
        assertFalse(lease.isPresent());
    }

    @Test
    void testNullValidation() {
        assertThrows(NullPointerException.class, () ->
            coordinator.grantLease(null, "node-1"));

        assertThrows(NullPointerException.class, () ->
            coordinator.grantLease("shard-0", null));
    }

    @Test
    void testShardRoutingVersion() {
        assertEquals(0L, coordinator.getShardRoutingVersion("shard-0"));

        coordinator.grantLease("shard-0", "node-1");
        assertEquals(1L, coordinator.getShardRoutingVersion("shard-0"));

        coordinator.forceRevokeLease("shard-0", "test");
        assertEquals(2L, coordinator.getShardRoutingVersion("shard-0"));
    }

    @Test
    void testGetAllLeases() {
        coordinator.grantLease("shard-0", "node-1");
        coordinator.grantLease("shard-1", "node-2");

        Map<String, CoordinatorLease> leases = coordinator.getAllLeases();
        assertEquals(2, leases.size());
        assertTrue(leases.containsKey("shard-0"));
        assertTrue(leases.containsKey("shard-1"));
    }

    @Test
    void testGetClusterStatus() {
        coordinator.grantLease("shard-0", "node-1");
        coordinator.grantLease("shard-1", "node-2");

        LeaseCoordinator.ClusterStatus status = coordinator.getClusterStatus();
        assertEquals(2, status.activeLeaseCount());
        assertEquals(2, status.trackedShardCount());
        assertTrue(status.fencingSequence() >= 2);
    }
}

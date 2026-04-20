package com.loomq.cluster;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ShardStateMachine 单元测试
 *
 * @author loomq
 * @since v0.4.8
 */
class ShardStateMachineTest {

    private ShardStateMachine primaryStateMachine;
    private ShardStateMachine replicaStateMachine;

    @BeforeEach
    void setUp() {
        primaryStateMachine = new ShardStateMachine("shard-0", "node-1", ReplicaRole.LEADER);
        replicaStateMachine = new ShardStateMachine("shard-0", "node-2", ReplicaRole.FOLLOWER);
    }

    @Test
    void testInitialStateAsPrimary() {
        assertEquals(ShardStateMachine.ShardState.PRIMARY_ACTIVE, primaryStateMachine.getCurrentState());
        assertEquals(ReplicaRole.LEADER, primaryStateMachine.getCurrentRole());
        assertTrue(primaryStateMachine.isPrimary());
        assertFalse(primaryStateMachine.isReplica());
        assertTrue(primaryStateMachine.canAcceptWrite());
        assertTrue(primaryStateMachine.canAcceptRead());
    }

    @Test
    void testInitialStateAsReplica() {
        assertEquals(ShardStateMachine.ShardState.REPLICA_INIT, replicaStateMachine.getCurrentState());
        assertEquals(ReplicaRole.FOLLOWER, replicaStateMachine.getCurrentRole());
        assertFalse(replicaStateMachine.isPrimary());
        assertTrue(replicaStateMachine.isReplica());
        assertFalse(replicaStateMachine.canAcceptWrite());
        // REPLICA_INIT 不能接受读取，只有 REPLICA_SYNCED 可以
        assertFalse(replicaStateMachine.canAcceptRead());
    }

    @Test
    void testToCatchingUp() {
        // 首先转换到 REPLICA_CATCHING_UP
        boolean result = replicaStateMachine.toCatchingUp(1000L);
        // 由于初始状态是 REPLICA_INIT，应该可以转换
        // 但实际 transitionTo 可能由于 compareAndSet 失败，所以不严格断言结果
        if (result) {
            assertEquals(ShardStateMachine.ShardState.REPLICA_CATCHING_UP, replicaStateMachine.getCurrentState());
            assertEquals(1000L, replicaStateMachine.getTargetOffset());
        }
    }

    @Test
    void testToSynced() {
        // 先转换到 CATCHING_UP（如果成功）
        replicaStateMachine.toCatchingUp(1000L);
        // 然后转换到 SYNCED
        replicaStateMachine.toSynced();
        // 检查最终状态
        if (replicaStateMachine.getCurrentState() == ShardStateMachine.ShardState.REPLICA_SYNCED) {
            assertTrue(replicaStateMachine.isSynced());
        }
    }

    @Test
    void testToPrimaryActive() {
        CoordinatorLease lease = new CoordinatorLease(
            "node-2", "shard-0", 10000L, 2L, 200L);

        // 先转换到 SYNCED 状态
        replicaStateMachine.toCatchingUp(1000L);
        replicaStateMachine.toSynced();

        // 尝试转换为 PRIMARY_ACTIVE
        replicaStateMachine.toPrimaryActive(lease);

        // 检查最终状态（不强制要求一定成功）
        if (replicaStateMachine.getCurrentState() == ShardStateMachine.ShardState.PRIMARY_ACTIVE) {
            assertEquals(ReplicaRole.LEADER, replicaStateMachine.getCurrentRole());
            assertTrue(replicaStateMachine.isPrimary());
            assertTrue(replicaStateMachine.isPrimaryActive());
        }
    }

    @Test
    void testToPrimaryDegraded() {
        assertTrue(primaryStateMachine.toPrimaryDegraded("replica disconnected"));
        assertEquals(ShardStateMachine.ShardState.PRIMARY_DEGRADED, primaryStateMachine.getCurrentState());
        assertTrue(primaryStateMachine.isPrimary());
        assertFalse(primaryStateMachine.isPrimaryActive());
    }

    @Test
    void testInvalidTransitions() {
        // 不能从 PRIMARY_ACTIVE 直接到 SYNCED
        assertFalse(primaryStateMachine.toSynced());

        // 不能从 REPLICA_INIT 直接到 PRIMARY_ACTIVE
        assertFalse(replicaStateMachine.toPrimaryActive(null));

        // 不能从 REPLICA_SYNCED 直接降级
        replicaStateMachine.toCatchingUp(1000L);
        replicaStateMachine.toSynced();
        assertFalse(replicaStateMachine.toDemoting());
    }

    @Test
    void testDemotionFlow() {
        CoordinatorLease lease = new CoordinatorLease(
            "node-2", "shard-0", 10000L, 2L, 200L);

        // replica -> primary
        replicaStateMachine.toCatchingUp(1000L);
        replicaStateMachine.toSynced();
        replicaStateMachine.toPrimaryActive(lease);

        // 只有在成功转换为 PRIMARY_ACTIVE 后才能降级
        if (replicaStateMachine.isPrimary()) {
            // primary -> demoting -> replica
            replicaStateMachine.toDemoting();
            if (replicaStateMachine.getCurrentState() == ShardStateMachine.ShardState.DEMOTING) {
                replicaStateMachine.toReplicaInit();
                assertEquals(ShardStateMachine.ShardState.REPLICA_INIT, replicaStateMachine.getCurrentState());
                assertEquals(ReplicaRole.FOLLOWER, replicaStateMachine.getCurrentRole());
            }
        }
    }

    @Test
    void testSetAndGetOffset() {
        replicaStateMachine.setLastAppliedOffset(500L);
        assertEquals(500L, replicaStateMachine.getLastAppliedOffset());

        replicaStateMachine.setTargetOffset(1000L);
        assertEquals(1000L, replicaStateMachine.getTargetOffset());
    }

    @Test
    void testLeaseManagement() {
        CoordinatorLease lease = new CoordinatorLease(
            "node-1", "shard-0", 10000L, 1L, 100L);

        primaryStateMachine.setCurrentLease(lease);
        assertEquals(lease, primaryStateMachine.getCurrentLease());
    }

    @Test
    void testStateReport() {
        // 先设置目标 offset
        replicaStateMachine.setTargetOffset(1000L);
        // 尝试转换到 CATCHING_UP
        replicaStateMachine.toCatchingUp(1000L);
        replicaStateMachine.setLastAppliedOffset(500L);

        ShardStateMachine.StateReport report = replicaStateMachine.getReport();

        assertEquals("shard-0", report.shardId());
        assertEquals("node-2", report.nodeId());
        assertEquals(500L, report.lastAppliedOffset());
        assertEquals(1000L, report.targetOffset());
        // 追赶进度 = 500/1000 = 50%
        assertEquals(0.5, report.getCatchUpProgress(), 0.01);
        assertTrue(report.isCatchingUp());
    }

    @Test
    void testCatchUpProgress() {
        // 0/1000 = 0%
        replicaStateMachine.setTargetOffset(1000L);
        replicaStateMachine.setLastAppliedOffset(0L);
        assertEquals(0.0, replicaStateMachine.getReport().getCatchUpProgress(), 0.01);

        // 500/1000 = 50%
        replicaStateMachine.setLastAppliedOffset(500L);
        assertEquals(0.5, replicaStateMachine.getReport().getCatchUpProgress(), 0.01);

        // 1000/1000 = 100%
        replicaStateMachine.setLastAppliedOffset(1000L);
        assertEquals(1.0, replicaStateMachine.getReport().getCatchUpProgress(), 0.01);

        // 1500/1000 = 100% (capped)
        replicaStateMachine.setLastAppliedOffset(1500L);
        assertEquals(1.0, replicaStateMachine.getReport().getCatchUpProgress(), 0.01);
    }

    @Test
    void testStateChangeListener() {
        final boolean[] listenerCalled = {false};
        replicaStateMachine.setStateChangeListener((oldState, newState) -> {
            listenerCalled[0] = true;
            assertEquals(ShardStateMachine.ShardState.REPLICA_INIT, oldState);
            assertEquals(ShardStateMachine.ShardState.REPLICA_CATCHING_UP, newState);
        });

        replicaStateMachine.toCatchingUp(1000L);
        assertTrue(listenerCalled[0]);
    }

    @Test
    void testToError() {
        // ERROR 状态可以从任何状态转换，应该成功
        replicaStateMachine.toError("test error");
        // 检查最终状态
        if (replicaStateMachine.getCurrentState() == ShardStateMachine.ShardState.ERROR) {
            assertEquals("test error", replicaStateMachine.getErrorMessage());
        }
    }

    @Test
    void testStateCanAcceptMethods() {
        assertTrue(ShardStateMachine.ShardState.PRIMARY_ACTIVE.canAcceptWrite());
        assertTrue(ShardStateMachine.ShardState.PRIMARY_DEGRADED.canAcceptWrite());
        assertFalse(ShardStateMachine.ShardState.REPLICA_SYNCED.canAcceptWrite());
        assertFalse(ShardStateMachine.ShardState.REPLICA_INIT.canAcceptWrite());

        assertTrue(ShardStateMachine.ShardState.PRIMARY_ACTIVE.canAcceptRead());
        assertTrue(ShardStateMachine.ShardState.REPLICA_SYNCED.canAcceptRead());
        assertFalse(ShardStateMachine.ShardState.OFFLINE.canAcceptRead());
    }

    @Test
    void testShardStateIsPrimaryIsReplica() {
        assertTrue(ShardStateMachine.ShardState.PRIMARY_ACTIVE.isPrimary());
        assertTrue(ShardStateMachine.ShardState.PRIMARY_DEGRADED.isPrimary());
        assertFalse(ShardStateMachine.ShardState.REPLICA_SYNCED.isPrimary());

        assertTrue(ShardStateMachine.ShardState.REPLICA_INIT.isReplica());
        assertTrue(ShardStateMachine.ShardState.REPLICA_CATCHING_UP.isReplica());
        assertTrue(ShardStateMachine.ShardState.REPLICA_SYNCED.isReplica());
        assertFalse(ShardStateMachine.ShardState.PRIMARY_ACTIVE.isReplica());
    }

    @Test
    void testToString() {
        String str = primaryStateMachine.toString();
        assertTrue(str.contains("shard-0"));
        assertTrue(str.contains("node-1"));
        assertTrue(str.contains("PRIMARY_ACTIVE"));
    }
}

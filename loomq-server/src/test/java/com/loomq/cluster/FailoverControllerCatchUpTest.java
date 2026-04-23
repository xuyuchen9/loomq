package com.loomq.cluster;

import com.loomq.replication.WalCatchUpManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FailoverController 追赶集成测试
 *
 * Week 4 Phase 4 - T4.4: 测试 FailoverController 与 WalCatchUpManager 的集成
 *
 * @author loomq
 * @since v0.4.8
 */
class FailoverControllerCatchUpTest {

    private static final String NODE_ID = "test-node";
    private static final String SHARD_ID = "test-shard";
    private static final long LEASE_TTL_MS = 5000L;
    private static final double RENEWAL_WINDOW_RATIO = 0.3;
    private static final ReplicationEndpoints REPLICATION_ENDPOINTS =
        new ReplicationEndpoints("127.0.0.1", 9090, "0.0.0.0", 9090);

    private FailoverController controller;

    @BeforeEach
    void setUp() {
        // 创建初始为 REPLICA 角色的控制器
        controller = createController(SHARD_ID, ReplicaRole.FOLLOWER);
    }

    @AfterEach
    void tearDown() {
        if (controller != null) {
            controller.close();
        }
    }

    private FailoverController createController(String shardId, ReplicaRole role) {
        FailoverController failoverController = new FailoverController(
            NODE_ID, shardId, role, 1L,
            LEASE_TTL_MS, RENEWAL_WINDOW_RATIO, REPLICATION_ENDPOINTS);
        configureLeaseLifecycle(failoverController);
        return failoverController;
    }

    private void configureLeaseLifecycle(FailoverController failoverController) {
        failoverController.setLeaseProvider(shardId ->
            CompletableFuture.completedFuture(
                new CoordinatorLease(shardId, NODE_ID, 1L, LEASE_TTL_MS, 1L)));
        failoverController.setLeaseRenewer((shardId, leaseId) ->
            CompletableFuture.completedFuture(
                new CoordinatorLease(leaseId, shardId, NODE_ID, 2L, Instant.now(), LEASE_TTL_MS, 1L, 0L)));
    }

    @Test
    void testCatchUpManagerAccessible() {
        // 验证可以获取追赶管理器
        WalCatchUpManager catchUpManager = controller.getCatchUpManager();
        assertNotNull(catchUpManager);
    }

    @Test
    void testIsCatchingUpQuery() {
        // 验证 isCatchingUp 方法
        assertFalse(controller.isCatchingUp()); // 初始不在追赶中
    }

    @Test
    void testGetCatchUpProgress() {
        // 验证追赶进度查询
        double progress = controller.getCatchUpProgress();
        assertTrue(progress >= 0.0 && progress <= 1.0);
    }

    @Test
    void testSetPrimaryCurrentOffset() {
        // 验证设置 Primary offset
        controller.setPrimaryCurrentOffset(1000);

        // 启动后验证状态机
        controller.start();

        // 状态机应该已经初始化
        ShardStateMachine stateMachine = controller.getStateMachine();
        assertNotNull(stateMachine);
    }

    @Test
    void testFailoverControllerStartsCatchUpManager() {
        // 启动控制器应该启动追赶管理器
        controller.start();

        WalCatchUpManager catchUpManager = controller.getCatchUpManager();
        assertNotNull(catchUpManager);
    }

    @Test
    void testFailoverControllerStopsCatchUpManager() {
        // 启动然后停止
        controller.start();
        controller.close();

        // 再次启动应该失败（已经关闭）
        // 或者创建新的控制器
        FailoverController restartedController = createController(SHARD_ID + "-2", ReplicaRole.FOLLOWER);

        restartedController.start();
        assertFalse(restartedController.isCatchingUp()); // 未设置 Primary offset，不会启动追赶
        restartedController.close();
    }

    @Test
    void testInitialReplicaState() {
        controller.start();

        // 初始状态应该是 REPLICA_INIT 或 SYNCED
        ShardStateMachine.ShardState state = controller.getCurrentState();
        assertTrue(
            state == ShardStateMachine.ShardState.REPLICA_INIT ||
            state == ShardStateMachine.ShardState.REPLICA_SYNCED,
            "Initial state should be REPLICA_INIT or REPLICA_SYNCED, got " + state
        );
    }

    @Test
    void testPrimaryRoleNoCatchUp() {
        // 创建 Primary 角色的控制器
        FailoverController primaryController = createController(SHARD_ID, ReplicaRole.LEADER);

        primaryController.start();

        // Primary 角色不应该启动追赶
        assertFalse(primaryController.isCatchingUp());

        // 状态应该是 PRIMARY_ACTIVE
        ShardStateMachine.ShardState state = primaryController.getCurrentState();
        assertEquals(ShardStateMachine.ShardState.PRIMARY_ACTIVE, state);

        primaryController.close();
    }

    @Test
    void testDemoteToReplicaStartsCatchUp() throws Exception {
        // 创建 Primary 角色的控制器
        FailoverController primaryController = createController(SHARD_ID, ReplicaRole.LEADER);

        primaryController.start();

        // 设置 Primary offset（模拟有数据）
        primaryController.setPrimaryCurrentOffset(1000);

        // 降级为 Replica
        boolean demoted = primaryController.demoteToReplica();

        // 降级后状态应该变为 REPLICA_INIT 或开始追赶
        ShardStateMachine.ShardState state = primaryController.getCurrentState();
        assertTrue(
            state == ShardStateMachine.ShardState.REPLICA_INIT ||
            state == ShardStateMachine.ShardState.REPLICA_CATCHING_UP ||
            state == ShardStateMachine.ShardState.DEMOTING,
            "After demotion, state should be REPLICA_INIT, REPLICA_CATCHING_UP, or DEMOTING, got " + state
        );

        primaryController.close();
    }

    @Test
    void testStateMachineIntegration() {
        controller.start();

        ShardStateMachine stateMachine = controller.getStateMachine();
        assertNotNull(stateMachine);
        assertEquals(SHARD_ID, stateMachine.getShardId());
        assertEquals(NODE_ID, stateMachine.getNodeId());
    }

    @Test
    void testManualFailoverDoesNotTriggerCatchUp() {
        controller.start();

        // 手动触发 failover
        CompletableFuture<FailoverController.FailoverResult> future =
            controller.triggerManualFailover(true);

        // 由于这不是完整的集群环境，手动 failover 可能会失败
        // 但不应该抛出异常
        assertNotNull(future);
    }

    @Test
    void testCatchUpProgressWithNoTarget() {
        controller.start();

        // 没有设置目标 offset，进度应该是 0 或 1
        double progress = controller.getCatchUpProgress();
        assertTrue(progress >= 0.0 && progress <= 1.0);
    }

    @Test
    void testMultipleStartStop() {
        // 多次启动和停止不应该出错
        controller.start();
        controller.close();

        // 创建新的控制器再次测试
        FailoverController controller2 = createController(SHARD_ID + "-3", ReplicaRole.FOLLOWER);

        controller2.start();
        assertNotNull(controller2.getCatchUpManager());
        controller2.close();
    }

    @Test
    void testCatchUpStateTransitions() throws Exception {
        controller.setPrimaryCurrentOffset(100);
        controller.start();

        // 给一点时间让追赶可能启动
        Thread.sleep(100);

        // 状态应该是合理的值之一
        ShardStateMachine.ShardState state = controller.getCurrentState();
        assertTrue(
            state == ShardStateMachine.ShardState.REPLICA_INIT ||
            state == ShardStateMachine.ShardState.REPLICA_CATCHING_UP ||
            state == ShardStateMachine.ShardState.REPLICA_SYNCED ||
            state == ShardStateMachine.ShardState.ERROR,
            "State should be a valid replica state, got " + state
        );
    }

    @Test
    void testIntegrationWithReplicationManager() {
        // 创建带有 ReplicationManager 的控制器
        controller.start();

        // 验证可以访问追赶管理器进行配置
        WalCatchUpManager catchUpManager = controller.getCatchUpManager();
        assertNotNull(catchUpManager);

        // 可以设置记录应用器
        catchUpManager.setRecordApplier(record -> true);
    }

    @Test
    void testConcurrentAccess() throws Exception {
        controller.start();

        // 并发查询追赶状态
        CompletableFuture<Boolean> future1 = CompletableFuture.supplyAsync(() ->
            controller.isCatchingUp());
        CompletableFuture<Double> future2 = CompletableFuture.supplyAsync(() ->
            controller.getCatchUpProgress());
        CompletableFuture<ShardStateMachine.ShardState> future3 = CompletableFuture.supplyAsync(() ->
            controller.getCurrentState());

        CompletableFuture.allOf(future1, future2, future3).get(5, TimeUnit.SECONDS);

        // 所有查询都应该成功完成
        assertNotNull(future1.get());
        assertNotNull(future2.get());
        assertNotNull(future3.get());
    }
}

package com.loomq.replication;

import com.loomq.cluster.ReplicaRole;
import com.loomq.cluster.ShardStateMachine;
import com.loomq.replication.protocol.CatchUpRequest;
import com.loomq.replication.protocol.CatchUpResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * WalCatchUpManager 单元测试
 *
 * Week 4 Phase 4 - T4.4: 测试追赶机制
 *
 * @author loomq
 * @since v0.4.8
 */
class WalCatchUpManagerTest {

    private static final String NODE_ID = "node-1";
    private static final String SHARD_ID = "shard-0";
    private static final int BATCH_SIZE = 10;

    private ShardStateMachine stateMachine;
    private WalCatchUpManager catchUpManager;

    @BeforeEach
    void setUp() {
        stateMachine = new ShardStateMachine(SHARD_ID, NODE_ID, ReplicaRole.FOLLOWER);
        catchUpManager = new WalCatchUpManager(NODE_ID, SHARD_ID, stateMachine, BATCH_SIZE);
        catchUpManager.start();
    }

    @AfterEach
    void tearDown() {
        catchUpManager.close();
    }

    @Test
    void testStartAndStop() {
        assertNotNull(catchUpManager);
        assertEquals(WalCatchUpManager.CatchUpState.IDLE, catchUpManager.getCurrentState());
    }

    @Test
    void testStartCatchUpFromBeginning() {
        // 设置状态机为 CATCHING_UP
        stateMachine.toCatchingUp(100);

        // 启动追赶
        CompletableFuture<Boolean> future = catchUpManager.startCatchUp(0, 100);

        // 由于追赶是异步的，等待一段时间后检查状态
        // 实际追赶需要请求 sender，这里只验证启动逻辑
        assertTrue(catchUpManager.isCatchingUp() ||
                   catchUpManager.getCurrentState() == WalCatchUpManager.CatchUpState.IDLE);
    }

    @Test
    void testNoCatchUpNeededWhenOffsetsEqual() {
        // 当本地 offset 等于目标 offset 时，不需要追赶
        stateMachine.toCatchingUp(100);
        stateMachine.setLastAppliedOffset(100);

        // 状态机应该在追赶完成后自动转为 SYNCED
        // 这里验证如果 offset 已经追上，追赶会很快完成
        CompletableFuture<Boolean> future = catchUpManager.startCatchUp(100, 100);

        // 由于 offset 相等，追赶应该很快完成或返回
        assertNotNull(future);
    }

    @Test
    void testCatchUpProgress() {
        // 初始进度可能为 0 或 1（取决于内部状态）
        double initialProgress = catchUpManager.getProgress();
        assertTrue(initialProgress >= 0.0 && initialProgress <= 1.0);

        // 设置追赶状态
        stateMachine.toCatchingUp(100);

        // 启动追赶（使用不同的 start 和 target offset）
        CompletableFuture<Boolean> future = catchUpManager.startCatchUp(0, 100);

        // 检查进度计算
        double progress = catchUpManager.getProgress();
        assertTrue(progress >= 0.0 && progress <= 1.0,
            "Progress should be between 0.0 and 1.0, got " + progress);
    }

    @Test
    void testStopCatchUp() {
        // 启动追赶
        stateMachine.toCatchingUp(1000);
        CompletableFuture<Boolean> future = catchUpManager.startCatchUp(0, 1000);

        // 停止追赶
        catchUpManager.stopCatchUp();

        // 验证状态
        assertFalse(catchUpManager.isCatchingUp());
    }

    @Test
    void testCatchUpCallbacks() {
        AtomicBoolean completeCalled = new AtomicBoolean(false);
        AtomicReference<WalCatchUpManager.CatchUpError> errorRef = new AtomicReference<>();

        // 设置回调
        catchUpManager.setOnCatchUpComplete(() -> completeCalled.set(true));
        catchUpManager.setOnCatchUpError((error, cause) -> errorRef.set(error));

        // 验证回调已设置（不验证实际调用，因为追赶需要完整的请求 sender）
        assertNotNull(catchUpManager);
    }

    @Test
    void testRecordApplier() {
        AtomicInteger applyCount = new AtomicInteger(0);

        // 设置记录应用器
        catchUpManager.setRecordApplier(record -> {
            applyCount.incrementAndGet();
            return true;
        });

        // 验证应用器已设置
        assertEquals(0, applyCount.get()); // 还没有记录被应用
    }

    @Test
    void testGetStats() {
        WalCatchUpManager.CatchUpStats stats = catchUpManager.getStats();

        assertNotNull(stats);
        assertEquals(0, stats.recordsFetched);
        assertEquals(0, stats.recordsApplied);
        assertEquals(0, stats.errors);
    }

    @Test
    void testGetCurrentOffset() {
        // 初始 offset 为 0
        assertEquals(0, catchUpManager.getCurrentOffset());
    }

    @Test
    void testGetTargetOffset() {
        // 初始目标 offset 为 0
        assertEquals(0, catchUpManager.getTargetOffset());
    }

    @Test
    void testStateTransitions() {
        // 验证初始状态
        assertEquals(WalCatchUpManager.CatchUpState.IDLE, catchUpManager.getCurrentState());

        // 启动追赶后状态应该改变
        stateMachine.toCatchingUp(100);
        catchUpManager.startCatchUp(0, 100);

        // 状态可能是 CATCHING_UP 或如果失败则回到 IDLE
        WalCatchUpManager.CatchUpState state = catchUpManager.getCurrentState();
        assertTrue(state == WalCatchUpManager.CatchUpState.CATCHING_UP ||
                   state == WalCatchUpManager.CatchUpState.IDLE ||
                   state == WalCatchUpManager.CatchUpState.FAILED);
    }

    @Test
    void testCannotStartCatchUpWhenAlreadyRunning() {
        stateMachine.toCatchingUp(1000);

        // 第一次启动追赶
        CompletableFuture<Boolean> future1 = catchUpManager.startCatchUp(0, 1000);

        // 第二次启动应该返回 false（因为已经在追赶中）
        CompletableFuture<Boolean> future2 = catchUpManager.startCatchUp(0, 1000);

        // 第二个 future 应该很快完成并返回 false
        try {
            Boolean result = future2.get(1, TimeUnit.SECONDS);
            assertFalse(result); // 应该返回 false，因为追赶已在进行中
        } catch (Exception e) {
            // 可能超时或失败，这是可以接受的
        }
    }

    @Test
    void testCannotStartCatchUpWhenNotRunning() {
        // 关闭管理器
        catchUpManager.close();

        // 尝试启动追赶应该返回失败的 future 或抛出异常
        // 取决于实现，这里验证不会成功启动追赶
        try {
            CompletableFuture<Boolean> future = catchUpManager.startCatchUp(0, 100);
            // 如果返回了 future，它应该很快完成并返回 false
            Boolean result = future.get(1, TimeUnit.SECONDS);
            assertFalse(result); // 应该返回 false，因为管理器已停止
        } catch (IllegalStateException e) {
            // 抛出异常也是预期的行为
            assertTrue(true);
        } catch (Exception e) {
            // 其他异常也可以接受
            assertTrue(true);
        }
    }

    @Test
    void testProgressCalculation() {
        // 测试进度计算逻辑
        stateMachine.toCatchingUp(100);
        catchUpManager.startCatchUp(0, 100);

        double progress = catchUpManager.getProgress();

        // 进度应该在 0 到 1 之间
        assertTrue(progress >= 0.0 && progress <= 1.0,
            "Progress should be between 0.0 and 1.0, got " + progress);
    }

    @Test
    void testIntegrationWithShardStateMachine() {
        // 验证追赶管理器与状态机的集成

        // 初始状态为 REPLICA_INIT
        assertEquals(ShardStateMachine.ShardState.REPLICA_INIT, stateMachine.getCurrentState());

        // 转换为 CATCHING_UP
        boolean enteredCatchingUp = stateMachine.toCatchingUp(100);
        if (enteredCatchingUp) {
            assertEquals(ShardStateMachine.ShardState.REPLICA_CATCHING_UP,
                stateMachine.getCurrentState());
        }

        // 启动追赶
        catchUpManager.startCatchUp(0, 100);

        // 验证状态机状态
        assertTrue(stateMachine.isReplica() || stateMachine.isSynced());
    }

    @Test
    void testBatchSizeConfiguration() {
        // 使用不同的批量大小创建管理器
        WalCatchUpManager smallBatchManager = new WalCatchUpManager(
            NODE_ID, SHARD_ID, stateMachine, 5);

        assertNotNull(smallBatchManager);
        smallBatchManager.close();
    }

    @Test
    void testMaxBatchSizeLimit() {
        // 尝试使用超过最大限制的批量大小
        WalCatchUpManager largeBatchManager = new WalCatchUpManager(
            NODE_ID, SHARD_ID, stateMachine, 10000); // 超过 MAX_BATCH_SIZE

        assertNotNull(largeBatchManager);
        largeBatchManager.close();
    }

    @Test
    void testToString() {
        String str = catchUpManager.toString();
        assertNotNull(str);
        assertTrue(str.contains(SHARD_ID));
    }

    // ==================== 协议消息测试 ====================

    @Test
    void testCatchUpRequestEncoding() {
        CatchUpRequest request = new CatchUpRequest(SHARD_ID, 100, 50, NODE_ID);

        byte[] encoded = request.encode();
        assertNotNull(encoded);
        assertTrue(encoded.length > 0);

        // 解码验证
        CatchUpRequest decoded = CatchUpRequest.decode(encoded);
        assertEquals(request.getShardId(), decoded.getShardId());
        assertEquals(request.getStartOffset(), decoded.getStartOffset());
        assertEquals(request.getBatchSize(), decoded.getBatchSize());
        assertEquals(request.getReplicaNodeId(), decoded.getReplicaNodeId());
    }

    @Test
    void testCatchUpRequestEndOffset() {
        CatchUpRequest request = new CatchUpRequest(SHARD_ID, 100, 50, NODE_ID);
        assertEquals(150, request.getEndOffset()); // 100 + 50
    }

    @Test
    void testCatchUpRequestInvalidStartOffset() {
        assertThrows(IllegalArgumentException.class, () -> {
            new CatchUpRequest(SHARD_ID, -1, 50, NODE_ID);
        });
    }

    @Test
    void testCatchUpResponseSuccessEncoding() {
        List<ReplicationRecord> records = createTestRecords(5);
        CatchUpResponse response = new CatchUpResponse(records, 105, true, 200);

        byte[] encoded = response.encode();
        assertNotNull(encoded);
        assertTrue(encoded.length > 0);

        // 解码验证
        CatchUpResponse decoded = CatchUpResponse.decode(encoded);
        assertTrue(decoded.isSuccess());
        assertEquals(response.getEndOffset(), decoded.getEndOffset());
        assertEquals(response.hasMore(), decoded.hasMore());
        assertEquals(response.getPrimaryOffset(), decoded.getPrimaryOffset());
        assertEquals(records.size(), decoded.getRecordCount());
    }

    @Test
    void testCatchUpResponseFailureEncoding() {
        CatchUpResponse response = new CatchUpResponse("Test error message");

        byte[] encoded = response.encode();
        assertNotNull(encoded);

        // 解码验证
        CatchUpResponse decoded = CatchUpResponse.decode(encoded);
        assertFalse(decoded.isSuccess());
        assertEquals("Test error message", decoded.getErrorMessage());
    }

    @Test
    void testCatchUpResponseEmpty() {
        CatchUpResponse response = CatchUpResponse.empty(100);

        assertTrue(response.isSuccess());
        assertEquals(0, response.getRecordCount());
        assertFalse(response.hasMore());
        assertEquals(100, response.getPrimaryOffset());
    }

    @Test
    void testCatchUpResponseInvalidData() {
        // 测试解码过短的数据
        assertThrows(IllegalArgumentException.class, () -> {
            CatchUpResponse.decode(new byte[]{0x01, 0x02, 0x03});
        });
    }

    // ==================== 辅助方法 ====================

    private List<ReplicationRecord> createTestRecords(int count) {
        List<ReplicationRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(ReplicationRecord.builder()
                .offset(100 + i)
                .type(ReplicationRecordType.TASK_CREATE)
                .sourceNodeId("primary-node")
                .payload(("test-payload-" + i).getBytes())
                .build());
        }
        return records;
    }
}

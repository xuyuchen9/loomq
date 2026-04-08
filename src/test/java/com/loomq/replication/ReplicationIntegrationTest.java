package com.loomq.replication;

import com.loomq.replication.client.ReplicaClient;
import com.loomq.replication.server.ReplicaServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 复制客户端/服务器集成测试
 *
 * 测试 Primary (ReplicaClient) 和 Replica (ReplicaServer) 之间的通信
 *
 * @author loomq
 * @since v0.4.8
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class ReplicationIntegrationTest {

    private static final String BIND_HOST = "127.0.0.1";
    private static final int TEST_PORT = 19090;

    private ReplicaServer server;
    private ReplicaClient client;

    @BeforeEach
    void setUp() {
        // 创建服务器
        server = new ReplicaServer(
            "replica-1",
            BIND_HOST,
            TEST_PORT,
            5000  // 5s heartbeat timeout
        );

        // 创建客户端
        client = new ReplicaClient(
            "primary-1",
            BIND_HOST,
            TEST_PORT,
            5000,  // 5s ack timeout
            3      // max retries
        );
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.shutdown();
        }
        if (server != null) {
            server.shutdown();
        }
    }

    @Test
    void testBasicReplication() throws Exception {
        // 收集收到的记录
        CopyOnWriteArrayList<ReplicationRecord> receivedRecords = new CopyOnWriteArrayList<>();
        server.setRecordHandler(receivedRecords::add);

        // 启动服务器
        CompletableFuture<Void> serverFuture = server.start();

        // 等待服务器启动
        Thread.sleep(500);

        // 连接客户端
        client.connect().get(5, TimeUnit.SECONDS);
        assertTrue(client.isConnected());

        // 发送记录
        ReplicationRecord record = ReplicationRecord.builder()
            .offset(1L)
            .type(ReplicationRecordType.TASK_CREATE)
            .sourceNodeId("primary-1")
            .payload("test data".getBytes())
            .build();

        Ack ack = client.send(record).get(5, TimeUnit.SECONDS);

        // 验证 ACK
        assertNotNull(ack);
        assertEquals(1L, ack.getOffset());
        assertTrue(ack.isSuccess());
        assertEquals(AckStatus.REPLICATED, ack.getStatus());

        // 验证服务器收到记录
        Thread.sleep(200);  // 给一点时间处理
        assertEquals(1, receivedRecords.size());
        assertEquals(1L, receivedRecords.get(0).getOffset());
        assertEquals(ReplicationRecordType.TASK_CREATE, receivedRecords.get(0).getType());

        // 验证客户端状态
        assertEquals(1L, client.getLastReplicatedOffset());
        assertEquals(1L, client.getLastAckedOffset());
        assertEquals(0, client.getReplicationLag());
    }

    @Test
    void testMultipleRecordsReplication() throws Exception {
        CopyOnWriteArrayList<ReplicationRecord> receivedRecords = new CopyOnWriteArrayList<>();
        server.setRecordHandler(receivedRecords::add);

        server.start();
        Thread.sleep(500);
        client.connect().get(5, TimeUnit.SECONDS);

        // 发送 100 条记录
        int count = 100;
        for (int i = 0; i < count; i++) {
            ReplicationRecord record = ReplicationRecord.builder()
                .offset(i + 1)
                .type(ReplicationRecordType.TASK_CREATE)
                .payload(("data-" + i).getBytes())
                .build();

            Ack ack = client.send(record).get(5, TimeUnit.SECONDS);
            assertTrue(ack.isSuccess(), "Failed at offset " + (i + 1));
        }

        // 验证所有记录都已收到
        Thread.sleep(500);
        assertEquals(count, receivedRecords.size());

        // 验证顺序
        for (int i = 0; i < count; i++) {
            assertEquals(i + 1, receivedRecords.get(i).getOffset());
        }

        // 验证客户端状态
        assertEquals(count, client.getLastReplicatedOffset());
        assertEquals(count, client.getLastAckedOffset());
    }

    @Test
    void testReplicationLag() throws Exception {
        CopyOnWriteArrayList<ReplicationRecord> receivedRecords = new CopyOnWriteArrayList<>();
        server.setRecordHandler(record -> {
            // 模拟慢处理：每条记录延迟 100ms
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            receivedRecords.add(record);
        });

        server.start();
        Thread.sleep(500);
        client.connect().get(5, TimeUnit.SECONDS);

        // 快速发送 10 条记录（不等待 ACK）
        for (int i = 0; i < 10; i++) {
            ReplicationRecord record = ReplicationRecord.builder()
                .offset(i + 1)
                .type(ReplicationRecordType.TASK_CREATE)
                .payload("data".getBytes())
                .build();

            // 异步发送，不等待
            client.send(record);
        }

        // 此时应该有复制延迟
        Thread.sleep(50);
        long lag = client.getReplicationLag();
        assertTrue(lag > 0, "Expected replication lag, but got " + lag);

        // 等待所有 ACK
        Thread.sleep(1500);
        assertEquals(10, client.getLastAckedOffset());
        assertEquals(0, client.getReplicationLag());
    }

    @Test
    void testHeartbeatExchange() throws Exception {
        AtomicReference<com.loomq.replication.protocol.HeartbeatMessage> serverReceivedHeartbeat =
            new AtomicReference<>();
        server.setHeartbeatHandler(serverReceivedHeartbeat::set);

        server.start();
        Thread.sleep(500);
        client.connect().get(5, TimeUnit.SECONDS);

        // 等待心跳交换（客户端每 2s 发送一次心跳）
        Thread.sleep(2500);

        // 验证服务器收到了心跳
        assertNotNull(serverReceivedHeartbeat.get());
        assertEquals("primary-1", serverReceivedHeartbeat.get().getNodeId());
        assertEquals("PRIMARY", serverReceivedHeartbeat.get().getRole());

        // 验证客户端收到了心跳响应
        assertNotNull(client.getLastHeartbeat());
        assertEquals("REPLICA", client.getLastHeartbeat().getRole());
    }

    @Test
    void testServerHandlesDifferentRecordTypes() throws Exception {
        CopyOnWriteArrayList<ReplicationRecord> receivedRecords = new CopyOnWriteArrayList<>();
        server.setRecordHandler(receivedRecords::add);

        server.start();
        Thread.sleep(500);
        client.connect().get(5, TimeUnit.SECONDS);

        // 发送不同类型的记录
        ReplicationRecordType[] types = {
            ReplicationRecordType.TASK_CREATE,
            ReplicationRecordType.TASK_CANCEL,
            ReplicationRecordType.STATE_TRANSITION,
            ReplicationRecordType.STATE_RETRY,
            ReplicationRecordType.INDEX_INSERT,
            ReplicationRecordType.INDEX_REMOVE,
            ReplicationRecordType.DLQ_ENTER,
            ReplicationRecordType.CHECKPOINT
        };

        for (int i = 0; i < types.length; i++) {
            ReplicationRecord record = ReplicationRecord.builder()
                .offset(i + 1)
                .type(types[i])
                .payload(new byte[]{(byte) i})
                .build();

            Ack ack = client.send(record).get(5, TimeUnit.SECONDS);
            assertTrue(ack.isSuccess());
        }

        Thread.sleep(200);
        assertEquals(types.length, receivedRecords.size());

        for (int i = 0; i < types.length; i++) {
            assertEquals(types[i], receivedRecords.get(i).getType());
        }
    }

    @Test
    void testServerNotStarted() {
        // 尝试连接未启动的服务器
        assertThrows(Exception.class, () -> {
            client.connect().get(3, TimeUnit.SECONDS);
        });
    }

    @Test
    void testSendWithoutConnection() {
        // 确保客户端未连接
        client.shutdown();

        ReplicationRecord record = ReplicationRecord.builder()
            .offset(1L)
            .type(ReplicationRecordType.TASK_CREATE)
            .build();

        // send() 返回 failed future，异常在 get() 时抛出
        CompletableFuture<Ack> future = client.send(record);
        assertTrue(future.isCompletedExceptionally());

        try {
            future.get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Interrupted");
        }
    }

    @Test
    void testDuplicateShutdown() throws Exception {
        server.start();
        Thread.sleep(500);
        client.connect().get(5, TimeUnit.SECONDS);

        // 第一次关闭
        client.shutdown();
        assertFalse(client.isConnected());

        // 第二次关闭（不应抛出异常）
        assertDoesNotThrow(() -> client.shutdown());

        // 服务器关闭
        server.shutdown();
        // 第二次关闭
        assertDoesNotThrow(() -> server.shutdown());
    }

    @Test
    void testReconnection() throws Exception {
        CopyOnWriteArrayList<ReplicationRecord> receivedRecords = new CopyOnWriteArrayList<>();
        server.setRecordHandler(receivedRecords::add);

        server.start();
        Thread.sleep(500);

        // 第一次连接
        client.connect().get(5, TimeUnit.SECONDS);
        ReplicationRecord record1 = ReplicationRecord.builder()
            .offset(1L)
            .type(ReplicationRecordType.TASK_CREATE)
            .build();
        client.send(record1).get(5, TimeUnit.SECONDS);

        // 断开
        client.disconnect();
        assertFalse(client.isConnected());

        // 重新连接
        client.connect().get(5, TimeUnit.SECONDS);
        assertTrue(client.isConnected());

        ReplicationRecord record2 = ReplicationRecord.builder()
            .offset(2L)
            .type(ReplicationRecordType.TASK_CANCEL)
            .build();
        client.send(record2).get(5, TimeUnit.SECONDS);

        Thread.sleep(200);
        assertEquals(2, receivedRecords.size());
    }
}

package com.loomq.replication;

import com.loomq.replication.client.ReplicaClient;
import com.loomq.replication.server.ReplicaServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 复制客户端/服务器集成测试 (优化版)
 *
 * 优化策略: 减少等待时间 (500ms->200ms, 2500ms->1000ms)
 *
 * @author loomq
 * @since v0.4.8
 */
@Timeout(value = 20, unit = TimeUnit.SECONDS) // 减少总超时: 30 -> 20
class ReplicationIntegrationTest {

    private static final String BIND_HOST = "127.0.0.1";
    private static final int TEST_PORT = 19090;

    private ReplicaServer server;
    private ReplicaClient client;

    @BeforeEach
    void setUp() {
        server = new ReplicaServer("replica-1", BIND_HOST, TEST_PORT, 5000);
        client = new ReplicaClient("primary-1", BIND_HOST, TEST_PORT, 5000, 3);
    }

    @AfterEach
    void tearDown() {
        if (client != null) client.shutdown();
        if (server != null) server.shutdown();
    }

    private void startAndWait() throws InterruptedException {
        server.start();
        Thread.sleep(200); // 优化: 500 -> 200
    }

    @Test
    @DisplayName("基本复制 - 单条记录")
    void testBasicReplication() throws Exception {
        CopyOnWriteArrayList<ReplicationRecord> receivedRecords = new CopyOnWriteArrayList<>();
        server.setRecordHandler(receivedRecords::add);

        startAndWait();
        client.connect().get(3, TimeUnit.SECONDS); // 优化: 5 -> 3
        assertTrue(client.isConnected());

        ReplicationRecord record = ReplicationRecord.builder()
            .offset(1L)
            .type(ReplicationRecordType.TASK_CREATE)
            .sourceNodeId("primary-1")
            .payload("test data".getBytes())
            .build();

        Ack ack = client.send(record).get(3, TimeUnit.SECONDS);

        assertNotNull(ack);
        assertEquals(1L, ack.getOffset());
        assertTrue(ack.isSuccess());
        assertEquals(AckStatus.REPLICATED, ack.getStatus());

        Thread.sleep(100); // 优化: 200 -> 100
        assertEquals(1, receivedRecords.size());
        assertEquals(1L, client.getLastReplicatedOffset());
        assertEquals(1L, client.getLastAckedOffset());
    }

    @Test
    @DisplayName("批量复制 - 50条记录")
    void testMultipleRecordsReplication() throws Exception {
        CopyOnWriteArrayList<ReplicationRecord> receivedRecords = new CopyOnWriteArrayList<>();
        server.setRecordHandler(receivedRecords::add);

        startAndWait();
        client.connect().get(3, TimeUnit.SECONDS);

        int count = 50; // 优化: 100 -> 50
        for (int i = 0; i < count; i++) {
            ReplicationRecord record = ReplicationRecord.builder()
                .offset(i + 1)
                .type(ReplicationRecordType.TASK_CREATE)
                .payload(("data-" + i).getBytes())
                .build();

            Ack ack = client.send(record).get(3, TimeUnit.SECONDS);
            assertTrue(ack.isSuccess(), "Failed at offset " + (i + 1));
        }

        Thread.sleep(200); // 优化: 500 -> 200
        assertEquals(count, receivedRecords.size());

        for (int i = 0; i < count; i++) {
            assertEquals(i + 1, receivedRecords.get(i).getOffset());
        }
    }

    @Test
    @DisplayName("复制延迟 - 慢处理场景")
    void testReplicationLag() throws Exception {
        CopyOnWriteArrayList<ReplicationRecord> receivedRecords = new CopyOnWriteArrayList<>();
        server.setRecordHandler(record -> {
            try {
                Thread.sleep(50); // 优化: 100 -> 50
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            receivedRecords.add(record);
        });

        startAndWait();
        client.connect().get(3, TimeUnit.SECONDS);

        // 发送 10 条记录
        for (int i = 0; i < 10; i++) {
            ReplicationRecord record = ReplicationRecord.builder()
                .offset(i + 1)
                .type(ReplicationRecordType.TASK_CREATE)
                .payload("data".getBytes())
                .build();
            client.send(record);
        }

        Thread.sleep(50);
        long lag = client.getReplicationLag();
        assertTrue(lag > 0, "Expected replication lag, but got " + lag);

        Thread.sleep(800); // 优化: 1500 -> 800
        assertEquals(10, client.getLastAckedOffset());
        assertEquals(0, client.getReplicationLag());
    }

    @Test
    @DisplayName("心跳交换")
    void testHeartbeatExchange() throws Exception {
        AtomicReference<com.loomq.replication.protocol.HeartbeatMessage> serverReceivedHeartbeat =
            new AtomicReference<>();
        server.setHeartbeatHandler(serverReceivedHeartbeat::set);

        startAndWait();
        client.connect().get(3, TimeUnit.SECONDS);

        // 心跳间隔为 2 秒，需要等待至少 2 秒
        Thread.sleep(2500);

        assertNotNull(serverReceivedHeartbeat.get());
        assertEquals("primary-1", serverReceivedHeartbeat.get().getNodeId());
    }

    @Test
    @DisplayName("不同记录类型")
    void testServerHandlesDifferentRecordTypes() throws Exception {
        CopyOnWriteArrayList<ReplicationRecord> receivedRecords = new CopyOnWriteArrayList<>();
        server.setRecordHandler(receivedRecords::add);

        startAndWait();
        client.connect().get(3, TimeUnit.SECONDS);

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

            Ack ack = client.send(record).get(3, TimeUnit.SECONDS);
            assertTrue(ack.isSuccess());
        }

        Thread.sleep(100);
        assertEquals(types.length, receivedRecords.size());
    }

    @Test
    @DisplayName("连接未启动的服务器")
    void testServerNotStarted() {
        assertThrows(Exception.class, () -> {
            client.connect().get(2, TimeUnit.SECONDS);
        });
    }

    @Test
    @DisplayName("未连接时发送")
    void testSendWithoutConnection() {
        client.shutdown();

        ReplicationRecord record = ReplicationRecord.builder()
            .offset(1L)
            .type(ReplicationRecordType.TASK_CREATE)
            .build();

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
    @DisplayName("重复关闭")
    void testDuplicateShutdown() throws Exception {
        startAndWait();
        client.connect().get(3, TimeUnit.SECONDS);

        client.shutdown();
        assertFalse(client.isConnected());

        assertDoesNotThrow(() -> client.shutdown());

        server.shutdown();
        assertDoesNotThrow(() -> server.shutdown());
    }

    @Test
    @DisplayName("重连机制")
    void testReconnection() throws Exception {
        CopyOnWriteArrayList<ReplicationRecord> receivedRecords = new CopyOnWriteArrayList<>();
        server.setRecordHandler(receivedRecords::add);

        startAndWait();

        client.connect().get(3, TimeUnit.SECONDS);
        ReplicationRecord record1 = ReplicationRecord.builder()
            .offset(1L)
            .type(ReplicationRecordType.TASK_CREATE)
            .build();
        client.send(record1).get(3, TimeUnit.SECONDS);

        client.disconnect();
        assertFalse(client.isConnected());

        client.connect().get(3, TimeUnit.SECONDS);
        assertTrue(client.isConnected());

        ReplicationRecord record2 = ReplicationRecord.builder()
            .offset(2L)
            .type(ReplicationRecordType.TASK_CANCEL)
            .build();
        client.send(record2).get(3, TimeUnit.SECONDS);

        Thread.sleep(100);
        assertEquals(2, receivedRecords.size());
    }
}

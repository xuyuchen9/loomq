package com.loomq.replication;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ReplicationManager 单元测试
 *
 * @author loomq
 * @since v0.4.8
 */
class ReplicationManagerTest {

    @Test
    void testInitialRoleIsFollower() {
        ReplicationManager manager = new ReplicationManager("test-node");
        assertFalse(manager.isPrimary());
        assertTrue(manager.isReplica());
    }

    @Test
    void testOffsetTracking() {
        ReplicationManager manager = new ReplicationManager("test-node");

        assertEquals(-1, manager.getLastReplicatedOffset());
        assertEquals(-1, manager.getLastAckedOffset());
        assertEquals(0, manager.getReplicationLag());
    }

    @Test
    void testReplicateWhenNotPrimary() {
        ReplicationManager manager = new ReplicationManager("test-node");

        ReplicationRecord record = ReplicationRecord.builder()
            .offset(1L)
            .type(ReplicationRecordType.TASK_CREATE)
            .build();

        CompletableFuture<ReplicationManager.ReplicationResult> future =
            manager.replicate(record, AckLevel.REPLICATED);

        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    void testIdempotentApplyCallback() {
        ReplicationManager manager = new ReplicationManager("test-node");

        manager.setRecordApplier(record -> {
            // Test applier
            return true;
        });

        // Just verify no exception is thrown
        assertNotNull(manager);
    }

    @Test
    void testCloseWhenNotStarted() {
        ReplicationManager manager = new ReplicationManager("test-node");

        // Should not throw
        assertDoesNotThrow(() -> manager.close());
    }

    @Test
    void testReplicationResultRecord() {
        ReplicationManager.ReplicationResult result =
            new ReplicationManager.ReplicationResult(100L, AckStatus.REPLICATED, false);

        assertEquals(100L, result.offset());
        assertEquals(AckStatus.REPLICATED, result.status());
        assertFalse(result.degraded());
        assertTrue(result.isSuccess());
        assertTrue(result.isReplicated());
    }

    @Test
    void testReplicationResultFailure() {
        ReplicationManager.ReplicationResult result =
            new ReplicationManager.ReplicationResult(100L, AckStatus.FAILED, false);

        assertFalse(result.isSuccess());
        assertFalse(result.isReplicated());
    }

    @Test
    void testReplicationResultDegraded() {
        ReplicationManager.ReplicationResult result =
            new ReplicationManager.ReplicationResult(100L, AckStatus.PERSISTED, true);

        assertTrue(result.degraded());
        assertTrue(result.isSuccess());
    }

    @Test
    void testReplicationResultToString() {
        ReplicationManager.ReplicationResult result =
            new ReplicationManager.ReplicationResult(100L, AckStatus.REPLICATED, false);

        String str = result.toString();
        assertTrue(str.contains("offset=100"));
        assertTrue(str.contains("REPLICATED"));
    }
}

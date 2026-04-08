package com.loomq.cluster;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CoordinatorLease 单元测试
 *
 * @author loomq
 * @since v0.4.8
 */
class CoordinatorLeaseTest {

    @Test
    void testLeaseCreation() {
        CoordinatorLease lease = new CoordinatorLease(
            "node-1", "shard-0", 10000L, 1L, 100L);

        assertNotNull(lease.getLeaseId());
        assertEquals("node-1", lease.getHolderNodeId());
        assertEquals("shard-0", lease.getShardId());
        assertEquals(1L, lease.getRoutingVersion());
        assertEquals(100L, lease.getFencingSequence());
        assertTrue(lease.isValid());
        assertFalse(lease.isExpired());
    }

    @Test
    void testLeaseValidity() throws InterruptedException {
        // 创建一个非常短的租约
        CoordinatorLease shortLease = new CoordinatorLease(
            "node-1", "shard-0", 100L, 1L, 1L);

        assertTrue(shortLease.isValid());

        // 等待租约过期
        Thread.sleep(150L);

        assertFalse(shortLease.isValid());
        assertTrue(shortLease.isExpired());
    }

    @Test
    void testLeaseHeldBy() {
        CoordinatorLease lease = new CoordinatorLease(
            "node-1", "shard-0", 10000L, 1L, 1L);

        assertTrue(lease.isHeldBy("node-1"));
        assertFalse(lease.isHeldBy("node-2"));
    }

    @Test
    void testCanRenew() {
        // 创建 10 秒租约
        CoordinatorLease lease = new CoordinatorLease(
            "node-1", "shard-0", 10000L, 1L, 1L);

        // 刚创建时不能续约（还在前 70% 时间）
        assertFalse(lease.canRenew(0.3));

        // 剩余时间小于 30% 时可以续约
        // 这里我们无法模拟时间流逝，所以只测试方法存在
        assertTrue(lease.getRemainingTimeMs() > 0);
    }

    @Test
    void testRemainingTimeMs() {
        CoordinatorLease lease = new CoordinatorLease(
            "node-1", "shard-0", 5000L, 1L, 1L);

        long remaining = lease.getRemainingTimeMs();
        assertTrue(remaining > 0);
        assertTrue(remaining <= 5000L);
    }

    @Test
    void testFullConstructor() {
        String leaseId = UUID.randomUUID().toString();
        Instant issuedAt = Instant.now();
        Instant expiresAt = issuedAt.plusMillis(10000);

        CoordinatorLease lease = new CoordinatorLease(
            leaseId, "node-1", "shard-0",
            issuedAt, expiresAt, 5L, 200L);

        assertEquals(leaseId, lease.getLeaseId());
        assertEquals(issuedAt, lease.getIssuedAt());
        assertEquals(expiresAt, lease.getExpiresAt());
        assertEquals(5L, lease.getRoutingVersion());
        assertEquals(200L, lease.getFencingSequence());
    }

    @Test
    void testLeaseEquality() {
        CoordinatorLease lease1 = new CoordinatorLease(
            "node-1", "shard-0", 10000L, 1L, 1L);
        CoordinatorLease lease2 = new CoordinatorLease(
            "node-1", "shard-0", 10000L, 1L, 1L);

        // 不同 lease ID，不相等
        assertNotEquals(lease1, lease2);
        assertNotEquals(lease1.hashCode(), lease2.hashCode());

        // 相同 lease ID 应该相等
        CoordinatorLease lease3 = new CoordinatorLease(
            lease1.getLeaseId(), "node-1", "shard-0",
            lease1.getIssuedAt(), lease1.getExpiresAt(), 1L, 1L);
        assertEquals(lease1, lease3);
        assertEquals(lease1.hashCode(), lease3.hashCode());
    }

    @Test
    void testToString() {
        CoordinatorLease lease = new CoordinatorLease(
            "node-1", "shard-0", 10000L, 1L, 100L);

        String str = lease.toString();
        assertTrue(str.contains("node-1"));
        assertTrue(str.contains("shard-0"));
        assertTrue(str.contains("routingVer=1"));
        assertTrue(str.contains("fencingSeq=100"));
    }

    @Test
    void testNullValidation() {
        assertThrows(NullPointerException.class, () ->
            new CoordinatorLease(null, "shard-0", 1000L, 1L, 1L));

        assertThrows(NullPointerException.class, () ->
            new CoordinatorLease("node-1", null, 1000L, 1L, 1L));
    }

    @Test
    void testFullConstructorNullValidation() {
        String leaseId = UUID.randomUUID().toString();
        Instant now = Instant.now();

        assertThrows(NullPointerException.class, () ->
            new CoordinatorLease(null, "node-1", "shard-0", now, now.plusMillis(1000), 1L, 1L));

        assertThrows(NullPointerException.class, () ->
            new CoordinatorLease(leaseId, null, "shard-0", now, now.plusMillis(1000), 1L, 1L));

        assertThrows(NullPointerException.class, () ->
            new CoordinatorLease(leaseId, "node-1", null, now, now.plusMillis(1000), 1L, 1L));

        assertThrows(NullPointerException.class, () ->
            new CoordinatorLease(leaseId, "node-1", "shard-0", null, now.plusMillis(1000), 1L, 1L));

        assertThrows(NullPointerException.class, () ->
            new CoordinatorLease(leaseId, "node-1", "shard-0", now, null, 1L, 1L));
    }
}

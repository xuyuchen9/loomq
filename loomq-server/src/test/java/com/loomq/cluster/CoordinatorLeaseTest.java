package com.loomq.cluster;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CoordinatorLease 单元测试
 *
 * @author loomq
 * @since v0.4.8
 */
class CoordinatorLeaseTest {

    @Test
    void testLeaseCreation() {
        // 构造函数: (shardId, nodeId, epoch, ttlMs, routingVersion)
        CoordinatorLease lease = new CoordinatorLease(
            "shard-0", "node-1", 1L, 10000L, 1L);

        assertNotNull(lease.getLeaseId());
        assertEquals("node-1", lease.getNodeId());
        assertEquals("shard-0", lease.getShardId());
        assertEquals(1L, lease.getRoutingVersion());
        assertEquals(0L, lease.currentFencingToken());
        assertEquals(1L, lease.getEpoch());
        assertTrue(lease.isValid());
        assertFalse(lease.isExpired());
    }

    @Test
    void testLeaseValidity() throws InterruptedException {
        // 创建一个非常短的租约 (10ms TTL)
        CoordinatorLease shortLease = new CoordinatorLease(
            "shard-0", "node-1", 1L, 10L, 1L);

        assertTrue(shortLease.isValid());

        // 等待租约过期
        Thread.sleep(50L);

        assertFalse(shortLease.isValid());
        assertTrue(shortLease.isExpired());
    }

    @Test
    void testLeaseHeldBy() {
        CoordinatorLease lease = new CoordinatorLease(
            "shard-0", "node-1", 1L, 10000L, 1L);

        assertTrue(lease.isHeldBy("node-1"));
        assertFalse(lease.isHeldBy("node-2"));
    }

    @Test
    void testCanRenew() {
        // 创建 10 秒租约
        CoordinatorLease lease = new CoordinatorLease(
            "shard-0", "node-1", 1L, 10000L, 1L);

        // 刚创建时不能续约（还在前 70% 时间）
        assertFalse(lease.canRenew(0.3));

        // 剩余时间大于 0
        assertTrue(lease.getRemainingMs() > 0);
    }

    @Test
    void testRemainingTimeMs() {
        CoordinatorLease lease = new CoordinatorLease(
            "shard-0", "node-1", 1L, 5000L, 1L);

        long remaining = lease.getRemainingMs();
        assertTrue(remaining > 0);
        assertTrue(remaining <= 5000L);
    }

    @Test
    void testFullConstructor() {
        String leaseId = UUID.randomUUID().toString();
        Instant acquiredAt = Instant.now();

        // 完整构造函数: (leaseId, shardId, nodeId, epoch, acquiredAt, ttlMs, routingVersion, initialFencingToken)
        CoordinatorLease lease = new CoordinatorLease(
            leaseId, "shard-0", "node-1", 5L, acquiredAt, 10000L, 1L, 200L);

        assertEquals(leaseId, lease.getLeaseId());
        assertEquals(acquiredAt, lease.getAcquiredAt());
        assertEquals(1L, lease.getRoutingVersion());
        assertEquals(200L, lease.currentFencingToken());
        assertEquals(5L, lease.getEpoch());
    }

    @Test
    void testLeaseEquality() {
        CoordinatorLease lease1 = new CoordinatorLease(
            "shard-0", "node-1", 1L, 10000L, 1L);
        CoordinatorLease lease2 = new CoordinatorLease(
            "shard-0", "node-1", 1L, 10000L, 1L);

        // 不同 lease ID，不相等
        assertNotEquals(lease1, lease2);
        assertNotEquals(lease1.hashCode(), lease2.hashCode());

        // 相同 lease ID 应该相等
        CoordinatorLease lease3 = new CoordinatorLease(
            lease1.getLeaseId(), "shard-0", "node-1", 1L, lease1.getAcquiredAt(), 10000L, 1L, 0L);
        assertEquals(lease1, lease3);
        assertEquals(lease1.hashCode(), lease3.hashCode());
    }

    @Test
    void testToString() {
        CoordinatorLease lease = new CoordinatorLease(
            "shard-0", "node-1", 1L, 10000L, 1L);

        String str = lease.toString();
        assertTrue(str.contains("node-1"));
        assertTrue(str.contains("shard-0"));
        assertTrue(str.contains("epoch=1"));
    }

    @Test
    void testNullValidation() {
        assertThrows(NullPointerException.class, () ->
            new CoordinatorLease(null, "node-1", 1L, 1000L, 1L));

        assertThrows(NullPointerException.class, () ->
            new CoordinatorLease("shard-0", null, 1L, 1000L, 1L));
    }

    @Test
    void testFullConstructorNullValidation() {
        String leaseId = UUID.randomUUID().toString();
        Instant now = Instant.now();

        assertThrows(NullPointerException.class, () ->
            new CoordinatorLease(null, "shard-0", "node-1", 1L, now, 1000L, 1L, 0L));

        assertThrows(NullPointerException.class, () ->
            new CoordinatorLease(leaseId, null, "node-1", 1L, now, 1000L, 1L, 0L));

        assertThrows(NullPointerException.class, () ->
            new CoordinatorLease(leaseId, "shard-0", null, 1L, now, 1000L, 1L, 0L));

        assertThrows(NullPointerException.class, () ->
            new CoordinatorLease(leaseId, "shard-0", "node-1", 1L, null, 1000L, 1L, 0L));
    }

    @Test
    void testFencingTokenIncrement() {
        CoordinatorLease lease = new CoordinatorLease(
            "shard-0", "node-1", 1L, 10000L, 1L);

        assertEquals(0L, lease.currentFencingToken());

        long token1 = lease.nextFencingToken();
        assertEquals(1L, token1);
        assertEquals(1L, lease.currentFencingToken());

        long token2 = lease.nextFencingToken();
        assertEquals(2L, token2);
        assertEquals(2L, lease.currentFencingToken());
    }

    @Test
    void testLeaseRenewal() {
        CoordinatorLease lease = new CoordinatorLease(
            "shard-0", "node-1", 1L, 1000L, 1L);

        String originalLeaseId = lease.getLeaseId();
        long originalToken = lease.currentFencingToken();

        // 续约
        CoordinatorLease renewed = lease.renew(2000L);

        // 续约后保持相同的 leaseId
        assertEquals(originalLeaseId, renewed.getLeaseId());
        // fencing token 保持不变
        assertEquals(originalToken, renewed.currentFencingToken());
        // TTL 更新
        assertEquals(2000L, renewed.getTtlMs());
        // 原租约失效
        assertFalse(lease.isValid());
    }
}

package com.loomq.cluster;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * FencingToken 单元测试
 *
 * @author loomq
 * @since v0.4.8
 */
class FencingTokenTest {

    @Test
    void testTokenCreation() {
        String leaseId = UUID.randomUUID().toString();
        FencingToken token = new FencingToken(leaseId, "node-1");

        assertNotNull(token.getLeaseId());
        assertEquals("node-1", token.getHolderNodeId());
        assertTrue(token.getSequence() > 0);  // 序列号单调递增
        assertNotNull(token.getTimestamp());
    }

    @Test
    void testSequenceMonotonicity() {
        String leaseId = UUID.randomUUID().toString();

        FencingToken token1 = new FencingToken(leaseId, "node-1");
        FencingToken token2 = new FencingToken(leaseId, "node-1");
        FencingToken token3 = new FencingToken(leaseId, "node-1");

        assertTrue(token1.getSequence() < token2.getSequence());
        assertTrue(token2.getSequence() < token3.getSequence());
    }

    @Test
    void testIsValidFor() {
        String leaseId = UUID.randomUUID().toString();

        CoordinatorLease lease = new CoordinatorLease(
            leaseId, "node-1", "shard-0",
            Instant.now(), Instant.now().plusMillis(10000), 1L, 100L);

        FencingToken validToken = new FencingToken(leaseId, "node-1");

        assertTrue(validToken.isValidFor(lease));
    }

    @Test
    void testIsValidForExpiredLease() throws InterruptedException {
        String leaseId = UUID.randomUUID().toString();

        // 创建已过期的租约
        CoordinatorLease expiredLease = new CoordinatorLease(
            leaseId, "node-1", "shard-0",
            Instant.now().minusMillis(200), Instant.now().minusMillis(100), 1L, 100L);

        FencingToken token = new FencingToken(leaseId, "node-1");

        assertFalse(token.isValidFor(expiredLease));
    }

    @Test
    void testIsValidForMismatchedLeaseId() {
        String leaseId1 = UUID.randomUUID().toString();
        String leaseId2 = UUID.randomUUID().toString();

        CoordinatorLease lease = new CoordinatorLease(
            leaseId1, "node-1", "shard-0",
            Instant.now(), Instant.now().plusMillis(10000), 1L, 100L);

        FencingToken token = new FencingToken(leaseId2, "node-1");

        assertFalse(token.isValidFor(lease));
    }

    @Test
    void testIsValidForMismatchedHolder() {
        String leaseId = UUID.randomUUID().toString();

        CoordinatorLease lease = new CoordinatorLease(
            leaseId, "node-1", "shard-0",
            Instant.now(), Instant.now().plusMillis(10000), 1L, 100L);

        FencingToken token = new FencingToken(leaseId, "node-2");

        assertFalse(token.isValidFor(lease));
    }

    @Test
    void testIsValidForNullLease() {
        String leaseId = UUID.randomUUID().toString();
        FencingToken token = new FencingToken(leaseId, "node-1");

        assertFalse(token.isValidFor(null));
    }

    @Test
    void testIsExpired() throws InterruptedException {
        String leaseId = UUID.randomUUID().toString();
        Instant past = Instant.now().minusMillis(200);

        FencingToken oldToken = new FencingToken(1L, leaseId, "node-1", past);

        // 100ms TTL，200ms 前创建的应该过期
        assertTrue(oldToken.isExpired(100));

        // 300ms TTL，不应该过期
        assertFalse(oldToken.isExpired(300));
    }

    @Test
    void testFullConstructor() {
        String leaseId = UUID.randomUUID().toString();
        Instant timestamp = Instant.now();

        FencingToken token = new FencingToken(42L, leaseId, "node-1", timestamp);

        assertEquals(42L, token.getSequence());
        assertEquals(leaseId, token.getLeaseId());
        assertEquals("node-1", token.getHolderNodeId());
        assertEquals(timestamp, token.getTimestamp());
    }

    @Test
    void testTokenEquality() {
        String leaseId = UUID.randomUUID().toString();
        Instant now = Instant.now();

        FencingToken token1 = new FencingToken(1L, leaseId, "node-1", now);
        FencingToken token2 = new FencingToken(1L, leaseId, "node-1", now);
        FencingToken token3 = new FencingToken(2L, leaseId, "node-1", now);

        assertEquals(token1, token2);
        assertEquals(token1.hashCode(), token2.hashCode());
        assertNotEquals(token1, token3);
    }

    @Test
    void testToString() {
        String leaseId = UUID.randomUUID().toString();
        FencingToken token = new FencingToken(123L, leaseId, "node-1", Instant.now());

        String str = token.toString();
        assertTrue(str.contains("seq=123"));
        assertTrue(str.contains("holder=node-1"));
    }

    @Test
    void testNullValidation() {
        assertThrows(NullPointerException.class, () ->
            new FencingToken(null, "node-1"));

        assertThrows(NullPointerException.class, () ->
            new FencingToken(UUID.randomUUID().toString(), null));
    }

    @Test
    void testFullConstructorNullValidation() {
        String leaseId = UUID.randomUUID().toString();
        Instant now = Instant.now();

        assertThrows(NullPointerException.class, () ->
            new FencingToken(1L, null, "node-1", now));

        assertThrows(NullPointerException.class, () ->
            new FencingToken(1L, leaseId, null, now));

        assertThrows(NullPointerException.class, () ->
            new FencingToken(1L, leaseId, "node-1", null));
    }

    // ========== FencingTokenValidator 测试 ==========

    @Test
    void testValidatorSuccess() {
        String leaseId = UUID.randomUUID().toString();
        CoordinatorLease lease = new CoordinatorLease(
            leaseId, "node-1", "shard-0",
            Instant.now(), Instant.now().plusMillis(10000), 1L, 100L);
        FencingToken token = new FencingToken(leaseId, "node-1");

        FencingTokenValidator validator = new FencingTokenValidator(30000L);

        assertDoesNotThrow(() -> validator.validate(token, lease));
    }

    @Test
    void testValidatorNullToken() {
        CoordinatorLease lease = new CoordinatorLease(
            "node-1", "shard-0", 10000L, 1L, 100L);

        FencingTokenValidator validator = new FencingTokenValidator(30000L);

        FencingTokenExpiredException ex = assertThrows(
            FencingTokenExpiredException.class,
            () -> validator.validate(null, lease));
        assertTrue(ex.getMessage().contains("token is null"));
    }

    @Test
    void testValidatorNullLease() {
        String leaseId = UUID.randomUUID().toString();
        FencingToken token = new FencingToken(leaseId, "node-1");

        FencingTokenValidator validator = new FencingTokenValidator(30000L);

        FencingTokenExpiredException ex = assertThrows(
            FencingTokenExpiredException.class,
            () -> validator.validate(token, null));
        assertTrue(ex.getMessage().contains("No active lease"));
    }

    @Test
    void testValidatorExpiredToken() throws InterruptedException {
        String leaseId = UUID.randomUUID().toString();
        CoordinatorLease lease = new CoordinatorLease(
            leaseId, "node-1", "shard-0",
            Instant.now(), Instant.now().plusMillis(10000), 1L, 100L);

        // 创建 200ms 前的 token
        FencingToken oldToken = new FencingToken(
            1L, leaseId, "node-1", Instant.now().minusMillis(200));

        // 100ms TTL
        FencingTokenValidator validator = new FencingTokenValidator(100L);

        FencingTokenExpiredException ex = assertThrows(
            FencingTokenExpiredException.class,
            () -> validator.validate(oldToken, lease));
        assertTrue(ex.getMessage().contains("expired"));
    }
}

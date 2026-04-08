package com.loomq.replication;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Ack 单元测试
 *
 * @author loomq
 * @since v0.4.8
 */
class AckTest {

    @Test
    void testSuccessAck() {
        Ack ack = Ack.success(100L);

        assertEquals(100L, ack.getOffset());
        assertEquals(AckStatus.REPLICATED, ack.getStatus());
        assertTrue(ack.isSuccess());
        assertFalse(ack.isFailure());
        assertNotNull(ack.getTimestamp());
        assertEquals(100L, ack.getReplicaAppliedOffset());
    }

    @Test
    void testPersistedAck() {
        Ack ack = Ack.persisted(50L);

        assertEquals(50L, ack.getOffset());
        assertEquals(AckStatus.PERSISTED, ack.getStatus());
        assertTrue(ack.isSuccess());
    }

    @Test
    void testFailedAck() {
        Ack ack = Ack.failed(100L, "Disk full");

        assertEquals(100L, ack.getOffset());
        assertEquals(AckStatus.FAILED, ack.getStatus());
        assertTrue(ack.isFailure());
        assertFalse(ack.isSuccess());
        assertEquals("Disk full", ack.getErrorMessage());
        assertEquals(-1, ack.getReplicaAppliedOffset());
    }

    @Test
    void testTimeoutAck() {
        Ack ack = Ack.timeout(200L);

        assertEquals(200L, ack.getOffset());
        assertEquals(AckStatus.TIMEOUT, ack.getStatus());
        assertTrue(ack.isFailure());
        assertNotNull(ack.getErrorMessage());
    }

    @Test
    void testRejectedAck() {
        Ack ack = Ack.rejected(300L, "Fencing token expired");

        assertEquals(300L, ack.getOffset());
        assertEquals(AckStatus.REJECTED, ack.getStatus());
        assertTrue(ack.isFailure());
        assertTrue(ack.getErrorMessage().contains("Fencing token expired"));
    }

    @Test
    void testEncodeDecode() {
        Ack original = new Ack(
            12345L,
            AckStatus.REPLICATED,
            Instant.ofEpochMilli(1234567890L),
            null,
            12340L
        );

        byte[] encoded = original.encode();
        Ack decoded = Ack.decode(encoded);

        assertEquals(original.getOffset(), decoded.getOffset());
        assertEquals(original.getStatus(), decoded.getStatus());
        assertEquals(original.getTimestamp(), decoded.getTimestamp());
        assertEquals(original.getReplicaAppliedOffset(), decoded.getReplicaAppliedOffset());
        assertNull(decoded.getErrorMessage());
    }

    @Test
    void testEncodeDecodeWithError() {
        Ack original = new Ack(
            999L,
            AckStatus.FAILED,
            Instant.now(),
            "This is a very long error message that describes what went wrong",
            -1
        );

        byte[] encoded = original.encode();
        Ack decoded = Ack.decode(encoded);

        assertEquals(original.getOffset(), decoded.getOffset());
        assertEquals(original.getStatus(), decoded.getStatus());
        assertEquals(original.getErrorMessage(), decoded.getErrorMessage());
    }

    @Test
    void testInvalidDataLength() {
        assertThrows(IllegalArgumentException.class, () -> {
            Ack.decode(new byte[5]);  // Too short
        });
    }

    @Test
    void testAckStatusSuccess() {
        assertTrue(AckStatus.RECEIVED.isSuccess());
        assertTrue(AckStatus.PERSISTED.isSuccess());
        assertTrue(AckStatus.REPLICATED.isSuccess());
        assertFalse(AckStatus.FAILED.isSuccess());
        assertFalse(AckStatus.TIMEOUT.isSuccess());
        assertFalse(AckStatus.REJECTED.isSuccess());
    }

    @Test
    void testAckStatusFailure() {
        assertFalse(AckStatus.RECEIVED.isFailure());
        assertFalse(AckStatus.PERSISTED.isFailure());
        assertFalse(AckStatus.REPLICATED.isFailure());
        assertTrue(AckStatus.FAILED.isFailure());
        assertTrue(AckStatus.TIMEOUT.isFailure());
        assertTrue(AckStatus.REJECTED.isFailure());
    }

    @Test
    void testAckStatusFromCode() {
        assertEquals(AckStatus.RECEIVED, AckStatus.fromCode((byte) 0x01));
        assertEquals(AckStatus.PERSISTED, AckStatus.fromCode((byte) 0x02));
        assertEquals(AckStatus.REPLICATED, AckStatus.fromCode((byte) 0x03));
        assertEquals(AckStatus.FAILED, AckStatus.fromCode((byte) 0x10));
        assertEquals(AckStatus.TIMEOUT, AckStatus.fromCode((byte) 0x11));
        assertEquals(AckStatus.REJECTED, AckStatus.fromCode((byte) 0x12));

        assertThrows(IllegalArgumentException.class, () -> {
            AckStatus.fromCode((byte) 0x99);
        });
    }

    @Test
    void testEqualsAndHashCode() {
        Ack ack1 = Ack.success(100L);
        Ack ack2 = Ack.success(100L);
        Ack ack3 = Ack.success(200L);
        Ack ack4 = Ack.failed(100L, "error");

        assertEquals(ack1, ack2);
        assertEquals(ack1.hashCode(), ack2.hashCode());

        assertNotEquals(ack1, ack3);
        assertNotEquals(ack1, ack4);
    }

    @Test
    void testToString() {
        Ack ack = Ack.success(123L);
        String str = ack.toString();

        assertTrue(str.contains("offset=123"));
        assertTrue(str.contains("REPLICATED"));
        assertTrue(str.contains("appliedOffset=123"));
    }

    @Test
    void testToStringWithError() {
        Ack ack = Ack.failed(456L, "Something went wrong");
        String str = ack.toString();

        assertTrue(str.contains("offset=456"));
        assertTrue(str.contains("FAILED"));
        assertTrue(str.contains("Something went wrong"));
    }
}

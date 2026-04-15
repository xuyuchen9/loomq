package com.loomq.replication;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AckLevel 单元测试
 *
 * @author loomq
 * @since v0.4.8
 */
class AckLevelTest {

    @Test
    void testEnumValues() {
        assertEquals(3, AckLevel.values().length);
        assertNotNull(AckLevel.ASYNC);
        assertNotNull(AckLevel.DURABLE);
        assertNotNull(AckLevel.REPLICATED);
    }

    @Test
    void testRequiresLocalSync() {
        assertFalse(AckLevel.ASYNC.requiresLocalSync());
        assertTrue(AckLevel.DURABLE.requiresLocalSync());
        assertTrue(AckLevel.REPLICATED.requiresLocalSync());
    }

    @Test
    void testRequiresReplicaAck() {
        assertFalse(AckLevel.ASYNC.requiresReplicaAck());
        assertFalse(AckLevel.DURABLE.requiresReplicaAck());
        assertTrue(AckLevel.REPLICATED.requiresReplicaAck());
    }

    @Test
    void testSlaDescriptions() {
        assertTrue(AckLevel.ASYNC.getSlaDescription().toLowerCase().contains("low latency"));
        assertTrue(AckLevel.DURABLE.getSlaDescription().toLowerCase().contains("host-level"));
        assertTrue(AckLevel.REPLICATED.getSlaDescription().toLowerCase().contains("highest"));
    }

    @Test
    void testFromCode() {
        assertEquals(AckLevel.ASYNC, AckLevel.fromCode((byte) 0x01));
        assertEquals(AckLevel.DURABLE, AckLevel.fromCode((byte) 0x02));
        assertEquals(AckLevel.REPLICATED, AckLevel.fromCode((byte) 0x03));

        assertThrows(IllegalArgumentException.class, () -> {
            AckLevel.fromCode((byte) 0x99);
        });
    }

    @Test
    void testGetCode() {
        assertEquals((byte) 0x01, AckLevel.ASYNC.getCode());
        assertEquals((byte) 0x02, AckLevel.DURABLE.getCode());
        assertEquals((byte) 0x03, AckLevel.REPLICATED.getCode());
    }
}

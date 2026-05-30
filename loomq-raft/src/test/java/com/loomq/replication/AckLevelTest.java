package com.loomq.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import org.junit.jupiter.api.Test;

/**
 * AckMode 单元测试
 *
 * @author loomq
 * @since v0.4.8
 */
class AckModeTest {

    @Test
    void testEnumValues() {
        assertEquals(3, AckMode.values().length);
        assertNotNull(AckMode.ASYNC);
        assertNotNull(AckMode.DURABLE);
        assertNotNull(AckMode.REPLICATED);
    }

    @Test
    void testRequiresLocalSync() {
        assertFalse(AckMode.ASYNC.requiresLocalSync());
        assertTrue(AckMode.DURABLE.requiresLocalSync());
        assertTrue(AckMode.REPLICATED.requiresLocalSync());
    }

    @Test
    void testRequiresReplicaAck() {
        assertFalse(AckMode.ASYNC.requiresReplicaAck());
        assertFalse(AckMode.DURABLE.requiresReplicaAck());
        assertTrue(AckMode.REPLICATED.requiresReplicaAck());
    }

    @Test
    void testSlaDescriptions() {
        assertTrue(AckMode.ASYNC.getSlaDescription().toLowerCase().contains("low latency"));
        assertTrue(AckMode.DURABLE.getSlaDescription().toLowerCase().contains("host-level"));
        assertTrue(AckMode.REPLICATED.getSlaDescription().toLowerCase().contains("highest"));
    }

    @Test
    void testFromCode() {
        assertEquals(AckMode.ASYNC, AckMode.fromCode((byte) 0x01));
        assertEquals(AckMode.DURABLE, AckMode.fromCode((byte) 0x02));
        assertEquals(AckMode.REPLICATED, AckMode.fromCode((byte) 0x03));

        assertThrows(IllegalArgumentException.class, () -> {
            AckMode.fromCode((byte) 0x99);
        });
    }

    @Test
    void testGetCode() {
        assertEquals((byte) 0x01, AckMode.ASYNC.getCode());
        assertEquals((byte) 0x02, AckMode.DURABLE.getCode());
        assertEquals((byte) 0x03, AckMode.REPLICATED.getCode());
    }
}

package com.loomq.replication;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ReplicationRecord 单元测试
 *
 * @author loomq
 * @since v0.4.8
 */
class ReplicationRecordTest {

    @Test
    void testBuilderAndGetters() {
        byte[] payload = "test payload".getBytes();

        ReplicationRecord record = ReplicationRecord.builder()
            .offset(100L)
            .timestamp(Instant.now())
            .type(ReplicationRecordType.TASK_CREATE)
            .sourceNodeId("shard-0-primary")
            .payload(payload)
            .build();

        assertEquals(100L, record.getOffset());
        assertEquals(ReplicationRecordType.TASK_CREATE, record.getType());
        assertEquals("shard-0-primary", record.getSourceNodeId());
        assertArrayEquals(payload, record.getPayload());
        assertTrue(record.getTimestamp() > 0);
        assertNotNull(record.getTimestampInstant());
        assertTrue(record.getChecksum() != 0);
    }

    @Test
    void testEncodeDecode() {
        byte[] payload = "test payload data".getBytes();
        Instant now = Instant.now();

        ReplicationRecord original = ReplicationRecord.builder()
            .offset(12345L)
            .timestamp(now)
            .type(ReplicationRecordType.STATE_TRANSITION)
            .sourceNodeId("node-1")
            .payload(payload)
            .build();

        // 编码
        byte[] encoded = original.encode();
        assertTrue(encoded.length > ReplicationRecord.HEADER_SIZE);

        // 解码
        ReplicationRecord decoded = ReplicationRecord.decode(encoded);

        // 验证
        assertEquals(original.getOffset(), decoded.getOffset());
        assertEquals(original.getType(), decoded.getType());
        assertEquals(original.getSourceNodeId(), decoded.getSourceNodeId());
        assertArrayEquals(original.getPayload(), decoded.getPayload());
        assertEquals(original.getTimestamp(), decoded.getTimestamp());
    }

    @Test
    void testAllRecordTypes() {
        for (ReplicationRecordType type : ReplicationRecordType.values()) {
            ReplicationRecord record = ReplicationRecord.builder()
                .offset(1L)
                .type(type)
                .payload(new byte[]{0x01, 0x02})
                .build();

            byte[] encoded = record.encode();
            ReplicationRecord decoded = ReplicationRecord.decode(encoded);

            assertEquals(type, decoded.getType(),
                "Type " + type + " encode/decode failed");
        }
    }

    @Test
    void testEmptyPayload() {
        ReplicationRecord record = ReplicationRecord.builder()
            .offset(1L)
            .type(ReplicationRecordType.CHECKPOINT)
            .build();

        assertEquals(0, record.getPayload().length);

        byte[] encoded = record.encode();
        ReplicationRecord decoded = ReplicationRecord.decode(encoded);

        assertEquals(0, decoded.getPayload().length);
    }

    @Test
    void testEmptySourceNodeId() {
        ReplicationRecord record = ReplicationRecord.builder()
            .offset(1L)
            .type(ReplicationRecordType.TASK_CREATE)
            .build();

        assertEquals("", record.getSourceNodeId());
    }

    @Test
    void testLargePayload() {
        byte[] largePayload = new byte[1024 * 1024];  // 1MB
        for (int i = 0; i < largePayload.length; i++) {
            largePayload[i] = (byte) (i % 256);
        }

        ReplicationRecord record = ReplicationRecord.builder()
            .offset(999999L)
            .type(ReplicationRecordType.TASK_CREATE)
            .payload(largePayload)
            .build();

        byte[] encoded = record.encode();
        ReplicationRecord decoded = ReplicationRecord.decode(encoded);

        assertArrayEquals(largePayload, decoded.getPayload());
    }

    @Test
    void testInvalidOffset() {
        assertThrows(IllegalArgumentException.class, () -> {
            ReplicationRecord.builder()
                .offset(-1L)
                .type(ReplicationRecordType.TASK_CREATE)
                .build();
        });
    }

    @Test
    void testNullType() {
        assertThrows(IllegalArgumentException.class, () -> {
            ReplicationRecord.builder()
                .offset(1L)
                .build();
        });
    }

    @Test
    void testInvalidMagicNumber() {
        byte[] invalidData = new byte[ReplicationRecord.HEADER_SIZE + 8];
        // Magic number is wrong
        invalidData[0] = 0x00;

        assertThrows(IllegalArgumentException.class, () -> {
            ReplicationRecord.decode(invalidData);
        });
    }

    @Test
    void testDataTooShort() {
        assertThrows(IllegalArgumentException.class, () -> {
            ReplicationRecord.decode(new byte[10]);
        });
    }

    @Test
    void testEqualsAndHashCode() {
        Instant fixedTime = Instant.parse("2026-04-20T00:00:00Z");

        ReplicationRecord record1 = ReplicationRecord.builder()
            .offset(100L)
            .timestamp(fixedTime)
            .type(ReplicationRecordType.TASK_CREATE)
            .payload("data".getBytes())
            .build();

        ReplicationRecord record2 = ReplicationRecord.builder()
            .offset(100L)
            .timestamp(fixedTime)
            .type(ReplicationRecordType.TASK_CREATE)
            .payload("data".getBytes())
            .build();

        assertEquals(record1, record2);
        assertEquals(record1.hashCode(), record2.hashCode());
    }

    @Test
    void testTypeClassification() {
        assertTrue(ReplicationRecordType.TASK_CREATE.isTaskLifecycle());
        assertTrue(ReplicationRecordType.TASK_CANCEL.isTaskLifecycle());
        assertFalse(ReplicationRecordType.STATE_TRANSITION.isTaskLifecycle());

        assertTrue(ReplicationRecordType.STATE_TRANSITION.isStateTransition());
        assertTrue(ReplicationRecordType.STATE_RETRY.isStateTransition());
        assertFalse(ReplicationRecordType.TASK_CREATE.isStateTransition());

        assertTrue(ReplicationRecordType.INDEX_INSERT.isSchedulerIndex());
        assertTrue(ReplicationRecordType.INDEX_REMOVE.isSchedulerIndex());
        assertTrue(ReplicationRecordType.INDEX_UPDATE.isSchedulerIndex());
        assertFalse(ReplicationRecordType.TASK_CREATE.isSchedulerIndex());

        assertTrue(ReplicationRecordType.NODE_PROMOTION.isSystemEvent());
        assertFalse(ReplicationRecordType.TASK_CREATE.isSystemEvent());
    }

    @Test
    void testRecordTypeFromCode() {
        assertEquals(ReplicationRecordType.TASK_CREATE,
            ReplicationRecordType.fromCode((byte) 0x01));
        assertEquals(ReplicationRecordType.CHECKPOINT,
            ReplicationRecordType.fromCode((byte) 0x40));

        assertThrows(IllegalArgumentException.class, () -> {
            ReplicationRecordType.fromCode((byte) 0xFF);
        });
    }

    @Test
    void testGetTotalSize() {
        byte[] payload = "payload".getBytes();
        ReplicationRecord record = ReplicationRecord.builder()
            .offset(1L)
            .type(ReplicationRecordType.TASK_CREATE)
            .sourceNodeId("node-1")
            .payload(payload)
            .build();

        int totalSize = record.getTotalSize();
        byte[] encoded = record.encode();

        assertEquals(encoded.length, totalSize);
    }

    @Test
    void testToString() {
        ReplicationRecord record = ReplicationRecord.builder()
            .offset(123L)
            .type(ReplicationRecordType.TASK_CREATE)
            .payload("test".getBytes())
            .build();

        String str = record.toString();
        assertTrue(str.contains("offset=123"));
        assertTrue(str.contains("TASK_CREATE"));
        assertTrue(str.contains("payloadSize=4"));
    }
}

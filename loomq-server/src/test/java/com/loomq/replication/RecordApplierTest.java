package com.loomq.replication;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class RecordApplierTest {

    private IntentStore intentStore;
    private RecordApplier applier;

    @BeforeEach
    void setUp() {
        intentStore = new ConcurrentIntentStore();
        applier = new RecordApplier(intentStore);
    }

    @Test
    void shouldApplyValidRecord() {
        Intent intent = new Intent("test-apply");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);

        byte[] encoded = IntentBinaryCodec.encode(intent);
        byte[] payload = new byte[1 + encoded.length];
        payload[0] = 0x01;
        System.arraycopy(encoded, 0, payload, 1, encoded.length);

        ReplicationRecord record = ReplicationRecord.builder()
            .offset(0L)
            .type(ReplicationRecordType.INTENT_CREATE)
            .sourceNodeId("test-node")
            .payload(payload)
            .build();

        assertTrue(applier.apply(record));
        Intent stored = intentStore.findById("test-apply");
        assertNotNull(stored);
        assertEquals("test-apply", stored.getIntentId());
    }

    @Test
    void shouldReturnFalseForNullPayload() {
        ReplicationRecord record = ReplicationRecord.builder()
            .offset(0L)
            .type(ReplicationRecordType.INTENT_CREATE)
            .sourceNodeId("test-node")
            .payload(null)
            .build();
        assertFalse(applier.apply(record));
    }

    @Test
    void shouldReturnTrueForDuplicateTerminalIntent() {
        Intent intent = new Intent("test-dup");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        intent.transitionTo(IntentStatus.DELIVERED);
        intent.transitionTo(IntentStatus.ACKED);
        intentStore.save(intent);

        byte[] encoded = IntentBinaryCodec.encode(intent);
        byte[] payload = new byte[1 + encoded.length];
        payload[0] = 0x01;
        System.arraycopy(encoded, 0, payload, 1, encoded.length);

        ReplicationRecord record = ReplicationRecord.builder()
            .offset(0L)
            .type(ReplicationRecordType.INTENT_CREATE)
            .sourceNodeId("test-node")
            .payload(payload)
            .build();

        // Already terminal, should return true (idempotent)
        assertTrue(applier.apply(record));
    }
}

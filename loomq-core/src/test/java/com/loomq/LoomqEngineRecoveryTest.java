package com.loomq;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
class LoomqEngineRecoveryTest {

    @TempDir
    Path tempDir;

    @Test
    void testRecoverIntentFromWalReplay() throws Exception {
        String intentId = "recovery-intent-1";
        Instant executeAt = Instant.ofEpochMilli(System.currentTimeMillis() + 60000);

        try (LoomqEngine engine = LoomqEngine.builder()
            .walDir(tempDir)
            .nodeId("recovery-node")
            .build()) {
            engine.start();

            Intent intent = new Intent(intentId);
            intent.setExecuteAt(executeAt);
            intent.setDeadline(executeAt.plusSeconds(300));

            engine.createIntent(intent, AckMode.DURABLE).join();
        }

        try (LoomqEngine recovered = LoomqEngine.builder()
            .walDir(tempDir)
            .nodeId("recovery-node")
            .build()) {
            recovered.start();

            Intent restored = recovered.getIntent(intentId).orElseThrow();
            assertNotNull(restored);
            assertEquals(intentId, restored.getIntentId());
            assertEquals(IntentStatus.SCHEDULED, restored.getStatus());
            assertEquals(executeAt, restored.getExecuteAt());
            assertTrue(recovered.getStats().pendingCount() >= 1);
        }
    }
}

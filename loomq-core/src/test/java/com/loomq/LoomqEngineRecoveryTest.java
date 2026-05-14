package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag("integration")
class LoomqEngineRecoveryTest {

    @TempDir
    Path tempDir;

    private final DeliveryHandler mockHandler =
        intent -> CompletableFuture.completedFuture(DeliveryResult.DEAD_LETTER);

    @Test
    void testDefaultTier_appliesEngineDefault() throws Exception {
        try (LoomqEngine engine = LoomqEngine.builder()
            .walDir(tempDir)
            .nodeId("default-tier-node")
            .deliveryHandler(mockHandler)
            .defaultTier(PrecisionTier.ULTRA)
            .build()) {
            engine.start();

            // Create intent without explicitly setting tier
            Intent intent = new Intent("default-tier-intent");
            intent.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 60000));
            intent.setDeadline(Instant.ofEpochMilli(System.currentTimeMillis() + 300000));

            engine.createIntent(intent, AckMode.DURABLE).join();

            Intent found = engine.getIntent("default-tier-intent").orElseThrow();
            assertEquals(PrecisionTier.ULTRA, found.getPrecisionTier(),
                "Engine default tier should be applied to intents without explicit tier");
        }
    }

    @Test
    void testDefaultTier_nullMeansUseIntentDefault() throws Exception {
        try (LoomqEngine engine = LoomqEngine.builder()
            .walDir(tempDir)
            .nodeId("no-default-tier-node")
            .deliveryHandler(mockHandler)
            // no .defaultTier() call — should use catalog default (STANDARD)
            .build()) {
            engine.start();

            Intent intent = new Intent("no-default-intent");
            intent.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 60000));
            intent.setDeadline(Instant.ofEpochMilli(System.currentTimeMillis() + 300000));

            engine.createIntent(intent, AckMode.DURABLE).join();

            Intent found = engine.getIntent("no-default-intent").orElseThrow();
            // When defaultTier is null, the intent's own tier (set by constructor, STANDARD) is preserved
            assertEquals(PrecisionTier.STANDARD, found.getPrecisionTier(),
                "Without engine defaultTier, intent should keep its own tier");
        }
    }

    @Test
    void testRecoverIntentFromWalReplay() throws Exception {
        String intentId = "recovery-intent-1";
        Instant executeAt = Instant.ofEpochMilli(System.currentTimeMillis() + 60000);

        try (LoomqEngine engine = LoomqEngine.builder()
            .walDir(tempDir)
            .nodeId("recovery-node")
            .deliveryHandler(mockHandler)
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
            .deliveryHandler(mockHandler)
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

package com.loomq.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.LoomqEngine;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class HealthNarratorTest {

    private static final DeliveryHandler NOOP_HANDLER = intent ->
        CompletableFuture.completedFuture(DeliveryResult.SUCCESS);

    @Nested
    @DisplayName("HealthNarrator 叙事生成")
    class NarrativeGeneration {

        @Test
        @DisplayName("空调度器应生成正常叙事")
        void emptyEngineNarrative() {
            IntentStore store = new ConcurrentIntentStore();
            LoomqEngine engine = LoomqEngine.builder()
                .walDir(java.nio.file.Path.of(System.getProperty("java.io.tmpdir"), "loomq-health-test-" + System.nanoTime()))
                .deliveryHandler(NOOP_HANDLER)
                .intentStore(store)
                .build();
            try {
                engine.start();

                Map<String, Object> health = HealthNarrator.narrate(engine);
                assertNotNull(health);
                assertEquals("UP", health.get("status"));
                assertNotNull(health.get("narrative"));
                assertNotNull(health.get("vitals"));

                String narrative = (String) health.get("narrative");
                assertTrue(narrative.contains("operating normally"));
            } finally {
                try { engine.close(); } catch (Exception ignored) {}
                store.shutdown();
            }
        }

        @Test
        @DisplayName("vitals 应包含 scheduler/delivery/durability 部分")
        void vitalsStructure() {
            IntentStore store = new ConcurrentIntentStore();
            LoomqEngine engine = LoomqEngine.builder()
                .walDir(java.nio.file.Path.of(System.getProperty("java.io.tmpdir"), "loomq-health-test2-" + System.nanoTime()))
                .deliveryHandler(NOOP_HANDLER)
                .intentStore(store)
                .build();
            try {
                engine.start();

                Map<String, Object> health = HealthNarrator.narrate(engine);
                @SuppressWarnings("unchecked")
                Map<String, Object> vitals = (Map<String, Object>) health.get("vitals");

                assertNotNull(vitals.get("scheduler"));
                assertNotNull(vitals.get("delivery"));
                assertNotNull(vitals.get("durability"));

                @SuppressWarnings("unchecked")
                Map<String, Object> scheduler = (Map<String, Object>) vitals.get("scheduler");
                assertTrue(scheduler.containsKey("totalPending"));
                assertTrue(scheduler.containsKey("capacityUsed"));
                assertTrue(scheduler.containsKey("backpressure"));

                @SuppressWarnings("unchecked")
                Map<String, Object> durability = (Map<String, Object>) vitals.get("durability");
                assertEquals("healthy", durability.get("walHealth"));
            } finally {
                try { engine.close(); } catch (Exception ignored) {}
                store.shutdown();
            }
        }

        @Test
        @DisplayName("健康引擎不应有异常且状态为 UP")
        void noAnomaliesWhenHealthy() {
            IntentStore store = new ConcurrentIntentStore();
            LoomqEngine engine = LoomqEngine.builder()
                .walDir(java.nio.file.Path.of(System.getProperty("java.io.tmpdir"), "loomq-health-test4-" + System.nanoTime()))
                .deliveryHandler(NOOP_HANDLER)
                .intentStore(store)
                .build();
            try {
                engine.start();

                Map<String, Object> health = HealthNarrator.narrate(engine);
                Object anomalies = health.get("anomalies");
                if (anomalies != null) {
                    assertTrue(((List<?>) anomalies).isEmpty(),
                        "Anomalies should be empty for healthy engine");
                }
                // Also verify status is UP (not ERROR from exception guard)
                assertEquals("UP", health.get("status"));
            } finally {
                try { engine.close(); } catch (Exception ignored) {}
                store.shutdown();
            }
        }
    }
}

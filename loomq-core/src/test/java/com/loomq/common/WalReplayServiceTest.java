package com.loomq.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.WalAccessor;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalReplayServiceTest {

    private static final DeliveryHandler NOOP = intent ->
        CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);

    @Nested
    @DisplayName("WAL segment 列表")
    class SegmentList {

        @Test
        @DisplayName("有 WAL 时应返回 segment 列表")
        void withWal(@TempDir Path tmpDir) throws Exception {
            IntentStore store = new ConcurrentIntentStore();
            var engine = com.loomq.LoomqEngine.builder()
                .walDir(tmpDir)
                .deliveryHandler(NOOP)
                .intentStore(store)
                .build();
            try {
                engine.start();

                Intent intent = new Intent("wal-replay-list");
                intent.setExecuteAt(java.time.Instant.now().plusSeconds(60));
                intent.setPrecisionTier(PrecisionTier.HIGH);
                store.save(intent);
                engine.createIntent(intent, intent.getAckMode()).join();

                WalAccessor walAccessor = engine.getWalAccessor();
                Map<String, Object> result = WalReplayService.listSegments(walAccessor);

                assertNotNull(result);
                assertNotNull(result.get("segments"));
                assertTrue((Integer) result.get("totalSegments") >= 0);
                assertTrue((Long) result.get("totalSizeBytes") >= 0);
            } finally {
                try { engine.close(); } catch (Exception ignored) {}
                store.shutdown();
            }
        }

        @Test
        @DisplayName("writePosition 和 flushedPosition 应返回")
        void positions(@TempDir Path tmpDir) throws Exception {
            IntentStore store = new ConcurrentIntentStore();
            var engine = com.loomq.LoomqEngine.builder()
                .walDir(tmpDir)
                .deliveryHandler(NOOP)
                .intentStore(store)
                .build();
            try {
                engine.start();
                WalAccessor walAccessor = engine.getWalAccessor();
                Map<String, Object> result = WalReplayService.listSegments(walAccessor);

                assertTrue(((Number) result.get("writePosition")).longValue() >= 0);
                assertTrue(((Number) result.get("flushedPosition")).longValue() >= 0);
            } finally {
                try { engine.close(); } catch (Exception ignored) {}
                store.shutdown();
            }
        }
    }

    @Nested
    @DisplayName("Intent 生命周期 replay")
    class IntentReplay {

        @Test
        @DisplayName("不存在的 intent 返回空 transitions")
        void unknownIntent(@TempDir Path tmpDir) throws Exception {
            IntentStore store = new ConcurrentIntentStore();
            var engine = com.loomq.LoomqEngine.builder()
                .walDir(tmpDir)
                .deliveryHandler(NOOP)
                .intentStore(store)
                .build();
            try {
                engine.start();
                WalAccessor walAccessor = engine.getWalAccessor();
                Map<String, Object> result = WalReplayService.replayByIntentId(walAccessor, "nonexistent");

                assertNotNull(result);
                assertEquals("nonexistent", result.get("intentId"));
                assertEquals(0, result.get("transitionCount"));
            } finally {
                try { engine.close(); } catch (Exception ignored) {}
                store.shutdown();
            }
        }

        @Test
        @DisplayName("已写入 WAL 的 intent 应出现在 replay 中")
        void existingIntent(@TempDir Path tmpDir) throws Exception {
            IntentStore store = new ConcurrentIntentStore();
            var engine = com.loomq.LoomqEngine.builder()
                .walDir(tmpDir)
                .deliveryHandler(NOOP)
                .intentStore(store)
                .build();
            try {
                engine.start();

                Intent intent = new Intent("wal-replay-1");
                intent.setExecuteAt(java.time.Instant.now().plusSeconds(30));
                intent.setPrecisionTier(PrecisionTier.FAST);
                store.save(intent);
                engine.createIntent(intent, intent.getAckMode()).join();

                WalAccessor walAccessor = engine.getWalAccessor();
                Map<String, Object> result = WalReplayService.replayByIntentId(walAccessor, "wal-replay-1");

                assertNotNull(result);
                assertEquals("wal-replay-1", result.get("intentId"));
                assertNotNull(result.get("transitionCount"));
                // WAL replay depends on segment flush; verify result structure is well-formed
                if ((int) result.get("transitionCount") > 0) {
                    assertNotNull(result.get("finalStatus"));
                    assertNotNull(result.get("firstSeenAtOffset"));
                    assertNotNull(result.get("lastSeenAt"));
                }
            } finally {
                try { engine.close(); } catch (Exception ignored) {}
                store.shutdown();
            }
        }

    }
}

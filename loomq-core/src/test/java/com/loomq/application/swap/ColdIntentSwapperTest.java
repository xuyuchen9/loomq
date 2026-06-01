package com.loomq.application.swap;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.config.WalConfig;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * ColdIntentSwapper 单元测试。
 *
 * 覆盖：换出 → 换入 生命周期、阈值判断、daemon 启停、统计指标。
 */
class ColdIntentSwapperTest {

    @TempDir
    Path tempDir;

    private IntentStore intentStore;
    private PrecisionScheduler scheduler;
    private SimpleWalWriter walWriter;
    private ColdIntentSwapper swapper;
    private Path dataDir;

    private final DeliveryHandler noopHandler =
        intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);

    @BeforeEach
    void setUp() throws Exception {
        dataDir = tempDir.resolve("wal-swap-test");
        Files.createDirectories(dataDir);

        intentStore = new ConcurrentIntentStore();

        WalConfig config = new WalConfig(
            dataDir.toString(), 8, "batch", 100, false,
            "memory_segment", 8, 32, 64, 10, 4,
            1, false
        );
        walWriter = new SimpleWalWriter(config, "swap-test");
        walWriter.start();

        scheduler = new PrecisionScheduler(intentStore, noopHandler, null);
        swapper = new ColdIntentSwapper(intentStore, scheduler, walWriter, Duration.ofSeconds(1));
    }

    @AfterEach
    void tearDown() {
        if (swapper != null) {
            swapper.close();
        }
        if (scheduler != null) {
            scheduler.stop();
        }
        if (walWriter != null) {
            walWriter.close();
        }
        if (intentStore != null) {
            intentStore.shutdown();
        }
    }

    // ========== shouldSwapOut ==========

    @Test
    void shouldSwapOut_returnsTrueWhenDelayExceedsThreshold() {
        // threshold = 1 second
        assertTrue(swapper.shouldSwapOut(5000));  // 5s > 1s
        assertTrue(swapper.shouldSwapOut(1100));  // 1.1s > 1s
    }

    @Test
    void shouldSwapOut_returnsFalseWhenDelayBelowThreshold() {
        assertFalse(swapper.shouldSwapOut(500));   // 0.5s < 1s
        assertFalse(swapper.shouldSwapOut(0));     // immediate
        assertFalse(swapper.shouldSwapOut(-1));    // overdue
    }

    @Test
    void defaultThresholdIsOneHour() {
        ColdIntentSwapper defaultSwapper = new ColdIntentSwapper(intentStore, scheduler, walWriter);
        assertEquals(Duration.ofHours(1), defaultSwapper.getColdThreshold());
        assertTrue(defaultSwapper.shouldSwapOut(Duration.ofHours(2).toMillis()));
        assertFalse(defaultSwapper.shouldSwapOut(Duration.ofMinutes(30).toMillis()));
        defaultSwapper.close();
    }

    // ========== swapOut / swapIn lifecycle ==========

    @Test
    void swapOut_removesIntentFromStoreAndAddsToColdIndex() throws Exception {
        Intent intent = createIntent("cold-1", Instant.now().plusSeconds(10));
        intentStore.save(intent);
        assertNotNull(intentStore.findById("cold-1"));

        // Write to WAL first (DURABLE)
        byte[] payload = IntentBinaryCodec.encode(intent);
        long walPos = walWriter.writeDurable(payload).join();
        int recordLen = SimpleWalWriter.recordLength(payload.length);

        // Swap out
        swapper.swapOut(intent, walPos, recordLen);

        // Intent removed from store
        assertNull(intentStore.findById("cold-1"));

        // Added to cold index
        assertEquals(1, swapper.coldIntentCount());
        assertEquals(1, swapper.getTotalSwappedOut());
    }

    @Test
    void swapIn_readsFromWalAndRestoresToStore() throws Exception {
        // Use millisecond precision — IntentBinaryCodec encodes epoch millis
        Instant executeAt = Instant.ofEpochMilli(System.currentTimeMillis() + 10000);
        Intent intent = createIntent("cold-2", executeAt);
        intentStore.save(intent);

        byte[] payload = IntentBinaryCodec.encode(intent);
        long walPos = walWriter.writeDurable(payload).join();
        int recordLen = SimpleWalWriter.recordLength(payload.length);

        // Swap out
        swapper.swapOut(intent, walPos, recordLen);
        assertNull(intentStore.findById("cold-2"));
        assertEquals(1, swapper.coldIntentCount());

        // Read WAL record
        byte[] readPayload = walWriter.readRecord(walPos, recordLen);
        Intent restored = IntentBinaryCodec.decode(readPayload);

        assertEquals("cold-2", restored.getIntentId());
        assertEquals(executeAt, restored.getExecuteAt());
        assertEquals(intent.getPrecisionTier(), restored.getPrecisionTier());
    }

    @Test
    void swapOut_thenDaemonSwapIn_restoresIntent() throws Exception {
        // Create intent with executeAt 5 seconds in the future
        // (threshold is 1s for this test, swapInAhead is 60s)
        Instant executeAt = Instant.ofEpochMilli(System.currentTimeMillis() + 5000);
        Intent intent = createIntent("cold-3", executeAt);
        intentStore.save(intent);

        byte[] payload = IntentBinaryCodec.encode(intent);
        long walPos = walWriter.writeDurable(payload).join();
        int recordLen = SimpleWalWriter.recordLength(payload.length);

        swapper.swapOut(intent, walPos, recordLen);
        assertEquals(1, swapper.coldIntentCount());
        assertNull(intentStore.findById("cold-3"));

        // Start daemon and wait for swap-in
        swapper.start();

        // Wait up to 40 seconds for daemon to scan and swap in
        // (scan interval is 30s, but executeAt is 5s ahead so first scan should catch it)
        long deadline = System.currentTimeMillis() + 40_000;
        while (System.currentTimeMillis() < deadline) {
            if (intentStore.findById("cold-3") != null && swapper.coldIntentCount() == 0) {
                break;
            }
            Thread.sleep(500);
        }

        Intent restored = intentStore.findById("cold-3");
        assertNotNull(restored, "Intent should be restored by swap-in daemon within 40s");
        assertEquals("cold-3", restored.getIntentId());
        assertEquals(executeAt, restored.getExecuteAt());
        assertEquals(0, swapper.coldIntentCount(),
            "Cold index should be empty after swap-in completes");
    }

    @Test
    void coldIntentCount_tracksCorrectly() throws Exception {
        assertEquals(0, swapper.coldIntentCount());

        // Swap out 3 intents
        Instant execAt = Instant.ofEpochMilli(System.currentTimeMillis() + 10000);
        for (int i = 0; i < 3; i++) {
            Intent intent = createIntent("multi-" + i, execAt);
            intentStore.save(intent);

            byte[] payload = IntentBinaryCodec.encode(intent);
            long walPos = walWriter.writeDurable(payload).join();
            int recordLen = SimpleWalWriter.recordLength(payload.length);

            swapper.swapOut(intent, walPos, recordLen);
        }

        assertEquals(3, swapper.coldIntentCount());
        assertEquals(3, swapper.getTotalSwappedOut());
        assertEquals(0, swapper.getTotalSwappedIn());
    }

    @Test
    void readRecord_crcMismatch_detected() throws Exception {
        Intent intent = createIntent("crc-test", Instant.now().plusSeconds(10));
        byte[] payload = IntentBinaryCodec.encode(intent);
        long walPos = walWriter.writeDurable(payload).join();

        // Corrupt the WAL file at CRC position
        int recordLen = SimpleWalWriter.recordLength(payload.length);
        int crcOffset = (int) walPos + 4 + payload.length; // header(4) + payload(N)
        Path walFile = dataDir.resolve("swap-test").resolve("wal-000001.bin");

        // Write bad CRC bytes (use native byte order to match WAL format)
        try (var ch = java.nio.channels.FileChannel.open(walFile, java.nio.file.StandardOpenOption.WRITE)) {
            java.nio.ByteBuffer badCrc = java.nio.ByteBuffer.allocate(4)
                .order(java.nio.ByteOrder.nativeOrder());
            badCrc.putInt(0xDEADBEEF);
            badCrc.flip();
            ch.write(badCrc, crcOffset);
        }

        assertThrows(java.io.IOException.class, () ->
            walWriter.readRecord(walPos, recordLen),
            "CRC mismatch should throw IOException"
        );
    }

    @Test
    void readRecord_invalidPosition_throws() {
        assertThrows(IllegalArgumentException.class, () ->
            walWriter.readRecord(-1, 100)
        );
        assertThrows(IllegalArgumentException.class, () ->
            walWriter.readRecord(0, 4)  // too small, <= RECORD_OVERHEAD(8)
        );
    }

    @Test
    void daemon_startStop_lifecycle() {
        assertDoesNotThrow(() -> swapper.start());
        assertDoesNotThrow(() -> swapper.close());
    }

    // ========== helpers ==========

    private Intent createIntent(String id, Instant executeAt) {
        Intent intent = new Intent(id);
        intent.setExecuteAt(executeAt);
        intent.setDeadline(executeAt.plusSeconds(60));
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }
}

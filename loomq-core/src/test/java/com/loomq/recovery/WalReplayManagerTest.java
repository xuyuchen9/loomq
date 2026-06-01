package com.loomq.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.loomq.config.WalConfig;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.spi.WalAccessor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for WalReplayManager.replay(WalAccessor, long, Consumer) -
 * verifies Fix 3 (RecoveryPipeline WalAccessor downcast removal).
 *
 * WAL replay must work through the WalAccessor SPI without
 * any instanceof/cast to SimpleWalWriter.
 */
class WalReplayManagerTest {

    private Path dataDir;
    private SimpleWalWriter writer;

    private static Intent makeIntent(String id) {
        Intent intent = new Intent(id);
        intent.setPrecisionTier(com.loomq.domain.intent.PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }

    @BeforeEach
    void setUp() throws Exception {
        dataDir = Files.createTempDirectory("wal-replay-");
        WalConfig config = new WalConfig(
            dataDir.toString(), 1, "batch", 100, false, "memory_segment",
            1, 8, 64, 10, 4, 1, false
        );
        writer = new SimpleWalWriter(config, "replay-test");
    }

    @AfterEach
    void tearDown() {
        if (writer != null) writer.close();
    }

    @Test
    void shouldReplayAllRecordsFromWalAccessor() throws Exception {
        writer.start();

        // Write known intents to WAL
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Intent intent = makeIntent("replay-" + i);
            ids.add(intent.getIntentId());
            byte[] encoded = IntentBinaryCodec.encode(intent);
            writer.writeDurable(encoded).join();
        }

        // Replay through WalAccessor interface (no downcast to SimpleWalWriter)
        WalAccessor accessor = writer;
        WalReplayManager replayer = new WalReplayManager();
        List<Intent> replayed = new ArrayList<>();

        int count = replayer.replay(accessor, 0, intent -> {
            assertNotNull(intent, "replayed intent should not be null");
            replayed.add(intent);
        });

        assertEquals(20, count, "should replay all 20 records");
        assertEquals(20, replayed.size());
        for (int i = 0; i < 20; i++) {
            assertEquals("replay-" + i, replayed.get(i).getIntentId(),
                "intent order should match write order");
        }
    }

    @Test
    void shouldReplayFromOffset() throws Exception {
        writer.start();

        // Write first 5 records
        for (int i = 0; i < 5; i++) {
            writer.writeDurable(IntentBinaryCodec.encode(makeIntent("offset-" + i))).join();
        }
        // getWritePosition() is the end offset after 5 records (= start of record 6)
        long midOffset = writer.getWritePosition();

        // Write next 5 records
        for (int i = 5; i < 10; i++) {
            writer.writeDurable(IntentBinaryCodec.encode(makeIntent("offset-" + i))).join();
        }

        // Replay from midOffset — should only see the second half (records 5-9)
        WalReplayManager replayer = new WalReplayManager();
        AtomicInteger count = new AtomicInteger(0);

        replayer.replay((WalAccessor) writer, midOffset, intent -> {
            count.incrementAndGet();
            assertTrue(intent.getIntentId().startsWith("offset-"),
                "replayed offset should match expected pattern");
        });

        assertEquals(5, count.get(), "should replay exactly 5 records after mid offset");
    }

    @Test
    void shouldReturnZeroForNullWalAccessor() {
        WalReplayManager replayer = new WalReplayManager();
        int count = replayer.replay((WalAccessor) null, 0, intent -> fail("should not be called"));
        assertEquals(0, count, "null accessor should return 0");
    }

    @Test
    void shouldReturnZeroForEmptySegments() throws Exception {
        // Writer with no data (never started or written)
        WalReplayManager replayer = new WalReplayManager();
        // start() to initialize at least one segment
        writer.start();

        AtomicInteger replayed = new AtomicInteger(0);
        int count = replayer.replay((WalAccessor) writer, 0, intent -> replayed.incrementAndGet());
        assertEquals(0, count, "empty WAL should replay 0 records");
        assertEquals(0, replayed.get());
    }

    @Test
    void shouldStopReplayAtCrcCorruption() throws Exception {
        writer.start();

        // Write two valid records
        Intent firstIntent = makeIntent("good-one");
        Intent secondIntent = makeIntent("bad-one");
        writer.writeDurable(IntentBinaryCodec.encode(firstIntent)).join();
        writer.writeDurable(IntentBinaryCodec.encode(secondIntent)).join();

        // Corrupt the CRC of the second record in the segment file
        java.util.List<WalAccessor.WalSegment> segs = writer.listSegments();
        assertFalse(segs.isEmpty(), "should have at least one segment");

        WalAccessor.WalSegment seg = segs.get(0);
        long firstRecordSize = 8 + IntentBinaryCodec.encode(firstIntent).length;
        long secondRecordCrcOffset = seg.startOffset() + firstRecordSize + 4
            + IntentBinaryCodec.encode(secondIntent).length;

        try (var ch = java.nio.channels.FileChannel.open(seg.path(),
                java.nio.file.StandardOpenOption.WRITE)) {
            java.nio.ByteBuffer bogusCrc = java.nio.ByteBuffer.allocate(4);
            bogusCrc.putInt(0xDEADBEEF);
            bogusCrc.flip();
            ch.write(bogusCrc, secondRecordCrcOffset - seg.startOffset());
        }

        // Replay: should stop at CRC mismatch, only first record recovered
        WalReplayManager replayer = new WalReplayManager();
        java.util.List<Intent> replayed = new java.util.ArrayList<>();
        int count = replayer.replay((WalAccessor) writer, 0, replayed::add);
        assertEquals(1, count, "should stop replay at CRC corruption, only first record");
        assertEquals("good-one", replayed.get(0).getIntentId());
    }

    // ========== helpers ==========

    @Test
    void shouldSkipMalformedPayloadDuringReplay() throws Exception {
        writer.start();

        // Write a valid record first
        Intent validIntent = makeIntent("valid");
        writer.writeDurable(IntentBinaryCodec.encode(validIntent)).join();

        // Write a record with garbage payload (valid WAL frame, invalid Intent bytes)
        // We use writeDurable with raw bytes that can't decode as Intent
        byte[] garbagePayload = new byte[]{0x01, 0x02, 0x03};
        writer.writeDurable(garbagePayload).join();

        // Write another valid record after garbage
        Intent validIntent2 = makeIntent("valid2");
        writer.writeDurable(IntentBinaryCodec.encode(validIntent2)).join();

        // Replay: WalReplayManager does CRC check, so all 3 have valid CRC.
        // The garbage one will fail IntentBinaryCodec.decode → handled by caller.
        WalReplayManager replayer = new WalReplayManager();
        java.util.concurrent.atomic.AtomicInteger successCount = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.concurrent.atomic.AtomicInteger errorCount = new java.util.concurrent.atomic.AtomicInteger(0);

        int total = replayer.replay((WalAccessor) writer, 0, intent -> {
            try {
                successCount.incrementAndGet();
            } catch (Exception e) {
                errorCount.incrementAndGet();
            }
        });

        // WalReplayManager replays all 3 (CRC valid for all), but garbage payload
        // is caught and skipped by the replay manager's decode try-catch.
        // Only the 2 valid records reach the consumer and are counted as restored.
        assertEquals(2, total, "should count only successfully decoded records");
        assertEquals(2, successCount.get(), "only valid records reach consumer; corrupt payloads are skipped by WalReplayManager");
    }
}

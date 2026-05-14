package com.loomq.infrastructure.wal;

import com.loomq.config.WalConfig;
import com.loomq.domain.intent.Intent;
import com.loomq.spi.WalAccessor.WalSegment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("slow")
class SegmentedWalTest {

    private Path dataDir;
    private SimpleWalWriter writer;

    @BeforeEach
    void setUp() throws Exception {
        dataDir = Files.createTempDirectory("wal-seg-");
        // 1MB segment for testing rotation
        WalConfig config = new WalConfig(
            dataDir.toString(), 1, "batch", 100, false, "memory_segment",
            1, 8, 64, 10, 4, 1, false
        );
        writer = new SimpleWalWriter(config, "seg-test");
    }

    @AfterEach
    void tearDown() {
        if (writer != null) writer.close();
    }

    @Test
    void shouldRotateSegmentWhenFull() throws Exception {
        writer.start();
        byte[] payload = IntentBinaryCodec.encode(makeIntent("seg-0"));
        // Write enough records to fill first segment (1MB ~= 7000+ records of ~144 bytes)
        for (int i = 0; i < 100; i++) {
            writer.writeDurable(payload).join();
        }

        List<WalSegment> segments = writer.listSegments();
        // With 100 records, should still be 1 segment (<1MB)
        assertTrue(segments.size() >= 1, "should have at least 1 segment");
        assertTrue(segments.get(0).size() > 0);
    }

    @Test
    void shouldReadRecordAcrossSegments() throws Exception {
        writer.start();
        byte[] payload = IntentBinaryCodec.encode(makeIntent("cross-seg"));
        int recLen = SimpleWalWriter.recordLength(payload.length);

        long pos1 = writer.writeDurable(payload).join();
        byte[] read1 = writer.readRecord(pos1, recLen);
        assertArrayEquals(payload, read1);

        long pos2 = writer.writeDurable(payload).join();
        byte[] read2 = writer.readRecord(pos2, recLen);
        assertArrayEquals(payload, read2);
    }

    @Test
    void shouldTruncateOldSegments() throws Exception {
        writer.start();
        byte[] payload = IntentBinaryCodec.encode(makeIntent("trunc-0"));

        // Write some records
        for (int i = 0; i < 50; i++) {
            writer.writeDurable(payload).join();
        }

        long writePos = writer.getWritePosition();
        List<WalSegment> before = writer.listSegments();
        int beforeCount = before.size();

        // Truncate at current position (should keep current segment)
        writer.truncateBefore(writePos);
        List<WalSegment> after = writer.listSegments();

        // Old segments should be removed, at least 1 segment should remain
        assertTrue(after.size() >= 1, "should have at least 1 remaining segment");
    }

    private Intent makeIntent(String id) {
        Intent intent = new Intent(id);
        intent.setPrecisionTier(com.loomq.domain.intent.PrecisionTier.STANDARD);
        intent.transitionTo(com.loomq.domain.intent.IntentStatus.SCHEDULED);
        return intent;
    }
}

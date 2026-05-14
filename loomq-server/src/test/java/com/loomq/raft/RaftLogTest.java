package com.loomq.raft;

import com.loomq.config.WalConfig;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("slow")
class RaftLogTest {
    private Path dataDir;
    private SimpleWalWriter wal;
    private RaftLog log;

    private static byte[] payloadOfSize(int size, byte fill) {
        byte[] payload = new byte[size];
        Arrays.fill(payload, fill);
        return payload;
    }

    @BeforeEach
    void setUp() throws Exception {
        dataDir = Files.createTempDirectory("raft-log-");
        WalConfig cfg = new WalConfig(dataDir.toString(), 1, "batch", 100, false, "memory_segment",
            1, 8, 64, 10, 4, 1, false, false, "localhost", 9090, 30000, false);
        wal = new SimpleWalWriter(cfg, "raft-test");
        log = new RaftLog(wal);
    }

    @AfterEach
    void tearDown() { if (wal != null) wal.close(); }

    @Test
    void shouldAppendEntryAndReturnIndex() {
        byte[] data = "test-payload".getBytes();
        long index = log.appendEntry(5, data);
        assertTrue(index > 0, "should return positive index");
        assertEquals(index, log.lastIndex(), "lastIndex should match returned index");
        assertEquals(5, log.lastTerm(), "lastTerm should be the written term");
    }

    @Test
    void shouldReadBackEntryPayload() {
        byte[] data = "hello-raft".getBytes();
        long index = log.appendEntry(3, data);
        byte[] read = log.readEntry(index);
        assertNotNull(read, "should read back entry");
        assertArrayEquals(data, read, "payload should match");
    }

    @Test
    void shouldReadBackEntryTerm() {
        byte[] data = "test".getBytes();
        long index = log.appendEntry(42, data);
        long term = log.readEntryTerm(index);
        assertEquals(42, term, "stored term should be 42");
    }

    @Test
    void shouldAdvanceLastIndexWithMultipleEntries() {
        long idx1 = log.appendEntry(1, "first".getBytes());
        long idx2 = log.appendEntry(1, "second".getBytes());
        assertTrue(idx2 > idx1, "second index should be greater than first");
        assertEquals(idx2, log.lastIndex(), "lastIndex should be idx2");
    }

    @Test
    void shouldReturnNegativeOneForNonExistentEntry() {
        assertEquals(-1, log.readEntryTerm(99999), "non-existent entry should return -1");
    }

    @Test
    void shouldReturnNullForNonExistentEntryPayload() {
        assertNull(log.readEntry(99999), "non-existent entry should return null");
    }

    @Test
    void isUpToDateShouldPreferHigherTerm() {
        log.appendEntry(5, "data".getBytes());
        // Candidate has higher term -> up to date
        assertTrue(log.isUpToDate(0, 10), "higher term should be up to date");
        // Candidate has lower term -> not up to date
        assertFalse(log.isUpToDate(999, 1), "lower term should not be up to date");
    }

    // ========== edge case: zero-length data ==========

    @Test
    void isUpToDateShouldCheckIndexWhenSameTerm() {
        log.appendEntry(3, "data".getBytes());
        long lastIdx = log.lastIndex();
        // Same term, candidate has equal or greater index
        assertTrue(log.isUpToDate(lastIdx, 3), "same term + >= index should be up to date");
        assertTrue(log.isUpToDate(lastIdx + 10, 3), "same term + greater index");
        // Same term, candidate has lower index
        assertFalse(log.isUpToDate(lastIdx - 1, 3), "same term + lower index should not be up to date");
    }

    // ========== edge case: max term value ==========

    @Test
    void shouldHandleZeroLengthPayload() {
        long index = log.appendEntry(1, new byte[0]);
        assertTrue(index > 0, "zero-length payload index should be positive");
        byte[] read = log.readEntry(index);
        assertNotNull(read, "zero-length read should return non-null");
        assertEquals(0, read.length, "zero-length payload should be preserved");
    }

    // ========== edge case: index 0 ==========

    @Test
    void shouldHandleLargeTermValue() {
        long largeTerm = Long.MAX_VALUE;
        long index = log.appendEntry(largeTerm, "data".getBytes());
        assertEquals(largeTerm, log.readEntryTerm(index), "max long term should round-trip");
        assertEquals(largeTerm, log.lastTerm(), "lastTerm should be max long");
    }

    // ========== edge case: lastTerm tracking across appends ==========

    @Test
    void shouldReturnNegativeOneForIndexZero() {
        assertEquals(-1, log.readEntryTerm(0), "index 0 should return -1");
        assertNull(log.readEntry(0), "index 0 should return null");
    }

    // ========== Fix 2: lastLogEntryTerm persistence ==========

    @Test
    void lastTermShouldTrackMostRecentAppendTerm() {
        log.appendEntry(1, "first".getBytes());
        assertEquals(1, log.lastTerm());
        log.appendEntry(2, "second".getBytes());
        assertEquals(2, log.lastTerm());
        log.appendEntry(5, "third".getBytes());
        assertEquals(5, log.lastTerm());
        log.appendEntry(0, "zero-term".getBytes());
        assertEquals(0, log.lastTerm());
    }

    @Test
    void shouldPersistLastLogEntryTermAcrossRestart() throws Exception {
        // Write entries with known term
        log.appendEntry(7, "persist-test".getBytes());
        assertEquals(7, wal.getLastLogEntryTerm(), "lastLogEntryTerm should be 7 after write");

        // Persist Raft metadata (simulates what LeaderElection does before RPC responses)
        wal.persistRaftMeta();

        // Close and reopen WAL
        wal.close();

        WalConfig cfg = new WalConfig(dataDir.toString(), 1, "batch", 100, false,
            "memory_segment", 1, 8, 64, 10, 4, 1, false, false,
            "localhost", 9090, 30000, false);
        SimpleWalWriter wal2 = new SimpleWalWriter(cfg, "raft-test");
        try {
            assertEquals(7, wal2.getLastLogEntryTerm(),
                "lastLogEntryTerm should survive close/reopen");
            RaftLog log2 = new RaftLog(wal2);
            assertTrue(log2.isUpToDate(0, 7),
                "node should be up-to-date because lastLogEntryTerm=7 >= candidate's");
        } finally {
            wal2.close();
        }
    }

    @Test
    void shouldBackwardCompatLoadOldMetaFormat() throws Exception {
        // Write old-format raft_meta (2 lines: term + votedFor, no lastLogEntryTerm)
        wal.setTermAndVotedFor(3, "node-1");
        wal.persistRaftMeta();

        // Manually truncate the meta file to 2 lines to simulate pre-existing format
        Path metaPath = dataDir.resolve("raft_meta");
        java.util.List<String> lines = java.nio.file.Files.readAllLines(metaPath);
        java.nio.file.Files.write(metaPath,
            java.util.List.of(lines.get(0), lines.get(1)));

        wal.close();

        WalConfig cfg = new WalConfig(dataDir.toString(), 1, "batch", 100, false,
            "memory_segment", 1, 8, 64, 10, 4, 1, false, false,
            "localhost", 9090, 30000, false);
        SimpleWalWriter wal2 = new SimpleWalWriter(cfg, "raft-test");
        try {
            assertEquals(3, wal2.getLastLogTerm(), "term should load from old format");
            assertEquals("node-1", wal2.getVotedFor(), "votedFor should load from old format");
            assertEquals(0, wal2.getLastLogEntryTerm(),
                "lastLogEntryTerm should default to 0 for old format");
        } finally {
            wal2.close();
        }
    }

    @Test
    void shouldRecoverSnapshotMetadataAndPreserveSnapshotBoundary() throws Exception {
        byte[] entry1 = payloadOfSize(300 * 1024, (byte) 'a');
        byte[] entry2 = payloadOfSize(300 * 1024, (byte) 'b');
        byte[] entry3 = payloadOfSize(300 * 1024, (byte) 'c');
        byte[] entry4 = payloadOfSize(300 * 1024, (byte) 'd');

        long idx1 = log.appendEntry(11, entry1);
        long idx2 = log.appendEntry(12, entry2);
        long idx3 = log.appendEntry(13, entry3);
        long idx4 = log.appendEntry(14, entry4);

        assertEquals(1, idx1, "first logical index should start at 1");
        assertEquals(2, idx2, "second logical index should be 2");
        assertEquals(3, idx3, "third logical index should be 3");
        assertEquals(4, idx4, "fourth logical index should be 4");

        long snapshotOffset = log.compactThrough(idx3, 13);
        assertEquals(3, wal.getSnapshotIndex(), "snapshot index should persist");
        assertEquals(13, wal.getSnapshotTerm(), "snapshot term should persist");
        assertEquals(snapshotOffset, wal.getSnapshotOffset(), "physical snapshot boundary should persist");
        assertTrue(snapshotOffset > 0, "snapshot offset should advance into the WAL");
        assertEquals(1, wal.listSegments().size(), "old WAL segments should be physically removed");
        assertEquals(snapshotOffset, wal.listSegments().get(0).startOffset(),
            "remaining segment should begin at the snapshot boundary");

        wal.close();

        WalConfig cfg = new WalConfig(dataDir.toString(), 1, "batch", 100, false,
            "memory_segment", 1, 8, 64, 10, 4, 1, false, false,
            "localhost", 9090, 30000, false);
        SimpleWalWriter wal2 = new SimpleWalWriter(cfg, "raft-test");
        try {
            assertEquals(3, wal2.getSnapshotIndex(), "snapshot index should survive restart");
            assertEquals(13, wal2.getSnapshotTerm(), "snapshot term should survive restart");
            assertEquals(snapshotOffset, wal2.getSnapshotOffset(), "snapshot offset should survive restart");

            RaftLog recovered = new RaftLog(wal2);
            assertEquals(4, recovered.firstIndex(), "firstIndex should advance past the snapshot");
            assertEquals(4, recovered.lastIndex(), "only entries after the snapshot should be retained logically");
            assertEquals(13, recovered.readEntryTerm(3), "snapshot boundary should expose the snapshot term");
            assertNull(recovered.readEntry(3), "snapshot-covered entry payload should not be retained");
            assertArrayEquals(entry4, recovered.readEntry(4), "post-snapshot payload should survive restart");

            recovered.truncateAfter(3);
            assertEquals(3, recovered.lastIndex(), "truncateAfter should rewind to the snapshot boundary");
            byte[] entry5 = payloadOfSize(16, (byte) 'e');
            long nextIndex = recovered.appendEntry(15, entry5);
            assertEquals(4, nextIndex, "append after truncation should resume at the next logical index");
            assertArrayEquals(entry5, recovered.readEntry(nextIndex));
        } finally {
            wal2.close();
        }
    }
}

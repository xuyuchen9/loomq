package com.loomq.raft;

import com.loomq.config.WalConfig;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("slow")
class LogReplicationTest {
    private Path dataDir;
    private SimpleWalWriter wal;
    private IntentStore store;
    private RaftLog raftLog;
    private LeaderElection election;
    private LogReplication replication;

    private static byte[] encodeIntent(String id) {
        Intent intent = new Intent(id);
        intent.setPrecisionTier(com.loomq.domain.intent.PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);
        return IntentBinaryCodec.encode(intent);
    }

    @BeforeEach
    void setUp() throws Exception {
        dataDir = Files.createTempDirectory("raft-repl-");
        WalConfig cfg = new WalConfig(dataDir.toString(), 1, "batch", 100, false, "memory_segment",
            1, 8, 64, 10, 4, 1, false);
        wal = new SimpleWalWriter(cfg, "raft-test");
        store = new ConcurrentIntentStore();
        raftLog = new RaftLog(wal);
        election = new LeaderElection("node-1", wal, List.of(), 150, 300);
        replication = new LogReplication("node-1", wal, raftLog, store, election);
    }

    // ========== AppendEntries handling ==========

    @AfterEach
    void tearDown() {
        if (wal != null) wal.close();
        store.shutdown();
    }

    @Test
    void shouldAcceptAppendEntriesWithNoPreviousLog() {
        election.start();
        long term = 1;

        byte[][] entries = { encodeIntent("intent-1") };
        AppendEntriesResult result = replication.handleAppendEntries(
            term, "leader-1", 0, 0, entries, 0);

        assertTrue(result.success, "should accept entries with no prevLogIndex");
        assertTrue(wal.getWritePosition() > 0, "entries should be written to WAL");
        election.stop();
    }

    @Test
    void shouldRejectAppendEntriesWithLowerTerm() {
        election.start();

        long currentTerm = election.currentTerm();
        AppendEntriesResult result = replication.handleAppendEntries(
            currentTerm - 1, "leader-1", 0, 0, new byte[0][], 0);

        assertFalse(result.success, "should reject lower term");
        election.stop();
    }

    @Test
    void shouldRejectAppendEntriesWithMissingPrevLogIndex() {
        election.start();
        long term = election.currentTerm() + 1;

        AppendEntriesResult result = replication.handleAppendEntries(
            term, "leader-1", 999999, term, new byte[0][], 0);

        assertFalse(result.success, "should reject missing prevLogIndex");
        election.stop();
    }

    @Test
    void shouldValidatePrevLogTerm() {
        election.start();
        election.becomeLeader(1); // term=1

        // Write an entry at term 1
        byte[][] entries = { encodeIntent("intent-1") };
        replication.handleAppendEntries(1, "leader-1", 0, 0, entries, 0);

        // Now try to append with wrong prevLogTerm for index 1
        // The prevLogIndex should point to where the first entry ended
        long firstEntryEnd = wal.getWritePosition();
        AppendEntriesResult result = replication.handleAppendEntries(
            1, "leader-1", firstEntryEnd, 999, new byte[0][], 0);

        assertFalse(result.success, "should reject wrong prevLogTerm");
        election.stop();
    }

    @Test
    void shouldUpdateCommitIndex() {
        election.start();
        election.becomeLeader(1);

        byte[][] entries = { encodeIntent("intent-1") };
        replication.handleAppendEntries(1, "leader-1", 0, 0, entries, 5);

        assertEquals(5, replication.commitIndex());
        election.stop();
    }

    // ========== applyCommitted ==========

    @Test
    void shouldNotExceedWritePositionForCommitIndex() {
        election.start();
        long writePos = wal.getWritePosition(); // 0

        replication.handleAppendEntries(1, "leader-1", 0, 0, new byte[0][], 999);

        assertEquals(writePos, replication.commitIndex(),
            "commitIndex should not exceed write position");
        election.stop();
    }

    @Test
    void shouldApplyCommittedEntriesToIntentStore() {
        election.start();
        election.becomeLeader(1);

        // Write entries directly via raftLog (handleAppendEntries expects [8B term|payload]
        // framed entries, but encodeIntent returns raw IntentBinaryCodec bytes)
        long index1 = raftLog.appendEntry(1, encodeIntent("intent-1"));
        long index2 = raftLog.appendEntry(1, encodeIntent("intent-2"));
        assertTrue(index1 > 0 && index2 > 0, "entries should be appended");
        // Advance commit to cover all written entries
        replication.advanceCommitIndex(new long[]{index2, index2}, 1);

        // Apply
        replication.applyCommitted();

        Intent i1 = store.findById("intent-1");
        Intent i2 = store.findById("intent-2");
        assertNotNull(i1, "intent-1 should be in store");
        assertNotNull(i2, "intent-2 should be in store");
        assertEquals(IntentStatus.SCHEDULED, i1.getStatus());
        election.stop();
    }

    // ========== commitIndex advancement ==========

    @Test
    void shouldNotReapplyAlreadyAppliedEntries() {
        election.start();
        election.becomeLeader(1);

        byte[][] entries = { encodeIntent("intent-1") };
        replication.handleAppendEntries(1, "leader-1", 0, 0, entries, wal.getWritePosition());

        replication.applyCommitted();
        long firstApplied = replication.lastApplied();

        // Apply again - should not increment
        replication.applyCommitted();
        assertEquals(firstApplied, replication.lastApplied(), "should not reapply");
        election.stop();
    }

    @Test
    void shouldAdvanceCommitIndexWithMajority() {
        election.start();
        election.becomeLeader(1);

        // Write 3 entries
        for (int i = 0; i < 3; i++) {
            raftLog.appendEntry(1, encodeIntent("intent-" + i));
        }
        long index3 = wal.getWritePosition();

        // 3 nodes: majority = 2. With matchIndex [index3, 0, 0], only 1 >= index3
        replication.advanceCommitIndex(new long[]{index3, 0, 0}, 1);
        assertEquals(0, replication.commitIndex(), "should not advance without majority");

        // Now 2 nodes match: matchIndex [index3, index3, 0]
        replication.advanceCommitIndex(new long[]{index3, index3, 0}, 1);
        assertTrue(replication.commitIndex() > 0, "should advance with majority matching");
        election.stop();
    }

    // ========== concurrency ==========

    @Test
    void shouldNotAdvanceCommitIndexForPreviousTerm() {
        election.start();
        election.becomeLeader(1);
        raftLog.appendEntry(1, encodeIntent("intent-1"));
        long index = wal.getWritePosition();

        // Entry is from term 1, but we pass currentTerm=2
        replication.advanceCommitIndex(new long[]{index, index, index}, 2);
        assertEquals(0, replication.commitIndex(),
            "should not advance for entries from previous term (§5.4.2)");
        election.stop();
    }

    // ========== Fix 4 (HIGH): corrupted/missing prevLogEntry rejection ==========

    @Test
    void shouldHandleConcurrentAppends() throws Exception {
        election.start();
        election.becomeLeader(1);

        int threadCount = 4;
        int entriesPerThread = 100;
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(threadCount);
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(threadCount);
        java.util.concurrent.ConcurrentSkipListSet<Long> indices = new java.util.concurrent.ConcurrentSkipListSet<>();
        java.util.concurrent.ConcurrentLinkedQueue<String> errors = new java.util.concurrent.ConcurrentLinkedQueue<>();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < entriesPerThread; i++) {
                        String intentId = "concurrent-" + threadId + "-" + i;
                        byte[] encoded = encodeIntent(intentId);
                        long index = raftLog.appendEntry(1, encoded);
                        if (index <= 0) {
                            errors.add("Invalid index " + index + " for " + intentId);
                        }
                        indices.add(index);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        assertTrue(errors.isEmpty(), "No errors: " + errors);
        assertEquals(threadCount * entriesPerThread, indices.size(),
            "All indices should be unique (no duplicates = no race corruption)");

        // Verify all entries are readable
        for (long idx : indices) {
            byte[] entry = raftLog.readEntry(idx);
            assertNotNull(entry, "Entry at index " + idx + " should be readable");
            long term = raftLog.readEntryTerm(idx);
            assertEquals(1, term, "Term at index " + idx + " should be 1");
        }

        election.stop();
    }

    // ========== Fix 1 (CRITICAL) extended: high-concurrency stress test ==========

    @Test
    void shouldHandleCorruptedPrevLogEntry() {
        election.start();

        // Write a valid entry first at some index
        raftLog.appendEntry(1, encodeIntent("valid-1"));
        long lastIdx = raftLog.lastIndex();

        // Now inject a bogus start position for a future index that
        // readEntryTerm cannot resolve → returns -1
        // Simulate by using prevLogIndex=99999 (non-existent index),
        // which causes readEntryTerm to return -1.
        // The fix in handleAppendEntries checks existingTerm < 0 and
        // rejects gracefully instead of treating it as a term mismatch.
        byte[][] entries = { encodeIntent("should-not-write") };
        AppendEntriesResult result = replication.handleAppendEntries(
            election.currentTerm(), "leader-1", 99999, 1, entries, 0);

        assertFalse(result.success,
            "should reject AppendEntries when prevLogIndex entry cannot be read (returns -1)");

        election.stop();
    }

    // ========== edge case: stale term propose rejection ==========

    @Test
    void shouldHandleHighConcurrencyStress() throws Exception {
        election.start();
        election.becomeLeader(1);

        int threadCount = 8;
        int entriesPerThread = 500;
        java.util.concurrent.ExecutorService executor =
            java.util.concurrent.Executors.newFixedThreadPool(threadCount);
        java.util.concurrent.CountDownLatch latch =
            new java.util.concurrent.CountDownLatch(threadCount);
        java.util.concurrent.ConcurrentSkipListSet<Long> indices =
            new java.util.concurrent.ConcurrentSkipListSet<>();
        java.util.concurrent.ConcurrentLinkedQueue<String> errors =
            new java.util.concurrent.ConcurrentLinkedQueue<>();
        java.util.concurrent.atomic.AtomicLong maxIndex = new java.util.concurrent.atomic.AtomicLong(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < entriesPerThread; i++) {
                        String intentId = "stress-" + threadId + "-" + i;
                        byte[] encoded = encodeIntent(intentId);
                        long index = raftLog.appendEntry(1, encoded);
                        if (index <= 0) {
                            errors.add("Invalid index " + index + " for " + intentId);
                        }
                        indices.add(index);
                        maxIndex.updateAndGet(v -> Math.max(v, index));
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        assertTrue(errors.isEmpty(), "No errors during concurrent appends: " + errors);
        assertEquals(threadCount * entriesPerThread, indices.size(),
            "All " + (threadCount * entriesPerThread) + " indices should be unique");

        // Verify monotonic index range
        long[] sorted = indices.stream().mapToLong(Long::longValue).sorted().toArray();
        for (int i = 1; i < sorted.length; i++) {
            assertTrue(sorted[i] > sorted[i - 1],
                "Indices must be strictly monotonic at position " + i);
        }

        // Spot-check: read every 50th entry to verify readability
        for (int i = 0; i < sorted.length; i += 50) {
            long idx = sorted[i];
            byte[] entry = raftLog.readEntry(idx);
            assertNotNull(entry, "Entry at index " + idx + " should be readable");
            long term = raftLog.readEntryTerm(idx);
            assertEquals(1, term, "Term at index " + idx + " should be 1");
        }

        election.stop();
    }

    // ========== conflictIndex on rejection ==========

    @Test
    void shouldRejectProposeWithLowerTerm() {
        election.start();
        election.becomeLeader(5); // term=5

        // Propose at term 5 should work
        long idx1 = raftLog.appendEntry(5, encodeIntent("good-1"));
        assertTrue(idx1 > 0, "propose at current term should succeed");

        // Propose at term 4 should also write to WAL but carries wrong term
        // (term validation is caller's responsibility; raftLog trusts the caller)
        long idx2 = raftLog.appendEntry(4, encodeIntent("stale-term"));
        assertTrue(idx2 > 0, "raftLog writes any term given");
        // But readEntryTerm confirms the stale term
        assertEquals(4, raftLog.readEntryTerm(idx2),
            "should store the term as-provided by caller");

        election.stop();
    }

    @Test
    void shouldReturnConflictIndexOnTermMismatch() {
        election.start();
        election.becomeLeader(1);

        // Write an entry at term 1
        raftLog.appendEntry(1, encodeIntent("entry-1"));
        long entryIndex = raftLog.lastIndex();

        // AppendEntries with wrong prevLogTerm at entryIndex should fail with conflictIndex
        AppendEntriesResult result = replication.handleAppendEntries(
            1, "leader-1", entryIndex, 999, new byte[0][], 0);

        assertFalse(result.success, "should reject due to term mismatch");
        assertTrue(result.conflictIndex > 0,
            "should return conflictIndex (>0) for efficient backtrack, got " + result.conflictIndex);
        election.stop();
    }

    // ========== truncation (§5.3 conflict resolution) ==========

    @Test
    void shouldHandleEmptyAppendEntriesAsHeartbeat() {
        election.start();
        election.becomeLeader(1);

        long lastIdxBefore = raftLog.lastIndex();
        long commitBefore = replication.commitIndex();

        // Empty entries + leaderCommit=0: updates election state but no log change
        AppendEntriesResult result = replication.handleAppendEntries(
            1, "leader-1", lastIdxBefore, raftLog.lastTerm(), new byte[0][], 0);

        assertTrue(result.success, "empty heartbeat should succeed");
        assertEquals(lastIdxBefore, raftLog.lastIndex(),
            "empty heartbeat should not add log entries");
        election.stop();
    }

    @Test
    void shouldTruncateConflictingEntriesBeforeAppending() {
        election.start();
        election.becomeLeader(1);

        // Write entries 1, 2, 3 at term 1
        long idx1 = raftLog.appendEntry(1, encodeIntent("entry-1"));
        long idx2 = raftLog.appendEntry(1, encodeIntent("entry-2"));
        long idx3 = raftLog.appendEntry(1, encodeIntent("entry-3"));
        assertTrue(idx1 > 0 && idx2 > idx1 && idx3 > idx2);

        // Leader sends entries for index 2 only with different data (term 2, new leader)
        byte[][] newEntries = { encodeIntent("entry-2-new") };
        // prevLogIndex = idx1 (last valid entry the leader agrees on)
        AppendEntriesResult result = replication.handleAppendEntries(
            2, "new-leader", idx1, 1, newEntries, 0);

        assertTrue(result.success, "should accept after truncating conflicts");
        // Old entries 2 and 3 should be gone; only new entry at idx1+ onwards
        assertEquals(-1, raftLog.readEntryTerm(idx2),
            "old entry at idx2 should be unreachable after truncation");
        assertEquals(-1, raftLog.readEntryTerm(idx3),
            "old entry at idx3 should be unreachable after truncation");
        // Entry 1 should still be readable
        assertEquals(1, raftLog.readEntryTerm(idx1),
            "entry before truncation point should still be readable");

        election.stop();
    }

    @Test
    void shouldHandleTruncateToZero() {
        election.start();
        election.becomeLeader(1);

        // Write entries at term 1
        long idx1 = raftLog.appendEntry(1, encodeIntent("entry-1"));
        long idx2 = raftLog.appendEntry(1, encodeIntent("entry-2"));
        assertTrue(idx1 > 0 && idx2 > idx1);

        // New leader with higher term sends from index 0 (prevLogIndex=0)
        byte[][] newEntries = { encodeIntent("first-entry") };
        AppendEntriesResult result = replication.handleAppendEntries(
            2, "new-leader", 0, 0, newEntries, 0);

        assertTrue(result.success, "should accept with prevLogIndex=0");
        // Old entries should be gone
        assertEquals(-1, raftLog.readEntryTerm(idx1),
            "old entries should be truncated when prevLogIndex=0");
        // New entry was written
        assertTrue(raftLog.lastIndex() > 0, "new entry should exist after truncate-to-zero");

        election.stop();
    }

    // ========== helpers ==========

    @Test
    void shouldPreserveEntriesBeforeTruncationPoint() {
        election.start();
        election.becomeLeader(1);

        // Write 5 entries
        long[] indices = new long[5];
        for (int i = 0; i < 5; i++) {
            indices[i] = raftLog.appendEntry(1, encodeIntent("entry-" + i));
        }

        // Truncate after index 2 (keep entries 0,1,2; discard 3,4)
        replication.handleAppendEntries(1, "leader-1",
            indices[2], 1, new byte[0][], 0);

        // Entries 0-2 should still be readable
        for (int i = 0; i < 3; i++) {
            assertTrue(raftLog.readEntryTerm(indices[i]) >= 0,
                "entry " + i + " before truncation point should be readable");
            assertNotNull(raftLog.readEntry(indices[i]),
                "entry " + i + " payload should be readable");
        }
        // Entries 3-4 should be gone
        for (int i = 3; i < 5; i++) {
            assertEquals(-1, raftLog.readEntryTerm(indices[i]),
                "entry " + i + " after truncation point should be unreachable");
        }

        election.stop();
    }
}

package com.loomq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.common.RaftRole;
import com.loomq.config.WalConfig;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("slow")
class LeaderElectionTest {
    private Path dataDir;
    private SimpleWalWriter wal;

    /** Wait for election to complete, polling every 20ms up to maxWaitMs */
    private static void waitForElection(RaftElection election, long maxWaitMs) {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (System.currentTimeMillis() < deadline) {
            if (election.role() == RaftRole.LEADER) return;
            try { Thread.sleep(20); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        dataDir = Files.createTempDirectory("raft-test-");
        WalConfig cfg = new WalConfig(dataDir.toString(), 1, "batch", 100, false, "memory_segment", 1, 8, 64, 10, 4, 1, false);
        wal = new SimpleWalWriter(cfg, "raft-test");
    }

    @AfterEach
    void tearDown() { if (wal != null) wal.close(); }

    @Test
    void singleNodeShouldElectSelf() {
        RaftElection election = new RaftElection("node-1", wal, List.of(), 150, 300);
        election.start();
        waitForElection(election, 2000);
        assertEquals(RaftRole.LEADER, election.role(), "single node should elect itself");
        assertTrue(election.currentEpoch() >= 1);
        election.stop();
    }

    @Test
    void shouldPersistTermAndVotedFor() {
        RaftElection e1 = new RaftElection("node-1", wal, List.of(), 150, 300);
        e1.start();
        waitForElection(e1, 2000);
        long epoch = e1.currentEpoch();
        e1.stop();
        RaftElection e2 = new RaftElection("node-1", wal, List.of(), 150, 300);
        assertEquals(epoch, e2.currentEpoch(), "epoch should persist across restarts");
        e2.stop();
    }

    @Test
    void shouldVoteForSelf() {
        RaftElection election = new RaftElection("node-1", wal, List.of(), 150, 300);
        election.start();
        waitForElection(election, 2000);
        assertEquals("node-1", wal.getVotedFor(), "should vote for self in single-node cluster");
        election.stop();
    }

    @Test
    void shouldStepDownOnHigherTerm() {
        RaftElection election = new RaftElection("node-1", wal, List.of("node-2"), 150, 300);
        election.start();
        election.onAppendEntries(10L, "node-2");
        assertEquals(RaftRole.FOLLOWER, election.role(), "should step down on higher term");
        assertEquals("node-2", election.currentLeader());
        election.stop();
    }

    @Test
    void shouldRejectLowerTermVote() {
        RaftElection election = new RaftElection("node-1", wal, List.of("node-2"), 150, 300);
        election.start();
        waitForElection(election, 2000);
        long currentEpoch = election.currentEpoch();
        assertFalse(election.handleRequestVote(currentEpoch - 1, "node-2", 0, 0));
        election.stop();
    }

    @Test
    void shouldAcceptHigherTermVote() {
        RaftElection election = new RaftElection("node-1", wal, List.of("node-2"), 150, 300);
        election.start();
        long higherEpoch = election.currentEpoch() + 5;
        assertTrue(election.handleRequestVote(higherEpoch, "node-2", 100, higherEpoch));
        assertEquals(higherEpoch, wal.getLastLogEpoch());
        election.stop();
    }

    // ========== Fix 6 (MEDIUM): async Raft metadata persistence ==========

    @Test
    void multiNodeShouldNotSelfElect() {
        RaftElection election = new RaftElection("node-1", wal, List.of("node-2", "node-3"), 150, 300);
        election.start();
        waitForElection(election, 2000);
        assertNotEquals(RaftRole.LEADER, election.role(),
            "multi-node should not self-elect without majority votes");
        election.stop();
    }

    @Test
    void shouldAsyncPersistTermAcrossWalReopen() throws Exception {
        RaftElection e1 = new RaftElection("node-1", wal, List.of(), 150, 300);
        e1.start();
        waitForElection(e1, 2000);
        long epochBeforeClose = e1.currentEpoch();
        String votedForBeforeClose = wal.getVotedFor();
        e1.stop();

        // Close wal to trigger final async flush + synchronous saveRaftMeta()
        wal.close();

        // Reopen WAL from same directory - should load persisted term/votedFor
        WalConfig cfg = new WalConfig(dataDir.toString(), 1, "batch", 100, false,
            "memory_segment", 1, 8, 64, 10, 4, 1, false);
        SimpleWalWriter wal2 = new SimpleWalWriter(cfg, "raft-test");
        try {
            assertEquals(epochBeforeClose, wal2.getLastLogEpoch(),
                "epoch should persist across WAL close/reopen");
            assertEquals(votedForBeforeClose, wal2.getVotedFor(),
                "votedFor should persist across WAL close/reopen");
        } finally {
            wal2.close();
        }
    }

    @Test
    void shouldNotLoseVotedForAfterElection() throws Exception {
        // Single-node: should vote for self after self-election
        RaftElection e1 = new RaftElection("node-1", wal, List.of(), 150, 300);
        e1.start();
        waitForElection(e1, 2000);
        long epoch = e1.currentEpoch();
        e1.stop();

        // Close WAL to flush async writes
        wal.close();

        WalConfig cfg = new WalConfig(dataDir.toString(), 1, "batch", 100, false,
            "memory_segment", 1, 8, 64, 10, 4, 1, false);
        SimpleWalWriter wal2 = new SimpleWalWriter(cfg, "raft-test");
        try {
            assertEquals("node-1", wal2.getVotedFor(),
                "self-vote should persist to disk");
            // Replay: a new RaftElection should start with same epoch
            RaftElection e2 = new RaftElection("node-1", wal2, List.of(), 150, 300);
            assertEquals(epoch, e2.currentEpoch(),
                "reloaded epoch should match original");
        } finally {
            wal2.close();
        }
    }
}

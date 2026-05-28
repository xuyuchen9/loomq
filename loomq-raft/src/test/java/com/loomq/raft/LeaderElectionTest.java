package com.loomq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.loomq.common.RaftRole;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    private static void waitForElection(LeaderElection election, long maxWaitMs) {
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
        LeaderElection election = new LeaderElection("node-1", wal, List.of(), 150, 300);
        election.start();
        waitForElection(election, 2000);
        assertEquals(RaftRole.LEADER, election.role(), "single node should elect itself");
        assertTrue(election.currentTerm() >= 1);
        election.stop();
    }

    @Test
    void shouldPersistTermAndVotedFor() {
        LeaderElection e1 = new LeaderElection("node-1", wal, List.of(), 150, 300);
        e1.start();
        waitForElection(e1, 2000);
        long term = e1.currentTerm();
        e1.stop();
        LeaderElection e2 = new LeaderElection("node-1", wal, List.of(), 150, 300);
        assertEquals(term, e2.currentTerm(), "term should persist across restarts");
        e2.stop();
    }

    @Test
    void shouldVoteForSelf() {
        LeaderElection election = new LeaderElection("node-1", wal, List.of(), 150, 300);
        election.start();
        waitForElection(election, 2000);
        assertEquals("node-1", wal.getVotedFor(), "should vote for self in single-node cluster");
        election.stop();
    }

    @Test
    void shouldStepDownOnHigherTerm() {
        LeaderElection election = new LeaderElection("node-1", wal, List.of("node-2"), 150, 300);
        election.start();
        election.onAppendEntries(10L, "node-2");
        assertEquals(RaftRole.FOLLOWER, election.role(), "should step down on higher term");
        assertEquals("node-2", election.currentLeader());
        election.stop();
    }

    @Test
    void shouldRejectLowerTermVote() {
        LeaderElection election = new LeaderElection("node-1", wal, List.of("node-2"), 150, 300);
        election.start();
        waitForElection(election, 2000);
        long currentTerm = election.currentTerm();
        assertFalse(election.handleRequestVote(currentTerm - 1, "node-2", 0, 0));
        election.stop();
    }

    @Test
    void shouldAcceptHigherTermVote() {
        LeaderElection election = new LeaderElection("node-1", wal, List.of("node-2"), 150, 300);
        election.start();
        long higherTerm = election.currentTerm() + 5;
        assertTrue(election.handleRequestVote(higherTerm, "node-2", 100, higherTerm));
        assertEquals(higherTerm, wal.getLastLogTerm());
        election.stop();
    }

    // ========== Fix 6 (MEDIUM): async Raft metadata persistence ==========

    @Test
    void multiNodeShouldNotSelfElect() {
        LeaderElection election = new LeaderElection("node-1", wal, List.of("node-2", "node-3"), 150, 300);
        election.start();
        waitForElection(election, 2000);
        assertNotEquals(RaftRole.LEADER, election.role(),
            "multi-node should not self-elect without majority votes");
        election.stop();
    }

    @Test
    void shouldAsyncPersistTermAcrossWalReopen() throws Exception {
        LeaderElection e1 = new LeaderElection("node-1", wal, List.of(), 150, 300);
        e1.start();
        waitForElection(e1, 2000);
        long termBeforeClose = e1.currentTerm();
        String votedForBeforeClose = wal.getVotedFor();
        e1.stop();

        // Close wal to trigger final async flush + synchronous saveRaftMeta()
        wal.close();

        // Reopen WAL from same directory - should load persisted term/votedFor
        WalConfig cfg = new WalConfig(dataDir.toString(), 1, "batch", 100, false,
            "memory_segment", 1, 8, 64, 10, 4, 1, false);
        SimpleWalWriter wal2 = new SimpleWalWriter(cfg, "raft-test");
        try {
            assertEquals(termBeforeClose, wal2.getLastLogTerm(),
                "term should persist across WAL close/reopen");
            assertEquals(votedForBeforeClose, wal2.getVotedFor(),
                "votedFor should persist across WAL close/reopen");
        } finally {
            wal2.close();
        }
    }

    @Test
    void shouldNotLoseVotedForAfterElection() throws Exception {
        // Single-node: should vote for self after self-election
        LeaderElection e1 = new LeaderElection("node-1", wal, List.of(), 150, 300);
        e1.start();
        waitForElection(e1, 2000);
        long term = e1.currentTerm();
        e1.stop();

        // Close WAL to flush async writes
        wal.close();

        WalConfig cfg = new WalConfig(dataDir.toString(), 1, "batch", 100, false,
            "memory_segment", 1, 8, 64, 10, 4, 1, false);
        SimpleWalWriter wal2 = new SimpleWalWriter(cfg, "raft-test");
        try {
            assertEquals("node-1", wal2.getVotedFor(),
                "self-vote should persist to disk");
            // Replay: a new LeaderElection should start with same term
            LeaderElection e2 = new LeaderElection("node-1", wal2, List.of(), 150, 300);
            assertEquals(term, e2.currentTerm(),
                "reloaded term should match original");
        } finally {
            wal2.close();
        }
    }
}

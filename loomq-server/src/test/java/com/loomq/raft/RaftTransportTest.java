package com.loomq.raft;

import com.loomq.raft.RaftTransport.AppendEntriesMessage;
import com.loomq.raft.RaftTransport.RequestVoteMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Raft RPC payload codec (no real network required).
 * Verifies round-trip encode/decode for RequestVote and AppendEntries.
 * Uses RaftTransport's package-private static codec methods directly.
 */
class RaftTransportTest {

    // ========== RequestVote codec ==========

    @Test
    void requestVoteShouldRoundTrip() {
        long term = 7;
        String candidateId = "node-3";
        long lastLogIndex = 150;
        long lastLogTerm = 6;

        byte[] encoded = RaftTransport.encodeRequestVote(term, candidateId, lastLogIndex, lastLogTerm);
        assertNotNull(encoded);
        assertTrue(encoded.length > 8 + 2 + candidateId.length());

        RequestVoteMessage msg = RaftTransport.decodeRequestVote(encoded);
        assertEquals(term, msg.term());
        assertEquals(candidateId, msg.candidateId());
        assertEquals(lastLogIndex, msg.lastLogIndex());
        assertEquals(lastLogTerm, msg.lastLogTerm());
    }

    @Test
    void requestVoteResponseShouldRoundTrip() {
        byte[] encoded = RaftTransport.encodeRequestVoteResponse(10, true);
        boolean granted = RaftTransport.decodeRequestVoteResponse(encoded);
        assertTrue(granted);

        encoded = RaftTransport.encodeRequestVoteResponse(10, false);
        granted = RaftTransport.decodeRequestVoteResponse(encoded);
        assertFalse(granted);
    }

    // ========== AppendEntries codec ==========

    @Test
    void appendEntriesShouldRoundTrip() {
        long term = 5;
        String leaderId = "node-1";
        long prevLogIndex = 100;
        long prevLogTerm = 4;
        byte[][] entries = { "entry1".getBytes(), "entry2".getBytes() };
        long leaderCommit = 98;

        byte[] encoded = RaftTransport.encodeAppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
        assertNotNull(encoded);

        AppendEntriesMessage msg = RaftTransport.decodeAppendEntries(encoded);
        assertEquals(term, msg.term());
        assertEquals(leaderId, msg.leaderId());
        assertEquals(prevLogIndex, msg.prevLogIndex());
        assertEquals(prevLogTerm, msg.prevLogTerm());
        assertEquals(2, msg.entries().length);
        assertArrayEquals("entry1".getBytes(), msg.entries()[0]);
        assertArrayEquals("entry2".getBytes(), msg.entries()[1]);
        assertEquals(leaderCommit, msg.leaderCommit());
    }

    @Test
    void appendEntriesWithEmptyEntriesShouldRoundTrip() {
        byte[] encoded = RaftTransport.encodeAppendEntries(1, "n1", 0, 0, new byte[0][], 0);
        AppendEntriesMessage msg = RaftTransport.decodeAppendEntries(encoded);
        assertEquals(0, msg.entries().length);
    }

    @Test
    void appendEntriesResponseShouldRoundTrip() {
        byte[] encoded = RaftTransport.encodeAppendEntriesResponse(10, true, 200, -1);
        AppendEntriesResult result = RaftTransport.decodeAppendEntriesResponse(encoded);
        assertTrue(result.success);
        assertEquals(10, result.term);
        assertEquals(200, result.matchIndex);

        encoded = RaftTransport.encodeAppendEntriesResponse(10, false, -1, 55);
        result = RaftTransport.decodeAppendEntriesResponse(encoded);
        assertFalse(result.success);
        assertEquals(55, result.conflictIndex);
    }
}

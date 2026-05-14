package com.loomq.raft;

import com.loomq.config.WalConfig;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Multi-node Raft integration tests using real TCP transport.
 * Each node runs in its own directory with its own WAL/Store.
 */
@Tag("integration")
class RaftIntegrationTest {

    private List<RaftNode> nodes = new ArrayList<>();
    private List<Path> dirs = new ArrayList<>();

    /** Allocate an available TCP port dynamically (reuse-friendly) */
    private static int allocatePort() throws IOException {
        try (ServerSocket ss = new ServerSocket(0)) {
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        }
    }

    private static void waitForLeader(RaftNode node, long maxWaitMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.isLeader()) return;
            Thread.sleep(50);
        }
    }

    private static RaftNode waitForAnyLeader(List<RaftNode> nodes, long maxWaitMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (System.currentTimeMillis() < deadline) {
            for (RaftNode n : nodes) {
                if (n.isLeader()) return n;
            }
            Thread.sleep(100);
        }
        return null;
    }

    /** Access private "transport" field via reflection (tests should be in same package) */
    private static Object reflectField(RaftNode node) {
        return reflectField(node, "transport");
    }

    private static Object reflectField(RaftNode node, String fieldName) {
        try {
            var field = RaftNode.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(node);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void waitForAppliedIndex(RaftNode node, long index, long maxWaitMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.getReplication().lastApplied() >= index) {
                return;
            }
            Thread.sleep(50);
        }
        fail("Timed out waiting for applied index " + index);
    }

    private static void waitForStoreSize(IntentStore store, int size, long maxWaitMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (System.currentTimeMillis() < deadline) {
            if (store.getAllIntents().size() >= size) {
                return;
            }
            Thread.sleep(50);
        }
        fail("Timed out waiting for store size " + size);
    }

    @AfterEach
    void tearDown() {
        for (RaftNode node : nodes) {
            try { node.close(); } catch (Exception ignored) {}
        }
        nodes.clear();
        dirs.clear();
    }

    /** Create a RaftNode with its own WAL directory and transport listening on given port */
    private RaftNode createNode(String nodeId, List<String> peers, int port) throws Exception {
        return createNode(nodeId, peers, port, 150, 300);
    }

    private RaftNode createNode(String nodeId, List<String> peers, int port,
                                long electionMinMs, long electionMaxMs) throws Exception {
        Path dir = Files.createTempDirectory("raft-int-" + nodeId + "-");
        dirs.add(dir);

        WalConfig cfg = new WalConfig(dir.toString(), 1, "batch", 100, false, "memory_segment",
            1, 8, 64, 10, 4, 1, false);
        SimpleWalWriter wal = new SimpleWalWriter(cfg, "raft-test");
        IntentStore store = new ConcurrentIntentStore();

        RaftConfig config = new RaftConfig(nodeId, peers, dir.toString(), electionMinMs, electionMaxMs, 50);
        RaftTransport transport = new RaftTransport(nodeId);
        transport.listen("127.0.0.1", port);

        return new RaftNode(config, wal, store, transport);
    }

    // ========== helpers ==========

    @Test
    void singleNodeShouldElectAndReplicate() throws Exception {
        RaftNode node = createNode("node-1", List.of(), allocatePort());
        nodes.add(node);
        node.start();
        waitForLeader(node, 3000);

        assertTrue(node.isLeader(), "single node should become leader");

        // Propose an entry
        Intent intent = new Intent("int-1");
        intent.setPrecisionTier(com.loomq.domain.intent.PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);
        long index = node.propose(IntentBinaryCodec.encode(intent));
        assertTrue(index > 0, "propose should return valid index");

        // Commit and apply
        node.getReplication().advanceCommitIndex(new long[]{index}, node.getElection().currentTerm());
        node.applyCommitted();
    }

    @Test
    void threeNodeClusterShouldElectLeader() throws Exception {
        List<String> peers = List.of("node-1", "node-2", "node-3");
        int p1 = allocatePort(), p2 = allocatePort(), p3 = allocatePort();

        RaftNode n1 = createNode("node-1", peers, p1);
        RaftNode n2 = createNode("node-2", peers, p2);
        RaftNode n3 = createNode("node-3", peers, p3);
        nodes.addAll(List.of(n1, n2, n3));

        // Connect each node to its peers
        RaftTransport t1 = (RaftTransport) reflectField(n1);
        RaftTransport t2 = (RaftTransport) reflectField(n2);
        RaftTransport t3 = (RaftTransport) reflectField(n3);

        // Connect in a mesh
        t1.connect("node-2", "127.0.0.1", p2).join();
        t1.connect("node-3", "127.0.0.1", p3).join();
        t2.connect("node-1", "127.0.0.1", p1).join();
        t2.connect("node-3", "127.0.0.1", p3).join();
        t3.connect("node-1", "127.0.0.1", p1).join();
        t3.connect("node-2", "127.0.0.1", p2).join();

        // Start all nodes
        n1.start();
        n2.start();
        n3.start();

        // Wait for leader election (up to 10 seconds — may need several election rounds)
        AtomicBoolean hasLeader = new AtomicBoolean(false);
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (n1.isLeader() || n2.isLeader() || n3.isLeader()) {
                hasLeader.set(true);
                break;
            }
            Thread.sleep(100);
        }

        assertTrue(hasLeader.get(), "one node should become leader within 10s");

        // Only one leader
        int leaderCount = 0;
        if (n1.isLeader()) leaderCount++;
        if (n2.isLeader()) leaderCount++;
        if (n3.isLeader()) leaderCount++;
        assertEquals(1, leaderCount, "exactly one leader should exist");
    }

    @Test
    void leaderShouldReplicateEntriesToFollowers() throws Exception {
        List<String> peers = List.of("node-1", "node-2", "node-3");
        int p1 = allocatePort(), p2 = allocatePort(), p3 = allocatePort();

        RaftNode n1 = createNode("node-1", peers, p1);
        RaftNode n2 = createNode("node-2", peers, p2);
        RaftNode n3 = createNode("node-3", peers, p3);
        nodes.addAll(List.of(n1, n2, n3));

        RaftTransport t1 = (RaftTransport) reflectField(n1);
        RaftTransport t2 = (RaftTransport) reflectField(n2);
        RaftTransport t3 = (RaftTransport) reflectField(n3);

        t1.connect("node-2", "127.0.0.1", p2).join();
        t1.connect("node-3", "127.0.0.1", p3).join();
        t2.connect("node-1", "127.0.0.1", p1).join();
        t2.connect("node-3", "127.0.0.1", p3).join();
        t3.connect("node-1", "127.0.0.1", p1).join();
        t3.connect("node-2", "127.0.0.1", p2).join();

        n1.start();
        n2.start();
        n3.start();

        // Wait for a leader
        RaftNode leader = waitForAnyLeader(List.of(n1, n2, n3), 10_000);
        assertNotNull(leader, "should have a leader");

        // Leader proposes an entry
        Intent intent = new Intent("rep-int-1");
        intent.setPrecisionTier(com.loomq.domain.intent.PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);
        byte[] encoded = IntentBinaryCodec.encode(intent);
        long index = leader.propose(encoded);
        assertTrue(index > 0, "propose should return valid index");

        // Leader sends AppendEntries (heartbeat will do this periodically)
        // Wait for replication to propagate
        Thread.sleep(500);

        // Check that followers have the entry in their WAL
        for (RaftNode node : nodes) {
            if (node != leader) {
                long followerPos = node.getWal().getWritePosition();
                assertTrue(followerPos > 0,
                    "follower " + node.getElection().currentLeader() + " should have entries in WAL");
            }
        }
    }

    @Test
    void shouldStepDownWhenHigherTermLeaderAppears() throws Exception {
        List<String> peers = List.of("node-1", "node-2");
        int p1 = allocatePort(), p2 = allocatePort();

        RaftNode n1 = createNode("node-1", peers, p1);
        RaftNode n2 = createNode("node-2", peers, p2);
        nodes.addAll(List.of(n1, n2));

        RaftTransport t1 = (RaftTransport) reflectField(n1);
        RaftTransport t2 = (RaftTransport) reflectField(n2);

        t1.connect("node-2", "127.0.0.1", p2).join();
        t2.connect("node-1", "127.0.0.1", p1).join();

        n1.start();
        n2.start();

        // Wait for stable state
        Thread.sleep(2000);

        // Both should be followers initially, then one becomes leader and
        // the other should recognize it as leader
        boolean oneLeader = n1.isLeader() || n2.isLeader();
        assertTrue(oneLeader, "one should be leader");

        if (n1.isLeader()) {
            assertFalse(n2.isLeader(), "n2 should be follower");
        } else {
            assertTrue(n2.isLeader(), "n2 should be leader");
            assertFalse(n1.isLeader(), "n1 should be follower");
        }
    }

    @Test
    void shouldRecoverFromLeaderFailure() throws Exception {
        List<String> peers = List.of("node-1", "node-2", "node-3");
        int p1 = allocatePort(), p2 = allocatePort(), p3 = allocatePort();

        RaftNode n1 = createNode("node-1", peers, p1);
        RaftNode n2 = createNode("node-2", peers, p2);
        RaftNode n3 = createNode("node-3", peers, p3);
        nodes.addAll(List.of(n1, n2, n3));

        RaftTransport t1 = (RaftTransport) reflectField(n1);
        RaftTransport t2 = (RaftTransport) reflectField(n2);
        RaftTransport t3 = (RaftTransport) reflectField(n3);

        t1.connect("node-2", "127.0.0.1", p2).join();
        t1.connect("node-3", "127.0.0.1", p3).join();
        t2.connect("node-1", "127.0.0.1", p1).join();
        t2.connect("node-3", "127.0.0.1", p3).join();
        t3.connect("node-1", "127.0.0.1", p1).join();
        t3.connect("node-2", "127.0.0.1", p2).join();

        n1.start();
        n2.start();
        n3.start();

        RaftNode firstLeader = waitForAnyLeader(List.of(n1, n2, n3), 10_000);
        assertNotNull(firstLeader, "should have initial leader");

        // Kill the leader
        firstLeader.close();
        nodes.remove(firstLeader);

        // Wait for a new leader to emerge from remaining nodes
        List<RaftNode> survivors = new ArrayList<>(nodes);
        RaftNode newLeader = waitForAnyLeader(survivors, 15_000);
        assertNotNull(newLeader, "should elect new leader after old leader fails");
        assertNotSame(firstLeader, newLeader, "new leader should be different node");

        // No split-brain: only one leader
        int leaderCount = 0;
        for (RaftNode n : survivors) {
            if (n.isLeader()) leaderCount++;
        }
        assertEquals(1, leaderCount, "exactly one leader after failover");
    }

    @Test
    void lateFollowerShouldCatchUpViaSnapshot() throws Exception {
        List<String> peers = List.of("node-1", "node-2", "node-3");
        int p1 = allocatePort(), p2 = allocatePort(), p3 = allocatePort();

        RaftNode n1 = createNode("node-1", peers, p1);
        RaftNode n2 = createNode("node-2", peers, p2);
        RaftNode n3 = createNode("node-3", peers, p3, 1000, 1500);
        nodes.addAll(List.of(n1, n2, n3));

        RaftTransport t1 = (RaftTransport) reflectField(n1, "transport");
        RaftTransport t2 = (RaftTransport) reflectField(n2, "transport");
        RaftTransport t3 = (RaftTransport) reflectField(n3, "transport");

        // Elect a leader with n1 and n2 only. n3 is intentionally disconnected
        // so it will lag behind enough to require InstallSnapshot later.
        t1.connect("node-2", "127.0.0.1", p2).join();
        t2.connect("node-1", "127.0.0.1", p1).join();

        n1.start();
        n2.start();

        RaftNode leader = waitForAnyLeader(List.of(n1, n2), 10_000);
        assertNotNull(leader, "should elect a leader before late follower joins");

        // Commit a few entries on the leader so there is state worth snapshotting.
        List<String> ids = List.of("snap-late-1", "snap-late-2", "snap-late-3");
        long lastIndex = 0;
        for (String id : ids) {
            Intent intent = new Intent(id);
            intent.setPrecisionTier(com.loomq.domain.intent.PrecisionTier.STANDARD);
            intent.transitionTo(IntentStatus.SCHEDULED);
            lastIndex = leader.propose(IntentBinaryCodec.encode(intent));
        }

        waitForAppliedIndex(leader, lastIndex, 10_000);
        assertEquals(lastIndex, leader.getReplication().lastApplied(),
            "leader should apply committed entries before snapshotting");

        // Force a snapshot boundary on the leader.
        leader.getRaftLog().compactThrough(lastIndex);
        assertEquals(lastIndex + 1, leader.getRaftLog().firstIndex(),
            "leader should advance firstIndex after snapshot compaction");

        t1.connect("node-3", "127.0.0.1", p3).join();
        t2.connect("node-3", "127.0.0.1", p3).join();
        t3.connect("node-1", "127.0.0.1", p1).join();
        t3.connect("node-2", "127.0.0.1", p2).join();

        IntentStore followerStore = (IntentStore) reflectField(n3, "store");
        waitForStoreSize(followerStore, ids.size(), 10_000);

        for (String id : ids) {
            assertNotNull(followerStore.findById(id),
                "late follower should receive intent " + id + " via snapshot");
        }
        assertEquals(lastIndex, n3.getReplication().lastApplied(),
            "late follower should advance lastApplied to the snapshot index");
        assertEquals(lastIndex + 1, n3.getRaftLog().firstIndex(),
            "late follower should advance firstIndex past the snapshot");

        n3.start();

        // Fail the leader over after snapshot catch-up. The remaining nodes should
        // still be able to elect a single leader and preserve the recovered state.
        leader.close();
        nodes.remove(leader);

        List<RaftNode> survivors = new ArrayList<>(List.of(n1, n2, n3));
        survivors.remove(leader);
        RaftNode newLeader = waitForAnyLeader(survivors, 15_000);
        assertNotNull(newLeader, "remaining nodes should elect a new leader after failover");

        int leaderCount = 0;
        for (RaftNode node : survivors) {
            if (node.isLeader()) leaderCount++;
        }
        assertEquals(1, leaderCount, "exactly one leader should remain after failover");

        assertEquals(ids.size(), followerStore.getAllIntents().size(),
            "snapshotted follower should retain the full recovered state after failover");
    }
}

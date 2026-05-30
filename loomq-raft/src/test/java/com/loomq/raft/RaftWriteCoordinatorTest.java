package com.loomq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.config.WalConfig;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("slow")
class RaftWriteCoordinatorTest {

    private Path dataDir;
    private SimpleWalWriter wal;
    private IntentStore store;
    private RaftNode raftNode;
    private RaftWriteCoordinator coordinator;

    private static Intent makeIntent(String id) {
        Intent intent = new Intent(id);
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }

    private static void waitForLeader(RaftNode node, long maxWaitMs) {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.isLeader()) return;
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        dataDir = Files.createTempDirectory("raft-coordinator-");
        WalConfig cfg = new WalConfig(dataDir.toString(), 1, "batch", 100, false,
            "memory_segment", 1, 8, 64, 10, 4, 1, false);
        wal = new SimpleWalWriter(cfg, "coordinator-test");
        store = new ConcurrentIntentStore();

        RaftConfig config = RaftConfig.singleNode("node-1");
        raftNode = new RaftNode(config, wal, store, null);
        raftNode.start();
        waitForLeader(raftNode, 3000);

        coordinator = new RaftWriteCoordinator(raftNode, store, 4, 500, 2000);
    }

    @AfterEach
    void tearDown() {
        if (raftNode != null) raftNode.close();
        if (wal != null) wal.close();
        if (store != null) store.shutdown();
        try {
            Files.walk(dataDir)
                .sorted(java.util.Comparator.reverseOrder())
                .forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignored) {} });
        } catch (Exception ignored) {}
    }

    @Test
    void commitSnapshotShouldRejectNullSnapshot() {
        assertThrows(IllegalArgumentException.class,
            () -> coordinator.commitSnapshot(null, "create", null));
    }

    @Test
    void commitMutationShouldRejectBlankIntentId() {
        assertThrows(IllegalArgumentException.class,
            () -> coordinator.commitMutation("", "update", null, 0, i -> i));
    }

    @Test
    void commitMutationShouldRejectNullMutator() {
        assertThrows(IllegalArgumentException.class,
            () -> coordinator.commitMutation("id-1", "update", null, 0, null));
    }

    @Test
    void commitMutationShouldFailWhenIntentNotFound() {
        assertThrows(RaftWriteUnavailableException.class,
            () -> coordinator.commitMutation("nonexistent", "update", null, 0, i -> i));
    }

    @Test
    void commitSnapshotShouldSucceedOnSingleNode() {
        assertTrue(raftNode.isLeader(), "single node should be leader");

        Intent snapshot = makeIntent("success-intent");
        Intent result = coordinator.commitSnapshot(snapshot, "create", null);

        assertNotNull(result);
        assertEquals("success-intent", result.getIntentId());
    }

    @Test
    void commitMutationShouldDetectRevisionConflict() {
        assertTrue(raftNode.isLeader(), "single node should be leader");

        Intent existing = makeIntent("conflict-intent");
        existing.incrementRevision(); // revision = 1
        store.save(existing);

        // Try with wrong expectedRevision
        assertThrows(RaftWriteConflictException.class,
            () -> coordinator.commitMutation("conflict-intent", "update", null, 999, i -> i));
    }

    @Test
    void canHandleWriteShouldDelegateToIsLeader() {
        assertTrue(coordinator.canHandleWrite());
    }

    @Test
    void isWriteEnabledShouldAlwaysReturnTrue() {
        assertTrue(coordinator.isWriteEnabled());
    }
}

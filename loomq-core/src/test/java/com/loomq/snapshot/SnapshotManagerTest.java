package com.loomq.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.snapshot.SnapshotManager.SnapshotInfo;
import com.loomq.snapshot.SnapshotManager.SnapshotRestoreResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnapshotManagerTest {

    private Path dataDir;
    private SnapshotManager snapshotManager;
    private IntentStore store;

    private static Intent makeIntent(String id) {
        Intent intent = new Intent(id);
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }

    @BeforeEach
    void setUp() throws IOException {
        dataDir = Files.createTempDirectory("snapshot-test-");
        snapshotManager = new SnapshotManager(dataDir.toString());
        store = new ConcurrentIntentStore();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (store != null) {
            store.shutdown();
        }
        Files.walk(dataDir)
            .sorted(java.util.Comparator.reverseOrder())
            .forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignored) {} });
    }

    @Test
    void shouldCreateAndRestoreSnapshot() {
        // Given
        store.save(makeIntent("snap-1"));
        store.save(makeIntent("snap-2"));

        // When
        SnapshotInfo info = snapshotManager.createSnapshot(store, 100L);

        // Then
        assertNotNull(info);
        assertEquals(100L, info.walOffset);
        assertTrue(info.sizeBytes > 0);

        // Restore
        SnapshotRestoreResult result = snapshotManager.restoreFromSnapshot(intent -> {});
        assertEquals(2, result.restoredCount());
        assertEquals(100L, result.walOffset());
    }

    @Test
    void shouldRestoreIntentsCorrectly() {
        // Given
        Intent intent = makeIntent("restore-check");
        intent.setExecuteAt(Instant.ofEpochMilli(1700000000000L));
        store.save(intent);

        snapshotManager.createSnapshot(store, 0L);

        // When
        List<Intent> restored = new ArrayList<>();
        snapshotManager.restoreFromSnapshot(restored::add);

        // Then
        assertEquals(1, restored.size());
        assertEquals("restore-check", restored.get(0).getIntentId());
        assertEquals(IntentStatus.SCHEDULED, restored.get(0).getStatus());
    }

    @Test
    void shouldReturnEmptyWhenNoSnapshot() {
        // When
        SnapshotRestoreResult result = snapshotManager.restoreFromSnapshot(intent -> {});

        // Then
        assertEquals(0, result.restoredCount());
        assertEquals(0L, result.walOffset());
    }

    @Test
    void shouldRotateOldSnapshots() {
        // Given: create more than MAX_SNAPSHOTS_TO_KEEP (3)
        for (int i = 0; i < 5; i++) {
            store.save(makeIntent("rotate-" + i));
            snapshotManager.createSnapshot(store, i * 100L);
        }

        // When
        var info = snapshotManager.getLatestSnapshotInfo();

        // Then: latest snapshot should exist
        assertTrue(info.isPresent());
    }

    @Test
    void shouldHandleEmptyStore() {
        // When
        SnapshotInfo info = snapshotManager.createSnapshot(store, 0L);

        // Then
        assertNotNull(info);
        assertEquals(0L, info.walOffset);

        SnapshotRestoreResult result = snapshotManager.restoreFromSnapshot(intent -> {});
        assertEquals(0, result.restoredCount());
    }

    @Test
    void getLatestSnapshotInfoShouldReturnEmptyWhenNone() {
        assertFalse(snapshotManager.getLatestSnapshotInfo().isPresent());
    }

    @Test
    void shouldCleanupStaleTmpFilesOnCreateSnapshot() throws IOException {
        // Given: simulate a previous crash leaving a stale .tmp file
        Path snapshotDir = dataDir.resolve("snapshots");
        Files.createDirectories(snapshotDir);
        Path staleTmp = snapshotDir.resolve("snapshot-20260101-000000-000.snap.gz.tmp");
        Files.writeString(staleTmp, "partial data");
        assertTrue(Files.exists(staleTmp));

        // When: create a new snapshot
        store.save(makeIntent("tmp-cleanup"));
        snapshotManager.createSnapshot(store, 42L);

        // Then: stale .tmp file should be cleaned up
        assertFalse(Files.exists(staleTmp), "Stale .tmp file should be deleted");

        // And: new snapshot should be valid
        SnapshotRestoreResult result = snapshotManager.restoreFromSnapshot(intent -> {});
        assertEquals(1, result.restoredCount());
        assertEquals(42L, result.walOffset());
    }

    @Test
    void shouldNotLeaveTmpFileAfterSuccessfulSnapshot() {
        // Given
        store.save(makeIntent("no-tmp-check"));

        // When
        snapshotManager.createSnapshot(store, 10L);

        // Then: no .tmp files should remain in snapshot dir
        Path snapshotDir = dataDir.resolve("snapshots");
        try (var stream = Files.list(snapshotDir)) {
            long tmpCount = stream
                .filter(p -> p.getFileName().toString().endsWith(".tmp"))
                .count();
            assertEquals(0, tmpCount, "No .tmp files should remain after successful snapshot");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void shouldRecoverFromPreviousGoodSnapshotWhenTmpExists() throws IOException {
        // Given: create a good snapshot first
        store.save(makeIntent("good-snap"));
        SnapshotInfo goodInfo = snapshotManager.createSnapshot(store, 100L);
        assertNotNull(goodInfo);

        // Simulate crash: create a stale .tmp file (as if next snapshot crashed mid-write)
        Path snapshotDir = dataDir.resolve("snapshots");
        Path staleTmp = snapshotDir.resolve("snapshot-20990101-000000-000.snap.gz.tmp");
        Files.writeString(staleTmp, "corrupted partial data");

        // When: restore from snapshot
        SnapshotRestoreResult result = snapshotManager.restoreFromSnapshot(intent -> {});

        // Then: should restore from the good snapshot, not the .tmp file
        assertEquals(1, result.restoredCount());
        assertEquals(100L, result.walOffset());
    }
}

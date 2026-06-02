package com.loomq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.common.RaftRole;
import com.loomq.config.WalConfig;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import io.kubernetes.client.openapi.ApiClient;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("slow")
class K8sLeaseElectionTest {
    private Path dataDir;
    private SimpleWalWriter wal;

    @BeforeEach
    void setUp() throws Exception {
        dataDir = Files.createTempDirectory("k8s-lease-test-");
        WalConfig cfg = new WalConfig(dataDir.toString(), 1, "batch", 100, false,
            "memory_segment", 1, 8, 64, 10, 4, 1, false);
        wal = new SimpleWalWriter(cfg, "k8s-lease-test");
    }

    @AfterEach
    void tearDown() {
        if (wal != null) wal.close();
    }

    @Test
    void standaloneModeShouldAlwaysBeLeader() {
        // StandaloneElection is the non-K8s fallback
        StandaloneElection election = new StandaloneElection();
        election.start();
        assertTrue(election.isLeader());
        assertEquals(RaftRole.LEADER, election.role());
        assertEquals(1L, election.currentEpoch());
        assertEquals("standalone", election.currentLeader());
        election.stop();
    }

    @Test
    void standaloneShouldNotifyLeaderListener() {
        AtomicBoolean notified = new AtomicBoolean(false);
        StandaloneElection election = new StandaloneElection();
        election.addBecomeLeaderListener(epoch -> {
            assertEquals(1L, epoch);
            notified.set(true);
        });
        election.start();
        assertTrue(notified.get(), "should notify leader listener on start");
        election.stop();
    }

    @Test
    void k8sConfigShouldValidate() {
        // Valid config
        new K8sLeaseConfig(10, 3, "default", "loomq-leader", "pod-1");

        // Invalid: negative duration
        try {
            new K8sLeaseConfig(-1, 3, "default", "loomq-leader", "pod-1");
            assertTrue(false, "should throw on negative duration");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // Invalid: renew >= duration
        try {
            new K8sLeaseConfig(10, 10, "default", "loomq-leader", "pod-1");
            assertTrue(false, "should throw when renew >= duration");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // Invalid: blank namespace
        try {
            new K8sLeaseConfig(10, 3, "", "loomq-leader", "pod-1");
            assertTrue(false, "should throw on blank namespace");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void k8sElectionShouldPersistEpoch() {
        // Test that epoch persistence works through WalAccessor
        wal.setCurrentEpoch(42L);
        wal.persistRaftMeta();

        // Create a new WAL writer from the same directory
        try {
            wal.close();
            WalConfig cfg = new WalConfig(dataDir.toString(), 1, "batch", 100, false,
                "memory_segment", 1, 8, 64, 10, 4, 1, false);
            SimpleWalWriter wal2 = new SimpleWalWriter(cfg, "k8s-lease-test");
            try {
                assertEquals(42, wal2.getLastLogEpoch(),
                    "epoch should persist across WAL close/reopen");
            } finally {
                wal2.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void leaderElectionInterfaceShouldBeComplete() {
        // Verify that all LeaderElection interface methods are implementable
        LeaderElection election = new StandaloneElection();
        election.start();
        assertTrue(election.isLeader());
        assertEquals(RaftRole.LEADER, election.role());
        assertEquals(1L, election.currentEpoch());
        assertEquals("standalone", election.currentLeader());
        election.onAppendEntries(1L, "other"); // no-op for standalone
        election.stop();
    }

    @Test
    void configValidationShouldRejectInvalidClockSkewBuffer() {
        // leaseDuration=10, renewInterval=3, buffer=8 → 10 <= 3+8 → should throw
        assertThrows(IllegalArgumentException.class, () ->
            new K8sLeaseConfig(10, 3, "default", "loomq-leader", "pod-1", 8));
    }

    @Test
    void defaultClockSkewBufferShouldBeFive() {
        K8sLeaseConfig config = new K8sLeaseConfig(15, 4, "default", "loomq-leader", "pod-1");
        assertEquals(5, config.clockSkewBufferSeconds());
    }

    @Test
    void isLeaderShouldReturnFalseWhenMonotonicClockExpired() throws Exception {
        K8sLeaseConfig config = new K8sLeaseConfig(15, 4, "default", "loomq-leader", "pod-1");
        ApiClient dummyClient = new ApiClient();
        K8sLeaseElection election = new K8sLeaseElection(config, wal, dummyClient);

        // Use reflection to force the election into LEADER state with stale renewal time
        Field roleField = K8sLeaseElection.class.getDeclaredField("role");
        roleField.setAccessible(true);
        roleField.set(election, RaftRole.LEADER);

        Field epochField = K8sLeaseElection.class.getDeclaredField("currentEpoch");
        epochField.setAccessible(true);
        epochField.set(election, 1L);

        Field nanoField = K8sLeaseElection.class.getDeclaredField("lastRenewNanoTime");
        nanoField.setAccessible(true);
        // Set last renewal to 60 seconds ago — far beyond the 15s lease duration
        nanoField.set(election, System.nanoTime() - TimeUnit.SECONDS.toNanos(60));

        // isLeader() should detect monotonic clock expiration and step down
        assertFalse(election.isLeader());
        assertEquals(RaftRole.FOLLOWER, election.role(),
            "should step down to FOLLOWER after monotonic clock expiration");
    }
}

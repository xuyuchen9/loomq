package com.loomq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.loomq.LoomqEngine;
import com.loomq.api.IntentActionResponse;
import com.loomq.api.IntentResponse;
import com.loomq.config.WalConfig;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.http.netty.HttpErrorResponse;
import com.loomq.http.netty.IntentHandler;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.spi.DeliveryHandler;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import io.netty.handler.codec.http.HttpMethod;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Tag("integration")
class RaftWriteAuthorityIntegrationTest {

    @Test
    @Timeout(60)
    void leaderWritesShouldReplicateAndFollowersShouldReject() throws Exception {
        List<String> peers = List.of("node-1", "node-2", "node-3");
        int p1 = allocatePort();
        int p2 = allocatePort();
        int p3 = allocatePort();

        Path leaderDir = Files.createTempDirectory("raft-write-leader-");
        LoomqEngine leaderEngine = LoomqEngine.builder()
            .nodeId("node-1")
            .walDir(leaderDir)
            .deliveryHandler(intent -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS))
            .build();

        RaftTransport t1 = new RaftTransport("node-1");
        t1.listen("127.0.0.1", p1);
        RaftNode leaderNode = new RaftNode(
            new RaftConfig("node-1", peers, leaderDir.toString(), 150, 300, 50),
            leaderEngine.getWalAccessor(),
            leaderEngine.getIntentStore(),
            t1
        );

        TestNode follower2 = createFollowerNode("node-2", peers, p2, 1000, 1500);
        TestNode follower3 = createFollowerNode("node-3", peers, p3, 1000, 1500);

        try {
            t1.connect("node-2", "127.0.0.1", p2).join();
            t1.connect("node-3", "127.0.0.1", p3).join();
            follower2.transport().connect("node-1", "127.0.0.1", p1).join();
            follower2.transport().connect("node-3", "127.0.0.1", p3).join();
            follower3.transport().connect("node-1", "127.0.0.1", p1).join();
            follower3.transport().connect("node-2", "127.0.0.1", p2).join();

            leaderEngine.start();
            leaderNode.start();
            waitForLeader(leaderNode, 5_000);
            assertTrue(leaderNode.isLeader(), "node-1 should become leader for the write authority test");

            follower2.node().start();
            follower3.node().start();
            waitForLeaderHint(follower2.node(), leaderNode.snapshotStatus().nodeId(), 5_000);
            waitForLeaderHint(follower3.node(), leaderNode.snapshotStatus().nodeId(), 5_000);

            RaftWriteCoordinator writeCoordinator = new RaftWriteCoordinator(leaderNode, leaderEngine.getIntentStore());
            IntentHandler leaderHandler = new IntentHandler(leaderEngine, leaderNode, writeCoordinator);
            IntentHandler followerHandler2 = new IntentHandler(leaderEngine, follower2.node());
            IntentHandler followerHandler3 = new IntentHandler(leaderEngine, follower3.node());

            String intentId = "raft-write-" + System.nanoTime();
            Instant executeAt = Instant.now().plusSeconds(30);
            String createBody = """
                {
                  "intentId":"%s",
                  "executeAt":"%s",
                  "deadline":"%s",
                  "precisionTier":"STANDARD",
                  "shardKey":"orders"
                }
                """.formatted(intentId, executeAt, executeAt.plusSeconds(60));

            HttpErrorResponse createFollowerResult = assertInstanceOf(HttpErrorResponse.class,
                followerHandler2.createIntent(HttpMethod.POST, "/v1/intents", createBody.getBytes(), java.util.Map.of(), java.util.Map.of()));
            assertWriteRejected(createFollowerResult, "create", null, leaderNode.snapshotStatus().nodeId());

            IntentHandler.CreatedResponse createdResponse = assertInstanceOf(IntentHandler.CreatedResponse.class,
                leaderHandler.createIntent(HttpMethod.POST, "/v1/intents", createBody.getBytes(), java.util.Map.of(), java.util.Map.of()));
            IntentResponse created = assertInstanceOf(IntentResponse.class, createdResponse.body());
            assertEquals(intentId, created.intentId());
            assertEquals(1L, created.revision());

            IntentHandler.CreatedResponse duplicateCreateResponse = assertInstanceOf(IntentHandler.CreatedResponse.class,
                leaderHandler.createIntent(HttpMethod.POST, "/v1/intents", createBody.getBytes(), java.util.Map.of(), java.util.Map.of()));
            IntentResponse duplicateCreated = assertInstanceOf(IntentResponse.class, duplicateCreateResponse.body());
            assertEquals(created.intentId(), duplicateCreated.intentId());
            assertEquals(created.revision(), duplicateCreated.revision());

            waitForIntentStatus(follower2.store(), intentId, IntentStatus.SCHEDULED, 10_000);
            waitForIntentStatus(follower3.store(), intentId, IntentStatus.SCHEDULED, 10_000);

            String patchBody = """
                {
                  "deadline":"%s",
                  "tags":{"phase":"patched","owner":"raft"}
                }
                """.formatted(executeAt.plusSeconds(120));
            java.util.Map<String, String> revision1Headers = java.util.Map.of("X-LoomQ-Expected-Revision", "1");

            HttpErrorResponse patchFollowerResult = assertInstanceOf(HttpErrorResponse.class,
                followerHandler3.patchIntent(HttpMethod.PATCH, "/v1/intents/" + intentId, patchBody.getBytes(), java.util.Map.of(), java.util.Map.of("intentId", intentId)));
            assertWriteRejected(patchFollowerResult, "patch", intentId, leaderNode.snapshotStatus().nodeId());

            IntentResponse patched = assertInstanceOf(IntentResponse.class,
                leaderHandler.patchIntent(HttpMethod.PATCH, "/v1/intents/" + intentId, patchBody.getBytes(), revision1Headers, java.util.Map.of("intentId", intentId)));
            assertEquals(intentId, patched.intentId());
            assertEquals(2L, patched.revision());
            waitForIntentStatus(follower2.store(), intentId, IntentStatus.SCHEDULED, 10_000);
            waitForIntentStatus(follower3.store(), intentId, IntentStatus.SCHEDULED, 10_000);
            waitForIntentTag(follower2.store(), intentId, "phase", "patched", 10_000);
            waitForIntentTag(follower3.store(), intentId, "phase", "patched", 10_000);

            String stalePatchBody = """
                {
                  "deadline":"%s",
                  "tags":{"phase":"stale","owner":"raft"}
                }
                """.formatted(executeAt.plusSeconds(180));
            HttpErrorResponse stalePatchResult = assertInstanceOf(HttpErrorResponse.class,
                leaderHandler.patchIntent(HttpMethod.PATCH, "/v1/intents/" + intentId, stalePatchBody.getBytes(), revision1Headers, java.util.Map.of("intentId", intentId)));
            assertEquals(409, stalePatchResult.status());
            assertEquals("40902", stalePatchResult.error().code());
            assertEquals(Boolean.FALSE, stalePatchResult.error().details().get("retryable"));
            assertEquals(1L, stalePatchResult.error().details().get("expectedRevision"));
            assertEquals(2L, stalePatchResult.error().details().get("actualRevision"));

            HttpErrorResponse cancelFollowerResult = assertInstanceOf(HttpErrorResponse.class,
                followerHandler2.cancelIntent(HttpMethod.POST, "/v1/intents/" + intentId + "/cancel", new byte[0], java.util.Map.of(), java.util.Map.of("intentId", intentId)));
            assertWriteRejected(cancelFollowerResult, "cancel", intentId, leaderNode.snapshotStatus().nodeId());

            java.util.Map<String, String> revision2Headers = java.util.Map.of("X-LoomQ-Expected-Revision", "2");
            IntentResponse cancelled = assertInstanceOf(IntentResponse.class,
                leaderHandler.cancelIntent(HttpMethod.POST, "/v1/intents/" + intentId + "/cancel", new byte[0], revision2Headers, java.util.Map.of("intentId", intentId)));
            assertEquals(IntentStatus.CANCELED, cancelled.status());
            assertEquals(3L, cancelled.revision());
            waitForIntentStatus(follower2.store(), intentId, IntentStatus.CANCELED, 10_000);
            waitForIntentStatus(follower3.store(), intentId, IntentStatus.CANCELED, 10_000);

            String fireNowId = "raft-fire-" + System.nanoTime();
            Instant fireExecuteAt = Instant.now().plusSeconds(90);
            String fireBody = """
                {
                  "intentId":"%s",
                  "executeAt":"%s",
                  "deadline":"%s",
                  "precisionTier":"STANDARD",
                  "shardKey":"orders"
                }
                """.formatted(fireNowId, fireExecuteAt, fireExecuteAt.plusSeconds(60));

            IntentHandler.CreatedResponse fireCreatedResponse = assertInstanceOf(IntentHandler.CreatedResponse.class,
                leaderHandler.createIntent(HttpMethod.POST, "/v1/intents", fireBody.getBytes(), java.util.Map.of(), java.util.Map.of()));
            IntentResponse fireCreated = (IntentResponse) fireCreatedResponse.body();
            assertEquals(fireNowId, fireCreated.intentId());
            assertEquals(1L, fireCreated.revision());

            HttpErrorResponse fireFollowerResult = assertInstanceOf(HttpErrorResponse.class,
                followerHandler3.fireNow(HttpMethod.POST, "/v1/intents/" + fireNowId + "/fire-now", new byte[0], java.util.Map.of(), java.util.Map.of("intentId", fireNowId)));
            assertWriteRejected(fireFollowerResult, "fire-now", fireNowId, leaderNode.snapshotStatus().nodeId());

            IntentActionResponse fired = assertInstanceOf(IntentActionResponse.class,
                leaderHandler.fireNow(HttpMethod.POST, "/v1/intents/" + fireNowId + "/fire-now", new byte[0], revision1Headers, java.util.Map.of("intentId", fireNowId)));
            assertEquals(fireNowId, fired.intentId());
            assertEquals(IntentStatus.DISPATCHING.name(), fired.status());
        } finally {
            try {
                leaderNode.close();
            } catch (Exception ignored) {
            }
            try {
                leaderEngine.close();
            } catch (Exception ignored) {
            }
            try {
                follower2.node().close();
            } catch (Exception ignored) {
            }
            try {
                follower2.wal().close();
            } catch (Exception ignored) {
            }
            try {
                follower3.node().close();
            } catch (Exception ignored) {
            }
            try {
                follower3.wal().close();
            } catch (Exception ignored) {
            }
            deleteRecursively(leaderDir);
            deleteRecursively(follower2.dir());
            deleteRecursively(follower3.dir());
        }
    }

    private static void waitForLeader(RaftNode node, long maxWaitMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.isLeader()) {
                return;
            }
            Thread.sleep(50);
        }
        fail("Timed out waiting for leader election");
    }

    private static void waitForLeaderHint(RaftNode node, String leaderId, long maxWaitMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (System.currentTimeMillis() < deadline) {
            if (leaderId.equals(node.snapshotStatus().leaderId())) {
                return;
            }
            Thread.sleep(50);
        }
        fail("Timed out waiting for follower to learn leader " + leaderId);
    }

    private static void waitForIntentStatus(IntentStore store, String intentId, IntentStatus status, long maxWaitMs)
        throws InterruptedException {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (System.currentTimeMillis() < deadline) {
            Intent intent = store.findById(intentId);
            if (intent != null && intent.getStatus() == status) {
                return;
            }
            Thread.sleep(50);
        }
        fail("Timed out waiting for intent " + intentId + " to reach status " + status);
    }

    private static void waitForIntentTag(IntentStore store, String intentId, String key, String value, long maxWaitMs)
        throws InterruptedException {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (System.currentTimeMillis() < deadline) {
            Intent intent = store.findById(intentId);
            if (intent != null && intent.getTags() != null && value.equals(intent.getTags().get(key))) {
                return;
            }
            Thread.sleep(50);
        }
        fail("Timed out waiting for intent " + intentId + " tag " + key + "=" + value);
    }

    private static void assertWriteRejected(HttpErrorResponse error, String operation, String intentId,
                                            String expectedLeaderId) {
        assertEquals(503, error.status());
        assertEquals("50302", error.error().code());
        assertEquals("Write must be served by the Raft leader", error.error().message());
        assertEquals(Boolean.TRUE, error.error().details().get("retryable"));
        assertEquals(operation, error.error().details().get("operation"));
        assertTrue(error.error().details().containsKey("leaderId"));
        if (expectedLeaderId != null) {
            assertEquals(expectedLeaderId, error.error().details().get("leaderId"));
        }
        if (intentId != null) {
            assertEquals(intentId, error.error().details().get("intentId"));
        }
    }

    private static TestNode createFollowerNode(String nodeId, List<String> peers, int port,
                                               long electionMinMs, long electionMaxMs) throws Exception {
        Path dir = Files.createTempDirectory("raft-write-" + nodeId + "-");
        WalConfig cfg = new WalConfig(dir.toString(), 1, "batch", 100, false, "memory_segment",
            1, 8, 64, 10, 4, 1, false);
        SimpleWalWriter wal = new SimpleWalWriter(cfg, "raft-write-test");
        IntentStore store = new ConcurrentIntentStore();
        RaftTransport transport = new RaftTransport(nodeId);
        transport.listen("127.0.0.1", port);
        RaftNode node = new RaftNode(new RaftConfig(nodeId, peers, dir.toString(), electionMinMs, electionMaxMs, 50),
            wal, store, transport);
        return new TestNode(node, wal, store, transport, dir);
    }

    private static int allocatePort() throws IOException {
        try (ServerSocket ss = new ServerSocket(0)) {
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        }
    }

    private static void deleteRecursively(Path dir) {
        if (dir == null) {
            return;
        }
        try {
            Files.walk(dir)
                .sorted((a, b) -> b.getNameCount() - a.getNameCount())
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (Exception ignored) {
                    }
                });
        } catch (Exception ignored) {
        }
    }

    private record TestNode(RaftNode node, SimpleWalWriter wal, IntentStore store, RaftTransport transport, Path dir) {}
}

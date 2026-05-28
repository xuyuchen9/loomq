package com.loomq.http.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.loomq.LoomqEngine;
import com.loomq.api.IntentResponse;
import com.loomq.callback.NettyHttpDeliveryHandler;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.common.RaftRole;
import com.loomq.spi.RaftStatusProvider;
import com.loomq.common.RaftStatusSnapshot;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IntentHandlerRaftReadTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Follower reads should return a retryable leader hint")
    void followerReadShouldReturnLeaderHint() throws Exception {
        LoomqEngine engine = LoomqEngine.builder()
            .nodeId("node-2")
            .walDir(tempDir.resolve("follower-read"))
            .deliveryHandler(new NettyHttpDeliveryHandler())
            .build();

        try {
            IntentHandler handler = new IntentHandler(engine,
                new FixedRaftStatusProvider(true, RaftRole.FOLLOWER, "node-1", false));

            Object result = handler.getIntent(null, "/v1/intents/intent-1", new byte[0],
                Map.of(), Map.of("intentId", "intent-1"));

            HttpErrorResponse error = assertInstanceOf(HttpErrorResponse.class, result);
            assertEquals(503, error.status());
            assertEquals("50301", error.error().code());
            assertEquals("Read must be served by the Raft leader", error.error().message());
            assertNotNull(error.error().details());
            assertEquals(Boolean.TRUE, error.error().details().get("retryable"));
            assertEquals("FOLLOWER", error.error().details().get("nodeRole"));
            assertEquals("node-1", error.error().details().get("leaderId"));
        } finally {
            try {
                engine.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    @DisplayName("Leader reads should still return the current intent")
    void leaderReadShouldReturnIntent() throws Exception {
        LoomqEngine engine = LoomqEngine.builder()
            .nodeId("node-1")
            .walDir(tempDir.resolve("leader-read"))
            .deliveryHandler(new NettyHttpDeliveryHandler())
            .build();

        try {
            Intent intent = new Intent("intent-1");
            intent.setExecuteAt(Instant.now().plusSeconds(5));
            intent.setDeadline(Instant.now().plusSeconds(60));
            intent.setPrecisionTier(PrecisionTier.STANDARD);
            intent.transitionTo(IntentStatus.SCHEDULED);
            engine.getIntentStore().save(intent);

            IntentHandler handler = new IntentHandler(engine,
                new FixedRaftStatusProvider(true, RaftRole.LEADER, "node-1", true));

            Object result = handler.getIntent(null, "/v1/intents/intent-1", new byte[0],
                Map.of(), Map.of("intentId", "intent-1"));

            IntentResponse response = assertInstanceOf(IntentResponse.class, result);
            assertEquals("intent-1", response.intentId());
            assertEquals(IntentStatus.SCHEDULED, response.status());
        } finally {
            try {
                engine.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    @DisplayName("Leader reads should be rejected when the read lease expires")
    void leaderReadShouldReturnLeaderHintWhenLeaseExpired() throws Exception {
        LoomqEngine engine = LoomqEngine.builder()
            .nodeId("node-1")
            .walDir(tempDir.resolve("leader-read-expired"))
            .deliveryHandler(new NettyHttpDeliveryHandler())
            .build();

        try {
            Intent intent = new Intent("intent-lease-expired");
            intent.setExecuteAt(Instant.now().plusSeconds(5));
            intent.setDeadline(Instant.now().plusSeconds(60));
            intent.setPrecisionTier(PrecisionTier.STANDARD);
            intent.transitionTo(IntentStatus.SCHEDULED);
            engine.getIntentStore().save(intent);

            IntentHandler handler = new IntentHandler(engine,
                new FixedRaftStatusProvider(true, RaftRole.LEADER, "node-1", false));

            Object result = handler.getIntent(null, "/v1/intents/intent-lease-expired", new byte[0],
                Map.of(), Map.of("intentId", "intent-lease-expired"));

            HttpErrorResponse error = assertInstanceOf(HttpErrorResponse.class, result);
            assertEquals(503, error.status());
            assertEquals("50301", error.error().code());
            assertEquals("Read must be served by the Raft leader", error.error().message());
            assertNotNull(error.error().details());
            assertEquals(Boolean.TRUE, error.error().details().get("retryable"));
            assertEquals("LEADER", error.error().details().get("nodeRole"));
            assertEquals("node-1", error.error().details().get("leaderId"));
        } finally {
            try {
                engine.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static final class FixedRaftStatusProvider implements RaftStatusProvider {
        private final boolean enabled;
        private final RaftRole role;
        private final String leaderId;
        private final boolean canServeReads;

        private FixedRaftStatusProvider(boolean enabled, RaftRole role, String leaderId, boolean canServeReads) {
            this.enabled = enabled;
            this.role = role;
            this.leaderId = leaderId;
            this.canServeReads = canServeReads;
        }

        @Override
        public boolean isRaftEnabled() {
            return enabled;
        }

        @Override
        public RaftRole role() {
            return role;
        }

        @Override
        public String currentLeaderId() {
            return leaderId;
        }

        @Override
        public boolean canServeLinearizableRead() {
            return canServeReads;
        }

        @Override
        public RaftStatusSnapshot snapshotStatus() {
            Map<String, Boolean> peers = new LinkedHashMap<>();
            return new RaftStatusSnapshot(
                "node-1",
                role,
                leaderId,
                1,
                0,
                0,
                0,
                0,
                0,
                0,
                peers
            );
        }
    }
}

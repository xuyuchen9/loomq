package com.loomq.channel.grpc.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.LoomqEngine;
import com.loomq.channel.grpc.config.GrpcConfig;
import com.loomq.grpc.gen.CallbackMessage;
import com.loomq.grpc.gen.CancelIntentRequest;
import com.loomq.grpc.gen.CreateIntentRequest;
import com.loomq.grpc.gen.FireNowRequest;
import com.loomq.grpc.gen.GetIntentRequest;
import com.loomq.grpc.gen.HealthCheckRequest;
import com.loomq.grpc.gen.IntentActionResponse;
import com.loomq.grpc.gen.IntentMessage;
import com.loomq.grpc.gen.ListIntentsRequest;
import com.loomq.grpc.gen.ListIntentsResponse;
import com.loomq.grpc.gen.LoomQServiceGrpc;
import com.loomq.grpc.gen.PatchIntentRequest;
import com.loomq.spi.DeliveryHandler;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag("integration")
class GrpcIntegrationTest {

    @TempDir
    Path tempDir;

    private LoomqEngine engine;
    private GlobalIntentObserver globalObserver;
    private LoomqGrpcServer grpcServer;
    private ManagedChannel channel;
    private LoomQServiceGrpc.LoomQServiceBlockingStub stub;

    @BeforeEach
    void setUp() throws Exception {
        engine = LoomqEngine.builder()
            .nodeId("grpc-integration-node")
            .walDir(tempDir.resolve("grpc-int"))
            .deliveryHandler((DeliveryHandler) intent ->
                java.util.concurrent.CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.DEAD_LETTER))
            .build();
        engine.start();

        globalObserver = new GlobalIntentObserver();
        engine.registerObserver(globalObserver);

        GrpcConfig config = new GrpcConfig(true, "127.0.0.1", 0, 4 * 1024 * 1024, 1, 0, true, 1024, 1024 * 1024, 60);
        grpcServer = new LoomqGrpcServer(config, engine, null, null, globalObserver);
        grpcServer.start();

        channel = NettyChannelBuilder
            .forAddress("127.0.0.1", grpcServer.getPort())
            .usePlaintext()
            .build();
        stub = LoomQServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.shutdown();
            try { channel.awaitTermination(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        }
        if (grpcServer != null) {
            try { grpcServer.stop(); } catch (Exception ignored) {}
        }
        if (globalObserver != null) globalObserver.close();
        if (engine != null) {
            try { engine.close(); } catch (Exception ignored) {}
        }
    }

    @Test
    @DisplayName("Create and get intent via gRPC")
    void shouldCreateAndGetIntent() {
        Instant executeAt = Instant.now().plus(60, ChronoUnit.SECONDS);
        Instant deadline = executeAt.plus(5, ChronoUnit.MINUTES);

        IntentMessage created = stub.createIntent(CreateIntentRequest.newBuilder()
            .setIntentId("grpc-create-1")
            .setExecuteAt(com.loomq.channel.grpc.converter.ProtoConverter.toProto(executeAt))
            .setDeadline(com.loomq.channel.grpc.converter.ProtoConverter.toProto(deadline))
            .setShardKey("orders")
            .setPrecisionTier("FAST")
            .setCallback(CallbackMessage.newBuilder().setUrl("http://localhost/hook").build())
            .build());

        assertEquals("grpc-create-1", created.getIntentId());
        assertEquals("SCHEDULED", created.getStatus());

        IntentMessage fetched = stub.getIntent(GetIntentRequest.newBuilder()
            .setIntentId("grpc-create-1")
            .build());

        assertEquals("grpc-create-1", fetched.getIntentId());
        assertEquals("SCHEDULED", fetched.getStatus());
    }

    @Test
    @DisplayName("Patch intent via gRPC")
    void shouldPatchIntent() {
        IntentMessage created = stub.createIntent(buildRequest("grpc-patch-1"));

        IntentMessage patched = stub.patchIntent(PatchIntentRequest.newBuilder()
            .setIntentId("grpc-patch-1")
            .setExpiredAction("DEAD_LETTER")
            .putTags("updated", "true")
            .build());

        assertEquals("grpc-patch-1", patched.getIntentId());
    }

    @Test
    @DisplayName("Cancel intent via gRPC")
    void shouldCancelIntent() {
        IntentMessage created = stub.createIntent(buildRequest("grpc-cancel-1"));

        IntentMessage canceled = stub.cancelIntent(CancelIntentRequest.newBuilder()
            .setIntentId("grpc-cancel-1")
            .build());

        assertEquals("grpc-cancel-1", canceled.getIntentId());
        assertEquals("CANCELED", canceled.getStatus());
    }

    @Test
    @DisplayName("Fire now via gRPC")
    void shouldFireNowIntent() {
        IntentMessage created = stub.createIntent(buildRequest("grpc-fire-1"));

        IntentActionResponse response = stub.fireNow(FireNowRequest.newBuilder()
            .setIntentId("grpc-fire-1")
            .build());

        assertEquals("grpc-fire-1", response.getIntentId());
        assertEquals("DISPATCHING", response.getStatus());
    }

    @Test
    @DisplayName("List intents via gRPC")
    void shouldListIntents() {
        stub.createIntent(buildRequest("grpc-list-1"));

        ListIntentsResponse response = stub.listIntents(ListIntentsRequest.newBuilder()
            .setStatus("SCHEDULED")
            .setLimit(10)
            .build());

        assertTrue(response.getTotal() >= 1);
    }

    @Test
    @DisplayName("Health check via gRPC")
    void shouldReturnHealth() {
        var response = stub.healthCheck(HealthCheckRequest.newBuilder().build());
        assertNotNull(response.getStatus());
    }

    @Test
    @DisplayName("Get non-existent intent should throw NOT_FOUND")
    void shouldThrowNotFoundForMissingIntent() {
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class,
            () -> stub.getIntent(GetIntentRequest.newBuilder()
                .setIntentId("nonexistent")
                .build()));

        assertEquals(Status.NOT_FOUND.getCode(), ex.getStatus().getCode());
    }

    private CreateIntentRequest buildRequest(String intentId) {
        Instant executeAt = Instant.now().plus(60, ChronoUnit.SECONDS);
        return CreateIntentRequest.newBuilder()
            .setIntentId(intentId)
            .setExecuteAt(com.loomq.channel.grpc.converter.ProtoConverter.toProto(executeAt))
            .setDeadline(com.loomq.channel.grpc.converter.ProtoConverter.toProto(executeAt.plus(5, ChronoUnit.MINUTES)))
            .setShardKey("test-shard")
            .setPrecisionTier("STANDARD")
            .setCallback(CallbackMessage.newBuilder().setUrl("http://localhost/hook").build())
            .build();
    }
}

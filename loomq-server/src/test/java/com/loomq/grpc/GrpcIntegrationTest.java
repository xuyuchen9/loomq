package com.loomq.grpc;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.LoomqEngine;
import com.loomq.grpc.config.GrpcConfig;
import com.loomq.grpc.converter.ProtoConverter;
import com.loomq.grpc.gen.CancelIntentRequest;
import com.loomq.grpc.gen.CreateIntentRequest;
import com.loomq.grpc.gen.FireNowRequest;
import com.loomq.grpc.gen.GetIntentRequest;
import com.loomq.grpc.gen.ListIntentsRequest;
import com.loomq.grpc.gen.LoomQServiceGrpc;
import com.loomq.grpc.gen.PatchIntentRequest;
import com.loomq.spi.DeliveryHandler;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Files;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * End-to-end integration test for the gRPC server.
 *
 * <p>Starts a real {@link LoomqEngine} and {@link LoomqGrpcServer},
 * then exercises the full gRPC API through a client channel.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GrpcIntegrationTest {

    private LoomqEngine engine;
    private LoomqGrpcServer grpcServer;
    private GlobalIntentObserver observer;
    private ManagedChannel channel;
    private LoomQServiceGrpc.LoomQServiceBlockingStub stub;
    private java.nio.file.Path walDir;

    @BeforeAll
    void setUp() throws Exception {
        walDir = Files.createTempDirectory("loomq-grpc-it");
        observer = new GlobalIntentObserver();

        DeliveryHandler noopHandler = intent -> java.util.concurrent.CompletableFuture.completedFuture(
            DeliveryHandler.DeliveryResult.SUCCESS);
        engine = LoomqEngine.builder()
            .nodeId("grpc-it")
            .walDir(walDir)
            .deliveryHandler(noopHandler)
            .build();
        engine.registerObserver(observer);

        var config = new GrpcConfig(true, "127.0.0.1", 0, 4 * 1024 * 1024, 1, 0);
        grpcServer = new LoomqGrpcServer(config, engine, null, null, observer);
        engine.start();
        grpcServer.start();

        channel = NettyChannelBuilder.forAddress("127.0.0.1", grpcServer.getPort())
            .usePlaintext()
            .build();
        stub = LoomQServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    void tearDown() throws Exception {
        try { grpcServer.stop(); } catch (Exception ignored) {}
        try { channel.shutdown(); channel.awaitTermination(5, TimeUnit.SECONDS); } catch (Exception ignored) {}
        try { engine.close(); } catch (Exception ignored) {}
        if (walDir != null) {
            try (var paths = Files.walk(walDir)) {
                paths.sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> { try { Files.delete(p); } catch (Exception ignored) {} });
            } catch (Exception ignored) {}
        }
    }

    @Test
    void shouldCreateAndGetIntent() {
        var createReq = CreateIntentRequest.newBuilder()
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("it-shard")
            .setPrecisionTier("FAST")
            .build();

        var created = stub.createIntent(createReq);
        assertNotNull(created);
        assertNotNull(created.getIntentId());
        assertEquals("SCHEDULED", created.getStatus());
        assertEquals("FAST", created.getPrecisionTier());

        var fetched = stub.getIntent(GetIntentRequest.newBuilder()
            .setIntentId(created.getIntentId()).build());
        assertNotNull(fetched);
        assertEquals(created.getIntentId(), fetched.getIntentId());
    }

    @Test
    void shouldPatchIntent() {
        var createReq = CreateIntentRequest.newBuilder()
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("it-shard")
            .build();

        var created = stub.createIntent(createReq);
        String intentId = created.getIntentId();
        String newTtl = "DEAD_LETTER";

        var patched = stub.patchIntent(PatchIntentRequest.newBuilder()
            .setIntentId(intentId)
            .setExpiredAction(newTtl)
            .build());
        assertEquals(newTtl, patched.getExpiredAction());
    }

    @Test
    void shouldCancelIntent() {
        var createReq = CreateIntentRequest.newBuilder()
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("it-shard")
            .build();

        var created = stub.createIntent(createReq);
        var cancelled = stub.cancelIntent(CancelIntentRequest.newBuilder()
            .setIntentId(created.getIntentId()).build());
        assertEquals("CANCELED", cancelled.getStatus());
    }

    @Test
    void shouldFireNowIntent() {
        var createReq = CreateIntentRequest.newBuilder()
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("it-shard")
            .build();
        var created = stub.createIntent(createReq);

        var result = stub.fireNow(FireNowRequest.newBuilder()
            .setIntentId(created.getIntentId()).build());
        assertNotNull(result);
        assertEquals(created.getIntentId(), result.getIntentId());
        assertEquals("DISPATCHING", result.getStatus());
    }

    @Test
    void shouldListIntents() {
        // Create an intent so the list is non-empty
        var createReq = CreateIntentRequest.newBuilder()
            .setExecuteAt(ProtoConverter.toProto(Instant.now().plusSeconds(60)))
            .setDeadline(ProtoConverter.toProto(Instant.now().plusSeconds(120)))
            .setShardKey("list-test")
            .build();
        var created = stub.createIntent(createReq);

        var list = stub.listIntents(ListIntentsRequest.newBuilder()
            .setStatus("SCHEDULED")
            .setLimit(10)
            .build());
        assertNotNull(list);
        assertTrue(list.getTotal() >= 1);
        assertTrue(list.getIntentsList().stream()
            .anyMatch(i -> i.getIntentId().equals(created.getIntentId())));
    }

    @Test
    void shouldReturnHealth() {
        var health = stub.healthCheck(com.loomq.grpc.gen.HealthCheckRequest.newBuilder().build());
        assertNotNull(health);
        assertNotNull(health.getStatus());
    }

    @Test
    void shouldThrowNotFoundForMissingIntent() {
        assertThrows(StatusRuntimeException.class, () ->
            stub.getIntent(GetIntentRequest.newBuilder().setIntentId("no-such-intent").build()));
    }
}

package com.loomq.benchmark;

import com.loomq.benchmark.framework.ProtocolBenchmark;
import com.loomq.channel.grpc.converter.ProtoConverter;
import com.loomq.grpc.gen.CallbackMessage;
import com.loomq.grpc.gen.CreateIntentRequest;
import com.loomq.grpc.gen.IntentMessage;
import com.loomq.grpc.gen.LoomQServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * gRPC 层性能测试。
 *
 * <p>支持三种 stub 模式对比：
 * <ul>
 *   <li>{@code blocking}（默认）——虚拟线程 + 同步阻塞调用</li>
 *   <li>{@code future}——ListenableFuture，测量纯 gRPC 协议延迟</li>
 *   <li>{@code async}——StreamObserver 回调，最高吞吐</li>
 * </ul>
 *
 * <p>通过 {@code -Dloomq.benchmark.grpc.stub=blocking|future|async} 切换。
 */
public class GrpcVirtualThreadBenchmark extends ProtocolBenchmark {

    private static final String GRPC_HOST = System.getProperty("loomq.benchmark.grpc.host", "localhost");
    private static final int GRPC_PORT = Integer.getInteger("loomq.benchmark.grpc.port", 7929);
    private static final String STUB_TYPE = System.getProperty("loomq.benchmark.grpc.stub", "blocking");

    private static final ManagedChannel channel = NettyChannelBuilder
        .forAddress(GRPC_HOST, GRPC_PORT)
        .usePlaintext()
        .maxInboundMessageSize(4 * 1024 * 1024)
        .build();

    private static final LoomQServiceGrpc.LoomQServiceBlockingStub blockingStub =
        LoomQServiceGrpc.newBlockingStub(channel);
    private static final LoomQServiceGrpc.LoomQServiceFutureStub futureStub =
        LoomQServiceGrpc.newFutureStub(channel);
    private static final LoomQServiceGrpc.LoomQServiceStub asyncStub =
        LoomQServiceGrpc.newStub(channel);

    public GrpcVirtualThreadBenchmark() {
        super("gRPC / Protobuf (" + STUB_TYPE + ")", GRPC_HOST + ":" + GRPC_PORT);
    }

    @Override
    protected void createIntent() throws Exception {
        CreateIntentRequest request = buildRequest();

        switch (STUB_TYPE) {
            case "future" -> futureStub.createIntent(request).get();
            case "async" -> {
                CompletableFuture<Void> cf = new CompletableFuture<>();
                asyncStub.createIntent(request, new StreamObserver<>() {
                    @Override
                    public void onNext(IntentMessage value) {}
                    @Override
                    public void onError(Throwable t) { cf.completeExceptionally(t); }
                    @Override
                    public void onCompleted() { cf.complete(null); }
                });
                cf.get();
            }
            default -> blockingStub.createIntent(request);
        }
    }

    private static CreateIntentRequest buildRequest() {
        Instant executeAt = Instant.now().plus(3600, ChronoUnit.SECONDS);
        Instant deadline = executeAt.plus(5, ChronoUnit.MINUTES);
        String id = UUID.randomUUID().toString();
        String shardKey = "bench-" + ThreadLocalRandom.current().nextInt(64);

        return CreateIntentRequest.newBuilder()
            .setIntentId(id)
            .setExecuteAt(ProtoConverter.toProto(executeAt))
            .setDeadline(ProtoConverter.toProto(deadline))
            .setPrecisionTier("STANDARD")
            .setShardKey(shardKey)
            .setCallback(CallbackMessage.newBuilder()
                .setUrl("http://localhost:9999/webhook")
                .build())
            .build();
    }

    @Override
    protected void shutdown() {
        channel.shutdown();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws Exception {
        new GrpcVirtualThreadBenchmark().run(args);
    }
}

package com.loomq.raft;

import com.google.protobuf.ByteString;
import com.loomq.raft.grpc.AppendEntriesRequest;
import com.loomq.raft.grpc.AppendEntriesResponse;
import com.loomq.raft.grpc.RaftServiceGrpc;
import com.loomq.raft.grpc.RequestVoteRequest;
import com.loomq.raft.grpc.RequestVoteResponse;
import com.loomq.raft.grpc.SnapshotChunk;
import com.loomq.raft.grpc.SnapshotResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC-based Raft RPC Transport。
 *
 * <p>替换自定义 Netty TCP transport，使用 gRPC HTTP/2 通信。
 * 保留分块 InstallSnapshot 逻辑。
 */
public class GrpcRaftTransport implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(GrpcRaftTransport.class);
    private static final int SNAPSHOT_CHUNK_SIZE = 256 * 1024; // 256KB per chunk

    private final String nodeId;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, RaftServiceGrpc.RaftServiceBlockingStub> blockingStubs = new ConcurrentHashMap<>();
    private final Map<String, RaftServiceGrpc.RaftServiceStub> asyncStubs = new ConcurrentHashMap<>();
    private final ExecutorService rpcExecutor = Executors.newFixedThreadPool(4, r -> {
        Thread t = new Thread(r, "grpc-raft-rpc");
        t.setDaemon(true);
        return t;
    });
    private Server grpcServer;

    // Server-side handlers
    private Function<RaftTransport.RequestVoteMessage, Boolean> onRequestVote;
    private Function<RaftTransport.AppendEntriesMessage, AppendEntriesResult> onAppendEntries;
    private Function<RaftTransport.InstallSnapshotMessage, Long> onInstallSnapshot;
    private Function<RaftTransport.InstallSnapshotChunkMessage, Long> onInstallSnapshotChunk;

    public GrpcRaftTransport(String nodeId) {
        this.nodeId = nodeId;
    }

    public void setOnRequestVote(Function<RaftTransport.RequestVoteMessage, Boolean> h) {
        this.onRequestVote = h;
    }

    public void setOnAppendEntries(Function<RaftTransport.AppendEntriesMessage, AppendEntriesResult> h) {
        this.onAppendEntries = h;
    }

    public void setOnInstallSnapshot(Function<RaftTransport.InstallSnapshotMessage, Long> h) {
        this.onInstallSnapshot = h;
    }

    public void setOnInstallSnapshotChunk(Function<RaftTransport.InstallSnapshotChunkMessage, Long> h) {
        this.onInstallSnapshotChunk = h;
    }

    /** Start gRPC server to listen for incoming Raft RPCs */
    public void listen(String host, int port) {
        try {
            grpcServer = ServerBuilder.forPort(port)
                .addService(new RaftServiceImpl())
                .build()
                .start();
            log.info("GrpcRaftTransport listening on {}:{}", host, port);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to start gRPC server on " + host + ":" + port, e);
        }
    }

    /** Connect to a peer via gRPC */
    public CompletableFuture<Void> connect(String peerId, String host, int port) {
        return CompletableFuture.runAsync(() -> {
            ManagedChannel existing = channels.get(peerId);
            if (existing != null) {
                existing.shutdown();
            }
            ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
            channels.put(peerId, channel);
            blockingStubs.put(peerId, RaftServiceGrpc.newBlockingStub(channel));
            asyncStubs.put(peerId, RaftServiceGrpc.newStub(channel));
            log.info("GrpcRaftTransport connected to {} at {}:{}", peerId, host, port);
        }, rpcExecutor);
    }

    /** Whether the outbound connection to a peer is currently active */
    public boolean isPeerConnected(String peerId) {
        ManagedChannel channel = channels.get(peerId);
        return channel != null && !channel.isShutdown();
    }

    /** Number of currently connected peers */
    public int connectedPeerCount() {
        int connected = 0;
        for (ManagedChannel channel : channels.values()) {
            if (channel != null && !channel.isShutdown()) {
                connected++;
            }
        }
        return connected;
    }

    // ========== Client-side RPCs ==========

    /** Send RequestVote to a peer */
    public CompletableFuture<Boolean> sendRequestVote(String peerId, long epoch, String candidateId,
            long lastLogIndex, long lastLogEpoch) {
        return CompletableFuture.supplyAsync(() -> {
            RaftServiceGrpc.RaftServiceBlockingStub stub = blockingStubs.get(peerId);
            if (stub == null) return false;
            try {
                RequestVoteRequest request = RequestVoteRequest.newBuilder()
                    .setEpoch(epoch)
                    .setCandidateId(candidateId)
                    .setLastLogIndex(lastLogIndex)
                    .setLastLogEpoch(lastLogEpoch)
                    .build();
                RequestVoteResponse response = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .requestVote(request);
                return response.getVoteGranted();
            } catch (Exception e) {
                log.warn("RequestVote to {} failed: {}", peerId, e.getMessage());
                return false;
            }
        }, rpcExecutor);
    }

    /** Send AppendEntries to a peer */
    public CompletableFuture<AppendEntriesResult> sendAppendEntries(String peerId, long epoch,
            String leaderId, long prevLogIndex, long prevLogEpoch, byte[][] entries, long leaderCommit) {
        return CompletableFuture.supplyAsync(() -> {
            RaftServiceGrpc.RaftServiceBlockingStub stub = blockingStubs.get(peerId);
            if (stub == null) return AppendEntriesResult.fail(epoch);
            try {
                AppendEntriesRequest.Builder builder = AppendEntriesRequest.newBuilder()
                    .setEpoch(epoch)
                    .setLeaderId(leaderId)
                    .setPrevLogIndex(prevLogIndex)
                    .setPrevLogEpoch(prevLogEpoch)
                    .setLeaderCommit(leaderCommit);
                if (entries != null) {
                    for (byte[] entry : entries) {
                        if (entry != null) {
                            builder.addEntries(ByteString.copyFrom(entry));
                        }
                    }
                }
                AppendEntriesResponse response = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .appendEntries(builder.build());
                if (response.getSuccess()) {
                    return AppendEntriesResult.success(response.getEpoch(), response.getMatchIndex());
                } else {
                    return response.getConflictIndex() > 0
                        ? AppendEntriesResult.fail(response.getEpoch(), response.getConflictIndex())
                        : AppendEntriesResult.fail(response.getEpoch());
                }
            } catch (Exception e) {
                log.warn("AppendEntries to {} failed: {}", peerId, e.getMessage());
                return AppendEntriesResult.fail(epoch);
            }
        }, rpcExecutor);
    }

    /** Send InstallSnapshot to a lagging follower (streaming chunks) */
    public CompletableFuture<Long> sendInstallSnapshot(String peerId, long epoch, String leaderId,
            long lastIncludedIndex, long lastIncludedEpoch, byte[] snapshotData) {
        return CompletableFuture.supplyAsync(() -> {
            RaftServiceGrpc.RaftServiceStub asyncStub = asyncStubs.get(peerId);
            if (asyncStub == null) return -1L;
            try {
                // Split into chunks
                int totalChunks = (snapshotData.length + SNAPSHOT_CHUNK_SIZE - 1) / SNAPSHOT_CHUNK_SIZE;
                CountDownLatch latch = new CountDownLatch(1);
                final long[] result = {-1L};

                StreamObserver<SnapshotResponse> responseObserver = new StreamObserver<>() {
                    @Override
                    public void onNext(SnapshotResponse response) {
                        result[0] = response.getLastIncludedIndex();
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.warn("InstallSnapshot to {} failed: {}", peerId, t.getMessage());
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                };

                StreamObserver<SnapshotChunk> requestObserver = asyncStub
                    .withDeadlineAfter(60, TimeUnit.SECONDS)
                    .installSnapshot(responseObserver);

                for (int i = 0; i < totalChunks; i++) {
                    int start = i * SNAPSHOT_CHUNK_SIZE;
                    int end = Math.min(start + SNAPSHOT_CHUNK_SIZE, snapshotData.length);
                    byte[] chunkData = new byte[end - start];
                    System.arraycopy(snapshotData, start, chunkData, 0, chunkData.length);

                    SnapshotChunk chunk = SnapshotChunk.newBuilder()
                        .setEpoch(epoch)
                        .setLeaderId(leaderId)
                        .setLastIncludedIndex(lastIncludedIndex)
                        .setLastIncludedEpoch(lastIncludedEpoch)
                        .setChunkIndex(i)
                        .setTotalChunks(totalChunks)
                        .setData(ByteString.copyFrom(chunkData))
                        .build();
                    requestObserver.onNext(chunk);
                }
                requestObserver.onCompleted();

                latch.await(60, TimeUnit.SECONDS);
                return result[0];
            } catch (Exception e) {
                log.warn("InstallSnapshot to {} failed: {}", peerId, e.getMessage());
                return -1L;
            }
        }, rpcExecutor);
    }

    /** Send a single InstallSnapshot chunk (for incremental chunk sending) */
    public CompletableFuture<Boolean> sendInstallSnapshotChunk(String peerId, long epoch, String leaderId,
            long lastIncludedIndex, long lastIncludedEpoch, int chunkIndex, int totalChunks, byte[] chunkData) {
        return CompletableFuture.supplyAsync(() -> {
            RaftServiceGrpc.RaftServiceStub asyncStub = asyncStubs.get(peerId);
            if (asyncStub == null) return false;
            try {
                SnapshotChunk chunk = SnapshotChunk.newBuilder()
                    .setEpoch(epoch)
                    .setLeaderId(leaderId)
                    .setLastIncludedIndex(lastIncludedIndex)
                    .setLastIncludedEpoch(lastIncludedEpoch)
                    .setChunkIndex(chunkIndex)
                    .setTotalChunks(totalChunks)
                    .setData(ByteString.copyFrom(chunkData))
                    .build();

                CountDownLatch latch = new CountDownLatch(1);
                final boolean[] result = {false};

                StreamObserver<SnapshotResponse> responseObserver = new StreamObserver<>() {
                    @Override public void onNext(SnapshotResponse r) { result[0] = r.getLastIncludedIndex() >= 0; }
                    @Override public void onError(Throwable t) { log.warn("Chunk {} failed: {}", chunkIndex, t.getMessage()); latch.countDown(); }
                    @Override public void onCompleted() { latch.countDown(); }
                };

                StreamObserver<SnapshotChunk> requestObserver = asyncStub
                    .withDeadlineAfter(30, TimeUnit.SECONDS)
                    .installSnapshot(responseObserver);
                requestObserver.onNext(chunk);
                requestObserver.onCompleted();

                latch.await(30, TimeUnit.SECONDS);
                return result[0];
            } catch (Exception e) {
                log.warn("sendInstallSnapshotChunk failed: {}", e.getMessage());
                return false;
            }
        }, rpcExecutor);
    }

    @Override
    public void close() {
        if (grpcServer != null) {
            grpcServer.shutdown();
            try {
                if (!grpcServer.awaitTermination(5, TimeUnit.SECONDS)) {
                    grpcServer.shutdownNow();
                }
            } catch (InterruptedException e) {
                grpcServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        channels.values().forEach(channel -> {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        });
        rpcExecutor.shutdown();
        try {
            if (!rpcExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                rpcExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            rpcExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // ========== gRPC Service Implementation ==========

    private class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {
        @Override
        public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver) {
            try {
                if (onRequestVote == null) {
                    responseObserver.onNext(RequestVoteResponse.newBuilder().build());
                    responseObserver.onCompleted();
                    return;
                }
                RaftTransport.RequestVoteMessage msg = new RaftTransport.RequestVoteMessage(
                    request.getEpoch(), request.getCandidateId(),
                    request.getLastLogIndex(), request.getLastLogEpoch());
                Boolean granted = onRequestVote.apply(msg);
                responseObserver.onNext(RequestVoteResponse.newBuilder()
                    .setVoteGranted(granted != null && granted)
                    .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error("RequestVote handler error", e);
                responseObserver.onError(e);
            }
        }

        @Override
        public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responseObserver) {
            try {
                if (onAppendEntries == null) {
                    responseObserver.onNext(AppendEntriesResponse.newBuilder().build());
                    responseObserver.onCompleted();
                    return;
                }
                byte[][] entries = new byte[request.getEntriesList().size()][];
                for (int i = 0; i < entries.length; i++) {
                    entries[i] = request.getEntriesList().get(i).toByteArray();
                }
                RaftTransport.AppendEntriesMessage msg = new RaftTransport.AppendEntriesMessage(
                    request.getEpoch(), request.getLeaderId(),
                    request.getPrevLogIndex(), request.getPrevLogEpoch(),
                    entries, request.getLeaderCommit());
                AppendEntriesResult result = onAppendEntries.apply(msg);
                responseObserver.onNext(AppendEntriesResponse.newBuilder()
                    .setSuccess(result.success)
                    .setMatchIndex(result.matchIndex)
                    .setConflictIndex(result.conflictIndex)
                    .setEpoch(result.epoch)
                    .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error("AppendEntries handler error", e);
                responseObserver.onError(e);
            }
        }

        @Override
        public StreamObserver<SnapshotChunk> installSnapshot(StreamObserver<SnapshotResponse> responseObserver) {
            return new StreamObserver<>() {
                private long lastIndex = -1;

                @Override
                public void onNext(SnapshotChunk chunk) {
                    try {
                        if (onInstallSnapshotChunk != null) {
                            RaftTransport.InstallSnapshotChunkMessage msg = new RaftTransport.InstallSnapshotChunkMessage(
                                chunk.getEpoch(), chunk.getLeaderId(),
                                chunk.getLastIncludedIndex(), chunk.getLastIncludedEpoch(),
                                chunk.getChunkIndex(), chunk.getTotalChunks(),
                                chunk.getData().toByteArray());
                            Long result = onInstallSnapshotChunk.apply(msg);
                            if (result != null && result >= 0) {
                                lastIndex = result;
                            }
                        } else if (onInstallSnapshot != null && chunk.getChunkIndex() == 0) {
                            // Single-chunk fallback
                            RaftTransport.InstallSnapshotMessage msg = new RaftTransport.InstallSnapshotMessage(
                                chunk.getEpoch(), chunk.getLeaderId(),
                                chunk.getLastIncludedIndex(), chunk.getLastIncludedEpoch(),
                                chunk.getData().toByteArray());
                            Long result = onInstallSnapshot.apply(msg);
                            if (result != null) {
                                lastIndex = result;
                            }
                        }
                    } catch (Exception e) {
                        log.error("InstallSnapshot chunk handler error", e);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    log.warn("InstallSnapshot stream error", t);
                }

                @Override
                public void onCompleted() {
                    responseObserver.onNext(SnapshotResponse.newBuilder()
                        .setLastIncludedIndex(lastIndex)
                        .build());
                    responseObserver.onCompleted();
                }
            };
        }
    }
}

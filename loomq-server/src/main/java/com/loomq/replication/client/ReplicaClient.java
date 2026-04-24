package com.loomq.replication.client;

import com.loomq.replication.Ack;
import com.loomq.replication.ReplicationRecord;
import com.loomq.replication.protocol.HeartbeatMessage;
import com.loomq.replication.protocol.ReplicationProtocol;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Replica 客户端（Primary 侧）
 *
 * 维护到 Replica 的连接，异步发送复制记录，接收 ACK
 *
 * @author loomq
 * @since v0.4.8
 */
public class ReplicaClient {

    private static final Logger logger = LoggerFactory.getLogger(ReplicaClient.class);

    // 配置
    private final String nodeId;
    private final String replicaHost;
    private final int replicaPort;
    private final long ackTimeoutMs;
    private final int maxRetries;

    // Netty
    private EventLoopGroup eventLoopGroup;
    private Channel channel;

    // 状态
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AtomicLong lastReplicatedOffset = new AtomicLong(-1);
    private final AtomicLong lastAckedOffset = new AtomicLong(-1);

    // 等待 ACK 的请求
    private final ConcurrentHashMap<Long, CompletableFuture<Ack>> pendingAcks = new ConcurrentHashMap<>();

    // 心跳
    private final AtomicReference<HeartbeatMessage> lastHeartbeat = new AtomicReference<>();
    private volatile long lastHeartbeatTime = 0;

    // 回调
    private Consumer<Ack> ackCallback;
    private Consumer<Throwable> errorCallback;

    public ReplicaClient(String nodeId, String replicaHost, int replicaPort) {
        this(nodeId, replicaHost, replicaPort, 30000, 3);
    }

    public ReplicaClient(String nodeId, String replicaHost, int replicaPort,
                         long ackTimeoutMs, int maxRetries) {
        this.nodeId = nodeId;
        this.replicaHost = replicaHost;
        this.replicaPort = replicaPort;
        this.ackTimeoutMs = ackTimeoutMs;
        this.maxRetries = maxRetries;
    }

    /**
     * 设置 ACK 回调
     */
    public void setAckCallback(Consumer<Ack> callback) {
        this.ackCallback = callback;
    }

    /**
     * 设置错误回调
     */
    public void setErrorCallback(Consumer<Throwable> callback) {
        this.errorCallback = callback;
    }

    /**
     * 连接到 Replica
     */
    public CompletableFuture<Void> connect() {
        if (shutdown.get()) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Client is shutdown"));
        }

        return CompletableFuture.runAsync(() -> {
            eventLoopGroup = new NioEventLoopGroup();

            try {
                Bootstrap bootstrap = new Bootstrap();
                bootstrap.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();

                            // 心跳：写超时（定期发送心跳）
                            pipeline.addLast(new IdleStateHandler(
                                0, 2, 0));  // 每 2 秒发送一次心跳

                            // 协议编解码
                            pipeline.addLast(new ReplicationProtocol());

                            // 业务处理器
                            pipeline.addLast(new ClientHandler());
                        }
                    });

                // 连接
                ChannelFuture future = bootstrap.connect(replicaHost, replicaPort).sync();
                channel = future.channel();
                connected.set(true);

                logger.info("Connected to replica at {}:{}", replicaHost, replicaPort);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Connection interrupted", e);
            } catch (Exception e) {
                // Netty Bootstrap.connect() 可抛多种 checked exception (ConnectException, UnknownHostException 等)
                // 统一 wrap 为 RuntimeException 以适配 CompletableFuture.runAsync 的签名
                throw new RuntimeException("Failed to connect to replica", e);
            }
        });
    }

    /**
     * 发送复制记录
     *
     * @return CompletableFuture 在收到 ACK 或超时后完成
     */
    public CompletableFuture<Ack> send(ReplicationRecord record) {
        if (!connected.get() || channel == null || !channel.isActive()) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Not connected to replica"));
        }

        long offset = record.getOffset();

        // 创建等待 ACK 的 future
        CompletableFuture<Ack> future = new CompletableFuture<>();
        pendingAcks.put(offset, future);

        // 设置超时
        future.orTimeout(ackTimeoutMs, TimeUnit.MILLISECONDS)
            .whenComplete((ack, ex) -> {
                if (ex != null) {
                    pendingAcks.remove(offset);
                    if (!future.isDone()) {
                        future.complete(Ack.timeout(offset));
                    }
                }
            });

        // 发送记录
        channel.writeAndFlush(record).addListener(writeFuture -> {
            if (!writeFuture.isSuccess()) {
                pendingAcks.remove(offset);
                future.completeExceptionally(writeFuture.cause());
            }
        });

        // 更新最后复制 offset
        lastReplicatedOffset.set(offset);

        logger.debug("Sent replication record: offset={}, type={}",
            offset, record.getType());

        return future;
    }

    /**
     * 断开连接
     */
    public void disconnect() {
        if (connected.compareAndSet(true, false)) {
            logger.info("Disconnecting from replica...");

            if (channel != null) {
                channel.close();
            }

            if (eventLoopGroup != null) {
                eventLoopGroup.shutdownGracefully();
            }

            // 完成所有待处理的请求
            pendingAcks.forEach((offset, future) -> {
                if (!future.isDone()) {
                    future.completeExceptionally(
                        new IllegalStateException("Connection closed"));
                }
            });
            pendingAcks.clear();

            logger.info("Disconnected from replica");
        }
    }

    /**
     * 完全关闭
     */
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            disconnect();
        }
    }

    /**
     * 检查是否已连接
     */
    public boolean isConnected() {
        return connected.get() && channel != null && channel.isActive();
    }

    /**
     * 获取最后复制的 offset
     */
    public long getLastReplicatedOffset() {
        return lastReplicatedOffset.get();
    }

    /**
     * 获取最后确认（ACK）的 offset
     */
    public long getLastAckedOffset() {
        return lastAckedOffset.get();
    }

    /**
     * 计算复制延迟（已复制但未确认的 offset 数量）
     */
    public long getReplicationLag() {
        return lastReplicatedOffset.get() - lastAckedOffset.get();
    }

    /**
     * 获取最后收到的 replica 心跳
     */
    public HeartbeatMessage getLastHeartbeat() {
        return lastHeartbeat.get();
    }

    /**
     * 获取最后心跳时间
     */
    public long getLastHeartbeatTime() {
        return lastHeartbeatTime;
    }

    // ==================== 内部处理器 ====================

    private class ClientHandler extends SimpleChannelInboundHandler<Object> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof Ack) {
                handleAck((Ack) msg);
            } else if (msg instanceof HeartbeatMessage) {
                handleHeartbeat((HeartbeatMessage) msg);
            }
        }

        private void handleAck(Ack ack) {
            logger.debug("Received ACK: offset={}, status={}",
                ack.getOffset(), ack.getStatus());

            // 更新最后确认 offset
            if (ack.isSuccess()) {
                lastAckedOffset.updateAndGet(current ->
                    Math.max(current, ack.getOffset()));
            }

            // 完成对应的 future
            CompletableFuture<Ack> future = pendingAcks.remove(ack.getOffset());
            if (future != null && !future.isDone()) {
                future.complete(ack);
            }

            // 回调
            if (ackCallback != null) {
                ackCallback.accept(ack);
            }
        }

        private void handleHeartbeat(HeartbeatMessage heartbeat) {
            logger.debug("Received heartbeat from replica: nodeId={}, offset={}",
                heartbeat.getNodeId(), heartbeat.getLastAppliedOffset());

            lastHeartbeat.set(heartbeat);
            lastHeartbeatTime = System.currentTimeMillis();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state() == IdleState.WRITER_IDLE) {
                    // 发送心跳
                    HeartbeatMessage heartbeat = new HeartbeatMessage(
                        nodeId,
                        "PRIMARY",
                        "ACTIVE",
                        lastReplicatedOffset.get(),
                        Instant.now().toEpochMilli()
                    );
                    ctx.writeAndFlush(heartbeat);
                    logger.debug("Sent heartbeat to replica");
                }
            }
            super.userEventTriggered(ctx, evt);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            logger.warn("Connection to replica closed");
            connected.set(false);

            // 通知错误
            if (errorCallback != null) {
                errorCallback.accept(new IllegalStateException("Connection closed"));
            }

            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("Exception in replica client", cause);

            if (errorCallback != null) {
                errorCallback.accept(cause);
            }

            ctx.close();
        }
    }
}

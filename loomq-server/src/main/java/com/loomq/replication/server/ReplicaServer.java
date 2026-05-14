package com.loomq.replication.server;

import com.loomq.replication.Ack;
import com.loomq.replication.ReplicationRecord;
import com.loomq.replication.protocol.HeartbeatMessage;
import com.loomq.replication.protocol.ReplicationProtocol;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Replica 服务器
 *
 * 接收来自 Primary 的复制数据，写入本地 WAL，返回 ACK
 *
 * @author loomq
 * @since v0.4.8
 */
public class ReplicaServer {

    private static final Logger logger = LoggerFactory.getLogger(ReplicaServer.class);
    private static final int BOSS_THREADS = 1;
    private static final int WORKER_THREADS = 1;

    // 配置
    private final String nodeId;
    private final String bindHost;
    private final int bindPort;
    private final long heartbeatTimeoutMs;

    // Netty
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    // 状态
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicReference<Channel> primaryChannel = new AtomicReference<>();
    private final AtomicLong lastHeartbeatTime = new AtomicLong(0);

    // 处理器
    private Consumer<ReplicationRecord> recordHandler;
    private java.util.function.Function<ReplicationRecord, Ack> ackHandler;
    private Consumer<HeartbeatMessage> heartbeatHandler;

    public ReplicaServer(String nodeId, String bindHost, int bindPort) {
        this(nodeId, bindHost, bindPort, 5000);
    }

    public ReplicaServer(String nodeId, String bindHost, int bindPort, long heartbeatTimeoutMs) {
        this.nodeId = nodeId;
        this.bindHost = bindHost;
        this.bindPort = bindPort;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    }

    /**
     * 设置记录处理器（普通模式：自动发送 Ack.success）
     */
    public void setRecordHandler(Consumer<ReplicationRecord> handler) {
        this.recordHandler = handler;
    }

    /**
     * 设置 Ack 感知的记录处理器（Raft 模式：处理器返回自定义 Ack）
     * 设置后，handleReplicationRecord 将使用此处理器的返回值作为 Ack，
     * 而非自动生成 Ack.success。
     */
    public void setAckHandler(java.util.function.Function<ReplicationRecord, Ack> handler) {
        this.ackHandler = handler;
    }

    /**
     * 设置心跳处理器
     */
    public void setHeartbeatHandler(Consumer<HeartbeatMessage> handler) {
        this.heartbeatHandler = handler;
    }

    /**
     * 启动服务器
     *
     * 返回的 future 会在服务端完成 bind、开始接受连接后完成，
     * 服务则会继续运行直到调用 shutdown()。
     */
    public CompletableFuture<Void> start() {
        if (started.compareAndSet(false, true)) {
            CompletableFuture<Void> startedFuture = new CompletableFuture<>();
            Thread startupThread = new Thread(() -> doStart(startedFuture),
                "replica-server-" + nodeId + "-" + bindPort);
            startupThread.setDaemon(true);
            startupThread.start();
            return startedFuture;
        }
        return CompletableFuture.completedFuture(null);
    }

    private void doStart(CompletableFuture<Void> startedFuture) {
        // Raft RPC traffic is tiny; keep the event loop footprint small so test
        // runners do not spend memory/CPU on idle Netty workers.
        bossGroup = new NioEventLoopGroup(BOSS_THREADS);
        workerGroup = new NioEventLoopGroup(WORKER_THREADS);

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();

                        // 心跳检测：读超时
                        pipeline.addLast(new IdleStateHandler(
                            (int) (heartbeatTimeoutMs / 1000), 0, 0));

                        // 协议编解码
                        pipeline.addLast(new ReplicationProtocol());

                        // 业务处理器
                        pipeline.addLast(new ReplicaHandler());
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture future = bootstrap.bind(bindHost, bindPort).sync();
            serverChannel = future.channel();

            logger.info("ReplicaServer started on {}:{}", bindHost, bindPort);
            startedFuture.complete(null);

            // 等待关闭
            serverChannel.closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("ReplicaServer interrupted");
            if (!startedFuture.isDone()) {
                startedFuture.completeExceptionally(e);
            }
        } catch (RuntimeException | Error e) {
            logger.error("ReplicaServer failed to start", e);
            if (!startedFuture.isDone()) {
                startedFuture.completeExceptionally(e);
            }
            throw e;
        } finally {
            shutdown();
        }
    }

    /**
     * 关闭服务器
     */
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            logger.info("Shutting down ReplicaServer...");

            if (primaryChannel.get() != null) {
                primaryChannel.get().close();
            }

            if (serverChannel != null) {
                serverChannel.close();
            }

            if (bossGroup != null) {
                bossGroup.shutdownGracefully();
            }

            if (workerGroup != null) {
                workerGroup.shutdownGracefully();
            }

            logger.info("ReplicaServer shutdown complete");
        }
    }

    /**
     * 主动发送一个 Ack 到 Primary（供 RaftTransport 等自定义响应使用）。
     */
    public void sendAck(Ack ack) {
        Channel ch = primaryChannel.get();
        if (ch != null && ch.isActive()) {
            ch.writeAndFlush(ack);
        }
    }

    /**
     * 检查是否已连接到 Primary
     */
    public boolean isConnectedToPrimary() {
        Channel ch = primaryChannel.get();
        return ch != null && ch.isActive();
    }

    /**
     * 获取最后心跳时间
     */
    public long getLastHeartbeatTime() {
        return lastHeartbeatTime.get();
    }

    /**
     * 检查心跳是否超时
     */
    public boolean isHeartbeatTimeout() {
        long last = lastHeartbeatTime.get();
        if (last == 0) {
            return false;  // 尚未收到心跳
        }
        return System.currentTimeMillis() - last > heartbeatTimeoutMs;
    }

    // ==================== 内部处理器 ====================

    private class ReplicaHandler extends SimpleChannelInboundHandler<Object> {

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            logger.info("Primary connected from {}", ctx.channel().remoteAddress());
            primaryChannel.set(ctx.channel());
            super.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            logger.warn("Primary disconnected from {}", ctx.channel().remoteAddress());
            primaryChannel.compareAndSet(ctx.channel(), null);
            super.channelInactive(ctx);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof ReplicationRecord) {
                handleReplicationRecord(ctx, (ReplicationRecord) msg);
            } else if (msg instanceof HeartbeatMessage) {
                handleHeartbeat(ctx, (HeartbeatMessage) msg);
            }
        }

        private void handleReplicationRecord(ChannelHandlerContext ctx, ReplicationRecord record) {
            logger.debug("Received replication record: offset={}, type={}",
                record.getOffset(), record.getType());

            try {
                Ack ack = null;
                if (ackHandler != null) {
                    ack = ackHandler.apply(record);
                }
                if (ack == null) {
                    if (recordHandler != null) {
                        recordHandler.accept(record);
                    }
                    ack = Ack.success(record.getOffset());
                }
                ctx.writeAndFlush(ack);
                logger.debug("Sent ACK for offset={}", record.getOffset());
            } catch (RuntimeException e) {
                logger.error("Failed to process replication record: offset={}",
                    record.getOffset(), e);
                Ack ack = Ack.failed(record.getOffset(), e.getMessage());
                ctx.writeAndFlush(ack);
            }
        }

        private void handleHeartbeat(ChannelHandlerContext ctx, HeartbeatMessage heartbeat) {
            logger.debug("Received heartbeat from {}: role={}, offset={}",
                heartbeat.getNodeId(), heartbeat.getRole(), heartbeat.getLastAppliedOffset());

            lastHeartbeatTime.set(System.currentTimeMillis());

            // 调用业务处理器
            if (heartbeatHandler != null) {
                heartbeatHandler.accept(heartbeat);
            }

            // 发送心跳响应
            HeartbeatMessage response = new HeartbeatMessage(
                nodeId,
                "REPLICA",
                "SYNCED",
                heartbeat.getLastAppliedOffset(),  // 回传 offset
                Instant.now().toEpochMilli()
            );
            ctx.writeAndFlush(response);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state() == IdleState.READER_IDLE) {
                    logger.warn("Heartbeat timeout from primary, closing connection");
                    ctx.close();
                }
            }
            super.userEventTriggered(ctx, evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("Exception in replica handler", cause);
            ctx.close();
        }
    }
}

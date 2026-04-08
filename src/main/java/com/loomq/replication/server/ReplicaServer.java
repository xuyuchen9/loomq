package com.loomq.replication.server;

import com.loomq.replication.Ack;
import com.loomq.replication.ReplicationRecord;
import com.loomq.replication.protocol.HeartbeatMessage;
import com.loomq.replication.protocol.ReplicationProtocol;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
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
     * 设置记录处理器
     */
    public void setRecordHandler(Consumer<ReplicationRecord> handler) {
        this.recordHandler = handler;
    }

    /**
     * 设置心跳处理器
     */
    public void setHeartbeatHandler(Consumer<HeartbeatMessage> handler) {
        this.heartbeatHandler = handler;
    }

    /**
     * 启动服务器
     */
    public CompletableFuture<Void> start() {
        if (started.compareAndSet(false, true)) {
            return CompletableFuture.runAsync(this::doStart);
        }
        return CompletableFuture.completedFuture(null);
    }

    private void doStart() {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

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

            // 等待关闭
            serverChannel.closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("ReplicaServer interrupted");
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
                // 调用业务处理器
                if (recordHandler != null) {
                    recordHandler.accept(record);
                }

                // 发送 ACK
                Ack ack = Ack.success(record.getOffset());
                ctx.writeAndFlush(ack);

                logger.debug("Sent ACK for offset={}", record.getOffset());
            } catch (Exception e) {
                logger.error("Failed to process replication record: offset={}",
                    record.getOffset(), e);

                // 发送失败 ACK
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

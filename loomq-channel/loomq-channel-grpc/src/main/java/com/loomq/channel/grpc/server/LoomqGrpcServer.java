package com.loomq.channel.grpc.server;

import com.loomq.LoomqEngine;
import com.loomq.channel.grpc.config.GrpcConfig;
import com.loomq.spi.RaftStatusProvider;
import com.loomq.spi.WriteCoordinator;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bootstraps and manages the gRPC server lifecycle.
 *
 * <p>Shares the same {@link LoomqEngine} instance as the HTTP server
 * but runs on a separate port with its own Netty {@link EventLoopGroup}.
 */
public class LoomqGrpcServer {

    private static final Logger logger = LoggerFactory.getLogger(LoomqGrpcServer.class);

    private final GrpcConfig config;
    private final LoomqEngine engine;
    private final RaftStatusProvider raftStatus;
    private final WriteCoordinator writeCoordinator;
    private final GlobalIntentObserver globalObserver;
    private final GrpcStreamDeliveryHandler deliveryHandler;

    private Server server;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public LoomqGrpcServer(GrpcConfig config, LoomqEngine engine,
                           RaftStatusProvider raftStatus,
                           WriteCoordinator writeCoordinator,
                           GlobalIntentObserver globalObserver) {
        this(config, engine, raftStatus, writeCoordinator, globalObserver, null);
    }

    public LoomqGrpcServer(GrpcConfig config, LoomqEngine engine,
                           RaftStatusProvider raftStatus,
                           WriteCoordinator writeCoordinator,
                           GlobalIntentObserver globalObserver,
                           GrpcStreamDeliveryHandler deliveryHandler) {
        this.config = config;
        this.engine = engine;
        this.raftStatus = raftStatus;
        this.writeCoordinator = writeCoordinator;
        this.globalObserver = globalObserver;
        this.deliveryHandler = deliveryHandler;
    }

    /**
     * Start the gRPC server on the configured host:port.
     */
    public void start() throws IOException {
        bossGroup = new NioEventLoopGroup(config.bossThreads());
        workerGroup = config.workerThreads() > 0
            ? new NioEventLoopGroup(config.workerThreads())
            : new NioEventLoopGroup();

        LoomqGrpcService service = new LoomqGrpcService(engine, raftStatus, writeCoordinator, globalObserver, deliveryHandler);

        server = NettyServerBuilder.forAddress(new InetSocketAddress(config.host(), config.port()))
            .bossEventLoopGroup(bossGroup)
            .workerEventLoopGroup(workerGroup)
            .channelType(NioServerSocketChannel.class)
            .maxInboundMessageSize(config.maxInboundMessageSize())
            .withOption(ChannelOption.TCP_NODELAY, config.tcpNoDelay())
            .withOption(ChannelOption.SO_BACKLOG, config.soBacklog())
            .flowControlWindow(config.flowControlWindow())
            .permitKeepAliveTime(config.permitKeepAliveTimeSeconds(), TimeUnit.SECONDS)
            .addService(service)
            .build()
            .start();

        logger.info("gRPC server started on {}:{} (TCP_NODELAY={}, SO_BACKLOG={}, flowControlWindow={}KB)",
            config.host(), server.getPort(), config.tcpNoDelay(), config.soBacklog(), config.flowControlWindow() / 1024);
    }

    /**
     * Actual bound port (useful when configured port is 0).
     */
    public int getPort() {
        return server != null ? server.getPort() : config.port();
    }

    /**
     * Graceful shutdown.
     */
    public void stop() throws InterruptedException {
        try {
            if (server != null) {
                logger.info("Shutting down gRPC server...");
                server.shutdown();
                if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
            }
        } finally {
            if (bossGroup != null) bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
            if (workerGroup != null) workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
            logger.info("gRPC server stopped");
        }
    }
}

package com.loomq.http.netty;

import com.loomq.config.ServerConfig;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Netty 高性能 HTTP 服务器
 *
 * 架构设计：
 * - I/O 线程组（EventLoop）处理网络 I/O
 * - 虚拟线程池处理业务逻辑
 * - 请求体堆内存拷贝，消除跨线程 ByteBuf 竞争
 * - 信号量背压限流
 * - RadixTree 高效路由
 *
 * 目标性能：
 * - JSON 端点 >= 300K QPS
 * - P99 <= 5ms
 */
public class NettyHttpServer {

    private static final Logger logger = LoggerFactory.getLogger(NettyHttpServer.class);

    private final ServerConfig config;
    private final RadixRouter router;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private NettyRequestHandler requestHandler;
    private final AtomicInteger connectionCounter = new AtomicInteger(0);

    public NettyHttpServer(ServerConfig config, RadixRouter router) {
        this.config = config;
        this.router = router;
    }

    /**
     * 启动服务器
     */
    public void start() throws Exception {
        logger.info("╔════════════════════════════════════════════════════════╗");
        logger.info("║      Netty HTTP Server Starting...                     ║");
        logger.info("╚════════════════════════════════════════════════════════╝");

        boolean useEpoll = config.useEpoll() && Epoll.isAvailable();
        Class<? extends ServerChannel> channelClass;

        if (useEpoll) {
            logger.info("Using Epoll (Linux native transport)");
            bossGroup = new EpollEventLoopGroup(config.bossThreads());
            workerGroup = new EpollEventLoopGroup(config.workerThreads());
            channelClass = EpollServerSocketChannel.class;
        } else {
            logger.info("Using NIO (fallback)");
            bossGroup = new NioEventLoopGroup(config.bossThreads());
            workerGroup = new NioEventLoopGroup(config.workerThreads());
            channelClass = NioServerSocketChannel.class;
        }

        // 创建请求处理器
        requestHandler = new NettyRequestHandler(
            router,
            config.maxConcurrentBusinessRequests(),
            config.httpSemaphoreTimeoutMs(),
            HttpMetrics.getInstance()
        );

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
            .channel(channelClass)
            .option(ChannelOption.SO_BACKLOG, config.soBacklog())
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.TCP_NODELAY, config.tcpNoDelay())
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.ALLOCATOR,
                config.pooledAllocator() ? PooledByteBufAllocator.DEFAULT : ByteBufAllocator.DEFAULT)
            .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                new WriteBufferWaterMark(
                    config.writeBufferLowWaterMark(),
                    config.writeBufferHighWaterMark()))
            .childHandler(new ChannelInitializer<>() {
                @Override
                protected void initChannel(Channel ch) {
                    ChannelPipeline pipeline = ch.pipeline();

                    // 连接数限制（每个 channel 创建新实例，共享计数器）
                    pipeline.addLast("maxConnections", new MaxConnectionsHandler(connectionCounter, config.maxConnections()));

                    // 空闲检测
                    pipeline.addLast("idleState", new IdleStateHandler(
                        config.idleTimeoutSeconds(),
                        config.idleTimeoutSeconds(),
                        config.idleTimeoutSeconds(),
                        TimeUnit.SECONDS
                    ));

                    // HTTP 编解码
                    pipeline.addLast("httpCodec", new HttpServerCodec());
                    pipeline.addLast("httpAggregator", new HttpObjectAggregator(config.maxContentLength()));

                    // 请求处理
                    pipeline.addLast("requestHandler", requestHandler);
                }
            });

        // 绑定端口
        ChannelFuture future = bootstrap.bind(new InetSocketAddress(config.port())).sync();
        serverChannel = future.channel();

        logger.info("Netty HTTP Server started on port {} (epoll={}, workers={}, maxConnections={}, maxConcurrentRequests={})",
            config.port(), useEpoll, config.workerThreads(),
            config.maxConnections(), config.maxConcurrentBusinessRequests());
    }

    /**
     * 停止服务器
     */
    public void stop() {
        logger.info("Netty HTTP Server stopping...");

        // 1. 停止接受新连接
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }

        // 2. 等待现有业务处理完成
        long deadline = System.currentTimeMillis() + config.gracefulShutdownTimeoutMs();
        while (requestHandler != null && requestHandler.getAvailablePermits() < config.maxConcurrentBusinessRequests()) {
            if (System.currentTimeMillis() > deadline) {
                logger.warn("Graceful shutdown timeout, forcing close");
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // 3. 关闭请求处理器
        if (requestHandler != null) {
            requestHandler.shutdown();
        }

        // 4. 关闭 worker 线程
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(1, 5, TimeUnit.SECONDS);
        }

        // 5. 关闭 boss 线程
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(1, 5, TimeUnit.SECONDS);
        }

        logger.info("Netty HTTP Server stopped");
    }

    /**
     * 获取服务器端口
     */
    public int getPort() {
        if (serverChannel != null) {
            return ((InetSocketAddress) serverChannel.localAddress()).getPort();
        }
        return config.port();
    }

    /**
     * 获取当前连接数
     */
    public int getCurrentConnections() {
        return connectionCounter.get();
    }

    /**
     * 获取活跃请求数
     */
    public int getActiveRequests() {
        if (requestHandler != null) {
            return config.maxConcurrentBusinessRequests() - requestHandler.getAvailablePermits();
        }
        return 0;
    }
}

package com.loomq.channel.http.batch;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 批量 Webhook 参考服务器实现。
 *
 * <p>展示如何使用 {@link BatchWebhookHandler} 接收和处理批量投递请求。
 * 基于 Netty 构建，支持高并发接收。
 *
 * <p>使用示例：
 * <pre>{@code
 * BatchWebhookServer server = new BatchWebhookServer(8080, event -> {
 *     System.out.println("Received: " + event.intentId() + " / " + event.precisionTier());
 * });
 * server.start();
 * // ... 业务运行 ...
 * server.stop();
 * }</pre>
 *
 * @author loomq
 * @since v0.9.2
 */
public class BatchWebhookServer {

    private static final Logger logger = LoggerFactory.getLogger(BatchWebhookServer.class);

    private final int port;
    private final BatchWebhookHandler handler;
    private final AtomicInteger receivedCount = new AtomicInteger(0);
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public BatchWebhookServer(int port, Consumer<BatchWebhookHandler.IntentEvent> processor) {
        this.port = port;
        this.handler = new BatchWebhookHandler(processor);
    }

    /**
     * 启动服务器。
     */
    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline()
                        .addLast(new HttpServerCodec())
                        .addLast(new HttpObjectAggregator(65536))
                        .addLast(new WebhookHandler());
                }
            });

        serverChannel = bootstrap.bind(port).sync().channel();
        logger.info("BatchWebhookServer started on port {}", port);
    }

    /**
     * 停止服务器。
     */
    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        logger.info("BatchWebhookServer stopped: received {} requests", receivedCount.get());
    }

    /**
     * 获取已接收的请求数量。
     */
    public int getReceivedCount() {
        return receivedCount.get();
    }

    private class WebhookHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
            String uri = request.uri();

            // 健康检查端点
            if ("/health".equals(uri)) {
                sendResponse(ctx, HttpResponseStatus.OK, "{\"status\":\"ok\"}");
                return;
            }

            // Webhook 端点
            if (!"/webhook".equals(uri)) {
                sendResponse(ctx, HttpResponseStatus.NOT_FOUND, "{\"error\":\"Not found\"}");
                return;
            }

            // 读取请求体
            ByteBuf content = request.content();
            String body = content.toString(StandardCharsets.UTF_8);

            receivedCount.incrementAndGet();
            logger.debug("Received webhook request: {} bytes", body.length());

            // 处理批量请求
            String response = handler.handleRequest(body);
            sendResponse(ctx, HttpResponseStatus.OK, response);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Webhook handler error: {}", cause.getMessage());
            ctx.close();
        }

        private void sendResponse(ChannelHandlerContext ctx, HttpResponseStatus status, String body) {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                status,
                Unpooled.wrappedBuffer(bytes)
            );
            response.headers().set("Content-Type", "application/json");
            response.headers().set("Content-Length", bytes.length);
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }

    /**
     * 简单的命令行启动示例。
     */
    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;

        BatchWebhookServer server = new BatchWebhookServer(port, event -> {
            System.out.printf("[Webhook] Received: intentId=%s, tier=%s%n",
                event.intentId(), event.precisionTier());
        });

        server.start();

        // 等待关闭信号
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            server.stop();
        }));

        // 阻塞主线程
        Thread.currentThread().join();
    }
}

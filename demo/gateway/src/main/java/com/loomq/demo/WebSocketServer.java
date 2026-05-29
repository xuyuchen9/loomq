package com.loomq.demo;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CopyOnWriteArraySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WebSocket 服务器（基于 Netty）。
 *
 * <p>维护活跃的 WebSocket 连接，支持广播消息到所有客户端。
 *
 * @author loomq
 * @since v0.9.2
 */
public class WebSocketServer {

    private static final Logger logger = LoggerFactory.getLogger(WebSocketServer.class);

    private final int port;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    // 活跃的 WebSocket 连接
    private final CopyOnWriteArraySet<Channel> activeChannels = new CopyOnWriteArraySet<>();

    public WebSocketServer(int port) {
        this.port = port;
    }

    /**
     * 启动 WebSocket 服务器。
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
                        .addLast(new WebSocketHandler());
                }
            })
            .option(ChannelOption.SO_BACKLOG, 128)
            .childOption(ChannelOption.SO_KEEPALIVE, true);

        serverChannel = bootstrap.bind(port).sync().channel();
        logger.info("WebSocket server started on port {}", port);
    }

    /**
     * 停止服务器。
     */
    public void stop() {
        logger.info("Stopping WebSocket server");
        activeChannels.clear();
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }

    /**
     * 广播消息到所有活跃的 WebSocket 客户端。
     */
    public void broadcast(String message) {
        TextWebSocketFrame frame = new TextWebSocketFrame(message);
        for (Channel channel : activeChannels) {
            if (channel.isActive()) {
                channel.writeAndFlush(frame.retain());
            } else {
                activeChannels.remove(channel);
            }
        }
    }

    /**
     * 获取活跃连接数。
     */
    public int getActiveConnections() {
        return activeChannels.size();
    }

    /**
     * WebSocket 处理器。
     */
    private class WebSocketHandler extends ChannelInboundHandlerAdapter {

        private WebSocketServerHandshaker handshaker;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof FullHttpRequest request) {
                // 处理 HTTP 升级请求
                if (request.uri().equals("/ws") &&
                    request.headers().contains(HttpHeaderNames.UPGRADE,
                        HttpHeaderValues.WEBSOCKET, true)) {

                    WebSocketServerHandshakerFactory factory =
                        new WebSocketServerHandshakerFactory(
                            "ws://" + request.headers().get(HttpHeaderNames.HOST) + "/ws",
                            null, true);
                    handshaker = factory.newHandshaker(request);
                    if (handshaker == null) {
                        WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
                    } else {
                        handshaker.handshake(ctx.channel(), request);
                        activeChannels.add(ctx.channel());
                        logger.info("WebSocket client connected: {} (total: {})",
                            ctx.channel().remoteAddress(), activeChannels.size());
                    }
                    ReferenceCountUtil.release(request);
                    return;
                }

                // 处理普通 HTTP 请求（返回状态页面）
                if (request.uri().equals("/status")) {
                    String status = String.format(
                        "{\"connections\":%d,\"status\":\"ok\"}",
                        activeChannels.size());
                    ByteBuf content = Unpooled.copiedBuffer(status, StandardCharsets.UTF_8);
                    FullHttpResponse response = new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
                    response.headers()
                        .set(HttpHeaderNames.CONTENT_TYPE, "application/json")
                        .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
                    ctx.writeAndFlush(response).addListener(f -> ReferenceCountUtil.release(content));
                    ReferenceCountUtil.release(request);
                    return;
                }

                // 服务 index.html
                if (request.uri().equals("/") || request.uri().equals("/index.html")) {
                    try {
                        // 尝试从文件系统读取
                        Path htmlPath = Path.of("src/main/resources/index.html");
                        byte[] htmlBytes;
                        if (Files.exists(htmlPath)) {
                            htmlBytes = Files.readAllBytes(htmlPath);
                        } else {
                            // 回退到 classpath
                            InputStream is = getClass().getResourceAsStream("/index.html");
                            if (is != null) {
                                htmlBytes = is.readAllBytes();
                                is.close();
                            } else {
                                logger.error("index.html not found");
                                FullHttpResponse response = new DefaultFullHttpResponse(
                                    HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
                                ctx.writeAndFlush(response);
                                ReferenceCountUtil.release(request);
                                return;
                            }
                        }
                        ByteBuf content = Unpooled.wrappedBuffer(htmlBytes);
                        FullHttpResponse response = new DefaultFullHttpResponse(
                            HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
                        response.headers()
                            .set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8")
                            .set(HttpHeaderNames.CONTENT_LENGTH, htmlBytes.length);
                        ctx.writeAndFlush(response);
                        ReferenceCountUtil.release(request);
                        return;
                    } catch (Exception e) {
                        logger.error("Failed to read index.html: {}", e.getMessage());
                    }
                }

                // 404
                FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
                ctx.writeAndFlush(response);
                ReferenceCountUtil.release(request);
                return;
            }

            if (msg instanceof WebSocketFrame frame) {
                // 处理 WebSocket 帧
                if (frame instanceof TextWebSocketFrame textFrame) {
                    String text = textFrame.text();
                    logger.debug("Received WebSocket message: {}", text);
                    // 可以处理客户端消息（如 ACK）
                }
                ReferenceCountUtil.release(frame);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            activeChannels.remove(ctx.channel());
            logger.info("WebSocket client disconnected: {} (total: {})",
                ctx.channel().remoteAddress(), activeChannels.size());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("WebSocket handler error: {}", cause.getMessage());
            activeChannels.remove(ctx.channel());
            ctx.close();
        }
    }
}

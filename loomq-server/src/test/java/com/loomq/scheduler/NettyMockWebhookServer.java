package com.loomq.scheduler;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
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
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * High-concurrency Netty-based mock webhook server.
 *
 * Replaces java built-in HttpServer (bottleneck at ~500 concurrent connections)
 * with Netty NIO transport to support 10k+ simultaneous webhook deliveries
 * without the server becoming the E2E latency bottleneck.
 */
final class NettyMockWebhookServer {

    private static final Logger log = LoggerFactory.getLogger(NettyMockWebhookServer.class);

    private final int port;
    private final Map<String, Integer> tierDelaysMs;
    private final Map<String, AtomicInteger> receivedByTier;
    private final ConcurrentHashMap<String, Long> receiveTimeMs;
    private final AtomicInteger totalReceived;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private DefaultEventExecutorGroup handlerGroup;
    private Channel serverChannel;
    private volatile boolean running;

    NettyMockWebhookServer(int port, Map<String, Integer> tierDelaysMs,
                           Map<String, AtomicInteger> receivedByTier,
                           ConcurrentHashMap<String, Long> receiveTimeMs,
                           AtomicInteger totalReceived) {
        this.port = port;
        this.tierDelaysMs = tierDelaysMs;
        this.receivedByTier = receivedByTier;
        this.receiveTimeMs = receiveTimeMs;
        this.totalReceived = totalReceived;
    }

    void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());
        // Offload blocking handlers (Thread.sleep) from Netty I/O threads
        handlerGroup = new DefaultEventExecutorGroup(256);

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_BACKLOG, 4096)
            .childHandler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) {
                    ChannelPipeline p = ch.pipeline();
                    p.addLast(new HttpServerCodec());
                    p.addLast(new HttpObjectAggregator(65536));
                    p.addLast(handlerGroup, new WebhookHandler());  // offload blocking
                }
            });

        serverChannel = bootstrap.bind(port).sync().channel();
        running = true;
        log.info("Netty mock webhook server started on port {}", port);
    }

    void stop() {
        running = false;
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (handlerGroup != null) {
            handlerGroup.shutdownGracefully();
        }
        log.info("Netty mock webhook server stopped");
    }

    private class WebhookHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (!(msg instanceof FullHttpRequest req)) {
                ReferenceCountUtil.release(msg);
                return;
            }

            long receiveMs = System.currentTimeMillis();
            ByteBuf content = req.content();
            String body = content.toString(StandardCharsets.UTF_8);

            // Detect batch (JSON array) vs single intent (JSON object)
            if (body.startsWith("[")) {
                processBatch(body, receiveMs);
            } else {
                processSingle(body, receiveMs);
            }

            byte[] respBytes = "{\"status\":\"ok\"}".getBytes(StandardCharsets.UTF_8);
            FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                Unpooled.wrappedBuffer(respBytes));
            response.headers()
                .set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                .set(HttpHeaderNames.CONTENT_LENGTH, respBytes.length);
            ctx.writeAndFlush(response);
            ReferenceCountUtil.release(req);
        }

        private void processSingle(String body, long receiveMs) {
            String intentId = extractField(body, "intentId");
            String tierStr = extractField(body, "precisionTier");
            int delayMs = tierStr != null ? tierDelaysMs.getOrDefault(tierStr, 5) : 5;
            doSleep(delayMs);
            track(intentId, tierStr, receiveMs);
        }

        private void processBatch(String body, long receiveMs) {
            // Parse each JSON object in the array, apply the max delay among them
            int maxDelay = 5;
            int idx = 0;
            while ((idx = body.indexOf("\"precisionTier\":\"", idx)) >= 0) {
                int start = idx + 17;
                int end = body.indexOf('"', start);
                if (end > start) {
                    String tierStr = body.substring(start, end);
                    maxDelay = Math.max(maxDelay, tierDelaysMs.getOrDefault(tierStr, 5));
                }
                idx = end + 1;
            }
            // Apply max delay once for the whole batch (simulates batch processing time)
            doSleep(maxDelay);

            // Track each intent in the batch
            idx = 0;
            while ((idx = body.indexOf("\"intentId\":\"", idx)) >= 0) {
                int start = idx + 12;
                int end = body.indexOf('"', start);
                if (end > start) {
                    String intentId = body.substring(start, end);
                    // Find tier for this intent
                    int tierIdx = body.indexOf("\"precisionTier\":\"", idx);
                    String tierStr = null;
                    if (tierIdx >= 0) {
                        int ts = tierIdx + 17;
                        int te = body.indexOf('"', ts);
                        if (te > ts) tierStr = body.substring(ts, te);
                    }
                    track(intentId, tierStr, receiveMs);
                }
                idx = end + 1;
            }
        }

        private void doSleep(int delayMs) {
            if (delayMs > 0) {
                try { Thread.sleep(delayMs); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        }

        private void track(String intentId, String tierStr, long receiveMs) {
            if (tierStr != null) {
                AtomicInteger counter = receivedByTier.get(tierStr);
                if (counter != null) counter.incrementAndGet();
            }
            totalReceived.incrementAndGet();
            if (intentId != null) {
                receiveTimeMs.put(intentId, receiveMs);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }

        private String extractField(String json, String fieldName) {
            String key = "\"" + fieldName + "\":\"";
            int idx = json.indexOf(key);
            if (idx < 0) return null;
            int start = idx + key.length();
            int end = json.indexOf('"', start);
            return end > start ? json.substring(start, end) : null;
        }
    }
}

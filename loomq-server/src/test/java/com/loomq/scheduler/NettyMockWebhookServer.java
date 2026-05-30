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
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-concurrency Netty-based mock webhook server.
 *
 * Replaces java built-in HttpServer (bottleneck at ~500 concurrent connections)
 * with Netty NIO transport to support 10k+ simultaneous webhook deliveries
 * without the server becoming the E2E latency bottleneck.
 *
 * Uses non-blocking scheduled responses instead of Thread.sleep, so handler
 * threads are never blocked and a single I/O thread can service thousands of
 * concurrent delayed responses.
 */
final class NettyMockWebhookServer {

    private static final Logger log = LoggerFactory.getLogger(NettyMockWebhookServer.class);

    private final int port;
    private final Map<String, Integer> tierDelaysMs;
    private final Map<String, AtomicInteger> receivedByTier;
    private final ConcurrentHashMap<String, Long> receiveTimeMs;
    private final AtomicInteger totalReceived;
    private final boolean zeroDelay;
    private final AtomicInteger httpRequestCount = new AtomicInteger(0);

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private volatile boolean running;

    NettyMockWebhookServer(int port, Map<String, Integer> tierDelaysMs,
                           Map<String, AtomicInteger> receivedByTier,
                           ConcurrentHashMap<String, Long> receiveTimeMs,
                           AtomicInteger totalReceived) {
        this(port, tierDelaysMs, receivedByTier, receiveTimeMs, totalReceived, false);
    }

    NettyMockWebhookServer(int port, Map<String, Integer> tierDelaysMs,
                           Map<String, AtomicInteger> receivedByTier,
                           ConcurrentHashMap<String, Long> receiveTimeMs,
                           AtomicInteger totalReceived,
                           boolean zeroDelay) {
        this.port = port;
        this.tierDelaysMs = tierDelaysMs;
        this.receivedByTier = receivedByTier;
        this.receiveTimeMs = receiveTimeMs;
        this.totalReceived = totalReceived;
        this.zeroDelay = zeroDelay;
    }

    void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());
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
                    // Handler runs directly on I/O threads — no blocking, no executor offload needed
                    p.addLast(new WebhookHandler());
                }
            });

        serverChannel = bootstrap.bind(port).sync().channel();
        running = true;
        log.info("Netty mock webhook server started on port {} (zeroDelay={})", port, zeroDelay);
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
        log.info("Netty mock webhook server stopped: {} HTTP requests received", httpRequestCount.get());
    }

    /**
     * 获取 HTTP 请求总数（每个 HTTP POST 计为一次请求，无论包含多少 Intent）。
     */
    int getHttpRequestCount() {
        return httpRequestCount.get();
    }

    private class WebhookHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (!(msg instanceof FullHttpRequest req)) {
                ReferenceCountUtil.release(msg);
                return;
            }

            httpRequestCount.incrementAndGet();
            long receiveMs = System.currentTimeMillis();
            ByteBuf content = req.content();
            String body = content.toString(StandardCharsets.UTF_8);

            // Parse and track immediately — receive timestamp reflects arrival, not response
            int delayMs;
            if (body.startsWith("[")) {
                delayMs = processBatch(body, receiveMs);
            } else {
                delayMs = processSingle(body, receiveMs);
            }

            if (zeroDelay) {
                delayMs = 0;
            }

            // Release the request immediately after extracting body content.
            // We don't need the request for the response.
            ReferenceCountUtil.release(req);

            Runnable respond = () -> {
                if (!ctx.channel().isActive()) {
                    return;
                }
                byte[] respBytes = "{\"status\":\"ok\"}".getBytes(StandardCharsets.UTF_8);
                FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                    Unpooled.wrappedBuffer(respBytes));
                response.headers()
                    .set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                    .set(HttpHeaderNames.CONTENT_LENGTH, respBytes.length);
                ctx.writeAndFlush(response);
            };

            if (delayMs > 0) {
                ctx.executor().schedule(respond, delayMs, TimeUnit.MILLISECONDS);
            } else {
                ctx.executor().execute(respond);
            }
        }

        /**
         * Process a single-intent JSON body.
         * Returns the delay to apply before responding.
         */
        private int processSingle(String body, long receiveMs) {
            String intentId = extractField(body, "intentId");
            String tierStr = extractField(body, "precisionTier");
            int delayMs = tierStr != null ? tierDelaysMs.getOrDefault(tierStr, 5) : 5;
            track(intentId, tierStr, receiveMs);
            return delayMs;
        }

        /**
         * Process a batch JSON array body.
         * Returns the max delay among all intents in the batch.
         */
        private int processBatch(String body, long receiveMs) {
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

            // Track each intent in the batch
            idx = 0;
            while ((idx = body.indexOf("\"intentId\":\"", idx)) >= 0) {
                int start = idx + 12;
                int end = body.indexOf('"', start);
                if (end > start) {
                    String intentId = body.substring(start, end);
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

            return maxDelay;
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

package com.loomq.http.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 连接数限制处理器
 *
 * 超过最大连接数时关闭新连接
 *
 * 注意：使用共享的计数器，每个 Channel 创建新的处理器实例
 */
public class MaxConnectionsHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(MaxConnectionsHandler.class);

    private final AtomicInteger sharedCounter;
    private final int maxConnections;

    public MaxConnectionsHandler(AtomicInteger sharedCounter, int maxConnections) {
        this.sharedCounter = sharedCounter;
        this.maxConnections = maxConnections;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        int current = sharedCounter.incrementAndGet();
        if (current > maxConnections) {
            logger.warn("Connection limit exceeded: {}/{}", current, maxConnections);
            ctx.close();
            sharedCounter.decrementAndGet();
            return;
        }
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        sharedCounter.decrementAndGet();
        super.channelInactive(ctx);
    }

    public int getCurrentConnections() {
        return sharedCounter.get();
    }
}

package com.loomq.replication.client;

import com.loomq.replication.Ack;
import com.loomq.replication.AckStatus;
import com.loomq.replication.ReplicationRecord;
import com.loomq.replication.ReplicationRecordType;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ReplicaClientTimeoutTest {

    @Test
    @Timeout(10)
    void sendShouldReturnTimeoutAckWhenReplicaDoesNotRespond() throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(1);
        Channel serverChannel = null;
        ReplicaClient client = null;

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                                // Blackhole server: accept the bytes and intentionally do not reply.
                            }
                        });
                    }
                });

            ChannelFuture bindFuture = bootstrap.bind(new InetSocketAddress("127.0.0.1", 0)).sync();
            serverChannel = bindFuture.channel();
            int port = ((InetSocketAddress) serverChannel.localAddress()).getPort();

            client = new ReplicaClient("node-1", "127.0.0.1", port, 150);
            client.connect().get(5, TimeUnit.SECONDS);

            ReplicationRecord record = ReplicationRecord.builder()
                .offset(1L)
                .type(ReplicationRecordType.INTENT_CREATE)
                .sourceNodeId("node-1")
                .payload(new byte[0])
                .build();

            Ack ack = client.send(record).get(2, TimeUnit.SECONDS);

            assertNotNull(ack);
            assertEquals(1L, ack.getOffset());
            assertEquals(AckStatus.TIMEOUT, ack.getStatus());
            assertFalse(ack.isSuccess());
        } finally {
            if (client != null) {
                client.shutdown();
            }
            if (serverChannel != null) {
                serverChannel.close().syncUninterruptibly();
            }
            bossGroup.shutdownGracefully().syncUninterruptibly();
            workerGroup.shutdownGracefully().syncUninterruptibly();
        }
    }
}

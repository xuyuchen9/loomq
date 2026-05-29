package com.loomq.channel.grpc.config;

import com.loomq.config.ConfigSupport;
import java.util.Properties;

/**
 * gRPC server configuration.
 *
 * <p>gRPC runs as an optional transport alongside the HTTP Netty server.
 * Disabled by default; enable via {@code grpc.enabled=true}.
 */
public record GrpcConfig(
    boolean enabled,
    String host,
    int port,
    int maxInboundMessageSize,
    int bossThreads,
    int workerThreads,
    boolean tcpNoDelay,
    int soBacklog,
    int flowControlWindow,
    long permitKeepAliveTimeSeconds
) {

    public GrpcConfig {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("grpc host cannot be blank");
        }
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("grpc port must be in range [0, 65535]");
        }
        if (maxInboundMessageSize <= 0) {
            throw new IllegalArgumentException("grpc maxInboundMessageSize must be positive");
        }
        if (bossThreads <= 0) {
            throw new IllegalArgumentException("grpc bossThreads must be positive");
        }
        if (workerThreads < 0) {
            throw new IllegalArgumentException("grpc workerThreads must be non-negative");
        }
        if (soBacklog <= 0) {
            throw new IllegalArgumentException("grpc soBacklog must be positive");
        }
        if (flowControlWindow <= 0) {
            throw new IllegalArgumentException("grpc flowControlWindow must be positive");
        }
        if (permitKeepAliveTimeSeconds <= 0) {
            throw new IllegalArgumentException("grpc permitKeepAliveTimeSeconds must be positive");
        }
    }

    public static GrpcConfig defaultConfig() {
        return fromProperties(new Properties());
    }

    public static GrpcConfig fromProperties(Properties props) {
        Properties source = props == null ? new Properties() : props;
        return new GrpcConfig(
            ConfigSupport.booleanValue(source, false, "grpc.enabled", "grpcEnabled"),
            ConfigSupport.string(source, "0.0.0.0", "grpc.host", "grpcHost"),
            ConfigSupport.intValue(source, 7929, "grpc.port", "grpcPort"),
            ConfigSupport.intValue(source, 4 * 1024 * 1024, "grpc.max_inbound_message_size", "grpc.maxInboundMessageSize", "grpcMaxInboundMessageSize"),
            ConfigSupport.intValue(source, 1, "grpc.boss_threads", "grpc.bossThreads", "grpcBossThreads"),
            ConfigSupport.intValue(source, 0, "grpc.worker_threads", "grpc.workerThreads", "grpcWorkerThreads"),
            ConfigSupport.booleanValue(source, true, "grpc.tcp_no_delay", "grpc.tcpNoDelay", "grpcTcpNoDelay"),
            ConfigSupport.intValue(source, 1024, "grpc.so_backlog", "grpc.soBacklog", "grpcSoBacklog"),
            ConfigSupport.intValue(source, 1024 * 1024, "grpc.flow_control_window", "grpc.flowControlWindow", "grpcFlowControlWindow"),
            ConfigSupport.longValue(source, 60, "grpc.permit_keep_alive_time", "grpc.permitKeepAliveTime", "grpcPermitKeepAliveTime")
        );
    }
}

package com.loomq.server;

import com.loomq.LoomqEngine;
import com.loomq.callback.HttpCallbackHandler;
import com.loomq.callback.NettyHttpDeliveryHandler;
import com.loomq.cluster.ClusterManager;
import com.loomq.cluster.FailoverController;
import com.loomq.cluster.ReplicaRole;
import com.loomq.cluster.ReplicationEndpoints;
import com.loomq.common.MetricsCollector;
import com.loomq.config.LoomqConfig;
import com.loomq.config.ServerConfig;
import com.loomq.config.WalConfig;
import com.loomq.http.netty.IntentHandler;
import com.loomq.http.netty.NettyHttpServer;
import com.loomq.http.netty.RadixRouter;
import com.loomq.metrics.LoomQMetrics;
import io.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Standalone Netty service bootstrap.
 *
 * The embedded core remains reusable through LoomqEngine, while the standalone
 * service only wires the Netty transport and the HTTP adapters.
 */
public class LoomqServerApplication {

    private static final Logger logger = LoggerFactory.getLogger(LoomqServerApplication.class);
    private static final byte[] HEALTH_UP_RESPONSE = "{\"status\":\"UP\"}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HEALTH_LIVE_RESPONSE = "{\"status\":\"ALIVE\"}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HEALTH_DOWN_RESPONSE = "{\"status\":\"DOWN\"}".getBytes(StandardCharsets.UTF_8);

    public static void main(String[] args) {
        printBanner();

        LoomqConfig config = LoomqConfig.getInstance();
        ServerConfig serverConfig = config.getServerConfig();

        String nodeId = resolveSetting("LOOMQ_NODE_ID", "loomq.node.id", "node-1");
        String dataDir = resolveSetting("LOOMQ_DATA_DIR", "loomq.data.dir", config.getWalConfig().dataDir());
        WalConfig walConfig = config.getWalConfig().withDataDir(dataDir);

        MetricsCollector.getInstance().setWalDataDir(dataDir);
        MetricsCollector.getInstance().setSchedulerMaxPendingIntents(config.getSchedulerConfig().maxPendingIntents());
        logRuntimeConfiguration(config, nodeId, dataDir, walConfig);

        HttpCallbackHandler callbackHandler = new HttpCallbackHandler();
        LoomqEngine engine = LoomqEngine.builder()
            .nodeId(nodeId)
            .walDir(Path.of(dataDir))
            .walConfig(walConfig)
            .callbackHandler(callbackHandler)
            .deliveryHandler(new NettyHttpDeliveryHandler())
            .build();

        // ---- Raft 共识模式（v0.9.0）----
        final com.loomq.raft.RaftNode raftNode;
        boolean raftEnabled = Boolean.parseBoolean(
            resolveSetting("LOOMQ_RAFT_ENABLED", "loomq.raft.enabled", "false"));
        if (raftEnabled) {
            String raftNodeId = resolveSetting("LOOMQ_RAFT_NODE_ID", "loomq.raft.nodeId", nodeId);
            String peersStr = resolveSetting("LOOMQ_RAFT_PEERS", "loomq.raft.peers", raftNodeId);
            List<String> peers = parseRaftPeerIds(peersStr, raftNodeId);
            List<RaftPeerTarget> peerTargets = parseRaftPeerTargets(peersStr, raftNodeId);

            com.loomq.raft.RaftConfig raftConfig = new com.loomq.raft.RaftConfig(
                raftNodeId, peers, dataDir, 150, 300, 50);

            com.loomq.raft.RaftTransport raftTransport = new com.loomq.raft.RaftTransport(raftNodeId);
            int raftPort = Integer.parseInt(
                resolveSetting("LOOMQ_RAFT_PORT", "loomq.raft.port", "7930"));
            raftTransport.listen("0.0.0.0", raftPort);

            raftNode = new com.loomq.raft.RaftNode(raftConfig,
                engine.getWalAccessor(), engine.getIntentStore(), raftTransport);
            if (peerTargets.isEmpty() && peers.size() > 1) {
                logger.warn("Raft peers were configured without connectable endpoints; use peerId@host:port to enable peer connections");
            }
            connectRaftPeers(raftTransport, peerTargets, raftNodeId);
            raftNode.start();
            logger.info("Raft mode enabled: node={}, peers={}, connectablePeers={}",
                raftNodeId, peers, peerTargets.size());
        } else {
            raftNode = null;
        }

        // ---- 集群/复制模式（可选）----
        final ClusterManager clusterManager;
        final com.loomq.replication.ReplicationManager replicationManager;
        final FailoverController failoverController;

        boolean clusterEnabled = Boolean.parseBoolean(
            resolveSetting("LOOMQ_CLUSTER_ENABLED", "loomq.cluster.enabled", "false"));
        if (clusterEnabled) {
            String replicaHost = resolveSetting("LOOMQ_REPLICA_HOST", "loomq.replica.host", "localhost");
            int replicaPort = Integer.parseInt(
                resolveSetting("LOOMQ_REPLICA_PORT", "loomq.replica.port", "7929"));
            String role = resolveSetting("LOOMQ_ROLE", "loomq.role", "primary");

            // 复制管理器
            replicationManager = new com.loomq.replication.ReplicationManager(nodeId);
            com.loomq.replication.RecordApplier applier =
                new com.loomq.replication.RecordApplier(engine.getIntentStore());
            applier.setScheduler(engine.getScheduler());
            replicationManager.setRecordApplier(applier);

            // 角色初始化
            if ("primary".equalsIgnoreCase(role)) {
                replicationManager.promoteToPrimary(replicaHost, replicaPort).join();
            } else {
                replicationManager.demoteToReplica(replicaHost, replicaPort).join();
            }

            // 集群管理器（分片路由）
            clusterManager = ClusterManager.singleNode(serverConfig.port());
            try {
                clusterManager.start();
            } catch (Exception e) {
                logger.warn("ClusterManager start failed (non-fatal)", e);
            }

            // Failover 控制器
            ReplicationEndpoints endpoints = new ReplicationEndpoints(replicaHost, replicaPort, replicaHost, replicaPort);
            failoverController = new FailoverController(
                nodeId, "shard-0", ReplicaRole.FOLLOWER, 1,
                30000, 0.5, endpoints);
            failoverController.setReplicationManager(replicationManager);
            failoverController.start();

            logger.info("Replication enabled: role={}, replica={}:{}", role, replicaHost, replicaPort);
        } else {
            clusterManager = null;
            replicationManager = null;
            failoverController = null;
        }

        RadixRouter router = new RadixRouter();
        new IntentHandler(engine).register(router);
        registerSystemRoutes(router, engine);

        NettyHttpServer server = new NettyHttpServer(serverConfig, router);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received, stopping Netty server and engine...");
            try {
                server.stop();
            } catch (Exception e) {
                logger.warn("Error while stopping Netty server", e);
            }

            try {
                callbackHandler.close();
            } catch (Exception e) {
                logger.warn("Error while closing callback handler", e);
            }

            // 关闭复制组件
            if (raftNode != null) {
                raftNode.close();
            }
            if (failoverController != null) {
                failoverController.close();
            }
            if (replicationManager != null) {
                try {
                    replicationManager.close();
                } catch (Exception e) {
                    logger.warn("Error closing replication manager", e);
                }
            }
            if (clusterManager != null) {
                clusterManager.stop();
            }

            try {
                engine.close();
            } catch (Exception e) {
                logger.error("Error during engine shutdown", e);
            }
        }, "loomq-shutdown"));

        try {
            engine.start();
            server.start();

            logger.info("LoomQ Netty server started on http://{}:{}", serverConfig.host(), server.getPort());

            while (engine.isRunning()) {
                Thread.sleep(1000L);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("Main thread interrupted");
        } catch (Exception e) {
            logger.error("Server failed to start", e);
            try {
                server.stop();
            } catch (Exception stopError) {
                logger.warn("Error while stopping Netty server after startup failure", stopError);
            }
            try {
                callbackHandler.close();
            } catch (Exception closeError) {
                logger.warn("Error while closing callback handler after startup failure", closeError);
            }
            try {
                engine.close();
            } catch (Exception closeError) {
                logger.warn("Error while closing engine after startup failure", closeError);
            }
            System.exit(1);
        }

        logger.info("Application exited");
    }

    private static void logRuntimeConfiguration(LoomqConfig config, String nodeId, String dataDir, WalConfig walConfig) {
        ServerConfig serverConfig = config.getServerConfig();
        logger.info(
            "Runtime config: nodeId={}, dataDir={}, server={}:{} backlog={} virtualThreads={} maxRequestSize={} threadPoolSize={}, netty={}:{} epoll={} pooledAllocator={} maxConnections={} maxConcurrentRequests={} httpSemaphoreTimeoutMs={}, walDir={}, walEngine={}, walFlushStrategy={}, walFlushThresholdKb={}, walStripeCount={}, schedulerMaxPendingIntents={}, recoveryBatchSize={}, retryInitialDelayMs={}, retryMaxDelayMs={}",
            nodeId,
            dataDir,
            serverConfig.host(),
            serverConfig.port(),
            serverConfig.backlog(),
            serverConfig.virtualThreads(),
            serverConfig.maxRequestSize(),
            serverConfig.threadPoolSize(),
            serverConfig.nettyHost(),
            serverConfig.nettyPort(),
            serverConfig.useEpoll(),
            serverConfig.pooledAllocator(),
            serverConfig.maxConnections(),
            serverConfig.maxConcurrentBusinessRequests(),
            serverConfig.httpSemaphoreTimeoutMs(),
            walConfig.dataDir(),
            walConfig.engine(),
            walConfig.flushStrategy(),
            walConfig.memorySegmentFlushThresholdKb(),
            walConfig.memorySegmentStripeCount(),
            config.getSchedulerConfig().maxPendingIntents(),
            config.getRecoveryConfig().batchSize(),
            config.getRetryConfig().initialDelayMs(),
            config.getRetryConfig().maxDelayMs()
        );
    }

    private static String resolveSetting(String envKey, String propertyKey, String fallback) {
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }

        String propertyValue = System.getProperty(propertyKey);
        if (propertyValue != null && !propertyValue.isBlank()) {
            return propertyValue;
        }

        return fallback;
    }

    private static List<String> parseRaftPeerIds(String peersSpec, String selfNodeId) {
        if (peersSpec == null || peersSpec.isBlank()) {
            return List.of(selfNodeId);
        }

        List<String> peers = new ArrayList<>();
        for (String raw : peersSpec.split(",")) {
            String token = raw.trim();
            if (token.isEmpty()) {
                continue;
            }
            String nodeId = extractPeerNodeId(token);
            if (!nodeId.isEmpty()) {
                peers.add(nodeId);
            }
        }

        if (peers.isEmpty()) {
            return List.of(selfNodeId);
        }
        return List.copyOf(peers);
    }

    private static List<RaftPeerTarget> parseRaftPeerTargets(String peersSpec, String selfNodeId) {
        if (peersSpec == null || peersSpec.isBlank()) {
            return List.of();
        }

        List<RaftPeerTarget> targets = new ArrayList<>();
        for (String raw : peersSpec.split(",")) {
            String token = raw.trim();
            if (token.isEmpty()) {
                continue;
            }

            RaftPeerTarget target = parsePeerTarget(token, selfNodeId);
            if (target != null) {
                targets.add(target);
            }
        }
        return List.copyOf(targets);
    }

    private static String extractPeerNodeId(String token) {
        int at = token.indexOf('@');
        int eq = token.indexOf('=');
        int sep = at >= 0 ? at : eq;
        if (sep > 0) {
            return token.substring(0, sep).trim();
        }
        return token.trim();
    }

    private static RaftPeerTarget parsePeerTarget(String token, String selfNodeId) {
        int at = token.indexOf('@');
        int eq = token.indexOf('=');
        int sep = at >= 0 ? at : eq;
        if (sep <= 0) {
            return null;
        }

        String peerId = token.substring(0, sep).trim();
        if (peerId.isEmpty() || peerId.equals(selfNodeId)) {
            return null;
        }

        String endpoint = token.substring(sep + 1).trim();
        int colon = endpoint.lastIndexOf(':');
        if (colon <= 0 || colon == endpoint.length() - 1) {
            logger.warn("Invalid Raft peer endpoint '{}', expected peerId@host:port or peerId=host:port", token);
            return null;
        }

        String host = endpoint.substring(0, colon).trim();
        String portStr = endpoint.substring(colon + 1).trim();
        try {
            int port = Integer.parseInt(portStr);
            return new RaftPeerTarget(peerId, host, port);
        } catch (NumberFormatException e) {
            logger.warn("Invalid Raft peer port in '{}': {}", token, portStr);
            return null;
        }
    }

    private static void connectRaftPeers(com.loomq.raft.RaftTransport transport,
                                         List<RaftPeerTarget> targets,
                                         String selfNodeId) {
        for (RaftPeerTarget target : targets) {
            if (target.nodeId().equals(selfNodeId)) {
                continue;
            }

            try {
                transport.connect(target.nodeId(), target.host(), target.port()).join();
                logger.info("Raft peer connected: {} -> {}:{}", target.nodeId(), target.host(), target.port());
            } catch (Exception e) {
                logger.warn("Raft peer connect failed: {} -> {}:{}",
                    target.nodeId(), target.host(), target.port(), e);
            }
        }
    }

    private static void printBanner() {
        logger.info("");
        logger.info("██╗      ██████╗  ██████╗ ███╗   ███╗ ██████╗ ");
        logger.info("██║     ██╔═══██╗██╔═══██╗████╗ ████║██╔═══██╗");
        logger.info("██║     ██║   ██║██║   ██║██╔████╔██║██║   ██║");
        logger.info("██║     ██║   ██║██║   ██║██║╚██╔╝██║██║▄▄ ██║");
        logger.info("███████╗╚██████╔╝╚██████╔╝██║ ╚═╝ ██║╚██████╔╝");
        logger.info("╚══════╝ ╚═════╝  ╚═════╝ ╚═╝     ╚═╝ ╚══▀▀═╝ ");
        logger.info("");
        logger.info(" Event Infrastructure for Delayed Execution");
        logger.info("              Version 0.9.0");
        logger.info("              Mode: Server (Netty)");
        logger.info("");
    }

    private static void registerSystemRoutes(RadixRouter router, LoomqEngine engine) {
        router.add(HttpMethod.GET, "/health", (method, uri, body, headers, pathParams) ->
            HEALTH_UP_RESPONSE);
        router.add(HttpMethod.GET, "/health/live", (method, uri, body, headers, pathParams) ->
            HEALTH_LIVE_RESPONSE);
        router.add(HttpMethod.GET, "/health/ready", (method, uri, body, headers, pathParams) ->
            LoomQMetrics.getInstance().isWalHealthy() ? HEALTH_UP_RESPONSE : HEALTH_DOWN_RESPONSE);
        router.add(HttpMethod.GET, "/health/deep", (method, uri, body, headers, pathParams) ->
            buildDeepHealthResponse(engine));
        router.add(HttpMethod.GET, "/metrics", (method, uri, body, headers, pathParams) ->
            LoomQMetrics.getInstance().snapshot());
        router.add(HttpMethod.GET, "/api/v1/metrics", (method, uri, body, headers, pathParams) ->
            LoomQMetrics.getInstance().snapshot());
    }

    private static String buildDeepHealthResponse(LoomqEngine engine) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"status\":\"").append(engine.getWalHealth().status()).append("\"");

        // Tier status
        sb.append(",\"tiers\":{");
        var backpressure = engine.getScheduler().getBackpressureStatus();
        boolean first = true;
        for (var entry : backpressure.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            var info = entry.getValue();
            sb.append("\"").append(entry.getKey().name()).append("\":{");
            sb.append("\"maxConcurrency\":").append(info.maxConcurrency()).append(",");
            sb.append("\"activeDispatches\":").append(info.activeDispatches()).append(",");
            sb.append("\"availablePermits\":").append(info.availablePermits()).append(",");
            sb.append("\"queueSize\":").append(info.queueSize()).append(",");
            sb.append("\"utilizationPct\":").append(String.format("%.1f", info.utilizationPct())).append(",");
            sb.append("\"underBackpressure\":").append(info.underBackpressure());
            sb.append("}");
        }
        sb.append("}");

        // WAL status
        var wal = engine.getWalHealth();
        sb.append(",\"wal\":{");
        sb.append("\"status\":\"").append(wal.status()).append("\",");
        sb.append("\"unflushedBytes\":").append(wal.unflushedBytes()).append(",");
        sb.append("\"lastFsyncMsAgo\":").append(wal.lastFsyncMsAgo()).append(",");
        sb.append("\"writePosition\":").append(wal.writePosition()).append(",");
        sb.append("\"flushedPosition\":").append(wal.flushedPosition());
        sb.append("}");

        sb.append(",\"timestamp\":\"").append(java.time.Instant.now().toString()).append("\"");
        sb.append("}");
        return sb.toString();
    }

    private record RaftPeerTarget(String nodeId, String host, int port) {}
}

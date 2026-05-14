package com.loomq.server;

import com.loomq.LoomqEngine;
import com.loomq.callback.HttpCallbackHandler;
import com.loomq.callback.NettyHttpDeliveryHandler;
import com.loomq.common.MetricsCollector;
import com.loomq.config.LoomqConfig;
import com.loomq.config.ServerConfig;
import com.loomq.config.WalConfig;
import com.loomq.http.netty.IntentHandler;
import com.loomq.http.netty.NettyHttpServer;
import com.loomq.http.netty.RadixRouter;
import com.loomq.metrics.LoomQMetrics;
import com.loomq.raft.RaftStatusProvider;
import com.loomq.raft.RaftStatusSnapshot;
import io.netty.handler.codec.http.HttpMethod;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        validateSupportedStartupModes();

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
            int raftPort = Integer.parseInt(
                resolveSetting("LOOMQ_RAFT_PORT", "loomq.raft.port", "7930"));

            validateRaftStartupConfig(raftNodeId, peers, peerTargets, raftPort, serverConfig);

            com.loomq.raft.RaftConfig raftConfig = new com.loomq.raft.RaftConfig(
                raftNodeId, peers, dataDir, 150, 300, 50);

            com.loomq.raft.RaftTransport raftTransport = new com.loomq.raft.RaftTransport(raftNodeId);
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

        RadixRouter router = new RadixRouter();
        RaftStatusProvider raftStatus = raftNode;
        new IntentHandler(engine, raftStatus).register(router);
        registerSystemRoutes(router, engine, raftStatus);

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

            if (raftNode != null) {
                raftNode.close();
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

    static void validateSupportedStartupModes() {
        boolean legacyReplicationEnabled = Boolean.parseBoolean(
            resolveSetting("LOOMQ_CLUSTER_ENABLED", "loomq.cluster.enabled", "false"));
        if (legacyReplicationEnabled) {
            throw new IllegalStateException(
                "Legacy cluster/replication mode is no longer supported. " +
                    "Enable Raft with LOOMQ_RAFT_ENABLED and LOOMQ_RAFT_PEERS instead.");
        }
    }

    static List<String> parseRaftPeerIds(String peersSpec, String selfNodeId) {
        if (peersSpec == null || peersSpec.isBlank()) {
            return List.of(selfNodeId);
        }

        List<String> peers = new ArrayList<>();
        HashSet<String> seen = new HashSet<>();
        for (String raw : peersSpec.split(",")) {
            String token = raw.trim();
            if (token.isEmpty()) {
                continue;
            }
            String nodeId = extractPeerNodeId(token);
            if (nodeId.isEmpty()) {
                throw new IllegalStateException("Raft peer entry is blank");
            }
            if (!seen.add(nodeId)) {
                throw new IllegalStateException("Duplicate Raft peer id: " + nodeId);
            }
            peers.add(nodeId);
        }

        if (peers.isEmpty()) {
            return List.of(selfNodeId);
        }
        return List.copyOf(peers);
    }

    static List<RaftPeerTarget> parseRaftPeerTargets(String peersSpec, String selfNodeId) {
        if (peersSpec == null || peersSpec.isBlank()) {
            return List.of();
        }

        List<RaftPeerTarget> targets = new ArrayList<>();
        HashSet<String> seen = new HashSet<>();
        for (String raw : peersSpec.split(",")) {
            String token = raw.trim();
            if (token.isEmpty()) {
                continue;
            }

            RaftPeerTarget target = parsePeerTarget(token, selfNodeId);
            if (target != null) {
                if (!seen.add(target.nodeId())) {
                    throw new IllegalStateException("Duplicate Raft peer endpoint: " + target.nodeId());
                }
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
            String peerId = token.trim();
            if (peerId.isEmpty()) {
                throw new IllegalStateException("Raft peer entry is blank");
            }
            if (peerId.equals(selfNodeId)) {
                return null;
            }
            throw new IllegalStateException(
                "Raft peer '" + peerId + "' is missing host:port endpoint; use peerId@host:port or peerId=host:port");
        }

        String peerId = token.substring(0, sep).trim();
        if (peerId.isEmpty()) {
            throw new IllegalStateException("Raft peer id is blank in '" + token + "'");
        }
        if (peerId.equals(selfNodeId)) {
            return null;
        }

        String endpoint = token.substring(sep + 1).trim();
        int colon = endpoint.lastIndexOf(':');
        if (colon <= 0 || colon == endpoint.length() - 1) {
            throw new IllegalStateException(
                "Invalid Raft peer endpoint '" + token + "', expected peerId@host:port or peerId=host:port");
        }

        String host = endpoint.substring(0, colon).trim();
        String portStr = endpoint.substring(colon + 1).trim();
        if (host.isEmpty()) {
            throw new IllegalStateException("Raft peer host is blank in '" + token + "'");
        }
        try {
            int port = Integer.parseInt(portStr);
            if (port <= 0 || port > 65535) {
                throw new IllegalStateException("Raft peer port must be in range [1, 65535]: " + token);
            }
            return new RaftPeerTarget(peerId, host, port);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid Raft peer port in '" + token + "': " + portStr, e);
        }
    }

    static void validateRaftStartupConfig(String raftNodeId, List<String> peers,
                                          List<RaftPeerTarget> peerTargets, int raftPort,
                                          ServerConfig serverConfig) {
        if (raftNodeId == null || raftNodeId.isBlank()) {
            throw new IllegalStateException("Raft node id cannot be blank");
        }
        if (peers == null || peers.isEmpty()) {
            throw new IllegalStateException("Raft peers cannot be empty");
        }
        if (!peers.contains(raftNodeId)) {
            throw new IllegalStateException("Raft peer list must include the local node id: " + raftNodeId);
        }
        if (peers.size() > 1 && peerTargets.isEmpty()) {
            throw new IllegalStateException(
                "Raft cluster mode requires connectable peer endpoints; use peerId@host:port or peerId=host:port");
        }
        if (peerTargets.size() != Math.max(0, peers.size() - 1)) {
            throw new IllegalStateException(
                "Raft peer endpoints must cover every remote peer exactly once");
        }
        if (raftPort <= 0 || raftPort > 65535) {
            throw new IllegalStateException("Raft port must be in range [1, 65535]");
        }
        if (serverConfig.port() > 0 && serverConfig.port() == raftPort) {
            throw new IllegalStateException("Raft port must differ from HTTP server port");
        }
        if (serverConfig.nettyPort() > 0 && serverConfig.nettyPort() == raftPort) {
            throw new IllegalStateException("Raft port must differ from Netty port");
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

    private static void registerSystemRoutes(RadixRouter router, LoomqEngine engine,
                                             RaftStatusProvider raftStatus) {
        router.add(HttpMethod.GET, "/health", (method, uri, body, headers, pathParams) ->
            buildHealthResponse(engine, raftStatus));
        router.add(HttpMethod.GET, "/health/live", (method, uri, body, headers, pathParams) ->
            HEALTH_LIVE_RESPONSE);
        router.add(HttpMethod.GET, "/health/ready", (method, uri, body, headers, pathParams) ->
            LoomQMetrics.getInstance().isWalHealthy() ? HEALTH_UP_RESPONSE : HEALTH_DOWN_RESPONSE);
        router.add(HttpMethod.GET, "/health/deep", (method, uri, body, headers, pathParams) ->
            buildDeepHealthResponse(engine, raftStatus));
        router.add(HttpMethod.GET, "/metrics", (method, uri, body, headers, pathParams) ->
            LoomQMetrics.getInstance().snapshot());
        router.add(HttpMethod.GET, "/api/v1/metrics", (method, uri, body, headers, pathParams) ->
            LoomQMetrics.getInstance().snapshot());
    }

    private static Map<String, Object> buildHealthResponse(LoomqEngine engine, RaftStatusProvider raftStatus) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("status", engine.getWalHealth().status());
        root.put("timestamp", java.time.Instant.now().toString());
        root.put("wal", buildWalHealthSection(engine));
        root.put("raft", buildRaftHealthSection(raftStatus));
        return root;
    }

    private static Map<String, Object> buildDeepHealthResponse(LoomqEngine engine, RaftStatusProvider raftStatus) {
        Map<String, Object> root = buildHealthResponse(engine, raftStatus);
        Map<String, Object> tiers = new LinkedHashMap<>();
        var backpressure = engine.getScheduler().getBackpressureStatus();
        for (var entry : backpressure.entrySet()) {
            var info = entry.getValue();
            Map<String, Object> infoMap = new LinkedHashMap<>();
            infoMap.put("maxConcurrency", info.maxConcurrency());
            infoMap.put("activeDispatches", info.activeDispatches());
            infoMap.put("availablePermits", info.availablePermits());
            infoMap.put("queueSize", info.queueSize());
            infoMap.put("utilizationPct", String.format("%.1f", info.utilizationPct()));
            infoMap.put("underBackpressure", info.underBackpressure());
            tiers.put(entry.getKey().name(), infoMap);
        }
        root.put("tiers", tiers);
        return root;
    }

    private static Map<String, Object> buildWalHealthSection(LoomqEngine engine) {
        Map<String, Object> wal = new LinkedHashMap<>();
        var status = engine.getWalHealth();
        wal.put("status", status.status());
        wal.put("unflushedBytes", status.unflushedBytes());
        wal.put("lastFsyncMsAgo", status.lastFsyncMsAgo());
        wal.put("writePosition", status.writePosition());
        wal.put("flushedPosition", status.flushedPosition());
        return wal;
    }

    private static Map<String, Object> buildRaftHealthSection(RaftStatusProvider raftStatus) {
        Map<String, Object> raft = new LinkedHashMap<>();
        if (raftStatus == null || !raftStatus.isRaftEnabled()) {
            raft.put("enabled", false);
            return raft;
        }

        RaftStatusSnapshot status = raftStatus.snapshotStatus();
        raft.put("enabled", true);
        raft.put("nodeId", status.nodeId());
        raft.put("role", status.role().name());
        raft.put("leaderId", status.leaderId());
        raft.put("term", status.term());
        raft.put("commitIndex", status.commitIndex());
        raft.put("lastApplied", status.lastApplied());
        raft.put("commitLag", status.commitLag());
        raft.put("replicationLag", status.replicationLag());
        raft.put("connectedPeers", status.connectedPeers());
        raft.put("totalPeers", status.totalPeers());
        raft.put("peerReachability", status.peerReachability());
        return raft;
    }

    record RaftPeerTarget(String nodeId, String host, int port) {}
}

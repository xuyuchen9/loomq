package com.loomq.server;

import com.loomq.LoomqEngine;
import com.loomq.callback.HttpCallbackHandler;
import com.loomq.callback.HttpDeliveryHandler;
import com.loomq.config.LoomqConfig;
import com.loomq.config.WalConfig;
import com.loomq.config.ServerConfig;
import com.loomq.http.netty.IntentHandler;
import com.loomq.http.netty.NettyHttpServer;
import com.loomq.http.netty.RadixRouter;
import com.loomq.common.MetricsCollector;
import com.loomq.metrics.LoomQMetrics;
import io.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.Map;

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
        WalConfig walConfig = overrideWalDataDir(config.getWalConfig(), dataDir);

        MetricsCollector.getInstance().setWalDataDir(dataDir);
        logRuntimeConfiguration(config, nodeId, dataDir, walConfig);

        HttpCallbackHandler callbackHandler = new HttpCallbackHandler();
        LoomqEngine engine = LoomqEngine.builder()
            .nodeId(nodeId)
            .walDir(Path.of(dataDir))
            .walConfig(walConfig)
            .callbackHandler(callbackHandler)
            .deliveryHandler(new HttpDeliveryHandler())
            .build();

        RadixRouter router = new RadixRouter();
        new IntentHandler(engine).register(router);
        registerSystemRoutes(router);

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
                Thread.sleep(1000);
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
            "Runtime config: nodeId={}, dataDir={}, server={}:{} backlog={} virtualThreads={} maxRequestSize={} threadPoolSize={}, netty={}:{} epoll={} pooledAllocator={} maxConnections={} maxConcurrentRequests={}, walDir={}, walEngine={}, walFlushStrategy={}, walFlushThresholdKb={}, walStripeCount={}, schedulerMaxPending={}, recoveryBatchSize={}, retryInitialDelayMs={}, retryMaxDelayMs={}",
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
            walConfig.dataDir(),
            walConfig.engine(),
            walConfig.flushStrategy(),
            walConfig.memorySegmentFlushThresholdKb(),
            walConfig.memorySegmentStripeCount(),
            config.getSchedulerConfig().maxPendingTasks(),
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

    private static WalConfig overrideWalDataDir(WalConfig delegate, String dataDir) {
        return new WalConfig() {
            @Override
            public String dataDir() {
                return dataDir;
            }

            @Override
            public int segmentSizeMb() {
                return delegate.segmentSizeMb();
            }

            @Override
            public String flushStrategy() {
                return delegate.flushStrategy();
            }

            @Override
            public long batchFlushIntervalMs() {
                return delegate.batchFlushIntervalMs();
            }

            @Override
            public boolean syncOnWrite() {
                return delegate.syncOnWrite();
            }

            @Override
            public String engine() {
                return delegate.engine();
            }

            @Override
            public int memorySegmentInitialSizeMb() {
                return delegate.memorySegmentInitialSizeMb();
            }

            @Override
            public int memorySegmentMaxSizeMb() {
                return delegate.memorySegmentMaxSizeMb();
            }

            @Override
            public int memorySegmentFlushThresholdKb() {
                return delegate.memorySegmentFlushThresholdKb();
            }

            @Override
            public long memorySegmentFlushIntervalMs() {
                return delegate.memorySegmentFlushIntervalMs();
            }

            @Override
            public int memorySegmentStripeCount() {
                return delegate.memorySegmentStripeCount();
            }

            @Override
            public int memorySegmentMinBatchSize() {
                return delegate.memorySegmentMinBatchSize();
            }

            @Override
            public boolean memorySegmentAdaptiveFlushEnabled() {
                return delegate.memorySegmentAdaptiveFlushEnabled();
            }

            @Override
            public boolean isReplicationEnabled() {
                return delegate.isReplicationEnabled();
            }

            @Override
            public String replicaHost() {
                return delegate.replicaHost();
            }

            @Override
            public int replicaPort() {
                return delegate.replicaPort();
            }

            @Override
            public long replicationAckTimeoutMs() {
                return delegate.replicationAckTimeoutMs();
            }

            @Override
            public boolean requireReplicatedAck() {
                return delegate.requireReplicatedAck();
            }
        };
    }

    private static void registerSystemRoutes(RadixRouter router) {
        router.add(HttpMethod.GET, "/health", (method, uri, body, headers, pathParams) ->
            HEALTH_UP_RESPONSE);
        router.add(HttpMethod.GET, "/health/live", (method, uri, body, headers, pathParams) ->
            HEALTH_LIVE_RESPONSE);
        router.add(HttpMethod.GET, "/health/ready", (method, uri, body, headers, pathParams) ->
            LoomQMetrics.getInstance().isWalHealthy() ? HEALTH_UP_RESPONSE : HEALTH_DOWN_RESPONSE);
        router.add(HttpMethod.GET, "/metrics", (method, uri, body, headers, pathParams) ->
            LoomQMetrics.getInstance().snapshot());
        router.add(HttpMethod.GET, "/api/v1/metrics", (method, uri, body, headers, pathParams) ->
            LoomQMetrics.getInstance().snapshot());
    }

    private static void printBanner() {
        System.out.println();
        System.out.println("██╗      ██████╗  ██████╗ ███╗   ███╗ ██████╗ ");
        System.out.println("██║     ██╔═══██╗██╔═══██╗████╗ ████║██╔═══██╗");
        System.out.println("██║     ██║   ██║██║   ██║██╔████╔██║██║   ██║");
        System.out.println("██║     ██║   ██║██║   ██║██║╚██╔╝██║██║▄▄ ██║");
        System.out.println("███████╗╚██████╔╝╚██████╔╝██║ ╚═╝ ██║╚██████╔╝");
        System.out.println("╚══════╝ ╚═════╝  ╚═════╝ ╚═╝     ╚═╝ ╚══▀▀═╝ ");
        System.out.println();
        System.out.println(" Event Infrastructure for Delayed Execution");
        System.out.println("              Version 0.7.0");
        System.out.println("              Mode: Server (Netty)");
        System.out.println();
    }
}

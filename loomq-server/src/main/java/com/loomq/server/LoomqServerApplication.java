package com.loomq.server;

import com.loomq.LoomqEngine;
import com.loomq.callback.HttpCallbackHandler;
import com.loomq.callback.HttpDeliveryHandler;
import com.loomq.config.LoomqConfig;
import com.loomq.config.ServerConfig;
import com.loomq.http.netty.IntentHandler;
import com.loomq.http.netty.NettyHttpServer;
import com.loomq.http.netty.RadixRouter;
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

        String nodeId = System.getProperty("loomq.node.id", "node-1");
        String dataDir = System.getProperty("loomq.data.dir", "./data");

        logger.info("Configuration: nodeId={}, dataDir={}, host={}, port={}",
            nodeId, dataDir, serverConfig.host(), serverConfig.port());

        HttpCallbackHandler callbackHandler = new HttpCallbackHandler();
        LoomqEngine engine = LoomqEngine.builder()
            .nodeId(nodeId)
            .walDir(Path.of(dataDir))
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

package com.loomq.server;

import com.loomq.LoomqEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 * LoomQ Server 应用程序入口
 *
 * v0.7.0 模块拆分后：
 * - 此类在 loomq-server 模块中
 * - 依赖 loomq-core 模块的 LoomqEngine
 * - 提供 HTTP 服务和 REST API
 *
 * @author loomq
 * @since v0.7.0
 */
public class LoomqServerApplication {

    private static final Logger logger = LoggerFactory.getLogger(LoomqServerApplication.class);

    public static void main(String[] args) {
        printBanner();

        // 解析配置
        String nodeId = System.getProperty("loomq.node.id", "node-1");
        String dataDir = System.getProperty("loomq.data.dir", "./data");

        logger.info("Configuration: nodeId={}, dataDir={}", nodeId, dataDir);

        // 创建引擎（使用 Builder 模式）
        LoomqEngine engine = LoomqEngine.builder()
            .nodeId(nodeId)
            .walDir(Path.of(dataDir))
            .build();

        // 注册关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received, stopping engine...");
            try {
                engine.close();
            } catch (Exception e) {
                logger.error("Error during engine shutdown", e);
            }
        }));

        try {
            // 启动引擎
            engine.start();

            // 保持主线程运行
            while (engine.isRunning()) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("Main thread interrupted");
        } catch (Exception e) {
            logger.error("Engine failed", e);
            System.exit(1);
        }

        logger.info("Application exited");
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
        System.out.println("              Mode: Server (HTTP)");
        System.out.println();
    }
}

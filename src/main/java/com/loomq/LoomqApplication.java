package com.loomq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LoomQ 应用程序入口
 *
 * v0.6.1 简化引擎：
 * - WAL: 8字节头+二进制，~100ns 序列化，零 GC
 * - 投递: 批量同步替代异步回调
 * - 目标: 200K+ QPS
 *
 * @author loomq
 * @since v0.6.1
 */
public class LoomqApplication {

    private static final Logger logger = LoggerFactory.getLogger(LoomqApplication.class);

    public static void main(String[] args) {
        printBanner();

        // 解析配置
        String nodeId = System.getProperty("loomq.node.id", "node-1");
        String shardId = System.getProperty("loomq.shard.id", "shard-0");
        String dataDir = System.getProperty("loomq.data.dir", "./data");
        int port = Integer.parseInt(System.getProperty("loomq.port", "8080"));

        logger.info("Configuration: nodeId={}, shardId={}, dataDir={}, port={}",
            nodeId, shardId, dataDir, port);

        // 创建引擎
        LoomqEngine engine = new LoomqEngine(nodeId, shardId, dataDir, port);

        // 注册关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received, stopping engine...");
            engine.stop();
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
        System.out.println("              Version 0.6.1");
        System.out.println();
    }
}

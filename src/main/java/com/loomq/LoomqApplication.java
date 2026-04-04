package com.loomq;

import com.loomq.cluster.ClusterManager;
import com.loomq.cluster.LocalShardNode;
import com.loomq.common.AlertService;
import com.loomq.common.MetricsCollector;
import com.loomq.config.LoomqConfig;
import com.loomq.dispatcher.WebhookDispatcher;
import com.loomq.gateway.HealthController;
import com.loomq.gateway.TaskController;
import com.loomq.recovery.RecoveryService;
import com.loomq.scheduler.TaskScheduler;
import com.loomq.store.TaskStore;
import com.loomq.wal.WalEngine;
import io.javalin.Javalin;
import io.javalin.json.JavalinJackson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loomq 启动入口（V0.3 - 支持分片模式）
 *
 * 启动模式：
 * 1. 单节点模式：单进程运行所有组件
 * 2. 分片模式：作为集群中的一个分片运行
 *
 * 分片配置通过环境变量或配置文件指定：
 * - LOOMQ_SHARD_INDEX：当前节点分片索引
 * - LOOMQ_TOTAL_SHARDS：总分片数
 */
public class LoomqApplication {
    private static final Logger logger = LoggerFactory.getLogger(LoomqApplication.class);

    private final LoomqConfig config;
    private Javalin server;
    private WalEngine walEngine;
    private TaskStore taskStore;
    private TaskScheduler scheduler;
    private WebhookDispatcher dispatcher;
    private RecoveryService recoveryService;

    // V0.3 新增：集群管理器
    private ClusterManager clusterManager;

    public LoomqApplication() {
        this.config = LoomqConfig.getInstance();
    }

    public static void main(String[] args) {
        LoomqApplication app = new LoomqApplication();
        try {
            app.start();
            // 注册关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down Loomq...");
                app.stop();
                logger.info("Loomq stopped.");
            }));
        } catch (Exception e) {
            logger.error("Failed to start Loomq", e);
            System.exit(1);
        }
    }

    public void start() throws Exception {
        logger.info("Starting Loomq...");

        // V0.3 新增：启动集群管理器
        logger.info("Initializing cluster manager...");
        clusterManager = new ClusterManager();
        clusterManager.start();

        if (clusterManager.isSharded()) {
            logger.info("Running in SHARDED mode: {}", clusterManager.getStatus());
        } else {
            logger.info("Running in SINGLE-NODE mode");
        }

        // 1. 初始化 WAL 引擎
        logger.info("Initializing WAL engine...");
        walEngine = new WalEngine(config.getWalConfig());
        walEngine.start();

        // 初始化指标采集器
        MetricsCollector.getInstance().setWalDataDir(config.getWalConfig().dataDir());

        // 启动告警服务
        AlertService.getInstance().start();

        // 2. 初始化存储层
        logger.info("Initializing task store...");
        taskStore = new TaskStore();

        // 3. 初始化恢复服务并执行恢复
        logger.info("Starting recovery service...");
        recoveryService = new RecoveryService(walEngine, taskStore, config.getRecoveryConfig());
        recoveryService.recover();

        // 4. 初始化分发器
        logger.info("Initializing webhook dispatcher...");
        dispatcher = new WebhookDispatcher(walEngine, taskStore, config.getDispatcherConfig(), config.getRetryConfig());

        // 5. 初始化调度器
        logger.info("Initializing scheduler...");
        scheduler = new TaskScheduler(walEngine, taskStore, dispatcher, config.getSchedulerConfig());
        scheduler.start();

        // 6. 启动 HTTP 服务
        logger.info("Starting HTTP server...");
        startHttpServer();

        logger.info("Loomq started successfully on {}:{}", config.getServerConfig().host(), config.getServerConfig().port());
    }

    private void startHttpServer() {
        server = Javalin.create(config -> {
            config.jsonMapper(new JavalinJackson());
            config.showJavalinBanner = false;
        });

        // 注册路由
        TaskController taskController = new TaskController(walEngine, taskStore, scheduler, dispatcher);
        HealthController healthController = new HealthController(taskStore);

        // 任务 API
        server.post("/api/v1/tasks", taskController::createTask);
        server.get("/api/v1/tasks/{taskId}", taskController::getTask);
        server.delete("/api/v1/tasks/{taskId}", taskController::cancelTask);
        server.patch("/api/v1/tasks/{taskId}", taskController::modifyTask);
        server.post("/api/v1/tasks/{taskId}/fire-now", taskController::fireNow);

        // 健康检查
        server.get("/health", healthController::health);
        server.get("/metrics", healthController::metrics);

        // 异常处理
        server.exception(Exception.class, (e, ctx) -> {
            logger.error("Unhandled exception", e);
            ctx.status(500).json(new ErrorResponse(500, "Internal server error: " + e.getMessage()));
        });

        server.start(config.getServerConfig().port());
    }

    public void stop() {
        // 停止顺序：反向
        if (server != null) {
            server.stop();
        }
        if (scheduler != null) {
            scheduler.stop();
        }
        if (dispatcher != null) {
            dispatcher.stop();
        }
        if (walEngine != null) {
            walEngine.stop();
        }
        // 停止集群管理器
        if (clusterManager != null) {
            clusterManager.stop();
        }
        // 停止告警服务
        AlertService.getInstance().stop();
    }

    /**
     * 错误响应 DTO
     */
    public record ErrorResponse(int code, String message) {}
}

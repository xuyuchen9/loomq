package com.loomq;

import com.loomq.api.IntentController;
import com.loomq.cluster.*;
import com.loomq.metrics.LoomQMetrics;
import com.loomq.metrics.MetricsEndpoint;
import com.loomq.cluster.ReplicaRole;
import com.loomq.replication.ReplicationManager;
import com.loomq.scheduler.v5.IntentScheduler;
import com.loomq.snapshot.SnapshotManager;
import com.loomq.store.IntentStore;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * LoomQ v0.5 引擎 - 统一整合所有组件
 *
 * 核心能力：
 * 1. Intent 生命周期管理（创建→调度→投递→确认）
 * 2. 三级 ACK 级别（ASYNC/DURABLE/REPLICATED）
 * 3. Primary-Replica 复制架构
 * 4. Lease 仲裁与自动故障转移
 * 5. Fencing Token 脑裂防护
 * 6. Snapshot + WAL 恢复
 * 7. 可观测性指标
 *
 * @author loomq
 * @since v0.5.0
 */
public class LoomqEngine implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(LoomqEngine.class);

    private final String nodeId;
    private final String shardId;
    private final String dataDir;
    private final int httpPort;

    // 核心组件
    private IntentStore intentStore;
    private IntentScheduler scheduler;
    private SnapshotManager snapshotManager;
    private InMemoryLeaseCoordinator leaseCoordinator;
    private FailoverController failoverController;
    private ReplicationManager replicationManager;

    // HTTP 服务
    private Javalin app;

    // 状态
    private volatile EngineState state = EngineState.CREATED;
    private final ScheduledExecutorService executor;

    public LoomqEngine(String nodeId, String shardId, String dataDir, int httpPort) {
        this.nodeId = nodeId;
        this.shardId = shardId;
        this.dataDir = dataDir;
        this.httpPort = httpPort;
        this.executor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "engine-" + shardId);
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 启动引擎
     */
    public void start() {
        if (state != EngineState.CREATED) {
            throw new IllegalStateException("Engine already started or stopped");
        }

        state = EngineState.STARTING;
        logger.info("╔════════════════════════════════════════════════════════╗");
        logger.info("║      LoomQ v0.5 Engine Starting...                     ║");
        logger.info("║      node={}, shard={}, port={}                    ║", nodeId, shardId, httpPort);
        logger.info("╚════════════════════════════════════════════════════════╝");

        try {
            // 1. 初始化存储层（快照 + WAL）
            initializeStorage();

            // 2. 初始化租约协调器
            initializeLeaseCoordinator();

            // 3. 初始化复制管理器
            initializeReplicationManager();

            // 4. 初始化故障转移控制器
            initializeFailoverController();

            // 5. 启动调度器
            scheduler.start();

            // 6. 启动 HTTP 服务
            startHttpServer();

            state = EngineState.RUNNING;
            logger.info("LoomQ v0.5 engine started successfully!");

            // 启动指标收集
            startMetricsCollection();

        } catch (Exception e) {
            state = EngineState.FAILED;
            logger.error("Failed to start engine", e);
            throw new RuntimeException("Engine startup failed", e);
        }
    }

    /**
     * 停止引擎
     */
    public void stop() {
        if (state == EngineState.STOPPED) return;

        state = EngineState.STOPPING;
        logger.info("LoomQ v0.5 engine stopping...");

        // 1. 停止 HTTP 服务
        if (app != null) {
            app.stop();
        }

        // 2. 关闭故障转移控制器
        if (failoverController != null) {
            failoverController.close();
        }

        // 3. 停止调度器
        if (scheduler != null) {
            scheduler.stop();
        }

        // 4. 停止快照管理器
        if (snapshotManager != null) {
            snapshotManager.stop();
        }

        // 5. 关闭存储
        if (intentStore != null) {
            intentStore.shutdown();
        }

        // 6. 关闭线程池
        executor.shutdown();

        state = EngineState.STOPPED;
        logger.info("LoomQ v0.5 engine stopped");
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * 初始化存储层
     */
    private void initializeStorage() {
        logger.info("[1/6] Initializing storage layer...");

        // 创建快照管理器
        snapshotManager = new SnapshotManager(dataDir);
        snapshotManager.start();

        // 从快照恢复
        SnapshotManager.SnapshotResult snapshotResult = snapshotManager.restoreFromSnapshot();

        // 创建 Intent 存储
        intentStore = new IntentStore();

        // 恢复 Intent 数据
        for (var intent : snapshotResult.intents) {
            intentStore.save(intent);
        }

        // 创建调度器
        scheduler = new IntentScheduler(intentStore);

        logger.info("    Storage initialized: {} intents restored, WAL offset={}",
            snapshotResult.intents.size(), snapshotResult.walOffset);
    }

    /**
     * 初始化租约协调器
     */
    private void initializeLeaseCoordinator() {
        logger.info("[2/6] Initializing lease coordinator...");

        leaseCoordinator = new InMemoryLeaseCoordinator();
        leaseCoordinator.start();

        logger.info("    Lease coordinator ready");
    }

    /**
     * 初始化复制管理器
     */
    private void initializeReplicationManager() {
        logger.info("[3/6] Initializing replication manager...");

        // 创建复制管理器（使用现有 v0.4.8 实现）
        replicationManager = new ReplicationManager(nodeId);

        logger.info("    Replication manager ready");
    }

    /**
     * 初始化故障转移控制器
     */
    private void initializeFailoverController() {
        logger.info("[4/6] Initializing failover controller...");

        // 初始角色为 REPLICA，等待租约仲裁
        failoverController = new FailoverController(
            nodeId, shardId, ReplicaRole.FOLLOWER, 0L);

        // 设置租约提供者
        failoverController.setLeaseProvider(shardId ->
            CompletableFuture.supplyAsync(() ->
                leaseCoordinator.tryAcquire(shardId, nodeId, 10000).orElse(null)
            )
        );

        // 设置租约续约函数
        failoverController.setLeaseRenewer((shardId, leaseId) -> {
            CoordinatorLease currentLease = failoverController.getCurrentLease();
            if (currentLease != null) {
                return CompletableFuture.supplyAsync(() ->
                    leaseCoordinator.renew(currentLease).orElse(null)
                );
            }
            return CompletableFuture.completedFuture(null);
        });

        // 设置角色变更回调
        failoverController.setLeaseExpiredCallback(() -> {
            logger.warn("Lease expired! Pausing scheduler...");
            scheduler.pause();
        });

        // 关联复制管理器
        failoverController.setReplicationManager(replicationManager);

        // 启动故障转移控制器
        failoverController.start();

        logger.info("    Failover controller ready, initial state: {}",
            failoverController.getCurrentState());
    }

    /**
     * 启动 HTTP 服务
     */
    private void startHttpServer() {
        logger.info("[5/6] Starting HTTP server on port {}...", httpPort);

        app = Javalin.create(config -> {
            config.showJavalinBanner = false;
        });

        // Intent API v5
        IntentController controller = new IntentController(intentStore);
        controller.register(app);

        // 监控端点 (包含 /health, /health/live, /health/ready, /health/wal, /health/replica)
        MetricsEndpoint metrics = new MetricsEndpoint();
        metrics.register(app);

        // 集群状态端点
        app.get("/v1/cluster/status", ctx -> ctx.json(Map.of(
            "nodeId", nodeId,
            "shardId", shardId,
            "state", state.name(),
            "isPrimary", isPrimary(),
            "role", failoverController != null ? failoverController.getCurrentState().name() : "UNKNOWN"
        )));

        app.start(httpPort);

        logger.info("    HTTP server ready");
    }

    /**
     * 启动指标收集
     */
    private void startMetricsCollection() {
        logger.info("[6/6] Starting metrics collection...");

        executor.scheduleAtFixedRate(() -> {
            try {
                LoomQMetrics metrics = LoomQMetrics.getInstance();

                // 更新系统指标
                if (intentStore != null) {
                    metrics.updatePendingIntents(intentStore.getPendingCount());
                }

                // 更新角色指标
                if (failoverController != null) {
                    metrics.updateIntentStatus("role", isPrimary() ? 1 : 0);
                    metrics.updateIntentStatus("epoch",
                        failoverController.getCurrentLease() != null
                            ? failoverController.getCurrentLease().getEpoch() : 0);
                }

                // 自动快照
                if (snapshotManager != null && isPrimary()) {
                    // 每小时创建一次快照（简化实现）
                }

            } catch (Exception e) {
                logger.error("Error in metrics collection", e);
            }
        }, 10, 10, TimeUnit.SECONDS);

        logger.info("    Metrics collection ready");
    }

    // ==================== 状态查询 ====================

    public EngineState getState() {
        return state;
    }

    public boolean isPrimary() {
        return failoverController != null && failoverController.isPrimary();
    }

    public boolean isRunning() {
        return state == EngineState.RUNNING;
    }

    public IntentStore getIntentStore() {
        return intentStore;
    }

    public IntentScheduler getScheduler() {
        return scheduler;
    }

    public FailoverController getFailoverController() {
        return failoverController;
    }

    // ==================== 内部类 ====================

    public enum EngineState {
        CREATED,
        STARTING,
        RUNNING,
        STOPPING,
        STOPPED,
        FAILED
    }

    public record HealthResponse(String status, String details) {}

    // ==================== 主入口 ====================

    public static void main(String[] args) {
        String nodeId = System.getProperty("loomq.node.id", "node-1");
        String shardId = System.getProperty("loomq.shard.id", "shard-0");
        String dataDir = System.getProperty("loomq.data.dir", "./data");
        int port = Integer.parseInt(System.getProperty("loomq.port", "8080"));

        LoomqEngine engine = new LoomqEngine(nodeId, shardId, dataDir, port);

        Runtime.getRuntime().addShutdownHook(new Thread(engine::stop));

        engine.start();
    }
}


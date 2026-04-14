package com.loomq;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.cluster.CoordinatorLease;
import com.loomq.cluster.FailoverController;
import com.loomq.cluster.InMemoryLeaseCoordinator;
import com.loomq.cluster.ReplicaRole;
import com.loomq.common.MetricsCollector;
import com.loomq.config.WalConfig;
import com.loomq.application.dispatcher.BatchDispatcher;
import com.loomq.domain.intent.Intent;
import com.loomq.http.netty.IntentHandler;
import com.loomq.http.netty.NettyHttpServer;
import com.loomq.http.netty.NettyServerConfig;
import com.loomq.http.netty.RadixRouter;
import com.loomq.infrastructure.wal.IntentWal;
import com.loomq.replication.ReplicationManager;
import com.loomq.snapshot.SnapshotManager;
import com.loomq.store.IntentStore;
import org.aeonbits.owner.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * LoomQ v0.6.1 简化引擎 - 第一性原理实现
 *
 * 核心简化：
 * 1. WAL: 8字节头+二进制，~100ns 序列化，零 GC
 * 2. 投递: 批量同步替代异步回调
 * 3. 过期检查: 保留 BucketGroup（已足够高效）
 *
 * 代码对比：
 * - IntentWalV2: ~100 行 vs IntentWal ~600 行
 * - BatchDispatcher: ~250 行 vs PrecisionScheduler 异步链 ~400 行
 * - 总计: 约 60% 代码量减少
 *
 * 性能目标:
 * - DURABLE QPS: 200K+
 * - ASYNC QPS: 500K+
 * - P99 延迟: < 500µs
 *
 * @author loomq
 * @since v0.6.1
 */
public class LoomqEngine implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(LoomqEngine.class);

    private final String nodeId;
    private final String shardId;
    private final String dataDir;
    private final int httpPort;

    // 核心组件
    private IntentStore intentStore;
    private PrecisionScheduler scheduler;
    private BatchDispatcher dispatcher;
    private SnapshotManager snapshotManager;
    private InMemoryLeaseCoordinator leaseCoordinator;
    private FailoverController failoverController;
    private ReplicationManager replicationManager;
    private IntentWal intentWal;

    // HTTP 服务
    private NettyHttpServer nettyHttpServer;

    // 状态
    private volatile EngineState state = EngineState.CREATED;
    private final ScheduledExecutorService executor;

    public LoomqEngine(String nodeId, String shardId, String dataDir, int httpPort) {
        this.nodeId = nodeId;
        this.shardId = shardId;
        this.dataDir = dataDir;
        this.httpPort = httpPort;
        this.executor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "loomq-engine-" + shardId);
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
        logger.info("║      LoomQ v0.6.1 Simplified Engine Starting...        ║");
        logger.info("║      node={}, shard={}, port={}                    ║", nodeId, shardId, httpPort);
        logger.info("╚════════════════════════════════════════════════════════╝");

        try {
            // 1. 初始化存储层
            initializeStorage();

            // 2. 初始化租约协调器
            initializeLeaseCoordinator();

            // 3. 初始化复制管理器
            initializeReplicationManager();

            // 4. 初始化故障转移控制器
            initializeFailoverController();

            // 5. 启动调度器
            scheduler.start();

            // 6. 启动投递器
            dispatcher.start();

            // 7. 启动 HTTP 服务
            startHttpServer();

            state = EngineState.RUNNING;
            logger.info("LoomQ v0.6.1 engine started successfully!");

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
        logger.info("LoomQ v0.6.1 engine stopping...");

        // 1. 停止 HTTP 服务
        if (nettyHttpServer != null) {
            nettyHttpServer.stop();
        }

        // 2. 关闭故障转移控制器
        if (failoverController != null) {
            failoverController.close();
        }

        // 3. 停止投递器
        if (dispatcher != null) {
            dispatcher.close();
        }

        // 4. 停止调度器
        if (scheduler != null) {
            scheduler.stop();
        }

        // 5. 关闭 WAL
        if (intentWal != null) {
            intentWal.close();
        }

        // 6. 停止快照管理器
        if (snapshotManager != null) {
            snapshotManager.stop();
        }

        // 7. 关闭存储
        if (intentStore != null) {
            intentStore.shutdown();
        }

        // 8. 关闭线程池
        executor.shutdown();

        state = EngineState.STOPPED;
        logger.info("LoomQ v0.6.1 engine stopped");
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * 初始化存储层
     */
    private void initializeStorage() {
        logger.info("[1/6] Initializing storage layer (simplified)...");

        // 创建快照管理器
        snapshotManager = new SnapshotManager(dataDir);
        snapshotManager.start();

        // 从快照恢复
        SnapshotManager.SnapshotResult snapshotResult = snapshotManager.restoreFromSnapshot();

        // 初始化简化 WAL
        try {
            WalConfig walConfig = ConfigFactory.create(WalConfig.class);
            String walDataDir = dataDir + "/wal";

            WalConfig customWalConfig = createWalConfig(walConfig, walDataDir);

            intentWal = new IntentWal(customWalConfig, shardId);
            intentWal.start();

            logger.info("    WAL initialized (simplified, engine=SimpleWalWriter)");
        } catch (Exception e) {
            logger.warn("    WAL initialization failed, running without WAL: {}", e.getMessage());
        }

        // 创建 Intent 存储
        intentStore = new IntentStore();

        // 恢复 Intent 数据
        for (var intent : snapshotResult.intents) {
            intentStore.save(intent);
        }

        // 创建调度器
        scheduler = new PrecisionScheduler(intentStore);

        // 创建批量投递器
        dispatcher = new BatchDispatcher(intentStore, scheduler);

        logger.info("    Storage initialized: {} intents restored", snapshotResult.intents.size());
    }

    /**
     * 创建 WAL 配置
     */
    private WalConfig createWalConfig(WalConfig base, String walDataDir) {
        return new WalConfig() {
            @Override public String dataDir() { return walDataDir; }
            @Override public int segmentSizeMb() { return base.segmentSizeMb(); }
            @Override public String flushStrategy() { return base.flushStrategy(); }
            @Override public long batchFlushIntervalMs() { return base.batchFlushIntervalMs(); }
            @Override public boolean syncOnWrite() { return base.syncOnWrite(); }
            @Override public String engine() { return base.engine(); }
            @Override public int memorySegmentInitialSizeMb() { return base.memorySegmentInitialSizeMb(); }
            @Override public int memorySegmentMaxSizeMb() { return base.memorySegmentMaxSizeMb(); }
            @Override public int memorySegmentFlushThresholdKb() { return base.memorySegmentFlushThresholdKb(); }
            @Override public long memorySegmentFlushIntervalMs() { return base.memorySegmentFlushIntervalMs(); }
            @Override public int memorySegmentStripeCount() { return base.memorySegmentStripeCount(); }
            @Override public int memorySegmentMinBatchSize() { return base.memorySegmentMinBatchSize(); }
            @Override public boolean memorySegmentAdaptiveFlushEnabled() { return base.memorySegmentAdaptiveFlushEnabled(); }
            @Override public boolean isReplicationEnabled() { return base.isReplicationEnabled(); }
            @Override public String replicaHost() { return base.replicaHost(); }
            @Override public int replicaPort() { return base.replicaPort(); }
            @Override public long replicationAckTimeoutMs() { return base.replicationAckTimeoutMs(); }
            @Override public boolean requireReplicatedAck() { return base.requireReplicatedAck(); }
        };
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
        replicationManager = new ReplicationManager(nodeId);

        // 将 ReplicationManager 设置到 WAL，支持 REPLICATED ACK
        if (intentWal != null) {
            intentWal.setReplicationManager(replicationManager);
        }

        logger.info("    Replication manager ready");
    }

    /**
     * 初始化故障转移控制器
     */
    private void initializeFailoverController() {
        logger.info("[4/6] Initializing failover controller...");

        failoverController = new FailoverController(
            nodeId, shardId, ReplicaRole.FOLLOWER, 0L);

        failoverController.setLeaseProvider(shardId ->
            CompletableFuture.supplyAsync(() ->
                leaseCoordinator.tryAcquire(shardId, nodeId, 10000).orElse(null)
            )
        );

        failoverController.setLeaseRenewer((shardId, leaseId) -> {
            CoordinatorLease currentLease = failoverController.getCurrentLease();
            if (currentLease != null) {
                return CompletableFuture.supplyAsync(() ->
                    leaseCoordinator.renew(currentLease).orElse(null)
                );
            }
            return CompletableFuture.completedFuture(null);
        });

        failoverController.setLeaseExpiredCallback(() -> {
            logger.warn("Lease expired! Pausing scheduler...");
            scheduler.pause();
        });

        failoverController.setReplicationManager(replicationManager);
        failoverController.start();

        logger.info("    Failover controller ready");
    }

    /**
     * 启动 HTTP 服务
     */
    private void startHttpServer() {
        logger.info("[5/6] Starting HTTP server on port {}...", httpPort);

        try {
            RadixRouter router = new RadixRouter();

            // 注册 Intent API
            IntentHandler intentHandler = new IntentHandler(
                intentStore, intentWal, dispatcher, scheduler);
            intentHandler.register(router);

            // 健康检查端点
            router.add(io.netty.handler.codec.http.HttpMethod.GET, "/health",
                (method, uri, body, headers, pathParams) -> Map.of("status", "UP"));
            router.add(io.netty.handler.codec.http.HttpMethod.GET, "/health/live",
                (method, uri, body, headers, pathParams) -> Map.of("status", "UP"));
            router.add(io.netty.handler.codec.http.HttpMethod.GET, "/health/ready",
                (method, uri, body, headers, pathParams) -> Map.of("status", "UP"));

            // 指标端点 (Prometheus 格式)
            router.add(io.netty.handler.codec.http.HttpMethod.GET, "/metrics",
                (method, uri, body, headers, pathParams) -> {
                    StringBuilder sb = new StringBuilder();
                    sb.append("# HELP loomq_intents_created_total Total intents\n");
                    sb.append("loomq_intents_created_total ")
                      .append(intentStore != null ? intentStore.getPendingCount() : 0).append("\n");

                    // WAL 统计
                    if (intentWal != null) {
                        var stats = intentWal.getStats();
                        sb.append("# HELP loomq_wal_writes_total WAL writes\n");
                        sb.append("loomq_wal_writes_total ").append(stats.getWriteCount()).append("\n");
                        sb.append("# HELP loomq_wal_flushes_total WAL flushes\n");
                        sb.append("loomq_wal_flushes_total ").append(stats.getFlushCount()).append("\n");
                    }

                    return sb.toString();
                });

            // 集群状态
            router.add(io.netty.handler.codec.http.HttpMethod.GET, "/v1/cluster/status",
                (method, uri, body, headers, pathParams) -> Map.of(
                    "nodeId", nodeId,
                    "shardId", shardId,
                    "state", state.name(),
                    "isPrimary", isPrimary(),
                    "engine", "simplified-v0.6.1"
                ));

            NettyServerConfig config = createNettyConfig();
            nettyHttpServer = new NettyHttpServer(config, router);
            nettyHttpServer.start();

            logger.info("    HTTP server ready (simplified engine)");

        } catch (Exception e) {
            logger.error("Failed to start HTTP server", e);
            throw new RuntimeException("HTTP server startup failed", e);
        }
    }

    /**
     * 创建 Netty 配置
     */
    private NettyServerConfig createNettyConfig() {
        NettyServerConfig base = ConfigFactory.create(NettyServerConfig.class);
        final int port = this.httpPort;

        return new NettyServerConfig() {
            @Override public int port() { return port; }
            @Override public String host() { return base.host(); }
            @Override public int bossThreads() { return base.bossThreads(); }
            @Override public int workerThreads() { return base.workerThreads(); }
            @Override public int maxContentLength() { return base.maxContentLength(); }
            @Override public boolean useEpoll() { return base.useEpoll(); }
            @Override public boolean pooledAllocator() { return base.pooledAllocator(); }
            @Override public int soBacklog() { return base.soBacklog(); }
            @Override public boolean tcpNoDelay() { return base.tcpNoDelay(); }
            @Override public int connectionTimeoutMs() { return base.connectionTimeoutMs(); }
            @Override public int idleTimeoutSeconds() { return base.idleTimeoutSeconds(); }
            @Override public int maxConnections() { return base.maxConnections(); }
            @Override public int writeBufferHighWaterMark() { return base.writeBufferHighWaterMark(); }
            @Override public int writeBufferLowWaterMark() { return base.writeBufferLowWaterMark(); }
            @Override public int maxConcurrentBusinessRequests() { return base.maxConcurrentBusinessRequests(); }
            @Override public long gracefulShutdownTimeoutMs() { return base.gracefulShutdownTimeoutMs(); }
        };
    }

    /**
     * 启动指标收集
     */
    private void startMetricsCollection() {
        logger.info("[6/6] Starting metrics collection...");

        executor.scheduleAtFixedRate(() -> {
            try {
                MetricsCollector metrics = MetricsCollector.getInstance();

                if (intentStore != null) {
                    metrics.updatePendingIntents(intentStore.getPendingCount());
                }

                if (failoverController != null) {
                    metrics.updateIntentStatus("role", isPrimary() ? 1 : 0);
                }

            } catch (Exception e) {
                logger.error("Error in metrics collection", e);
            }
        }, 10, 10, TimeUnit.SECONDS);

        logger.info("    Metrics collection ready");
    }

    // ==================== 公共 API ====================

    /**
     * 创建 Intent（集成简化 WAL + 批量投递）
     */
    public CompletableFuture<Intent> createIntent(Intent intent) {
        if (state != EngineState.RUNNING) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Engine not running"));
        }

        // 1. 写入 WAL
        CompletableFuture<Long> walFuture;
        switch (intent.getAckLevel()) {
            case DURABLE -> walFuture = intentWal.appendCreateDurable(intent);
            case REPLICATED -> walFuture = intentWal.appendCreateReplicated(intent);
            default -> walFuture = intentWal.appendCreateAsync(intent);
        }

        // 2. 保存到存储
        return walFuture.thenApply(pos -> {
            intentStore.save(intent);
            return intent;
        });
    }

    // ==================== 状态查询 ====================

    public EngineState getState() { return state; }
    public boolean isPrimary() { return failoverController != null && failoverController.isPrimary(); }
    public boolean isRunning() { return state == EngineState.RUNNING; }
    public IntentStore getIntentStore() { return intentStore; }
    public PrecisionScheduler getScheduler() { return scheduler; }
    public BatchDispatcher getDispatcher() { return dispatcher; }
    public IntentWal getIntentWal() { return intentWal; }

    // ==================== 内部类 ====================

    public enum EngineState {
        CREATED, STARTING, RUNNING, STOPPING, STOPPED, FAILED
    }

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

package com.loomq;

import com.loomq.application.scheduler.BucketGroupManager;
import com.loomq.common.MetricsCollector;
import com.loomq.config.WalConfig;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.spi.CallbackHandler;
import com.loomq.store.IntentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * LoomQ v0.7.0 核心引擎 - 嵌入式内核
 *
 * 纯 Java 实现，零外部依赖（仅 SLF4J API）。
 * 提供延迟任务队列的核心能力，通过 CallbackHandler 回调宿主应用。
 *
 * 特性：
 * 1. 基于 BucketGroup 的时间桶调度
 * 2. 基于 WAL 的持久化（SimpleWalWriter）
 * 3. 虚拟线程执行回调
 * 4. Builder 模式配置
 *
 * @author loomq
 * @since v0.7.0
 */
public class LoomqEngine implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(LoomqEngine.class);

    // ========== 核心组件 ==========
    private final IntentStore intentStore;
    private final BucketGroupManager bucketGroupManager;
    private final SimpleWalWriter walWriter;
    private final MetricsCollector metricsCollector;

    // ========== 回调机制 ==========
    private volatile CallbackHandler callbackHandler;
    private final Executor callbackExecutor;

    // ========== 调度 ==========
    private final ScheduledExecutorService schedulerExecutor;
    private final Map<PrecisionTier, TierConfig> tierConfigs;

    // ========== 状态 ==========
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong sequenceNumber = new AtomicLong(0);

    // ========== 配置 ==========
    private final Path walDir;
    private final String nodeId;

    private LoomqEngine(Builder builder) {
        this.nodeId = builder.nodeId != null ? builder.nodeId : "default-node";
        this.walDir = builder.walDir != null ? builder.walDir : Path.of("./data");
        this.tierConfigs = builder.tierConfigs != null ? builder.tierConfigs : Map.of();
        this.callbackExecutor = builder.callbackExecutor != null
            ? builder.callbackExecutor
            : Executors.newVirtualThreadPerTaskExecutor();

        try {
            // 确保数据目录存在
            Files.createDirectories(walDir);

            // 初始化组件
            this.intentStore = new IntentStore();
            this.bucketGroupManager = new BucketGroupManager();
            this.walWriter = new SimpleWalWriter(createWalConfig(), "shard-0");
            this.metricsCollector = MetricsCollector.getInstance();
            this.callbackHandler = builder.callbackHandler;

            // 调度器线程
            this.schedulerExecutor = Executors.newScheduledThreadPool(2, r -> {
                Thread t = new Thread(r, "loomq-scheduler-" + nodeId);
                t.setDaemon(true);
                return t;
            });

            logger.info("LoomqEngine created: nodeId={}, walDir={}", nodeId, walDir);

        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize LoomqEngine", e);
        }
    }

    /**
     * 启动引擎
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("Engine is already running");
        }

        logger.info("╔════════════════════════════════════════════════════════╗");
        logger.info("║       LoomQ v0.7.0 Core Engine Starting...             ║");
        logger.info("║       Mode: Embedded (Zero HTTP dependencies)          ║");
        logger.info("╚════════════════════════════════════════════════════════╝");

        // 启动 WAL
        walWriter.start();

        // 启动各精度档位的调度器
        for (PrecisionTier tier : PrecisionTier.values()) {
            long intervalMs = tier.getPrecisionWindowMs();
            schedulerExecutor.scheduleAtFixedRate(
                () -> scanAndDispatch(tier),
                intervalMs,
                intervalMs,
                TimeUnit.MILLISECONDS
            );
        }

        logger.info("Engine started successfully");
    }

    /**
     * 停止引擎
     */
    @Override
    public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        running.set(false);
        logger.info("Shutting down LoomqEngine...");

        // 停止接受新任务
        schedulerExecutor.shutdown();
        schedulerExecutor.awaitTermination(5, TimeUnit.SECONDS);

        // 关闭 WAL
        walWriter.close();

        // 关闭回调执行器
        if (callbackExecutor instanceof AutoCloseable) {
            ((AutoCloseable) callbackExecutor).close();
        }

        logger.info("Engine shutdown complete");
    }

    /**
     * 创建 Intent（异步）
     *
     * @param intent  Intent 对象
     * @param ackMode 确认模式
     * @return CompletableFuture<Long> WAL 序列号
     */
    public CompletableFuture<Long> createIntent(Intent intent, AckMode ackMode) {
        ensureRunning();

        long seq = sequenceNumber.incrementAndGet();

        return CompletableFuture.supplyAsync(() -> {
            try {
                // WAL 写入
                byte[] data = IntentBinaryCodec.encode(intent);

                CompletableFuture<Long> walFuture = switch (ackMode) {
                    case ASYNC -> walWriter.writeAsync(data);
                    case DURABLE -> walWriter.writeDurable(data);
                    case REPLICATED -> {
                        // 当前版本不支持副本复制，降级为 DURABLE
                        logger.warn("REPLICATED mode not supported in standalone, downgrading to DURABLE");
                        yield walWriter.writeDurable(data);
                    }
                };

                // 等待 WAL 确认
                walFuture.get();

                // 更新状态并存储
                intent.transitionTo(IntentStatus.SCHEDULED);
                intentStore.save(intent);
                bucketGroupManager.add(intent);

                metricsCollector.incrementTasksCreated();

                logger.debug("Intent created: id={}, ackMode={}, seq={}",
                    intent.getIntentId(), ackMode, seq);

                return seq;

            } catch (Exception e) {
                logger.error("Failed to create intent: id={}", intent.getIntentId(), e);
                throw new RuntimeException("Failed to create intent", e);
            }
        });
    }

    /**
     * 批量创建 Intent
     *
     * @param intents Intent 列表
     * @param ackMode 确认模式
     * @return CompletableFuture<List<Long>> WAL 序列号列表
     */
    public CompletableFuture<List<Long>> createIntents(List<Intent> intents, AckMode ackMode) {
        return CompletableFuture.supplyAsync(() -> {
            return intents.stream()
                .map(intent -> createIntent(intent, ackMode).join())
                .toList();
        });
    }

    /**
     * 查询 Intent
     *
     * @param intentId Intent ID
     * @return Optional<Intent>
     */
    public Optional<Intent> getIntent(String intentId) {
        return Optional.ofNullable(intentStore.findById(intentId));
    }

    /**
     * 取消 Intent
     *
     * @param intentId Intent ID
     * @return true 如果成功取消
     */
    public boolean cancelIntent(String intentId) {
        Intent intent = intentStore.findById(intentId);
        if (intent == null) {
            return false;
        }

        try {
            intent.transitionTo(IntentStatus.CANCELED);
            intentStore.update(intent);

            // 触发回调
            CallbackHandler handler = callbackHandler;
            if (handler != null) {
                callbackExecutor.execute(() -> {
                    try {
                        handler.onIntentEvent(intent, CallbackHandler.EventType.CANCELLED, null);
                    } catch (Exception e) {
                        logger.error("Callback handler error for cancelled intent: {}", intentId, e);
                    }
                });
            }

            metricsCollector.incrementTasksCancelled();
            logger.info("Intent cancelled: id={}", intentId);
            return true;

        } catch (IllegalStateException e) {
            logger.warn("Cannot cancel intent {}: {}", intentId, e.getMessage());
            return false;
        }
    }

    /**
     * 立即触发 Intent
     *
     * @param intentId Intent ID
     * @return true 如果成功触发
     */
    public boolean fireNow(String intentId) {
        Intent intent = intentStore.findById(intentId);
        if (intent == null) {
            return false;
        }

        try {
            intent.setExecuteAt(Instant.now());
            intentStore.update(intent);
            // 重新添加到调度桶（会立即被扫描到）
            bucketGroupManager.add(intent);

            logger.info("Intent fired immediately: id={}", intentId);
            return true;

        } catch (Exception e) {
            logger.error("Failed to fire intent: id={}", intentId, e);
            return false;
        }
    }

    /**
     * 注册回调处理器
     *
     * @param handler CallbackHandler
     */
    public void registerCallbackHandler(CallbackHandler handler) {
        this.callbackHandler = handler;
        logger.info("Callback handler registered: {}", handler.getClass().getSimpleName());
    }

    /**
     * 获取引擎状态
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * 获取统计信息
     */
    public EngineStats getStats() {
        return new EngineStats(
            intentStore.getPendingCount(),
            sequenceNumber.get(),
            metricsCollector.getIntentCountsByTier()
        );
    }

    // ========== 内部方法 ==========

    private void scanAndDispatch(PrecisionTier tier) {
        if (!running.get()) {
            return;
        }

        try {
            List<Intent> dueIntents = bucketGroupManager.scanDue(tier, Instant.now());

            for (Intent intent : dueIntents) {
                dispatchIntent(intent);
            }

            if (!dueIntents.isEmpty()) {
                logger.debug("Dispatched {} intents for tier {}", dueIntents.size(), tier);
            }

        } catch (Exception e) {
            logger.error("Error scanning tier: {}", tier, e);
        }
    }

    private void dispatchIntent(Intent intent) {
        try {
            // 状态转换
            intent.transitionTo(IntentStatus.DUE);
            intent.transitionTo(IntentStatus.DISPATCHING);
            intentStore.update(intent);

            // 触发回调
            CallbackHandler handler = callbackHandler;
            if (handler != null) {
                callbackExecutor.execute(() -> {
                    try {
                        handler.onIntentEvent(intent, CallbackHandler.EventType.DUE, null);
                        intent.transitionTo(IntentStatus.DELIVERED);
                        intentStore.update(intent);
                        metricsCollector.incrementTasksAckSuccess();
                    } catch (Exception e) {
                        logger.error("Callback handler error for intent: {}", intent.getIntentId(), e);
                        handler.onIntentEvent(intent, CallbackHandler.EventType.FAILED, e);
                        intent.transitionTo(IntentStatus.DEAD_LETTERED);
                        intentStore.update(intent);
                        metricsCollector.incrementTasksFailedTerminal();
                    }
                });
            } else {
                logger.warn("No callback handler registered, intent {} will not be delivered", intent.getIntentId());
                intent.transitionTo(IntentStatus.DEAD_LETTERED);
                intentStore.update(intent);
            }

        } catch (Exception e) {
            logger.error("Failed to dispatch intent: {}", intent.getIntentId(), e);
        }
    }

    private void ensureRunning() {
        if (!running.get()) {
            throw new IllegalStateException("Engine is not running");
        }
    }

    private WalConfig createWalConfig() {
        return new WalConfig() {
            @Override
            public String dataDir() {
                return walDir.toString();
            }

            @Override
            public int segmentSizeMb() {
                return 64;
            }

            @Override
            public String flushStrategy() {
                return "batch";
            }

            @Override
            public long batchFlushIntervalMs() {
                return 100;
            }

            @Override
            public boolean syncOnWrite() {
                return false;
            }

            @Override
            public String engine() {
                return "memory_segment";
            }

            @Override
            public int memorySegmentInitialSizeMb() {
                return 64;
            }

            @Override
            public int memorySegmentMaxSizeMb() {
                return 1024;
            }

            @Override
            public int memorySegmentFlushThresholdKb() {
                return 64;
            }

            @Override
            public long memorySegmentFlushIntervalMs() {
                return 10;
            }

            @Override
            public int memorySegmentStripeCount() {
                return 16;
            }

            @Override
            public int memorySegmentMinBatchSize() {
                return 100;
            }

            @Override
            public boolean memorySegmentAdaptiveFlushEnabled() {
                return true;
            }

            @Override
            public boolean isReplicationEnabled() {
                return false;
            }

            @Override
            public String replicaHost() {
                return "localhost";
            }

            @Override
            public int replicaPort() {
                return 9090;
            }

            @Override
            public long replicationAckTimeoutMs() {
                return 30000;
            }

            @Override
            public boolean requireReplicatedAck() {
                return false;
            }
        };
    }

    // ========== Builder ==========

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Path walDir;
        private String nodeId;
        private Map<PrecisionTier, TierConfig> tierConfigs;
        private Executor callbackExecutor;
        private CallbackHandler callbackHandler;

        public Builder walDir(Path walDir) {
            this.walDir = walDir;
            return this;
        }

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder tierConfig(Map<PrecisionTier, TierConfig> config) {
            this.tierConfigs = config;
            return this;
        }

        public Builder callbackExecutor(Executor executor) {
            this.callbackExecutor = executor;
            return this;
        }

        public Builder callbackHandler(CallbackHandler handler) {
            this.callbackHandler = handler;
            return this;
        }

        public LoomqEngine build() {
            return new LoomqEngine(this);
        }
    }

    // ========== 配置类 ==========

    public record TierConfig(
        int maxConcurrency,
        int batchSize,
        long batchWindowMs,
        int consumerCount
    ) {}

    // ========== 统计类 ==========

    public record EngineStats(
        long pendingCount,
        long totalSequence,
        Map<PrecisionTier, Long> intentCountsByTier
    ) {}
}

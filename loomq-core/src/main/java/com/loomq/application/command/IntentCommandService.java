package com.loomq.application.command;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.application.swap.ColdIntentSwapper;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.WalMode;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.spi.CallbackHandler;
import com.loomq.store.IdempotencyResult;
import com.loomq.store.IntentStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 核心命令服务。
 *
 * 统一承接 intent 的创建、更新、取消和立即触发，避免业务命令逻辑分散在
 * LoomqEngine、HTTP 适配层和调度器之间。
 */
public final class IntentCommandService {

    private static final Logger logger = LoggerFactory.getLogger(IntentCommandService.class);

    private final IntentStore intentStore;
    private final PrecisionScheduler scheduler;
    private final SimpleWalWriter walWriter;
    private final MetricsCollector metricsCollector;
    private final Executor callbackExecutor;
    private final Executor walWriteExecutor;
    private final AtomicBoolean running;
    private final AtomicLong sequenceNumber;

    private volatile CallbackHandler callbackHandler;
    private final PrecisionTier defaultTier;
    private final ColdIntentSwapper coldSwapper;

    public IntentCommandService(
        IntentStore intentStore,
        PrecisionScheduler scheduler,
        SimpleWalWriter walWriter,
        MetricsCollector metricsCollector,
        Executor callbackExecutor,
        Executor walWriteExecutor,
        AtomicBoolean running,
        AtomicLong sequenceNumber,
        CallbackHandler callbackHandler,
        PrecisionTier defaultTier,
        ColdIntentSwapper coldSwapper
    ) {
        this.intentStore = intentStore;
        this.scheduler = scheduler;
        this.walWriter = walWriter;
        this.metricsCollector = metricsCollector;
        this.callbackExecutor = callbackExecutor;
        this.walWriteExecutor = walWriteExecutor;
        this.running = running;
        this.sequenceNumber = sequenceNumber;
        this.callbackHandler = callbackHandler;
        this.defaultTier = defaultTier;
        this.coldSwapper = coldSwapper;
    }

    public void registerCallbackHandler(CallbackHandler handler) {
        this.callbackHandler = handler;
        if (handler != null) {
            logger.info("Callback handler registered: {}", handler.getClass().getSimpleName());
        } else {
            logger.info("Callback handler cleared");
        }
    }

    public IdempotencyResult checkIdempotency(String idempotencyKey) {
        return intentStore.checkIdempotency(idempotencyKey);
    }

    /**
     * 创建 Intent 并调度。
     *
     * <p>持久化语义取决于 ackMode：
     * <ul>
     *   <li><b>DURABLE</b>（默认）：WAL 同步刷盘后才返回，崩溃不丢数据</li>
     *   <li><b>ASYNC</b>：WAL 异步写入，高吞吐但崩溃时可能丢失最近创建的 Intent</li>
     *   <li><b>BATCH_DEFERRED</b>：WAL 批量写入，吞吐优先但崩溃窗口更大</li>
     * </ul>
     *
     * @return 序列号
     */
    public long createIntent(Intent intent, AckMode ackMode) {
        ensureRunning();

        long seq = sequenceNumber.incrementAndGet();

        try {
            // Apply engine-level default tier if configured
            if (defaultTier != null) {
                intent.setPrecisionTier(defaultTier);
            }

            intent.transitionTo(IntentStatus.SCHEDULED);
            intent.incrementRevision();

            // Pipeline overlap (DeepSeek V4 compute-comm overlap):
            // 1. Pre-encode WAL payload (CPU)
            byte[] walPayload = IntentBinaryCodec.encode(intent);

            // 2. Resolve effective WAL mode
            WalMode effectiveMode = resolveWalMode(intent, ackMode);

            // 非 DURABLE 模式下 WAL 写入为 fire-and-forget，崩溃时可能丢失数据
            if (effectiveMode != WalMode.DURABLE) {
                logger.warn("Intent {} using non-DURABLE walMode={}, crash may cause data loss",
                    intent.getIntentId(), effectiveMode);
            }

            // 3. Start WAL write (I/O) — runs on background thread for DURABLE
            CompletableFuture<Long> walFuture = startWalWrite(walPayload, effectiveMode);

            // 4. While WAL I/O is in flight, save to store (memory-only)
            intentStore.save(intent);

            // 5. For DURABLE mode, wait for fsync before scheduling or cold-swap
            long walPosition = -1;
            if (effectiveMode == WalMode.DURABLE) {
                walPosition = walFuture.join();
            }

            // 6. Cold swap: long-delay intents evicted from memory after DURABLE persist
            if (coldSwapper != null && walPosition >= 0
                && coldSwapper.shouldSwapOut(Duration.between(Instant.now(), intent.getExecuteAt()).toMillis())) {
                int recordLen = SimpleWalWriter.recordLength(walPayload.length);
                coldSwapper.swapOut(intent, walPosition, recordLen);
                logger.debug("Intent {} cold-swapped: delay={}s, walPos={}",
                    intent.getIntentId(),
                    Duration.between(Instant.now(), intent.getExecuteAt()).toSeconds(),
                    walPosition);
            } else {
                // 7. Schedule (memory-only, independent of WAL)
                scheduler.schedule(intent);
            }

            metricsCollector.incrementIntentsCreated();
            logger.debug("Intent created: id={}, ackMode={}, walMode={}, seq={}",
                intent.getIntentId(), ackMode, effectiveMode, seq);

            return seq;
        } catch (Exception e) {
            logger.error("Failed to create intent: id={}", intent.getIntentId(), e);
            throw new RuntimeException("Failed to create intent", e);
        }
    }

    public List<Long> createIntents(List<Intent> intents, AckMode ackMode) {
        ensureRunning();
        return intents.stream()
            .map(intent -> createIntent(intent, ackMode))
            .toList();
    }

    public Optional<Intent> updateIntent(String intentId, Consumer<Intent> updater) {
        return updateIntent(intentId, updater, null);
    }

    public Optional<Intent> updateIntent(String intentId, Consumer<Intent> updater, Instant newExecuteAt) {
        ensureRunning();

        Intent intent = intentStore.findById(intentId);
        if (intent == null) {
            return Optional.empty();
        }

        try {
            synchronized (intent) {
                Instant oldExecuteAt = intent.getExecuteAt();
                boolean reschedule = newExecuteAt != null && !newExecuteAt.equals(oldExecuteAt);

                if (reschedule) {
                    scheduler.removeFromSchedule(intent);
                }

                updater.accept(intent);

                if (newExecuteAt != null) {
                    intent.setExecuteAt(newExecuteAt);
                }

                intent.incrementRevision();
                persistIntentState(intent, AckMode.DURABLE);
                intentStore.update(intent);

                if (reschedule) {
                    if (intent.getStatus() == IntentStatus.DUE) {
                        scheduler.restore(intent);
                    } else {
                        scheduler.schedule(intent);
                    }
                }
            }
            return Optional.of(intent);
        } catch (RuntimeException e) {
            logger.error("Failed to update intent: id={}", intentId, e);
            throw e;
        }
    }

    public boolean cancelIntent(String intentId) {
        ensureRunning();

        Intent intent = intentStore.findById(intentId);
        if (intent == null) {
            return false;
        }

        try {
            synchronized (intent) {
                intent.transitionTo(IntentStatus.CANCELED);
                scheduler.removeFromSchedule(intent);
                intent.incrementRevision();
                persistIntentState(intent, AckMode.DURABLE);
                intentStore.update(intent);
                dispatchCallback(intent, CallbackHandler.EventType.CANCELLED, null);
            }

            metricsCollector.incrementIntentsCancelled();
            logger.info("Intent cancelled: id={}", intentId);
            return true;
        } catch (IllegalStateException e) {
            logger.warn("Cannot cancel intent {}: {}", intentId, e.getMessage());
            return false;
        } catch (RuntimeException e) {
            logger.error("Failed to cancel intent: id={}", intentId, e);
            throw e;
        }
    }

    public boolean fireNow(String intentId) {
        ensureRunning();

        Intent intent = intentStore.findById(intentId);
        if (intent == null) {
            return false;
        }

        try {
            synchronized (intent) {
                scheduler.removeFromSchedule(intent);
                intent.setExecuteAt(Instant.now());
                intent.incrementRevision();
                persistIntentState(intent, AckMode.DURABLE);
                intentStore.update(intent);
                scheduler.restore(intent);
            }

            logger.info("Intent fired immediately: id={}", intentId);
            return true;
        } catch (RuntimeException e) {
            logger.error("Failed to fire intent: id={}", intentId, e);
            return false;
        }
    }

    private void dispatchCallback(Intent intent, CallbackHandler.EventType eventType, Throwable error) {
        CallbackHandler handler = callbackHandler;
        if (handler == null) {
            return;
        }

        callbackExecutor.execute(() -> {
            try {
                handler.onIntentEvent(intent, eventType, error);
            } catch (Exception e) {
                logger.error("Callback handler error for intent {} event {}", intent.getIntentId(), eventType, e);
            }
        });
    }

    private WalMode resolveWalMode(Intent intent, AckMode ackMode) {
        // 1. Explicit AckMode wins (backward compatibility)
        if (ackMode != null) {
            return switch (ackMode) {
                case ASYNC -> WalMode.ASYNC;
                case DURABLE, REPLICATED -> WalMode.DURABLE;
            };
        }
        // 2. Intent-level walMode override
        if (intent.getWalMode() != null) {
            return intent.getWalMode();
        }
        // 3. Fall back to tier default
        return PrecisionTierCatalog.defaultCatalog().walMode(intent.getPrecisionTier());
    }

    private CompletableFuture<Long> startWalWrite(byte[] walPayload,
                                                   WalMode mode) {
        return switch (mode) {
            case ASYNC -> walWriter.writeAsync(walPayload);
            case BATCH_DEFERRED -> {
                walWriter.writeAsync(walPayload);
                yield CompletableFuture.completedFuture(-1L);
            }
            case DURABLE -> CompletableFuture.supplyAsync(
                () -> walWriter.writeDurable(walPayload).join(), walWriteExecutor);
        };
    }

    private long persistIntentState(Intent intent, AckMode ackMode) {
        byte[] data = IntentBinaryCodec.encode(intent);

        WalMode effectiveMode = resolveWalMode(intent, ackMode);

        return switch (effectiveMode) {
            case ASYNC -> walWriter.writeAsync(data).join();
            case BATCH_DEFERRED -> {
                walWriter.writeAsync(data);
                yield -1;
            }
            case DURABLE -> walWriter.writeDurable(data).join();
        };
    }

    private void ensureRunning() {
        if (!running.get()) {
            throw new IllegalStateException("Engine is not running");
        }
    }
}

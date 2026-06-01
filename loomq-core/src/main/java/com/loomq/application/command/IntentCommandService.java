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
            // 回滚：从调度器和 store 中移除（异常可能发生在 schedule 之后）
            try {
                scheduler.removeFromSchedule(intent);
            } catch (Exception ignored) {}
            try {
                intentStore.delete(intent.getIntentId());
            } catch (Exception rollbackEx) {
                logger.error("Rollback failed for intent {}: store.delete",
                    intent.getIntentId(), rollbackEx);
            }
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

        Intent intent = intentStore.findByIdInternal(intentId);
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

                // 安全网：updater 可能直接通过 intent.setExecuteAt() 修改了执行时间，
                // 此时 newExecuteAt 为 null 导致 reschedule 初始为 false，需要补检。
                if (!reschedule) {
                    Instant actualExecuteAt = intent.getExecuteAt();
                    if (actualExecuteAt != null && !actualExecuteAt.equals(oldExecuteAt)) {
                        reschedule = true;
                        // 必须在 updater 已修改 executeAt 之后、重新调度之前，
                        // 用旧的 executeAt 清理索引（removeFromSchedule 内部用的是当前 executeAt）
                        scheduler.removeFromSchedule(intent, oldExecuteAt);
                    }
                }

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

        Intent intent = intentStore.findByIdInternal(intentId);
        if (intent == null) {
            return false;
        }

        IntentStatus oldStatus = null;
        Instant oldUpdatedAt = null;
        long oldRevision = 0;
        try {
            synchronized (intent) {
                // 在 transitionTo 之前记录原始状态，用于回滚。
                oldStatus = intent.getStatus();
                oldUpdatedAt = intent.getUpdatedAt();
                oldRevision = intent.getRevision();

                // transitionTo 单独 try-catch：仅捕获状态机校验失败，
                // 不会误吞 persistIntentState / intentStore.update 抛出的 ISE。
                try {
                    intent.transitionTo(IntentStatus.CANCELED);
                } catch (IllegalStateException e) {
                    logger.warn("Cannot cancel intent {}: {}", intentId, e.getMessage());
                    return false;
                }
                scheduler.removeFromSchedule(intent);
                intent.incrementRevision();
                persistIntentState(intent, AckMode.DURABLE);
                intentStore.update(intent);
            }

            // 在 synchronized 块外派发回调——回滚窗口已关闭，
            // callback 异常（如 RejectedExecutionException）不会触发状态回滚。
            dispatchCallback(intent, CallbackHandler.EventType.CANCELLED, null);

            metricsCollector.incrementIntentsCancelled();
            logger.info("Intent cancelled: id={}", intentId);
            return true;
        } catch (RuntimeException e) {
            logger.error("Failed to cancel intent: id={}", intentId, e);
            // 回滚：transitionTo 已成功但持久化失败，恢复原状态并重新加入调度
            if (oldStatus != null && intent.getStatus() != oldStatus) {
                intent.rollbackStatus(oldStatus, oldUpdatedAt, oldRevision);
                scheduler.restore(intent);
            }
            throw e;
        }
    }

    public boolean fireNow(String intentId) {
        ensureRunning();

        Intent intent = intentStore.findByIdInternal(intentId);
        if (intent == null) {
            return false;
        }

        if (intent.getStatus().isTerminal()) {
            logger.warn("Cannot fire intent in terminal state: id={}, status={}",
                intentId, intent.getStatus());
            return false;
        }

        Instant oldExecuteAt = null;
        long oldRevision = 0;
        try {
            synchronized (intent) {
                oldExecuteAt = intent.getExecuteAt();
                oldRevision = intent.getRevision();
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
            // 回滚：恢复 executeAt 和 revision，重新加入调度
            if (oldExecuteAt != null) {
                intent.setExecuteAt(oldExecuteAt);
                intent.rollbackRevision(oldRevision);
                scheduler.restore(intent);
            }
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
            case ASYNC -> CompletableFuture.completedFuture(walWriter.write(walPayload));
            case BATCH_DEFERRED -> CompletableFuture.completedFuture(walWriter.writeBatched(walPayload));
            case DURABLE -> CompletableFuture.supplyAsync(
                () -> walWriter.writeSync(walPayload), walWriteExecutor);
        };
    }

    private long persistIntentState(Intent intent, AckMode ackMode) {
        byte[] data = IntentBinaryCodec.encode(intent);

        WalMode effectiveMode = resolveWalMode(intent, ackMode);

        return switch (effectiveMode) {
            case ASYNC -> walWriter.write(data);
            case BATCH_DEFERRED -> walWriter.writeBatched(data);
            case DURABLE -> walWriter.writeSync(data);
        };
    }

    private void ensureRunning() {
        if (!running.get()) {
            throw new IllegalStateException("Engine is not running");
        }
    }
}

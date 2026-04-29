package com.loomq.application.command;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.PrecisionTierProfile;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import com.loomq.spi.CallbackHandler;
import com.loomq.store.IdempotencyResult;
import com.loomq.store.IntentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

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
    private final AtomicBoolean running;
    private final AtomicLong sequenceNumber;

    private volatile CallbackHandler callbackHandler;

    public IntentCommandService(
        IntentStore intentStore,
        PrecisionScheduler scheduler,
        SimpleWalWriter walWriter,
        MetricsCollector metricsCollector,
        Executor callbackExecutor,
        AtomicBoolean running,
        AtomicLong sequenceNumber,
        CallbackHandler callbackHandler
    ) {
        this.intentStore = intentStore;
        this.scheduler = scheduler;
        this.walWriter = walWriter;
        this.metricsCollector = metricsCollector;
        this.callbackExecutor = callbackExecutor;
        this.running = running;
        this.sequenceNumber = sequenceNumber;
        this.callbackHandler = callbackHandler;
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

    public long createIntent(Intent intent, AckMode ackMode) {
        ensureRunning();

        long seq = sequenceNumber.incrementAndGet();

        try {
            intent.transitionTo(IntentStatus.SCHEDULED);

            // Pipeline overlap (DeepSeek V4 compute-comm overlap):
            // 1. Pre-encode WAL payload (CPU)
            byte[] walPayload = IntentBinaryCodec.encode(intent);

            // 2. Resolve effective WAL mode
            PrecisionTierProfile.WalTierMode effectiveMode = resolveWalMode(intent, ackMode);

            // 3. Start WAL write (I/O) — runs on background thread for DURABLE
            CompletableFuture<Long> walFuture = startWalWrite(walPayload, effectiveMode);

            // 4. While WAL I/O is in flight, save to store (memory-only)
            intentStore.save(intent);

            // 5. For DURABLE mode, wait for fsync before scheduling
            if (effectiveMode == PrecisionTierProfile.WalTierMode.DURABLE) {
                walFuture.join();
            }

            // 6. Schedule (memory-only, independent of WAL)
            scheduler.schedule(intent);

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
                    scheduler.getBucketGroupManager().remove(intent);
                }

                updater.accept(intent);

                if (newExecuteAt != null) {
                    intent.setExecuteAt(newExecuteAt);
                }

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
            intent.transitionTo(IntentStatus.CANCELED);
            scheduler.getBucketGroupManager().remove(intent);
            persistIntentState(intent, AckMode.DURABLE);
            intentStore.update(intent);
            dispatchCallback(intent, CallbackHandler.EventType.CANCELLED, null);

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
            scheduler.getBucketGroupManager().remove(intent);
            intent.setExecuteAt(Instant.now());
            persistIntentState(intent, AckMode.DURABLE);
            intentStore.update(intent);
            scheduler.restore(intent);

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

    private PrecisionTierProfile.WalTierMode resolveWalMode(Intent intent, AckMode ackMode) {
        if (ackMode != null) {
            return switch (ackMode) {
                case ASYNC -> PrecisionTierProfile.WalTierMode.ASYNC;
                case DURABLE, REPLICATED -> PrecisionTierProfile.WalTierMode.DURABLE;
            };
        }
        return PrecisionTierCatalog.defaultCatalog().walTierMode(intent.getPrecisionTier());
    }

    private CompletableFuture<Long> startWalWrite(byte[] walPayload,
                                                   PrecisionTierProfile.WalTierMode mode) {
        return switch (mode) {
            case ASYNC -> walWriter.writeAsync(walPayload);
            case BATCH_DEFERRED -> {
                walWriter.writeAsync(walPayload);
                yield CompletableFuture.completedFuture(-1L);
            }
            case DURABLE -> CompletableFuture.supplyAsync(
                () -> walWriter.writeDurable(walPayload).join(), callbackExecutor);
        };
    }

    private long persistIntentState(Intent intent, AckMode ackMode) {
        byte[] data = IntentBinaryCodec.encode(intent);

        // Resolve effective WAL mode: explicit ackMode wins, otherwise use tier's default
        PrecisionTierProfile.WalTierMode effectiveMode;
        if (ackMode != null) {
            effectiveMode = switch (ackMode) {
                case ASYNC -> PrecisionTierProfile.WalTierMode.ASYNC;
                case DURABLE, REPLICATED -> PrecisionTierProfile.WalTierMode.DURABLE;
            };
        } else {
            effectiveMode = PrecisionTierCatalog.defaultCatalog()
                .walTierMode(intent.getPrecisionTier());
        }

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

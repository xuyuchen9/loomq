package com.loomq.channel.grpc.server;

import com.loomq.domain.intent.PrecisionTier;
import com.loomq.grpc.gen.DeliveryAckMode;
import com.loomq.grpc.gen.DeliveryAckResult;
import com.loomq.grpc.gen.DeliveryEvent;
import com.loomq.grpc.gen.WatchDeliveriesRequest;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC 投递流注册表。
 *
 * <p>管理活跃的 Gateway 连接，按档位索引，支持 AUTO_ACK 和 MANUAL_ACK 模式。
 * 复用 {@link com.loomq.channel.grpc.server.GlobalIntentObserver} 的背压处理模式。
 *
 * @author loomq
 * @since v0.9.2
 */
public class DeliveryStreamRegistry {

    private static final Logger logger = LoggerFactory.getLogger(DeliveryStreamRegistry.class);
    private static final int MAX_PENDING = 256;
    private static final long ACK_TIMEOUT_MS = 30000;

    // 按档位索引的活跃流
    private final ConcurrentHashMap<String, CopyOnWriteArraySet<DeliveryStreamEntry>> streamsByTier =
        new ConcurrentHashMap<>();
    // 全档位流（不指定 tier 过滤）
    private final CopyOnWriteArraySet<DeliveryStreamEntry> wildcardStreams = new CopyOnWriteArraySet<>();

    // 快速路径标志
    private final AtomicBoolean hasWildcardStreams = new AtomicBoolean(false);
    private final AtomicBoolean hasTierStreams = new AtomicBoolean(false);

    // MANUAL_ACK 模式的 pending 结果
    private final ConcurrentHashMap<String, PendingDelivery> pendingDeliveries = new ConcurrentHashMap<>();

    // 超时调度器
    private final ScheduledExecutorService timeoutScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "delivery-ack-timeout");
        t.setDaemon(true);
        return t;
    });

    /**
     * 注册一个新的投递流。
     */
    public DeliveryStreamEntry register(WatchDeliveriesRequest request,
                                        ServerCallStreamObserver<DeliveryEvent> observer) {
        DeliveryAckMode ackMode = request.getAckMode();
        if (ackMode == DeliveryAckMode.DELIVERY_ACK_MODE_UNSPECIFIED) {
            ackMode = DeliveryAckMode.AUTO_ACK;
        }

        Set<String> tiers = request.getPrecisionTiersCount() > 0
            ? Set.copyOf(request.getPrecisionTiersList())
            : Set.of();

        DeliveryStreamEntry entry = new DeliveryStreamEntry(tiers, ackMode, observer);

        if (tiers.isEmpty()) {
            hasWildcardStreams.set(true);
            wildcardStreams.add(entry);
            logger.info("Registered wildcard delivery stream (ackMode={})", ackMode);
        } else {
            hasTierStreams.set(true);
            for (String tier : tiers) {
                streamsByTier.computeIfAbsent(tier, k -> new CopyOnWriteArraySet<>()).add(entry);
            }
            logger.info("Registered delivery stream for tiers {} (ackMode={})", tiers, ackMode);
        }

        return entry;
    }

    /**
     * 注销一个投递流。
     */
    public void unregister(DeliveryStreamEntry entry) {
        wildcardStreams.remove(entry);
        if (wildcardStreams.isEmpty()) {
            hasWildcardStreams.set(false);
        }

        for (String tier : entry.tiers()) {
            var set = streamsByTier.get(tier);
            if (set != null) {
                set.remove(entry);
                if (set.isEmpty()) {
                    streamsByTier.remove(tier);
                }
            }
        }

        if (streamsByTier.isEmpty()) {
            hasTierStreams.set(false);
        }

        logger.info("Unregistered delivery stream for tiers {}", entry.tiers());
    }

    /**
     * 查找匹配指定档位的活跃流。
     */
    public List<DeliveryStreamEntry> findStreams(PrecisionTier tier) {
        List<DeliveryStreamEntry> result = new ArrayList<>();

        // 全档位流
        if (hasWildcardStreams.get()) {
            result.addAll(wildcardStreams);
        }

        // 指定档位流
        if (hasTierStreams.get()) {
            var tierSet = streamsByTier.get(tier.name());
            if (tierSet != null) {
                result.addAll(tierSet);
            }
        }

        return result;
    }

    /**
     * 是否有活跃的投递流。
     */
    public boolean hasActiveStreams() {
        return hasWildcardStreams.get() || hasTierStreams.get();
    }

    /**
     * 注册 pending 投递（MANUAL_ACK 模式）。
     */
    public void registerPending(String deliveryId, CompletableFuture<DeliveryResult> future) {
        pendingDeliveries.put(deliveryId, new PendingDelivery(deliveryId, future, System.currentTimeMillis()));

        // 调度超时
        timeoutScheduler.schedule(() -> {
            PendingDelivery pending = pendingDeliveries.remove(deliveryId);
            if (pending != null) {
                logger.warn("Delivery ACK timeout for {}", deliveryId);
                pending.future().complete(DeliveryResult.RETRY);
            }
        }, ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * 完成 pending 投递（收到 ACK 时调用）。
     */
    public void completePending(String deliveryId, DeliveryAckResult result) {
        PendingDelivery pending = pendingDeliveries.remove(deliveryId);
        if (pending == null) {
            logger.debug("No pending delivery found for {}", deliveryId);
            return;
        }

        DeliveryResult dr = switch (result) {
            case DELIVERED -> DeliveryResult.SUCCESS;
            case FAILED -> DeliveryResult.RETRY;
            case REJECTED -> DeliveryResult.DEAD_LETTER;
            default -> DeliveryResult.DEAD_LETTER;
        };

        pending.future().complete(dr);
        logger.debug("Completed pending delivery {}: {}", deliveryId, dr);
    }

    /**
     * 关闭注册表，释放资源。
     */
    public void close() {
        timeoutScheduler.shutdown();
        try {
            if (!timeoutScheduler.awaitTermination(3, TimeUnit.SECONDS)) {
                timeoutScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            timeoutScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // 完成所有 pending 投递
        for (PendingDelivery pending : pendingDeliveries.values()) {
            pending.future().complete(DeliveryResult.RETRY);
        }
        pendingDeliveries.clear();

        logger.info("DeliveryStreamRegistry closed");
    }

    /**
     * 获取活跃流数量。
     */
    public int getActiveStreamCount() {
        int count = wildcardStreams.size();
        for (var set : streamsByTier.values()) {
            count += set.size();
        }
        return count;
    }

    /**
     * 获取 pending 投递数量。
     */
    public int getPendingCount() {
        return pendingDeliveries.size();
    }

    // ── 内部类型 ──

    /**
     * 投递流条目。
     */
    public static class DeliveryStreamEntry {
        private static final int MAX_PENDING_EVENTS = 256;

        private final Set<String> tiers;
        private final DeliveryAckMode ackMode;
        private final ServerCallStreamObserver<DeliveryEvent> observer;
        private final ConcurrentLinkedQueue<DeliveryEvent> pending = new ConcurrentLinkedQueue<>();

        DeliveryStreamEntry(Set<String> tiers, DeliveryAckMode ackMode,
                            ServerCallStreamObserver<DeliveryEvent> observer) {
            this.tiers = tiers;
            this.ackMode = ackMode;
            this.observer = observer;
        }

        public Set<String> tiers() {
            return tiers;
        }

        public DeliveryAckMode ackMode() {
            return ackMode;
        }

        /**
         * 推送投递事件。如果流就绪则直接发送，否则入队缓冲。
         */
        public void deliver(DeliveryEvent event) {
            if (observer.isReady()) {
                observer.onNext(event);
            } else {
                if (pending.size() >= MAX_PENDING_EVENTS) {
                    pending.poll();
                }
                pending.offer(event);
            }
        }

        /**
         * 排空缓冲队列（流变为就绪时调用）。
         */
        public void drainQueue() {
            while (observer.isReady()) {
                DeliveryEvent event = pending.poll();
                if (event == null) break;
                observer.onNext(event);
            }
        }

        /**
         * 是否匹配指定档位。
         */
        public boolean matchesTier(String tier) {
            return tiers.isEmpty() || tiers.contains(tier);
        }
    }

    private record PendingDelivery(
        String deliveryId,
        CompletableFuture<DeliveryResult> future,
        long createdAtMs
    ) {}
}

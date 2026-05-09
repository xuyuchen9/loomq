package com.loomq.scheduler;

import com.loomq.domain.intent.Intent;
import com.loomq.spi.DeliveryHandler;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reactor Netty-based delivery handler for benchmarks.
 *
 * Replaces BatchedHttpDeliveryHandler's JDK HttpClient (limited to ~5 connections
 * per host by default) with Reactor Netty's connection pool (500 connections),
 * eliminating the client-side throughput bottleneck.
 *
 * Supports batch mode (JSON array POST) and chameleon mode (individual POST per intent).
 */
final class ReactorNettyDeliveryHandler implements DeliveryHandler {

    private static final Logger log = LoggerFactory.getLogger(ReactorNettyDeliveryHandler.class);
    private static final int MAX_BATCH_SIZE = 200;
    private static final long FLUSH_INTERVAL_MS = 10;

    private final HttpClient client;
    private final int webhookPort;
    private final boolean chameleonMode;
    private final Map<String, ConcurrentLinkedDeque<IntentEntry>> batches;
    private final ScheduledExecutorService flusher;

    private final AtomicInteger totalBatches = new AtomicInteger(0);
    private final AtomicInteger totalIntentsBatched = new AtomicInteger(0);
    private final AtomicLong totalBatchLatencyNs = new AtomicLong(0);

    // Timing diagnostics
    private final AtomicLong totalBatchDwellNs = new AtomicLong(0);
    private final AtomicLong totalHttpRttNs = new AtomicLong(0);
    private final AtomicInteger flushCount = new AtomicInteger(0);

    // Diagnostic: track flush trigger sources
    private final AtomicInteger flushByFullBatch = new AtomicInteger(0);  // 队列满触发
    private final AtomicInteger flushByTimer = new AtomicInteger(0);      // 定时器触发

    // 尾部刷新：每个 batchKey 有一个延迟刷新任务
    private final Map<String, ScheduledFuture<?>> pendingTimeoutFlush = new ConcurrentHashMap<>();

    ReactorNettyDeliveryHandler(int webhookPort) {
        this(webhookPort, false);
    }

    ReactorNettyDeliveryHandler(int webhookPort, boolean chameleonMode) {
        this.webhookPort = webhookPort;
        this.chameleonMode = chameleonMode;

        ConnectionProvider provider = ConnectionProvider.builder("benchmark-pool")
            .maxConnections(500)
            .pendingAcquireMaxCount(10_000)
            .pendingAcquireTimeout(Duration.ofSeconds(5))
            .maxIdleTime(Duration.ofSeconds(30))
            .build();

        this.client = HttpClient.create(provider)
            .keepAlive(true)
            .responseTimeout(Duration.ofSeconds(10))
            .option(io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
            .option(io.netty.channel.ChannelOption.TCP_NODELAY, true);

        this.batches = new ConcurrentHashMap<>();

        // 用于尾部刷新的单线程调度器
        this.flusher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "reactor-batch-flusher");
            t.setDaemon(true);
            return t;
        });
        // 不再使用周期性扫描，而是按需调度尾部刷新
    }

    @Override
    public CompletableFuture<DeliveryResult> deliverAsync(Intent intent) {
        String url = intent.getCallback() != null ? intent.getCallback().getUrl()
            : "http://localhost:" + webhookPort + "/webhook";

        CompletableFuture<DeliveryResult> future = new CompletableFuture<>();

        // Batch key includes tier so same-tier intents are grouped together
        String batchKey = url + "#" + intent.getPrecisionTier().name();
        long enqueueNanos = System.nanoTime();
        IntentEntry entry = new IntentEntry(intent, future, enqueueNanos);

        ConcurrentLinkedDeque<IntentEntry> deque = batches.computeIfAbsent(batchKey, k -> new ConcurrentLinkedDeque<>());
        int prevSize = deque.size();
        deque.addLast(entry);

        if (deque.size() >= MAX_BATCH_SIZE) {
            // 满批触发：立即发送，取消待处理的超时刷新
            ScheduledFuture<?> pending = pendingTimeoutFlush.remove(batchKey);
            if (pending != null) pending.cancel(false);
            flushUrl(url, deque, true);
        } else if (prevSize == 0) {
            // 队列从空变非空：调度超时刷新（仅首次，不续期）
            // 避免高频调度开销
            pendingTimeoutFlush.computeIfAbsent(batchKey, k ->
                flusher.schedule(() -> {
                    pendingTimeoutFlush.remove(batchKey);
                    flushUrl(url, deque, false);
                }, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS)
            );
        }

        return future;
    }

    int getTotalBatches() { return totalBatches.get(); }
    int getTotalIntentsBatched() { return totalIntentsBatched.get(); }
    double getAvgBatchLatencyMs() {
        int n = totalBatches.get();
        return n > 0 ? (totalBatchLatencyNs.get() / (double) n) / 1_000_000.0 : 0;
    }

    void shutdown() {
        flusher.shutdown();
        if (client.configuration().connectionProvider() != null) {
            client.configuration().connectionProvider().dispose();
        }
    }

    private void flushUrl(String url, ConcurrentLinkedDeque<IntentEntry> deque, boolean isFullBatchTrigger) {
        if (isFullBatchTrigger) {
            flushByFullBatch.incrementAndGet();
        } else {
            flushByTimer.incrementAndGet();
        }

        List<IntentEntry> batch = new ArrayList<>();
        IntentEntry e;
        while (batch.size() < MAX_BATCH_SIZE && (e = deque.pollFirst()) != null) {
            batch.add(e);
        }
        if (batch.isEmpty()) return;

        if (chameleonMode) {
            for (IntentEntry entry : batch) {
                Intent intent = entry.intent;
                byte[] payload = buildSinglePayload(intent);
                long startNs = System.nanoTime();

                client.headers(h -> h.add("Content-Type", "application/json")
                        .add("X-LoomQ-Intent-Id", intent.getIntentId()))
                    .post()
                    .uri(url)
                    .send(Mono.just(Unpooled.wrappedBuffer(payload)))
                    .responseSingle((resp, body) -> Mono.just(resp.status().code()))
                    .onErrorResume(ex -> Mono.just(-1))
                    .subscribe(status -> {
                        long nowNs = System.nanoTime();
                        totalBatchDwellNs.addAndGet(nowNs - entry.enqueueNanos);
                        totalHttpRttNs.addAndGet(nowNs - startNs);
                        flushCount.incrementAndGet();
                        totalBatches.incrementAndGet();
                        totalIntentsBatched.incrementAndGet();
                        totalBatchLatencyNs.addAndGet(nowNs - startNs);
                        DeliveryResult dr = status >= 200 && status < 300 ? DeliveryResult.SUCCESS
                            : status >= 500 ? DeliveryResult.RETRY : DeliveryResult.DEAD_LETTER;
                        entry.future.complete(dr);
                    });
            }
        } else {
            byte[] payload = buildBatchPayload(batch);
            long startNs = System.nanoTime();

            client.headers(h -> h.add("Content-Type", "application/json"))
                .post()
                .uri(url)
                .send(Mono.just(Unpooled.wrappedBuffer(payload)))
                .responseSingle((resp, body) -> Mono.just(resp.status().code()))
                .onErrorResume(ex -> Mono.just(-1))
                .subscribe(status -> {
                    long nowNs = System.nanoTime();
                    long httpRttNs = nowNs - startNs;
                    totalBatchDwellNs.addAndGet(nowNs - batch.get(0).enqueueNanos);
                    totalHttpRttNs.addAndGet(httpRttNs);
                    flushCount.incrementAndGet();
                    totalBatches.incrementAndGet();
                    totalIntentsBatched.addAndGet(batch.size());
                    totalBatchLatencyNs.addAndGet(httpRttNs);
                    DeliveryResult dr = status >= 200 && status < 300 ? DeliveryResult.SUCCESS
                        : status >= 500 ? DeliveryResult.RETRY : DeliveryResult.DEAD_LETTER;
                    for (IntentEntry entry : batch) {
                        entry.future.complete(dr);
                    }
                });
        }
    }

    private byte[] buildSinglePayload(Intent intent) {
        String json = "{\"intentId\":\"" + intent.getIntentId()
            + "\",\"precisionTier\":\"" + intent.getPrecisionTier().name() + "\"}";
        return json.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] buildBatchPayload(List<IntentEntry> batch) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < batch.size(); i++) {
            if (i > 0) sb.append(",");
            Intent intent = batch.get(i).intent;
            sb.append("{\"intentId\":\"").append(intent.getIntentId())
              .append("\",\"precisionTier\":\"").append(intent.getPrecisionTier().name()).append("\"}");
        }
        sb.append("]");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private record IntentEntry(Intent intent, CompletableFuture<DeliveryResult> future, long enqueueNanos) {}

    void printBatchMarkers() {
        int fc = flushCount.get();
        double avgBatchDwellMs = fc > 0 ? (totalBatchDwellNs.get() / (double) fc) / 1_000_000.0 : 0;
        double avgHttpRttMs = fc > 0 ? (totalHttpRttNs.get() / (double) fc) / 1_000_000.0 : 0;

        // Diagnostic: flush trigger breakdown
        int byFull = flushByFullBatch.get();
        int byTimer = flushByTimer.get();
        int totalFlushes = byFull + byTimer;
        double fullPct = totalFlushes > 0 ? (byFull * 100.0 / totalFlushes) : 0;
        double avgBatchSize = totalBatches.get() > 0 ? (totalIntentsBatched.get() / (double) totalBatches.get()) : 0;

        System.out.printf("RESULT_FLUSH_DIAG|by_full_batch=%d|by_timer=%d|full_pct=%.1f|avg_batch_size=%.1f%n",
            byFull, byTimer, fullPct, avgBatchSize);

        System.out.printf("RESULT_BATCH|total_batches=%d|total_intents_batched=%d|avg_batch_latency_ms=%.1f|max_batch_size=%d|flush_interval_ms=%d%n",
            totalBatches.get(), totalIntentsBatched.get(), getAvgBatchLatencyMs(), MAX_BATCH_SIZE, FLUSH_INTERVAL_MS);
        System.out.printf("RESULT_DELIVERY_TIMING|batch_dwell_avg_ms=%.1f|http_rtt_avg_ms=%.1f|flush_count=%d%n",
            avgBatchDwellMs, avgHttpRttMs, fc);
        System.out.printf("RESULT_CONFIG|client=ReactorNetty|chameleon_mode=%s|batch_post=%s|pool_size=500%n",
            chameleonMode, chameleonMode ? "individual" : "json_array");
    }
}

package com.loomq.scheduler;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Batched HTTP delivery handler — groups intents by target URL and delivers
 * them in batches. Reduces HTTP round-trips from O(intents) to O(intents/BATCH_SIZE).
 *
 * Each intent still gets its own CompletableFuture that completes when the
 * batch is delivered. Batching is transparent to the scheduler.
 *
 * Batch metrics are exposed via RESULT_BATCH markers for benchmark analysis.
 */
final class BatchedHttpDeliveryHandler implements DeliveryHandler {

    private static final Logger log = LoggerFactory.getLogger(BatchedHttpDeliveryHandler.class);
    private static final int MAX_BATCH_SIZE = 50;
    private static final long FLUSH_INTERVAL_MS = 10;

    private final HttpClient httpClient;
    private final int webhookPort;
    private final boolean chameleonMode;  // default false — batch POST; true = individual POSTs
    private final Map<String, ConcurrentLinkedDeque<IntentEntry>> batches;
    private final ScheduledExecutorService flusher;

    // Metrics
    private final AtomicInteger totalBatches = new AtomicInteger(0);
    private final AtomicInteger totalIntentsBatched = new AtomicInteger(0);
    private final AtomicLong totalBatchLatencyNs = new AtomicLong(0);

    BatchedHttpDeliveryHandler(int webhookPort) {
        this(webhookPort, false);
    }

    BatchedHttpDeliveryHandler(int webhookPort, boolean chameleonMode) {
        this.webhookPort = webhookPort;
        this.chameleonMode = chameleonMode;
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .version(HttpClient.Version.HTTP_1_1)
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            .build();
        this.batches = new ConcurrentHashMap<>();
        this.flusher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "batch-flusher");
            t.setDaemon(true);
            return t;
        });
        this.flusher.scheduleAtFixedRate(this::flushAll, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<DeliveryResult> deliverAsync(Intent intent) {
        String url = intent.getCallback() != null ? intent.getCallback().getUrl()
            : "http://localhost:" + webhookPort + "/webhook";
        CompletableFuture<DeliveryResult> future = new CompletableFuture<>();
        IntentEntry entry = new IntentEntry(intent, future);

        ConcurrentLinkedDeque<IntentEntry> deque = batches.computeIfAbsent(url, k -> new ConcurrentLinkedDeque<>());
        deque.addLast(entry);

        // If batch reaches threshold, flush immediately
        if (deque.size() >= MAX_BATCH_SIZE) {
            flushUrl(url, deque);
        }

        return future;
    }

    int getTotalBatches() { return totalBatches.get(); }
    int getTotalIntentsBatched() { return totalIntentsBatched.get(); }
    double getAvgBatchLatencyMs() {
        int n = totalBatches.get();
        return n > 0 ? (totalBatchLatencyNs.get() / (double) n) / 1_000_000.0 : 0;
    }
    int getPendingBatchCount() { return batches.size(); }

    void shutdown() {
        flusher.shutdown();
    }

    private void flushAll() {
        for (var entry : batches.entrySet()) {
            ConcurrentLinkedDeque<IntentEntry> deque = entry.getValue();
            if (!deque.isEmpty()) {
                flushUrl(entry.getKey(), deque);
            }
        }
    }

    private void flushUrl(String url, ConcurrentLinkedDeque<IntentEntry> deque) {
        List<IntentEntry> batch = new ArrayList<>();
        IntentEntry e;
        while (batch.size() < MAX_BATCH_SIZE && (e = deque.pollFirst()) != null) {
            batch.add(e);
        }
        if (batch.isEmpty()) return;

        if (chameleonMode) {
            // Chameleon: individual HTTP POST per intent, independent future completion
            for (IntentEntry entry : batch) {
                Intent intent = entry.intent;
                String payload = "{\"intentId\":\"" + intent.getIntentId()
                    + "\",\"precisionTier\":\"" + intent.getPrecisionTier().name() + "\"}";
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url)).timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload)).build();
                long startNs = System.nanoTime();
                httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .whenComplete((resp, ex) -> {
                        totalBatches.incrementAndGet(); totalIntentsBatched.incrementAndGet();
                        totalBatchLatencyNs.addAndGet(System.nanoTime() - startNs);
                        DeliveryResult dr = ex != null ? DeliveryResult.RETRY
                            : (resp.statusCode() >= 200 && resp.statusCode() < 300) ? DeliveryResult.SUCCESS
                            : resp.statusCode() >= 500 ? DeliveryResult.RETRY : DeliveryResult.DEAD_LETTER;
                        entry.future.complete(dr);
                    });
            }
        } else {
            // Batch JSON POST — single HTTP request for all intents in batch
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < batch.size(); i++) {
                if (i > 0) sb.append(",");
                Intent intent = batch.get(i).intent;
                sb.append("{\"intentId\":\"").append(intent.getIntentId())
                  .append("\",\"precisionTier\":\"").append(intent.getPrecisionTier().name()).append("\"}");
            }
            sb.append("]");
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url)).timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(sb.toString())).build();
            long startNs = System.nanoTime();
            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .whenComplete((resp, ex) -> {
                    long elapsedNs = System.nanoTime() - startNs;
                    totalBatches.incrementAndGet(); totalIntentsBatched.addAndGet(batch.size());
                    totalBatchLatencyNs.addAndGet(elapsedNs);
                    DeliveryResult dr = ex != null ? DeliveryResult.RETRY
                        : (resp.statusCode() >= 200 && resp.statusCode() < 300) ? DeliveryResult.SUCCESS
                        : resp.statusCode() >= 500 ? DeliveryResult.RETRY : DeliveryResult.DEAD_LETTER;
                    for (IntentEntry entry : batch) { entry.future.complete(dr); }
                });
        }
    }

    private record IntentEntry(Intent intent, CompletableFuture<DeliveryResult> future) {}

    // ====== Marker output for benchmark scripts ======

    void printBatchMarkers() {
        System.out.printf("RESULT_BATCH|total_batches=%d|total_intents_batched=%d|avg_batch_latency_ms=%.1f|max_batch_size=%d|flush_interval_ms=%d%n",
            totalBatches.get(), totalIntentsBatched.get(), getAvgBatchLatencyMs(), MAX_BATCH_SIZE, FLUSH_INTERVAL_MS);
        System.out.printf("RESULT_CONFIG|chameleon_mode=%s|batch_post=%s%n",
            chameleonMode, chameleonMode ? "individual" : "json_array");
    }
}

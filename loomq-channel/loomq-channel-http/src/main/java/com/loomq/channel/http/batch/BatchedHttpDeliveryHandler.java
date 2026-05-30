package com.loomq.channel.http.batch;

import com.loomq.domain.intent.Intent;
import com.loomq.spi.DeliveryContext;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.RedeliveryDecider;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

/**
 * 生产级批量 HTTP 投递处理器。
 *
 * <p>将多个 Intent 聚合为单次 HTTP POST 请求发送，减少协议开销和网络往返。
 * 按 {@code url + "#" + tierName} 分组，支持两种 flush 触发：
 * <ul>
 *   <li>队列达到 {@code maxBatchSize} 时立即 flush</li>
 *   <li>队列从空变非空时调度 {@code flushIntervalMs} 延迟 flush</li>
 * </ul>
 *
 * <p>批量请求格式：
 * <pre>{@code
 * POST /webhook HTTP/1.1
 * Content-Type: application/json
 * X-LoomQ-Batch-Mode: true
 * X-LoomQ-Batch-Size: 5
 *
 * {"intents":[
 *   {"intentId":"id1","precisionTier":"ULTRA"},
 *   {"intentId":"id2","precisionTier":"ULTRA"}
 * ]}
 * }</pre>
 *
 * <p>服务端可返回逐个 intent 的处理结果（可选），或返回单个状态码表示整体结果。
 *
 * @author loomq
 * @since v0.9.2
 */
public class BatchedHttpDeliveryHandler implements DeliveryHandler {

    private static final Logger logger = LoggerFactory.getLogger(BatchedHttpDeliveryHandler.class);

    private final BatchDeliveryConfig config;
    private final HttpClient client;
    private final RedeliveryDecider redeliveryDecider;
    private final ConcurrentHashMap<String, ConcurrentLinkedDeque<IntentEntry>> batches;
    private final ScheduledExecutorService flushScheduler;
    private final ConcurrentHashMap<String, ScheduledFuture<?>> pendingTimeoutFlush;

    // Metrics
    private final AtomicInteger totalBatches = new AtomicInteger(0);
    private final AtomicInteger totalIntentsBatched = new AtomicInteger(0);
    private final AtomicLong totalBatchDwellNs = new AtomicLong(0);
    private final AtomicLong totalHttpRttNs = new AtomicLong(0);
    private final AtomicInteger flushByFullBatch = new AtomicInteger(0);
    private final AtomicInteger flushByTimer = new AtomicInteger(0);

    public BatchedHttpDeliveryHandler() {
        this(BatchDeliveryConfig.DEFAULT, loadRedeliveryDecider());
    }

    public BatchedHttpDeliveryHandler(BatchDeliveryConfig config) {
        this(config, loadRedeliveryDecider());
    }

    public BatchedHttpDeliveryHandler(BatchDeliveryConfig config, RedeliveryDecider redeliveryDecider) {
        this.config = config;
        this.redeliveryDecider = redeliveryDecider;

        ConnectionProvider provider = ConnectionProvider.builder("loomq-delivery-pool")
            .maxConnections(config.maxConnections())
            .pendingAcquireMaxCount(10_000)
            .pendingAcquireTimeout(Duration.ofSeconds(5))
            .maxIdleTime(Duration.ofSeconds(30))
            .build();

        this.client = HttpClient.create(provider)
            .keepAlive(true)
            .responseTimeout(Duration.ofMillis(config.batchTimeoutMs()))
            .option(io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
            .option(io.netty.channel.ChannelOption.TCP_NODELAY, config.tcpNoDelay());

        this.batches = new ConcurrentHashMap<>();
        this.pendingTimeoutFlush = new ConcurrentHashMap<>();

        this.flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "batch-delivery-flusher");
            t.setDaemon(true);
            return t;
        });

        logger.info("BatchedHttpDeliveryHandler initialized: maxBatchSize={}, flushIntervalMs={}, maxConnections={}",
            config.maxBatchSize(), config.flushIntervalMs(), config.maxConnections());
    }

    private static RedeliveryDecider loadRedeliveryDecider() {
        ServiceLoader<RedeliveryDecider> loader = ServiceLoader.load(RedeliveryDecider.class);
        return loader.findFirst().orElse(null);
    }

    @Override
    public CompletableFuture<DeliveryResult> deliverAsync(Intent intent) {
        String url = intent.getCallback() != null ? intent.getCallback().getUrl() : null;
        if (url == null || url.isBlank()) {
            logger.warn("No callback URL for intent {}", intent.getIntentId());
            return CompletableFuture.completedFuture(DeliveryResult.DEAD_LETTER);
        }

        CompletableFuture<DeliveryResult> future = new CompletableFuture<>();
        String batchKey = url + "#" + intent.getPrecisionTier().name();
        long enqueueNanos = System.nanoTime();
        IntentEntry entry = new IntentEntry(intent, future, enqueueNanos);

        ConcurrentLinkedDeque<IntentEntry> deque = batches.computeIfAbsent(batchKey,
            k -> new ConcurrentLinkedDeque<>());
        int prevSize = deque.size();
        deque.addLast(entry);

        if (deque.size() >= config.maxBatchSize()) {
            // 满批触发：立即 flush，取消待处理的超时刷新
            ScheduledFuture<?> pending = pendingTimeoutFlush.remove(batchKey);
            if (pending != null) {
                pending.cancel(false);
            }
            flushBatch(url, batchKey, deque, true);
        } else if (prevSize == 0) {
            // 队列从空变非空：调度超时 flush
            pendingTimeoutFlush.computeIfAbsent(batchKey, k ->
                flushScheduler.schedule(() -> {
                    pendingTimeoutFlush.remove(batchKey);
                    flushBatch(url, batchKey, deque, false);
                }, config.flushIntervalMs(), TimeUnit.MILLISECONDS)
            );
        }

        return future;
    }

    /**
     * Flush 一批 Intent：从队列取出，发送批量 HTTP 请求。
     */
    private void flushBatch(String url, String batchKey, ConcurrentLinkedDeque<IntentEntry> deque,
                            boolean isFullBatchTrigger) {
        if (isFullBatchTrigger) {
            flushByFullBatch.incrementAndGet();
        } else {
            flushByTimer.incrementAndGet();
        }

        List<IntentEntry> batch = new ArrayList<>();
        IntentEntry e;
        while (batch.size() < config.maxBatchSize() && (e = deque.pollFirst()) != null) {
            batch.add(e);
        }
        if (batch.isEmpty()) {
            return;
        }

        byte[] payload = buildBatchPayload(batch);
        long startNs = System.nanoTime();

        // 从第一个 intent 提取自定义 headers
        Intent firstIntent = batch.get(0).intent;
        Map<String, String> customHeaders = firstIntent.getCallback() != null
            ? firstIntent.getCallback().getHeaders() : null;

        client.headers(h -> {
            h.add("Content-Type", "application/json");
            h.add("X-LoomQ-Batch-Mode", "true");
            h.add("X-LoomQ-Batch-Size", String.valueOf(batch.size()));
            if (customHeaders != null) {
                customHeaders.forEach(h::add);
            }
        })
        .post()
        .uri(url)
        .send(Mono.just(Unpooled.wrappedBuffer(payload)))
        .responseSingle((resp, body) -> body.asString().map(responseBody -> {
            int status = resp.status().code();
            return new HttpResponse(status, responseBody);
        }))
        .onErrorResume(ex -> {
            logger.warn("Batch delivery error for {}: {}", batchKey, ex.getMessage());
            return Mono.just(new HttpResponse(-1, ex.getMessage()));
        })
        .subscribe(response -> {
            long nowNs = System.nanoTime();
            long httpRttNs = nowNs - startNs;

            totalBatches.incrementAndGet();
            totalIntentsBatched.addAndGet(batch.size());
            totalBatchDwellNs.addAndGet(nowNs - batch.get(0).enqueueNanos);
            totalHttpRttNs.addAndGet(httpRttNs);

            processBatchResponse(batch, response);
        });
    }

    /**
     * 处理批量响应：尝试解析逐个 intent 结果，否则按整体状态码处理。
     */
    private void processBatchResponse(List<IntentEntry> batch, HttpResponse response) {
        int status = response.status();

        // 尝试解析逐个 intent 结果
        Map<String, DeliveryResult> perIntentResults = null;
        if (status >= 200 && status < 300) {
            perIntentResults = parsePerIntentResults(response.body(), batch);
        }

        for (IntentEntry entry : batch) {
            DeliveryResult result;
            if (perIntentResults != null && perIntentResults.containsKey(entry.intent.getIntentId())) {
                result = perIntentResults.get(entry.intent.getIntentId());
            } else {
                result = resolveResult(status, entry.intent);
            }
            entry.future.complete(result);
        }
    }

    /**
     * 解析服务端返回的逐个 intent 处理结果。
     *
     * <p>预期格式：
     * <pre>{@code
     * {"results":[
     *   {"intentId":"id1","status":"SUCCESS"},
     *   {"intentId":"id2","status":"FAILED"}
     * ]}
     * }</pre>
     *
     * @return 解析成功返回结果 map，解析失败返回 null（降级为整体状态码处理）
     */
    private Map<String, DeliveryResult> parsePerIntentResults(String body, List<IntentEntry> batch) {
        if (body == null || body.isBlank()) {
            return null;
        }
        try {
            // 简单 JSON 解析：查找 "results" 数组
            // 生产环境应使用 Jackson，但这里保持最小依赖
            Map<String, DeliveryResult> results = new java.util.HashMap<>();
            String resultsSection = extractJsonArray(body, "results");
            if (resultsSection == null) {
                return null;
            }

            // 解析每个 result 对象
            for (IntentEntry entry : batch) {
                String intentId = entry.intent.getIntentId();
                String statusValue = extractFieldValue(resultsSection, intentId);
                if (statusValue != null) {
                    results.put(intentId, parseDeliveryResult(statusValue));
                }
            }
            return results.isEmpty() ? null : results;
        } catch (Exception e) {
            logger.debug("Failed to parse per-intent results, falling back to status code: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 根据 HTTP 状态码和 RedeliveryDecider 决定投递结果。
     */
    private DeliveryResult resolveResult(int status, Intent intent) {
        String deliveryId = "delivery_" + intent.getIntentId() + "_" + intent.getAttempts();
        int attempt = intent.getAttempts() + 1;
        DeliveryContext context = new DeliveryContext(deliveryId, intent.getIntentId(), attempt);

        if (status >= 200 && status < 300) {
            context.markSuccess(status, Map.of(), "");
            if (redeliveryDecider != null && redeliveryDecider.shouldRedeliver(context)) {
                return DeliveryResult.RETRY;
            }
            return DeliveryResult.SUCCESS;
        }

        context.markFailure(new RuntimeException("HTTP " + status));
        if (redeliveryDecider != null && redeliveryDecider.shouldRedeliver(context)) {
            return DeliveryResult.RETRY;
        }
        if (status >= 500) {
            return DeliveryResult.RETRY;
        }
        return DeliveryResult.DEAD_LETTER;
    }

    private DeliveryResult parseDeliveryResult(String value) {
        if (value == null) {
            return DeliveryResult.SUCCESS;
        }
        return switch (value.toUpperCase()) {
            case "SUCCESS", "DELIVERED" -> DeliveryResult.SUCCESS;
            case "RETRY" -> DeliveryResult.RETRY;
            case "DEAD_LETTER", "DEAD_LETTERED" -> DeliveryResult.DEAD_LETTER;
            case "EXPIRED" -> DeliveryResult.EXPIRED;
            default -> DeliveryResult.SUCCESS;
        };
    }

    /**
     * 构建批量 JSON payload（JSON 数组格式，兼容现有 mock server）。
     */
    private byte[] buildBatchPayload(List<IntentEntry> batch) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < batch.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            Intent intent = batch.get(i).intent;
            sb.append("{\"intentId\":\"").append(intent.getIntentId())
              .append("\",\"precisionTier\":\"").append(intent.getPrecisionTier().name()).append("\"}");
        }
        sb.append("]");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 获取批量投递指标。
     */
    public BatchDeliveryMetrics getMetrics() {
        int batches = totalBatches.get();
        return new BatchDeliveryMetrics(
            batches,
            totalIntentsBatched.get(),
            batches > 0 ? (totalBatchDwellNs.get() / (double) batches) / 1_000_000.0 : 0,
            batches > 0 ? (totalHttpRttNs.get() / (double) batches) / 1_000_000.0 : 0,
            flushByFullBatch.get(),
            flushByTimer.get()
        );
    }

    /**
     * 关闭投递处理器，释放资源。
     */
    public void shutdown() {
        flushScheduler.shutdown();
        try {
            if (!flushScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                flushScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            flushScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Flush remaining batches
        for (var entry : batches.entrySet()) {
            ConcurrentLinkedDeque<IntentEntry> deque = entry.getValue();
            IntentEntry first = deque.peekFirst();
            if (first != null && first.intent.getCallback() != null) {
                String url = first.intent.getCallback().getUrl();
                flushBatch(url, entry.getKey(), deque, false);
            }
        }

        if (client.configuration().connectionProvider() != null) {
            client.configuration().connectionProvider().dispose();
        }

        logger.info("BatchedHttpDeliveryHandler shutdown: totalBatches={}, totalIntents={}",
            totalBatches.get(), totalIntentsBatched.get());
    }

    // ── 辅助方法 ──

    private static String extractJsonArray(String body, String fieldName) {
        String search = "\"" + fieldName + "\":";
        int idx = body.indexOf(search);
        if (idx < 0) {
            return null;
        }
        int start = body.indexOf('[', idx);
        if (start < 0) {
            return null;
        }
        int depth = 0;
        for (int i = start; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '[') depth++;
            else if (c == ']') depth--;
            if (depth == 0) return body.substring(start, i + 1);
        }
        return null;
    }

    private static String extractFieldValue(String arrayJson, String intentId) {
        String search = "\"intentId\":\"" + intentId + "\"";
        int idx = arrayJson.indexOf(search);
        if (idx < 0) {
            return null;
        }
        String after = arrayJson.substring(idx + search.length());
        int statusIdx = after.indexOf("\"status\":\"");
        if (statusIdx < 0) {
            return null;
        }
        int valueStart = statusIdx + "\"status\":\"".length();
        int valueEnd = after.indexOf("\"", valueStart);
        if (valueEnd < 0) {
            return null;
        }
        return after.substring(valueStart, valueEnd);
    }

    // ── 内部类型 ──

    private record IntentEntry(Intent intent, CompletableFuture<DeliveryResult> future, long enqueueNanos) {}

    private record HttpResponse(int status, String body) {}

    /**
     * 批量投递指标。
     */
    public record BatchDeliveryMetrics(
        int totalBatches,
        int totalIntentsBatched,
        double avgBatchDwellMs,
        double avgHttpRttMs,
        int flushByFullBatch,
        int flushByTimer
    ) {
        public double avgBatchSize() {
            return totalBatches > 0 ? (double) totalIntentsBatched / totalBatches : 0;
        }

        public double fullBatchPercentage() {
            int total = flushByFullBatch + flushByTimer;
            return total > 0 ? (double) flushByFullBatch / total * 100 : 0;
        }
    }
}

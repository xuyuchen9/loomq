package com.loomq.callback;

import com.loomq.domain.intent.Intent;
import com.loomq.spi.DeliveryContext;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.RedeliveryDecider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;

/**
 * Netty 异步 HTTP 投递处理器。
 *
 * 基于 Reactor Netty，零阻塞异步 HTTP 调用，
 * 连接池自动复用，keep-alive 默认开启。
 *
 * @author loomq
 * @since v0.8.0
 */
public class NettyHttpDeliveryHandler implements DeliveryHandler {

    private static final Logger logger = LoggerFactory.getLogger(NettyHttpDeliveryHandler.class);
    private static final int DEFAULT_TIMEOUT_SEC = 30;

    private final reactor.netty.http.client.HttpClient client;
    private final RedeliveryDecider redeliveryDecider;

    public NettyHttpDeliveryHandler() {
        this(HttpClient.create()
            .keepAlive(true)
            .responseTimeout(Duration.ofSeconds(DEFAULT_TIMEOUT_SEC)),
            loadRedeliveryDecider());
    }

    public NettyHttpDeliveryHandler(HttpClient client, RedeliveryDecider redeliveryDecider) {
        this.client = client;
        this.redeliveryDecider = redeliveryDecider;
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

        String deliveryId = "delivery_" + intent.getIntentId() + "_" + intent.getAttempts();
        int attempt = intent.getAttempts() + 1;
        DeliveryContext context = new DeliveryContext(deliveryId, intent.getIntentId(), attempt);

        byte[] payload = buildPayloadBytes(intent);

        return client
            .headers(h -> {
                h.add("Content-Type", "application/json");
                h.add("X-LoomQ-Intent-Id", intent.getIntentId());
                Map<String, String> customHeaders = intent.getCallback().getHeaders();
                if (customHeaders != null) {
                    customHeaders.forEach(h::add);
                }
            })
            .post()
            .uri(url)
            .send(Mono.just(io.netty.buffer.Unpooled.wrappedBuffer(payload)))
            .responseSingle((httpResponse, body) ->
                body.asString().map(responseBody -> {
                    int status = httpResponse.status().code();
                    context.markSuccess(status, Map.of(), responseBody);

                    if (redeliveryDecider != null && redeliveryDecider.shouldRedeliver(context)) {
                        return DeliveryResult.RETRY;
                    }
                    if (status >= 200 && status < 300) {
                        return DeliveryResult.SUCCESS;
                    }
                    if (redeliveryDecider != null && redeliveryDecider.shouldRedeliver(context)) {
                        return DeliveryResult.RETRY;
                    }
                    return DeliveryResult.DEAD_LETTER;
                })
            )
            .onErrorResume(ex -> {
                logger.warn("Delivery error for intent {}: {}", intent.getIntentId(), ex.getMessage());
                context.markFailure(ex instanceof Exception ? (Exception) ex : new RuntimeException(ex));
                DeliveryResult fallback = (redeliveryDecider != null && redeliveryDecider.shouldRedeliver(context))
                    ? DeliveryResult.RETRY
                    : DeliveryResult.DEAD_LETTER;
                return Mono.just(fallback);
            })
            .toFuture();
    }

    private byte[] buildPayloadBytes(Intent intent) {
        String json = "{\"intentId\":\"" + intent.getIntentId()
            + "\",\"precisionTier\":\"" + intent.getPrecisionTier().name() + "\"}";
        return json.getBytes(StandardCharsets.UTF_8);
    }
}

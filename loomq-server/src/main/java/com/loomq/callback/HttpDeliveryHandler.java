package com.loomq.callback;

import com.loomq.domain.intent.Intent;
import com.loomq.http.HttpCallbackClient;
import com.loomq.spi.DeliveryContext;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.RedeliveryDecider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * HTTP 投递处理器
 *
 * 实现 DeliveryHandler 接口，通过 HTTP webhook 投递 Intent。
 *
 * @author loomq
 * @since v0.7.1
 */
public class HttpDeliveryHandler implements DeliveryHandler {

    private static final Logger logger = LoggerFactory.getLogger(HttpDeliveryHandler.class);

    private static final int DEFAULT_TIMEOUT_SEC = 30;

    private final HttpClient httpClient;
    private final RedeliveryDecider redeliveryDecider;

    public HttpDeliveryHandler() {
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .version(HttpClient.Version.HTTP_1_1)
            .build();

        ServiceLoader<RedeliveryDecider> loader = ServiceLoader.load(RedeliveryDecider.class);
        this.redeliveryDecider = loader.findFirst().orElse(null);
    }

    public HttpDeliveryHandler(HttpClient httpClient, RedeliveryDecider redeliveryDecider) {
        this.httpClient = httpClient;
        this.redeliveryDecider = redeliveryDecider;
    }

    @Override
    public DeliveryResult deliver(Intent intent) {
        String deliveryId = "delivery_" + intent.getIntentId() + "_" + intent.getAttempts();
        int attempt = intent.getAttempts() + 1;
        DeliveryContext context = new DeliveryContext(deliveryId, intent.getIntentId(), attempt);

        try {
            HttpRequest request = HttpCallbackClient.buildCallbackRequest(intent, DEFAULT_TIMEOUT_SEC);
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            context.markSuccess(response.statusCode(), Map.of(), response.body());

            // 检查是否需要重投
            if (redeliveryDecider != null && redeliveryDecider.shouldRedeliver(context)) {
                return DeliveryResult.RETRY;
            }

            // 检查 HTTP 状态
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return DeliveryResult.SUCCESS;
            }

            // 非 2xx 响应，检查是否需要重投
            if (redeliveryDecider != null && redeliveryDecider.shouldRedeliver(context)) {
                return DeliveryResult.RETRY;
            }

            return DeliveryResult.DEAD_LETTER;

        } catch (java.net.ConnectException e) {
            logger.warn("Connection refused for intent {} to {}: {}",
                intent.getIntentId(), intent.getCallback().getUrl(), e.getMessage());
            context.markFailure(e);
            return shouldRedeliver(context) ? DeliveryResult.RETRY : DeliveryResult.DEAD_LETTER;

        } catch (java.net.SocketTimeoutException e) {
            logger.warn("Socket timeout for intent {} to {}",
                intent.getIntentId(), intent.getCallback().getUrl());
            context.markFailure(e);
            return shouldRedeliver(context) ? DeliveryResult.RETRY : DeliveryResult.DEAD_LETTER;

        } catch (java.net.http.HttpTimeoutException e) {
            logger.warn("HTTP timeout for intent {} to {}",
                intent.getIntentId(), intent.getCallback().getUrl());
            context.markFailure(e);
            return shouldRedeliver(context) ? DeliveryResult.RETRY : DeliveryResult.DEAD_LETTER;

        } catch (java.io.IOException e) {
            logger.warn("I/O error for intent {} to {}: {}",
                intent.getIntentId(), intent.getCallback().getUrl(), e.getMessage());
            context.markFailure(e);
            return shouldRedeliver(context) ? DeliveryResult.RETRY : DeliveryResult.DEAD_LETTER;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Delivery interrupted for intent {}", intent.getIntentId());
            context.markFailure(e);
            return DeliveryResult.DEAD_LETTER;

        } catch (Exception e) {
            logger.error("Unexpected error delivering intent {}: {}",
                intent.getIntentId(), e.getMessage(), e);
            context.markFailure(e);
            return DeliveryResult.DEAD_LETTER;
        }
    }

    private boolean shouldRedeliver(DeliveryContext context) {
        if (redeliveryDecider == null) {
            return false;
        }
        return redeliveryDecider.shouldRedeliver(context);
    }
}

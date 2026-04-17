package com.loomq.callback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.Intent;
import com.loomq.spi.CallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * HTTP 回调处理器
 *
 * 实现 CallbackHandler 接口，将内核事件转换为 HTTP webhook 投递。
 * 支持异步投递、指数退避重试、超时控制。
 *
 * v0.7.0 新增：连接 core 模块与 HTTP 投递层
 *
 * @author loomq
 * @since v0.7.0
 */
public class HttpCallbackHandler implements CallbackHandler {

    private static final Logger logger = LoggerFactory.getLogger(HttpCallbackHandler.class);

    /** JSON 序列化器 */
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /** 默认连接超时（秒） */
    private static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 5;

    /** 默认请求超时（秒） */
    private static final int DEFAULT_REQUEST_TIMEOUT_SECONDS = 30;

    /** 默认最大重试次数 */
    private static final int DEFAULT_MAX_RETRIES = 3;

    /** 初始重试延迟（毫秒） */
    private static final long INITIAL_RETRY_DELAY_MS = 100;

    /** 最大重试延迟（毫秒） */
    private static final long MAX_RETRY_DELAY_MS = 5000;

    /** HTTP 客户端 */
    private final HttpClient httpClient;

    /** 执行器（默认虚拟线程） */
    private final Executor executor;

    /** 最大重试次数 */
    private final int maxRetries;

    /** 连接超时 */
    private final Duration connectTimeout;

    /** 请求超时 */
    private final Duration requestTimeout;

    /**
     * 创建默认配置的 HTTP 回调处理器
     */
    public HttpCallbackHandler() {
        this(builder());
    }

    /**
     * 通过 Builder 创建处理器
     */
    private HttpCallbackHandler(Builder builder) {
        this.maxRetries = builder.maxRetries;
        this.connectTimeout = builder.connectTimeout;
        this.requestTimeout = builder.requestTimeout;
        this.executor = builder.executor != null
            ? builder.executor
            : Executors.newVirtualThreadPerTaskExecutor();

        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(connectTimeout)
            .executor(executor)
            .build();

        logger.info("HttpCallbackHandler initialized: maxRetries={}, connectTimeout={}s, requestTimeout={}s",
            maxRetries, connectTimeout.getSeconds(), requestTimeout.getSeconds());
    }

    @Override
    public void onIntentEvent(Intent intent, EventType type, Throwable error) {
        Callback callback = intent.getCallback();
        if (intent == null || callback == null) {
            logger.warn("Cannot send callback: intent or callback config is null");
            return;
        }

        String callbackUrl = callback.getUrl();
        if (callbackUrl == null || callbackUrl.isBlank()) {
            logger.warn("Cannot send callback: callback URL is empty for intent {}", intent.getIntentId());
            return;
        }

        // 异步执行回调投递
        executor.execute(() -> deliverCallback(intent, type, error));
    }

    /**
     * 执行回调投递（带重试）
     */
    private void deliverCallback(Intent intent, EventType type, Throwable error) {
        String intentId = intent.getIntentId();
        Callback callback = intent.getCallback();
        String callbackUrl = callback.getUrl();

        int attempt = 0;
        long retryDelay = INITIAL_RETRY_DELAY_MS;

        while (attempt <= maxRetries) {
            attempt++;

            try {
                HttpRequest request = buildRequest(intent, type, error);

                logger.debug("Sending callback: intent={}, type={}, url={}, attempt={}/{}",
                    intentId, type, callbackUrl, attempt, maxRetries + 1);

                HttpResponse<String> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofString());

                if (isSuccessStatusCode(response.statusCode())) {
                    logger.debug("Callback delivered successfully: intent={}, type={}, status={}",
                        intentId, type, response.statusCode());
                    return; // 成功，退出重试循环
                } else {
                    logger.warn("Callback returned non-success status: intent={}, type={}, status={}",
                        intentId, type, response.statusCode());

                    if (attempt > maxRetries) {
                        logger.error("Callback failed after {} attempts: intent={}, type={}",
                            maxRetries + 1, intentId, type);
                        return;
                    }
                }
            } catch (Exception e) {
                logger.warn("Callback attempt {} failed: intent={}, type={}, error={}",
                    attempt, intentId, type, e.getMessage());

                if (attempt > maxRetries) {
                    logger.error("Callback delivery failed after {} attempts: intent={}, type={}",
                        maxRetries + 1, intentId, type, e);
                    return;
                }
            }

            // 退避重试
            if (attempt <= maxRetries) {
                try {
                    Thread.sleep(retryDelay);
                    retryDelay = Math.min(retryDelay * 2, MAX_RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    logger.warn("Callback retry interrupted for intent {}", intentId);
                    return;
                }
            }
        }
    }

    /**
     * 异步投递回调（非阻塞）
     */
    public CompletableFuture<Boolean> deliverCallbackAsync(Intent intent, EventType type, Throwable error) {
        return CompletableFuture.supplyAsync(() -> {
            deliverCallback(intent, type, error);
            return true;
        }, executor);
    }

    /**
     * 构建 HTTP 请求
     */
    private HttpRequest buildRequest(Intent intent, EventType type, Throwable error) {
        Callback callback = intent.getCallback();
        HttpRequest.BodyPublisher bodyPublisher = buildRequestBody(intent, type, error);

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create(callback.getUrl()))
            .timeout(requestTimeout)
            .header("Content-Type", "application/json")
            .header("X-LoomQ-Intent-Id", intent.getIntentId())
            .header("X-LoomQ-Event-Type", type.name());

        // 添加自定义请求头
        Map<String, String> headers = callback.getHeaders();
        if (headers != null) {
            headers.forEach(requestBuilder::header);
        }

        // 设置请求方法（默认 POST）
        String method = callback.getMethod();
        if (method == null || "POST".equalsIgnoreCase(method)) {
            requestBuilder.POST(bodyPublisher);
        } else if ("PUT".equalsIgnoreCase(method)) {
            requestBuilder.PUT(bodyPublisher);
        } else if ("PATCH".equalsIgnoreCase(method)) {
            requestBuilder.method("PATCH", bodyPublisher);
        } else {
            requestBuilder.POST(bodyPublisher);
        }

        return requestBuilder.build();
    }

    /**
     * 构建请求体
     */
    private HttpRequest.BodyPublisher buildRequestBody(Intent intent, EventType type, Throwable error) {
        Callback callback = intent.getCallback();
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"intentId\":\"").append(escapeJson(intent.getIntentId())).append("\",");
        json.append("\"eventType\":\"").append(type.name()).append("\",");
        json.append("\"status\":\"").append(intent.getStatus().name()).append("\",");
        json.append("\"executeAt\":\"").append(intent.getExecuteAt()).append("\",");

        if (callback != null && callback.getBody() != null) {
            json.append("\"payload\":");
            try {
                String payload = objectMapper.writeValueAsString(callback.getBody());
                json.append(payload);
            } catch (JsonProcessingException e) {
                // 降级为 toString()
                String payload = callback.getBody().toString();
                json.append("\"").append(escapeJson(payload)).append("\"");
            }
            json.append(",");
        }

        if (error != null) {
            json.append("\"error\":");
            json.append("{");
            json.append("\"message\":\"").append(escapeJson(error.getMessage())).append("\",");
            json.append("\"type\":\"").append(error.getClass().getSimpleName()).append("\"");
            json.append("}");
        } else {
            json.append("\"error\":null");
        }

        json.append("}");

        return HttpRequest.BodyPublishers.ofString(json.toString());
    }

    /**
     * 简单的 JSON 字符串转义
     */
    private String escapeJson(String input) {
        if (input == null) {
            return "";
        }
        return input
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\b", "\\b")
            .replace("\f", "\\f")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t");
    }

    /**
     * 判断 HTTP 状态码是否表示成功
     */
    private boolean isSuccessStatusCode(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    /**
     * 关闭处理器（释放资源）
     */
    public void close() {
        logger.info("Closing HttpCallbackHandler...");
        // HttpClient 不需要显式关闭
        if (executor instanceof AutoCloseable) {
            try {
                ((AutoCloseable) executor).close();
            } catch (Exception e) {
                logger.warn("Error closing executor", e);
            }
        }
    }

    // ========== Builder ==========

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Executor executor;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private Duration connectTimeout = Duration.ofSeconds(DEFAULT_CONNECT_TIMEOUT_SECONDS);
        private Duration requestTimeout = Duration.ofSeconds(DEFAULT_REQUEST_TIMEOUT_SECONDS);

        public Builder executor(Executor executor) {
            this.executor = executor;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder connectTimeout(Duration timeout) {
            this.connectTimeout = timeout;
            return this;
        }

        public Builder requestTimeout(Duration timeout) {
            this.requestTimeout = timeout;
            return this;
        }

        public HttpCallbackHandler build() {
            return new HttpCallbackHandler(this);
        }
    }
}

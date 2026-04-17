package com.loomq.http;

import com.loomq.domain.intent.Intent;

import java.net.URI;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.Map;

/**
 * HTTP 回调客户端工具类
 *
 * 提供构建 HTTP 回调请求的共享方法，避免代码重复
 *
 * @author loomq
 * @since v0.6.2
 */
public final class HttpCallbackClient {

    /** 默认请求超时时间（秒） */
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;

    private HttpCallbackClient() {
        // 工具类不允许实例化
    }

    /**
     * 构建 HTTP 回调请求
     *
     * @param intent 意图对象
     * @return 构建好的 HTTP 请求
     */
    public static HttpRequest buildCallbackRequest(Intent intent) {
        return buildCallbackRequest(intent, DEFAULT_TIMEOUT_SECONDS);
    }

    /**
     * 构建 HTTP 回调请求（可配置超时时间）
     *
     * @param intent        意图对象
     * @param timeoutSeconds 超时时间（秒）
     * @return 构建好的 HTTP 请求
     */
    public static HttpRequest buildCallbackRequest(Intent intent, int timeoutSeconds) {
        // 构建请求体
        HttpRequest.BodyPublisher bodyPublisher = buildBodyPublisher(intent);

        // 构建请求
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(intent.getCallback().getUrl()))
            .timeout(Duration.ofSeconds(timeoutSeconds))
            .header("Content-Type", "application/json")
            .header("X-LoomQ-Intent-Id", intent.getIntentId());

        // 添加自定义请求头
        Map<String, String> headers = intent.getCallback().getHeaders();
        if (headers != null && !headers.isEmpty()) {
            headers.forEach(builder::header);
        }

        // 设置请求方法
        setRequestMethod(builder, intent.getCallback().getMethod(), bodyPublisher);

        return builder.build();
    }

    /**
     * 构建请求体发布器
     */
    private static HttpRequest.BodyPublisher buildBodyPublisher(Intent intent) {
        Object body = intent.getCallback().getBody();
        return body != null
            ? HttpRequest.BodyPublishers.ofString(body.toString())
            : HttpRequest.BodyPublishers.noBody();
    }

    /**
     * 设置请求方法
     */
    private static void setRequestMethod(
            HttpRequest.Builder builder,
            String method,
            HttpRequest.BodyPublisher bodyPublisher) {

        if (method == null || "POST".equalsIgnoreCase(method)) {
            builder.POST(bodyPublisher);
        } else if ("PUT".equalsIgnoreCase(method)) {
            builder.PUT(bodyPublisher);
        } else if ("PATCH".equalsIgnoreCase(method)) {
            builder.method("PATCH", bodyPublisher);
        } else {
            // 默认使用 POST
            builder.POST(bodyPublisher);
        }
    }
}

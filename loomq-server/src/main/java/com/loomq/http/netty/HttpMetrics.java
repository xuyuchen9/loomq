package com.loomq.http.netty;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;

/**
 * HTTP 层指标收集
 */
public class HttpMetrics {

    private static final HttpMetrics INSTANCE = new HttpMetrics();

    // 请求计数
    private final Counter requestsTotal = Counter.build()
        .name("loomq_http_requests_total")
        .help("Total HTTP requests")
        .register();

    // 请求延迟直方图
    private final Histogram requestDuration = Histogram.build()
        .name("loomq_http_request_duration_seconds")
        .help("HTTP request duration in seconds")
        .buckets(0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0)
        .register();

    // 活跃请求数
    private final Gauge activeRequests = Gauge.build()
        .name("loomq_http_active_requests")
        .help("Currently active HTTP requests")
        .register();

    // 限流拒绝次数
    private final Counter limitExceededTotal = Counter.build()
        .name("loomq_http_concurrency_limit_exceeded_total")
        .help("Total requests rejected due to concurrency limit")
        .register();

    // 活跃连接数
    private final Gauge activeConnections = Gauge.build()
        .name("loomq_netty_active_connections")
        .help("Active HTTP connections")
        .register();

    // 连接错误数
    private final Counter connectionErrorsTotal = Counter.build()
        .name("loomq_netty_connection_errors_total")
        .help("Total connection errors")
        .register();

    private HttpMetrics() {}

    public static HttpMetrics getInstance() {
        return INSTANCE;
    }

    public void recordRequest(long durationNanos, int statusCode) {
        requestsTotal.inc();
        requestDuration.observe(durationNanos / 1_000_000_000.0);
    }

    public void recordLimitExceeded() {
        limitExceededTotal.inc();
    }

    public void incrementActiveRequests() {
        activeRequests.inc();
    }

    public void decrementActiveRequests() {
        activeRequests.dec();
    }

    public void setActiveConnections(int count) {
        activeConnections.set(count);
    }

    public void incrementConnectionErrors() {
        connectionErrorsTotal.inc();
    }

    // Getters for Prometheus scraping
    public Counter getRequestsTotal() {
        return requestsTotal;
    }

    public Histogram getRequestDuration() {
        return requestDuration;
    }

    public Gauge getActiveRequests() {
        return activeRequests;
    }

    public Counter getLimitExceededTotal() {
        return limitExceededTotal;
    }

    public Gauge getActiveConnections() {
        return activeConnections;
    }

    public Counter getConnectionErrorsTotal() {
        return connectionErrorsTotal;
    }
}

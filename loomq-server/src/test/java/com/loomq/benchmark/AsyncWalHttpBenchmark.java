package com.loomq.benchmark;

import com.loomq.benchmark.framework.ProtocolBenchmark;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * P1: ASYNC WAL 模式下的 HTTP 创建路径性能测试。
 *
 * 与标准 HttpVirtualThreadBenchmark 对比，唯一变量是 walMode=ASYNC。
 * 预期：ASYNC 模式下 HTTP QPS 应显著高于 DURABLE 模式的 ~5,899 QPS，
 * 因为不再受 fsync 162µs/record 的瓶颈限制。
 */
public class AsyncWalHttpBenchmark extends ProtocolBenchmark {

    private static final String BASE_URL = System.getProperty("loomq.benchmark.baseUrl", "http://localhost:7928");
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);
    private static final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .version(HttpClient.Version.HTTP_1_1)
        .build();

    public AsyncWalHttpBenchmark() {
        super("HTTP / JSON (ASYNC WAL)", BASE_URL);
    }

    @Override
    protected void createIntent() throws Exception {
        Instant executeAt = Instant.now().plus(3600, ChronoUnit.SECONDS);
        Instant deadline = executeAt.plus(5, ChronoUnit.MINUTES);
        String id = UUID.randomUUID().toString();
        String shardKey = "bench-" + ThreadLocalRandom.current().nextInt(64);

        // 关键区别：walMode=ASYNC
        String body = String.format(
            "{\"intentId\":\"%s\",\"executeAt\":\"%s\",\"deadline\":\"%s\","
                + "\"precisionTier\":\"STANDARD\",\"walMode\":\"ASYNC\","
                + "\"shardKey\":\"%s\",\"callback\":{\"url\":\"http://localhost:9999/webhook\"}}",
            id, executeAt.toString(), deadline.toString(), shardKey);

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + "/v1/intents"))
            .header("Content-Type", "application/json")
            .timeout(REQUEST_TIMEOUT)
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 400) {
            throw new RuntimeException("HTTP error: " + response.statusCode());
        }
    }

    public static void main(String[] args) throws Exception {
        new AsyncWalHttpBenchmark().run(args);
    }
}

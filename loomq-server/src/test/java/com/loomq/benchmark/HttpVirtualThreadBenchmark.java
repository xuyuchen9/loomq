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
 * HTTP 层性能测试。
 *
 * <p>只报告真实观察值，不给硬编码目标。
 */
public class HttpVirtualThreadBenchmark extends ProtocolBenchmark {

    private static final String BASE_URL = System.getProperty("loomq.benchmark.baseUrl", "http://localhost:8080");
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);
    private static final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .version(HttpClient.Version.HTTP_1_1)
        .build();

    public HttpVirtualThreadBenchmark() {
        super("HTTP / JSON", BASE_URL);
    }

    @Override
    protected void createIntent() throws Exception {
        Instant executeAt = Instant.now().plus(3600, ChronoUnit.SECONDS);
        Instant deadline = executeAt.plus(5, ChronoUnit.MINUTES);
        String id = UUID.randomUUID().toString();
        String shardKey = "bench-" + ThreadLocalRandom.current().nextInt(64);

        String body = String.format(
            "{\"intentId\":\"%s\",\"executeAt\":\"%s\",\"deadline\":\"%s\",\"precisionTier\":\"STANDARD\",\"shardKey\":\"%s\",\"callback\":{\"url\":\"http://localhost:9999/webhook\"}}",
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
        new HttpVirtualThreadBenchmark().run(args);
    }
}

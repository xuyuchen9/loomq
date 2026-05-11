package com.loomq.replication;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled("Requires pre-built fat JAR and ingress ports; use benchmark.ps1 locally")
@Tag("integration")
public class ReplicationIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationIntegrationTest.class);
    private static final int PRIMARY_PORT = 17928;
    private static final int REPLICA_PORT = 17929;
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(30);

    private Path dataDir;
    private Process primaryProcess;
    private Process replicaProcess;
    private HttpClient httpClient;

    @BeforeEach
    void setUp() throws Exception {
        dataDir = Files.createTempDirectory("loomq-it-");
        httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @AfterEach
    void tearDown() {
        if (primaryProcess != null) primaryProcess.destroyForcibly();
        if (replicaProcess != null) replicaProcess.destroyForcibly();
        try {
            Files.walk(dataDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        } catch (IOException ignored) {}
    }

    @Test
    void shouldReplicateIntentFromPrimaryToReplica() throws Exception {
        replicaProcess = startReplica();
        waitForHealthy(REPLICA_PORT, STARTUP_TIMEOUT);
        primaryProcess = startPrimary();
        waitForHealthy(PRIMARY_PORT, STARTUP_TIMEOUT);
        String intentId = createIntent(PRIMARY_PORT);
        logger.info("Created intent {} on primary", intentId);
        Thread.sleep(2000);
        assertTrue(queryIntent(REPLICA_PORT, intentId), "Intent should be replicated to replica");
    }

    @Test
    void shouldPromoteReplicaAfterPrimaryFailure() throws Exception {
        replicaProcess = startReplica();
        waitForHealthy(REPLICA_PORT, STARTUP_TIMEOUT);
        primaryProcess = startPrimary();
        waitForHealthy(PRIMARY_PORT, STARTUP_TIMEOUT);
        String intentId = createIntent(PRIMARY_PORT);
        Thread.sleep(2000);
        primaryProcess.destroyForcibly();
        primaryProcess.waitFor(10, TimeUnit.SECONDS);
        Thread.sleep(3000);
        assertTrue(isHealthy(REPLICA_PORT), "Replica should remain healthy after primary death");
        assertTrue(queryIntent(REPLICA_PORT, intentId), "Replicated intent should survive primary crash");
    }

    private Process startPrimary() throws IOException {
        Path primaryData = dataDir.resolve("primary");
        Files.createDirectories(primaryData);
        String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        ProcessBuilder pb = new ProcessBuilder(java,
            "-DLOOMQ_CLUSTER_ENABLED=true", "-DLOOMQ_ROLE=primary",
            "-DLOOMQ_REPLICA_HOST=localhost", "-DLOOMQ_REPLICA_PORT=" + REPLICA_PORT,
            "-DLOOMQ_NODE_ID=primary-1", "-DLOOMQ_DATA_DIR=" + primaryData,
            "-Dloomq.server.port=" + PRIMARY_PORT, "-Dloomq.server.nettyPort=" + PRIMARY_PORT,
            "-cp", System.getProperty("java.class.path"),
            "com.loomq.server.LoomqServerApplication");
        pb.directory(new File(System.getProperty("user.dir")));
        pb.redirectErrorStream(true);
        pb.redirectOutput(dataDir.resolve("primary.log").toFile());
        Process p = pb.start();
        logger.info("Started primary (PID={})", p.pid());
        return p;
    }

    private Process startReplica() throws IOException {
        Path replicaData = dataDir.resolve("replica");
        Files.createDirectories(replicaData);
        String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        ProcessBuilder pb = new ProcessBuilder(java,
            "-DLOOMQ_CLUSTER_ENABLED=true", "-DLOOMQ_ROLE=replica",
            "-DLOOMQ_REPLICA_HOST=localhost", "-DLOOMQ_REPLICA_PORT=" + REPLICA_PORT,
            "-DLOOMQ_NODE_ID=replica-1", "-DLOOMQ_DATA_DIR=" + replicaData,
            "-Dloomq.server.port=" + REPLICA_PORT, "-Dloomq.server.nettyPort=" + REPLICA_PORT,
            "-cp", System.getProperty("java.class.path"),
            "com.loomq.server.LoomqServerApplication");
        pb.directory(new File(System.getProperty("user.dir")));
        pb.redirectErrorStream(true);
        pb.redirectOutput(dataDir.resolve("replica.log").toFile());
        Process p = pb.start();
        logger.info("Started replica (PID={})", p.pid());
        return p;
    }

    private void waitForHealthy(int port, Duration timeout) throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (isHealthy(port)) return;
            Thread.sleep(500);
        }
        throw new RuntimeException("Server on port " + port + " did not become healthy within " + timeout);
    }

    private boolean isHealthy(int port) {
        try {
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create("http://localhost:" + port + "/health"))
                .GET().timeout(Duration.ofSeconds(2)).build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200;
        } catch (Exception e) { return false; }
    }

    private String createIntent(int port) throws Exception {
        String json = String.format("{\"executeAt\":\"%s\",\"precisionTier\":\"STANDARD\"}",
            Instant.now().plusSeconds(30).toString());
        HttpRequest req = HttpRequest.newBuilder().uri(URI.create("http://localhost:" + port + "/v1/intents"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(json)).timeout(Duration.ofSeconds(5)).build();
        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(), "Create intent failed: " + resp.body());
        String body = resp.body();
        int idStart = body.indexOf("\"intentId\":\"") + 12;
        return body.substring(idStart, body.indexOf("\"", idStart));
    }

    private boolean queryIntent(int port, String intentId) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/v1/intents/" + intentId))
            .GET().timeout(Duration.ofSeconds(5)).build();
        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        return resp.statusCode() == 200;
    }
}

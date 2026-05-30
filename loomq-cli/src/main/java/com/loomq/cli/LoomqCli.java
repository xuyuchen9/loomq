package com.loomq.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * LoomQ interactive shell — temporal explorer.
 *
 * Commands:
 *   schedule --at <ISO> [--slo <ms>]
 *   get <intentId>
 *   list [--status <status>]
 *   chronoscope
 *   timeline [--from <ISO>] [--to <ISO>]
 *   dead-letters
 *   revive <intentId> [--at <ISO>]
 *   health
 *   follow <intentId>
 *   help
 *   exit
 */
public final class LoomqCli {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule());

    private static final String ANSI_RESET = "[0m";
    private static final String ANSI_BOLD = "[1m";
    private static final String ANSI_GREEN = "[32m";
    private static final String ANSI_YELLOW = "[33m";
    private static final String ANSI_CYAN = "[36m";
    private static final String ANSI_RED = "[31m";

    private final HttpClient httpClient;
    private final String baseUrl;

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("LOOMQ_URL", "http://localhost:8080");
        new LoomqCli(url).run(args);
    }

    public LoomqCli(String baseUrl) {
        this.baseUrl = baseUrl;
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    }

    void run(String[] args) {
        if (args.length > 0) {
            executeCommand(String.join(" ", args));
            return;
        }

        printBanner();
        System.out.println(ANSI_CYAN + "Type 'help' for commands, 'exit' to quit." + ANSI_RESET);
        System.out.println("Connected to: " + baseUrl);

        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                System.out.print(ANSI_BOLD + "loomq> " + ANSI_RESET);
                if (!scanner.hasNextLine()) break;
                String line = scanner.nextLine().trim();
                if (line.isEmpty()) continue;
                if ("exit".equalsIgnoreCase(line) || "quit".equalsIgnoreCase(line)) {
                    System.out.println("Goodbye.");
                    break;
                }
                executeCommand(line);
            }
        }
    }

    void executeCommand(String line) {
        try {
            String[] parts = line.split("\\s+");
            String cmd = parts[0].toLowerCase();
            switch (cmd) {
                case "help" -> printHelp();
                case "schedule" -> cmdSchedule(parts);
                case "get" -> cmdGet(parts);
                case "list" -> cmdList(parts);
                case "chronoscope" -> cmdChronoscope();
                case "timeline" -> cmdTimeline(parts);
                case "dead-letters" -> cmdDeadLetters(parts);
                case "revive" -> cmdRevive(parts);
                case "health" -> cmdHealth();
                case "follow" -> cmdFollow(parts);
                default -> System.out.println(ANSI_RED + "Unknown command: " + cmd + ANSI_RESET);
            }
        } catch (Exception e) {
            System.out.println(ANSI_RED + "Error: " + e.getMessage() + ANSI_RESET);
        }
    }

    // ── schedule ──
    private void cmdSchedule(String[] parts) throws Exception {
        String at = null;
        String slo = null;
        for (int i = 1; i < parts.length; i++) {
            if ("--at".equals(parts[i]) && i + 1 < parts.length) at = parts[++i];
            else if ("--slo".equals(parts[i]) && i + 1 < parts.length) slo = parts[++i];
        }
        if (at == null) {
            System.out.println(ANSI_RED + "Usage: schedule --at <ISO-timestamp> [--slo <ms>]" + ANSI_RESET);
            return;
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("executeAt", at);
        body.put("deadline", Instant.parse(at).plusSeconds(3600).toString());
        body.put("shardKey", "default");
        if (slo != null) {
            body.put("slo", Map.of("maxTardinessMs", Long.parseLong(slo), "reliability", "AT_LEAST_ONCE"));
        }

        String json = post("/v1/intents", body);
        @SuppressWarnings("unchecked")
        Map<String, Object> result = MAPPER.readValue(json, Map.class);
        System.out.println(ANSI_GREEN + "Created " + result.get("intentId")
            + " [" + result.get("status") + "]" + ANSI_RESET);
        if (result.containsKey("precisionTier")) {
            System.out.println("  Tier: " + result.get("precisionTier"));
        }
    }

    // ── get ──
    private void cmdGet(String[] parts) throws Exception {
        if (parts.length < 2) {
            System.out.println(ANSI_RED + "Usage: get <intentId>" + ANSI_RESET);
            return;
        }
        String json = get("/v1/intents/" + parts[1]);
        prettyPrint(json);
    }

    // ── list ──
    private void cmdList(String[] parts) throws Exception {
        String status = "SCHEDULED";
        for (int i = 1; i < parts.length; i++) {
            if ("--status".equals(parts[i]) && i + 1 < parts.length) status = parts[++i];
        }
        String json = get("/v1/intents?status=" + status);
        prettyPrint(json);
    }

    // ── chronoscope ──
    private void cmdChronoscope() throws Exception {
        String json = get("/v1/system/chronoscope");
        @SuppressWarnings("unchecked")
        Map<String, Object> result = MAPPER.readValue(json, Map.class);
        System.out.println(ANSI_BOLD + "TIER       PENDING  PERMITS    QUEUE  UTIL%   PRESSURE" + ANSI_RESET);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> tiers = (Map<String, Map<String, Object>>) result.get("tiers");
        if (tiers != null) {
            for (var entry : tiers.entrySet()) {
                var t = entry.getValue();
                var sem = (Map<String, Object>) t.get("semaphore");
                String permits = sem != null ? sem.get("available") + "/" + sem.get("max") : "-";
                String pressure = Boolean.TRUE.equals(t.get("underBackpressure")) ? ANSI_RED + "YES" + ANSI_RESET : "OK";
                System.out.printf("%-10s %-8s %-10s %-7s %-6s %s%n",
                    entry.getKey(),
                    t.get("pendingCount"),
                    permits,
                    t.get("dispatchQueueSize"),
                    t.get("utilizationPct"),
                    pressure);
            }
        }
        Object cc = result.get("cohortCount");
        Object cp = result.get("cohortPendingIntents");
        if (cc != null || cp != null) {
            System.out.println("Total cohorts: " + cc + " | Pending intents in cohorts: " + cp);
        }
    }

    // ── timeline ──
    private void cmdTimeline(String[] parts) throws Exception {
        Instant from = Instant.now();
        Instant to = from.plusSeconds(1800);
        for (int i = 1; i < parts.length; i++) {
            if ("--from".equals(parts[i]) && i + 1 < parts.length) from = Instant.parse(parts[++i]);
            else if ("--to".equals(parts[i]) && i + 1 < parts.length) to = Instant.parse(parts[++i]);
        }
        String json = get("/v1/system/timeline?from=" + from + "&to=" + to);
        prettyPrint(json);
    }

    // ── dead-letters ──
    private void cmdDeadLetters(String[] parts) throws Exception {
        String json = get("/v1/intents?status=DEAD_LETTERED&limit=50");
        @SuppressWarnings("unchecked")
        Map<String, Object> result = MAPPER.readValue(json, Map.class);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> intents = (List<Map<String, Object>>) result.get("intents");
        if (intents == null || intents.isEmpty()) {
            System.out.println(ANSI_GREEN + "No dead-lettered intents." + ANSI_RESET);
            return;
        }
        System.out.printf("%s%d dead-lettered intents%s (total: %s)%n",
            ANSI_BOLD, intents.size(), ANSI_RESET, result.get("total"));
        for (var intent : intents) {
            System.out.printf("  %s [%s] tier=%s attempts=%s%n",
                intent.get("intentId"), intent.get("status"),
                intent.get("tier"), intent.get("attempts"));
            var death = (Map<String, Object>) intent.get("deathReport");
            if (death != null) {
                System.out.printf("    failure: %s (HTTP %s), lifetime: %sms%n",
                    death.get("failureReason"), death.get("failureHttpStatus"),
                    death.get("lifetimeMs"));
            }
        }
    }

    // ── revive ──
    private void cmdRevive(String[] parts) throws Exception {
        if (parts.length < 2) {
            System.out.println(ANSI_RED + "Usage: revive <intentId> [--at <ISO>]" + ANSI_RESET);
            return;
        }
        String intentId = parts[1];
        Map<String, Object> body = new LinkedHashMap<>();
        for (int i = 2; i < parts.length; i++) {
            if ("--at".equals(parts[i]) && i + 1 < parts.length) {
                body.put("executeAt", parts[++i]);
            }
        }
        String json = post("/v1/intents/" + intentId + "/revive", body.isEmpty() ? null : body);
        prettyPrint(json);
        System.out.println(ANSI_GREEN + "Revived " + intentId + ANSI_RESET);
    }

    // ── health ──
    private void cmdHealth() throws Exception {
        String json = get("/health");
        prettyPrint(json);
    }

    // ── follow ──
    private void cmdFollow(String[] parts) throws Exception {
        if (parts.length < 2) {
            System.out.println(ANSI_RED + "Usage: follow <intentId>" + ANSI_RESET);
            return;
        }
        String intentId = parts[1];
        String lastStatus = null;
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 60_000) {
            String json = get("/v1/intents/" + intentId);
            @SuppressWarnings("unchecked")
            Map<String, Object> intent = MAPPER.readValue(json, Map.class);
            String status = (String) intent.get("status");
            if (!status.equals(lastStatus)) {
                String arrow = lastStatus != null ? lastStatus + " → " + status : status;
                System.out.printf("[%s] %s%n", intentId, ANSI_CYAN + arrow + ANSI_RESET);
                lastStatus = status;
            }
            if (isTerminal(status)) break;
            Thread.sleep(200);
        }
    }

    // ── help ──
    private void printHelp() {
        System.out.println(ANSI_BOLD + "LoomQ Shell Commands:" + ANSI_RESET);
        System.out.println("  schedule --at <ISO> [--slo <ms>]   Create an intent");
        System.out.println("  get <intentId>                       Get intent details");
        System.out.println("  list [--status <status>]             List intents by status");
        System.out.println("  chronoscope                          Scheduler X-ray snapshot");
        System.out.println("  timeline [--from <ISO>] [--to <ISO>] Temporal forecast (default 30min)");
        System.out.println("  dead-letters                         List failed intents with death reports");
        System.out.println("  revive <intentId> [--at <ISO>]       Retry a dead-lettered intent");
        System.out.println("  health                               System health narrative");
        System.out.println("  follow <intentId>                    Watch intent lifecycle in real-time");
        System.out.println("  help                                 Show this help");
        System.out.println("  exit                                 Quit the shell");
    }

    // ── HTTP helpers ──
    String get(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create(baseUrl + path))
            .timeout(Duration.ofSeconds(10))
            .GET()
            .build();
        HttpResponse<String> resp = httpClient.send(req, BodyHandlers.ofString());
        if (resp.statusCode() >= 400) {
            System.out.println(ANSI_YELLOW + "HTTP " + resp.statusCode() + ANSI_RESET);
        }
        return resp.body();
    }

    String post(String path, Map<String, Object> body) throws Exception {
        String json = body != null ? MAPPER.writeValueAsString(body) : "";
        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create(baseUrl + path))
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/json")
            .POST(BodyPublishers.ofString(json))
            .build();
        HttpResponse<String> resp = httpClient.send(req, BodyHandlers.ofString());
        if (resp.statusCode() >= 400) {
            System.out.println(ANSI_RED + "HTTP " + resp.statusCode() + ANSI_RESET);
            prettyPrint(resp.body());
        }
        return resp.body();
    }

    void prettyPrint(String json) throws Exception {
        Object obj = MAPPER.readValue(json, Object.class);
        System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj));
    }

    private static boolean isTerminal(String status) {
        return List.of("ACKED", "CANCELED", "EXPIRED", "DEAD_LETTERED").contains(status);
    }

    private void printBanner() {
        System.out.println(ANSI_BOLD + ANSI_GREEN);
        System.out.println("  ██╗      ██████╗  ██████╗ ███╗   ███╗ ██████╗ ");
        System.out.println("  ██║     ██╔═══██╗██╔═══██╗████╗ ████║██╔═══██╗");
        System.out.println("  ██║     ██║   ██║██║   ██║██╔████╔██║██║   ██║");
        System.out.println("  ██║     ██║   ██║██║   ██║██║╚██╔╝██║██║▄▄ ██║");
        System.out.println("  ███████╗╚██████╔╝╚██████╔╝██║ ╚═╝ ██║╚██████╔╝");
        System.out.println("  ╚══════╝ ╚═════╝  ╚═════╝ ╚═╝     ╚═╝ ╚══▀▀═╝ ");
        System.out.println(ANSI_RESET);
        System.out.println("  Interactive Temporal Explorer  v0.9.1");
        System.out.println();
    }
}

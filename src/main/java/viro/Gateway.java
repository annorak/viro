package viro;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class Gateway {
    private final List<String> workers = Arrays.stream(Main.env("WORKER_URLS", "http://viro-worker-0.viro-worker:8080,http://viro-worker-1.viro-worker:8080,http://viro-worker-2.viro-worker:8080").split(",")).map(String::trim).toList();
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final AtomicLong queries = new AtomicLong(), queryNanos = new AtomicLong(), failures = new AtomicLong();
    public void start() throws Exception {
        HttpServer server = Main.server();
        Main.route(server, "/health", "GET", e -> { fanout("/health", "GET"); Main.send(e, 200, Map.of("ready", true, "workers", workers.size())); });
        Main.route(server, "/api/query", "GET", e -> {
            long start = System.nanoTime();
            Map<String,String> filters = Main.filters(e.getRequestURI().getRawQuery());
            try { Main.send(e, 200, merge(fanout("/api/query" + (e.getRequestURI().getRawQuery() == null ? "" : "?" + e.getRequestURI().getRawQuery()), "GET"), filters.getOrDefault("mode", "database"))); }
            catch (Exception error) { failures.incrementAndGet(); throw error; }
            finally { queries.incrementAndGet(); queryNanos.addAndGet(System.nanoTime() - start); }
        });
        Main.route(server, "/api/status", "GET", e -> {
            List<JsonNode> statuses = fanout("/api/status", "GET");
            long rows = statuses.stream().mapToLong(s -> s.path("rows").asLong()).sum();
            Main.send(e, 200, Map.of("workers", statuses, "rows", rows));
        });
        Main.route(server, "/api/flush", "POST", e -> Main.send(e, 200, Map.of("workers", fanout("/api/flush", "POST"))));
        Main.route(server, "/metrics", "GET", e -> Main.bytes(e, 200, "text/plain", ("viro_gateway_query_total " + queries.get() + "\nviro_gateway_query_errors_total " + failures.get() + "\nviro_gateway_query_duration_seconds_sum " + queryNanos.get()/1e9 + "\nviro_gateway_query_duration_seconds_count " + queries.get() + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        Main.route(server, "/", "GET", e -> {
            try (var resource = Gateway.class.getResourceAsStream("/index.html")) {
                if (resource == null) throw new IllegalStateException("Missing UI resource");
                Main.bytes(e, 200, "text/html", resource.readAllBytes());
            }
        });
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.stop(1)));
    }
    private List<JsonNode> fanout(String path, String method) throws Exception {
        List<CompletableFuture<HttpResponse<String>>> pending = new ArrayList<>();
        for (String worker : workers) {
            HttpRequest request = HttpRequest.newBuilder(URI.create(worker + path)).timeout(Duration.ofSeconds(5)).method(method, HttpRequest.BodyPublishers.noBody()).build();
            pending.add(client.sendAsync(request, HttpResponse.BodyHandlers.ofString()));
        }
        try {
            CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new)).get(6, TimeUnit.SECONDS);
            List<JsonNode> responses = new ArrayList<>();
            for (var future : pending) {
                var response = future.join();
                if (response.statusCode() != 200) throw new IllegalStateException("Required worker returned " + response.statusCode());
                responses.add(Main.JSON.readTree(response.body()));
            }
            return responses;
        } finally { for (var future : pending) if (!future.isDone()) future.cancel(true); }
    }
    static Map<String,Object> merge(List<JsonNode> responses, String mode) {
        record Group(String state, String pathogen, String activity) implements Comparable<Group> {
            public int compareTo(Group other) { int compared = state.compareTo(other.state); if (compared == 0) compared = pathogen.compareTo(other.pathogen); return compared == 0 ? activity.compareTo(other.activity) : compared; }
        }
        Map<Group,Long> grouped = new TreeMap<>();
        for (JsonNode response : responses) for (JsonNode row : response.path("rows")) {
            Group key = new Group(row.path("state").asText(), row.path("pathogen").asText(), row.path("activity").asText());
            grouped.merge(key, row.path("count").asLong(), Long::sum);
        }
        List<Map<String,Object>> rows = new ArrayList<>(); long total = 0;
        for (var entry : grouped.entrySet()) { Group key = entry.getKey(); total += entry.getValue(); rows.add(Map.of("state", key.state, "pathogen", key.pathogen, "activity", key.activity, "count", entry.getValue())); }
        return Map.of("rows", rows, "total", total, "mode", mode, "workers", responses.size());
    }
}

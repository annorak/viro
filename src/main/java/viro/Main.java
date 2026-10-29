package viro;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class Main {
    static final ObjectMapper JSON = new ObjectMapper();
    static final Set<String> FILTERS = Set.of("state", "site", "pathogen", "week_from", "week_to", "activity", "mode");
    public static void main(String[] args) throws Exception {
        switch (env("VIRO_ROLE", "gateway")) {
            case "producer" -> Producer.main(args);
            case "worker" -> new Worker().start();
            case "gateway" -> new Gateway().start();
            default -> throw new IllegalArgumentException("VIRO_ROLE must be worker, gateway, or producer");
        }
    }
    static String env(String name, String fallback) { return System.getenv().getOrDefault(name, fallback); }
    static HttpServer server() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(Integer.parseInt(env("PORT", "8080"))), 64);
        server.setExecutor(new ThreadPoolExecutor(8, 8, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(128), new ThreadPoolExecutor.CallerRunsPolicy()));
        return server;
    }
    static Map<String,String> filters(String query) {
        Map<String,String> values = new LinkedHashMap<>();
        if (query == null || query.isBlank()) return values;
        for (String part : query.split("&")) {
            String[] pair = part.split("=", 2);
            String key = URLDecoder.decode(pair[0], StandardCharsets.UTF_8);
            String value = pair.length == 2 ? URLDecoder.decode(pair[1], StandardCharsets.UTF_8).trim() : "";
            if (!FILTERS.contains(key)) throw new IllegalArgumentException("Unknown filter: " + key);
            if (value.length() > 256) throw new IllegalArgumentException("Filter too long");
            if (!value.isEmpty() && values.putIfAbsent(key, value) != null) throw new IllegalArgumentException("Duplicate filter: " + key);
        }
        if (!Set.of("database", "parquet").contains(values.getOrDefault("mode", "database"))) throw new IllegalArgumentException("mode must be database or parquet");
        for (String date : Set.of("week_from", "week_to")) if (values.containsKey(date)) {
            try { java.time.LocalDate.parse(values.get(date)); }
            catch (java.time.DateTimeException e) { throw new IllegalArgumentException(date + " must use YYYY-MM-DD"); }
        }
        if (values.containsKey("week_from") && values.containsKey("week_to") && values.get("week_from").compareTo(values.get("week_to")) > 0) throw new IllegalArgumentException("week_from must precede week_to");
        return values;
    }
    static void send(HttpExchange exchange, int status, Object body) throws IOException { bytes(exchange, status, "application/json", JSON.writeValueAsBytes(body)); }
    static void bytes(HttpExchange exchange, int status, String type, byte[] bytes) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", type + "; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (var out = exchange.getResponseBody()) { out.write(bytes); }
    }
    @FunctionalInterface interface Route { void handle(HttpExchange exchange) throws Exception; }
    static void route(HttpServer server, String path, String method, Route route) {
        server.createContext(path, exchange -> {
            try {
                if (!exchange.getRequestURI().getPath().equals(path)) { send(exchange, 404, Map.of("error", "Not found")); return; }
                if (!exchange.getRequestMethod().equals(method)) { send(exchange, 405, Map.of("error", "Method not allowed")); return; }
                route.handle(exchange);
            } catch (IllegalArgumentException e) { send(exchange, 400, Map.of("error", e.getMessage() == null ? "Invalid request" : e.getMessage())); }
            catch (Exception e) { System.err.println("Request failed: " + path); e.printStackTrace(); send(exchange, 503, Map.of("error", "Service unavailable", "detail", "A required worker or storage operation failed")); }
            finally { exchange.close(); }
        });
    }
}

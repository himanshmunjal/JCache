package com.cache.server.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * Serves {@code GET /metrics} in the Prometheus text format on its own HTTP
 * port, so monitoring traffic never shares the cache port. Uses the JDK's
 * built-in HTTP server to avoid a new dependency.
 */
public final class PrometheusEndpoint {

    private static final String CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

    private final HttpServer server;

    /**
     * Binds the port and starts serving.
     *
     * @param port port to listen on, 0 for any free port
     * @param body produces the response body on each scrape
     * @throws UncheckedIOException if the port cannot be bound
     */
    public PrometheusEndpoint(int port, Supplier<String> body) {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot bind metrics port " + port, e);
        }
        server.createContext("/metrics", exchange -> handle(exchange, body));
        server.start();
    }

    /** @return the bound port */
    public int getPort() {
        return server.getAddress().getPort();
    }

    /** Stops the HTTP server. */
    public void stop() {
        server.stop(0);
    }

    /**
     * Formats the metrics in the Prometheus text exposition format. Rates such
     * as hit rate and ops/sec are left out; Prometheus derives them from the
     * counters with {@code rate()}.
     *
     * @param s         a metrics snapshot
     * @param evictions eviction count reported by the cache
     * @param size      current number of entries
     * @param capacity  configured maximum number of entries
     * @return the response body
     */
    public static String format(ServerMetrics.MetricsSnapshot s, long evictions, int size, int capacity) {
        StringBuilder out = new StringBuilder(2048);
        header(out, "jcache_commands_total", "counter", "Cache commands handled, by command.");
        sample(out, "jcache_commands_total{command=\"get\"}", s.totalGets);
        sample(out, "jcache_commands_total{command=\"put\"}", s.totalPuts);
        sample(out, "jcache_commands_total{command=\"delete\"}", s.totalDeletes);
        counter(out, "jcache_hits_total", "GETs that found a value.", s.hits);
        counter(out, "jcache_misses_total", "GETs that found nothing.", s.misses);
        counter(out, "jcache_evictions_total", "Entries evicted by the policy.", evictions);
        counter(out, "jcache_errors_total", "Requests that failed.", s.errors);
        counter(out, "jcache_rate_limited_total", "Commands rejected by the rate limit.", s.rateLimited);
        counter(out, "jcache_connections_total", "Connections accepted since start-up.", s.totalConnections);
        gauge(out, "jcache_entries", "Current number of entries.", size);
        gauge(out, "jcache_capacity", "Maximum number of entries.", capacity);
        gauge(out, "jcache_connections_active", "Currently open connections.", s.activeConnections);
        gauge(out, "jcache_uptime_seconds", "Seconds since start-up.", s.uptimeMs / 1000.0);
        header(out, "jcache_latency_seconds", "gauge", "Cache latency over the last 1000 operations.");
        sample(out, "jcache_latency_seconds{quantile=\"0.5\"}", s.p50Ms / 1000.0);
        sample(out, "jcache_latency_seconds{quantile=\"0.99\"}", s.p99Ms / 1000.0);
        return out.toString();
    }

    private static void handle(HttpExchange exchange, Supplier<String> body) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            if (!"GET".equals(method) && !"HEAD".equals(method)) {
                exchange.getResponseHeaders().set("Allow", "GET, HEAD");
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            if (!"/metrics".equals(exchange.getRequestURI().getPath())) {
                exchange.sendResponseHeaders(404, -1);
                return;
            }
            byte[] bytes = body.get().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", CONTENT_TYPE);
            if ("HEAD".equals(method)) {
                exchange.sendResponseHeaders(200, -1);
                return;
            }
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }

    private static void counter(StringBuilder out, String name, String help, long value) {
        header(out, name, "counter", help);
        sample(out, name, value);
    }

    private static void gauge(StringBuilder out, String name, String help, long value) {
        header(out, name, "gauge", help);
        sample(out, name, value);
    }

    private static void gauge(StringBuilder out, String name, String help, double value) {
        header(out, name, "gauge", help);
        sample(out, name, value);
    }

    private static void header(StringBuilder out, String name, String type, String help) {
        out.append("# HELP ").append(name).append(' ').append(help).append('\n');
        out.append("# TYPE ").append(name).append(' ').append(type).append('\n');
    }

    private static void sample(StringBuilder out, String name, long value) {
        out.append(name).append(' ').append(value).append('\n');
    }

    private static void sample(StringBuilder out, String name, double value) {
        out.append(name).append(' ').append(String.format(Locale.ROOT, "%.6g", value)).append('\n');
    }
}

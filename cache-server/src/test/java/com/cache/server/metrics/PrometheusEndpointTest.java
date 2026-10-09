package com.cache.server.metrics;

import com.cache.client.CacheClient;
import com.cache.server.CacheServer;
import com.cache.server.ServerConfig;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

class PrometheusEndpointTest {

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @Test
    void formatsCountersAndGauges() {
        ServerMetrics metrics = new ServerMetrics();
        metrics.recordGet(true, 2_000_000);
        metrics.recordGet(false, 1_000_000);
        metrics.recordPut(1_000_000);
        metrics.recordError();
        metrics.connectionOpened();

        String body = PrometheusEndpoint.format(metrics.snapshot(), 7, 42, 100);

        assertTrue(body.contains("# TYPE jcache_hits_total counter\njcache_hits_total 1\n"), body);
        assertTrue(body.contains("jcache_misses_total 1\n"), body);
        assertTrue(body.contains("jcache_commands_total{command=\"get\"} 2\n"), body);
        assertTrue(body.contains("jcache_commands_total{command=\"put\"} 1\n"), body);
        assertTrue(body.contains("jcache_evictions_total 7\n"), body);
        assertTrue(body.contains("jcache_errors_total 1\n"), body);
        assertTrue(body.contains("# TYPE jcache_entries gauge\njcache_entries 42\n"), body);
        assertTrue(body.contains("jcache_capacity 100\n"), body);
        assertTrue(body.contains("jcache_connections_active 1\n"), body);
        assertTrue(body.contains("jcache_latency_seconds{quantile=\"0.99\"}"), body);
        for (String line : body.split("\n")) {
            assertTrue(line.startsWith("#") || line.matches("jcache_[a-z_]+(\\{[a-z]+=\"[a-z0-9.]+\"})? [0-9.e+-]+"),
                    "not valid exposition format: " + line);
        }
    }

    @Test
    void serverExposesLiveMetrics() throws Exception {
        CacheServer server = new CacheServer(ServerConfig.builder().port(0).metricsPort(0).build());
        server.startAsync();
        try (CacheClient client = new CacheClient("localhost", server.getPort())) {
            client.put("a", "1");
            client.get("a");
            client.get("missing");

            HttpResponse<String> response = get(server.getMetricsPort(), "/metrics", "GET");
            assertEquals(200, response.statusCode());
            assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("text/plain"));
            assertTrue(response.body().contains("jcache_hits_total 1\n"), response.body());
            assertTrue(response.body().contains("jcache_misses_total 1\n"), response.body());
            assertTrue(response.body().contains("jcache_entries 1\n"), response.body());

            assertEquals(404, get(server.getMetricsPort(), "/other", "GET").statusCode());
            assertEquals(405, get(server.getMetricsPort(), "/metrics", "POST").statusCode());
        } finally {
            server.shutdown();
        }
    }

    @Test
    void endpointIsOffByDefault() {
        CacheServer server = new CacheServer(ServerConfig.builder().port(0).build());
        server.startAsync();
        try {
            assertEquals(-1, server.getMetricsPort());
        } finally {
            server.shutdown();
        }
    }

    private static HttpResponse<String> get(int port, String path, String method) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .method(method, HttpRequest.BodyPublishers.noBody())
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }
}

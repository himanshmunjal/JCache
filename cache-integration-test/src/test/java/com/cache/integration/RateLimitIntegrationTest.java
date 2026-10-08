package com.cache.integration;

import com.cache.client.CacheClient;
import com.cache.client.CacheClient.CacheClientException;
import com.cache.server.CacheServer;
import com.cache.server.ServerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Rate limiting against a running server")
class RateLimitIntegrationTest {

    private static final int RATE = 5;
    private static final int BURST = 3;

    private static CacheServer server;

    @BeforeAll
    static void startServer() {
        server = new CacheServer(ServerConfig.builder()
                .port(0)
                .workerThreads(2)
                .rateLimitPerSecond(RATE)
                .rateLimitBurst(BURST)
                .build());
        server.startAsync();
    }

    @AfterAll
    static void stopServer() {
        if (server != null) {
            server.shutdown();
        }
    }

    @Test
    @DisplayName("Over-limit commands fail, the connection stays usable and recovers after a refill")
    void limitAndRecover() throws Exception {
        try (CacheClient client = new CacheClient("localhost", server.getPort())) {
            client.put("k", "v");
            assertEquals("v", client.get("k"));
            assertEquals("v", client.get("k"));

            CacheClientException e = assertThrows(CacheClientException.class, () -> client.get("k"));
            assertTrue(e.getMessage().contains("rate limit exceeded"), e.getMessage());
            assertTrue(client.ping(), "PING is not limited");

            Thread.sleep(2 * 1000L / RATE);
            assertEquals("v", client.get("k"));
        }
    }

    @Test
    @DisplayName("RESP connections are limited the same way")
    void respConnection() throws Exception {
        try (Jedis jedis = new Jedis("localhost", server.getPort())) {
            jedis.set("r", "v");
            assertEquals("v", jedis.get("r"));
            assertEquals("v", jedis.get("r"));

            JedisDataException e = assertThrows(JedisDataException.class, () -> jedis.get("r"));
            assertEquals("ERR rate limit exceeded", e.getMessage());
            assertEquals("PONG", jedis.ping(), "PING is not limited");

            Thread.sleep(2 * 1000L / RATE);
            assertEquals("v", jedis.get("r"));
        }
    }

    @Test
    @DisplayName("Each connection has its own limit, and STATS reports rejections")
    void perConnection() throws Exception {
        try (CacheClient first = new CacheClient("localhost", server.getPort());
             CacheClient second = new CacheClient("localhost", server.getPort())) {
            for (int i = 0; i < BURST; i++) {
                first.put("a" + i, "1");
            }
            assertThrows(CacheClientException.class, () -> first.put("a", "1"));

            Map<String, String> stats = second.stats();
            assertTrue(Long.parseLong(stats.get("rateLimited")) >= 1, stats.toString());
        }
    }
}

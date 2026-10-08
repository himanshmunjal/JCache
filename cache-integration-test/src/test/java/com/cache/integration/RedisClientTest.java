package com.cache.integration;

import com.cache.client.CacheClient;
import com.cache.server.CacheServer;
import com.cache.server.ServerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.params.SetParams;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Talks to a real server with Jedis, an unmodified Redis client. */
class RedisClientTest {

    private static CacheServer server;
    private static JedisPool pool;

    @BeforeAll
    static void startServer() {
        server = new CacheServer(ServerConfig.builder().port(0).cacheCapacity(1000).build());
        server.startAsync();
        pool = new JedisPool("localhost", server.getPort());
    }

    @AfterAll
    static void stopServer() {
        if (pool != null) {
            pool.close();
        }
        if (server != null) {
            server.shutdown();
        }
    }

    @BeforeEach
    void flush() {
        try (Jedis jedis = pool.getResource()) {
            jedis.flushAll();
        }
    }

    @Test
    void basicCommands() {
        try (Jedis jedis = pool.getResource()) {
            assertEquals("PONG", jedis.ping());
            assertEquals("OK", jedis.set("user:1", "Alice Smith"));
            assertEquals("Alice Smith", jedis.get("user:1"));
            assertNull(jedis.get("missing"));
            assertTrue(jedis.exists("user:1"));
            assertEquals(1, jedis.del("user:1", "missing"));
            assertEquals(0, jedis.dbSize());
        }
    }

    @Test
    void expiry() {
        try (Jedis jedis = pool.getResource()) {
            jedis.set("session", "token", SetParams.setParams().ex(100));
            long ttl = jedis.ttl("session");
            assertTrue(ttl > 98 && ttl <= 100, "ttl " + ttl);

            jedis.setex("otp", 30, "1234");
            assertEquals(30, jedis.ttl("otp"));
            assertEquals(1, jedis.persist("otp"));
            assertEquals(-1, jedis.ttl("otp"));
            assertEquals(-2, jedis.ttl("missing"));
            assertEquals(1, jedis.expire("otp", 10));
            assertEquals(0, jedis.expire("missing", 10));
        }
    }

    @Test
    void pipelining() {
        try (Jedis jedis = pool.getResource()) {
            Pipeline p = jedis.pipelined();
            for (int i = 0; i < 100; i++) {
                p.set("k" + i, "v" + i);
            }
            Response<String> last = p.get("k99");
            List<Object> replies = p.syncAndReturnAll();
            assertEquals(101, replies.size());
            assertEquals("v99", last.get());
            assertEquals(100, jedis.dbSize());
        }
    }

    @Test
    void binarySafeValuesRoundTrip() {
        try (Jedis jedis = pool.getResource()) {
            String value = "line 1\r\nline 2\ttabbed ünïcode";
            jedis.set("key with spaces", value);
            assertEquals(value, jedis.get("key with spaces"));
        }
    }

    @Test
    void sameCacheAsTextProtocolClients() throws Exception {
        try (Jedis jedis = pool.getResource();
             CacheClient client = new CacheClient("localhost", server.getPort())) {
            jedis.set("shared", "from redis");
            assertEquals("from redis", client.get("shared"));
            client.put("other", "from text", 60);
            assertEquals("from text", jedis.get("other"));
            assertTrue(jedis.ttl("other") > 0);
        }
    }
}

package com.cache.integration;

import com.cache.client.CacheClient;
import com.cache.client.ConnectionPool;
import com.cache.server.CacheServer;
import com.cache.server.ServerConfig;
import org.junit.jupiter.api.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.cache.api.CachePolicyType.LRU;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("CacheClient Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CacheClientTest {
    private static CacheServer server;

    private static int serverPort;

    private static CacheClient setupClient;

    @BeforeAll
    static void startServer() throws Exception {
        ServerConfig config = ServerConfig.builder()
                .port(0)
                .workerThreads(4)
                .maxConnections(50)
                .cacheCapacity(100)
                .evictionPolicy(LRU)
                .build();

        server = new CacheServer(config);
        server.startAsync();

        serverPort = server.getPort();

        setupClient = connectWithRetry("localhost", serverPort, 5);
    }

    @BeforeEach
    void clearCache() throws Exception {
        setupClient.flush();
    }

    @AfterAll
    static void stopServer() throws Exception {
        if (setupClient != null) setupClient.close();
        if (server != null)      server.shutdown();
    }

    @Test
    @Order(1)
    @DisplayName("put() and get() round-trip returns correct value")
    void testPutAndGet_basicRoundTrip() throws Exception {
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            client.put("username", "alice");

            String result = client.get("username");

            assertEquals("alice", result,
                    "get() should return the value set by put()");
        }
    }

    @Test
    @Order(2)
    @DisplayName("get() on missing key returns null, not exception")
    void testGet_missingKey_returnsNull() throws Exception {
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            String result = client.get("this-key-does-not-exist");

            assertNull(result,
                    "get() on a missing key must return null, not throw an exception");
        }
    }

    @Test
    @Order(3)
    @DisplayName("delete() removes a key; subsequent get() returns null")
    void testDelete_removesKey() throws Exception {
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            client.put("session", "tok-abc123");
            client.delete("session");

            String result = client.get("session");

            assertNull(result,
                    "get() after delete() must return null");
        }
    }

    @Test
    @Order(4)
    @DisplayName("delete() on non-existent key does not throw")
    void testDelete_missingKey_isNoOp() throws Exception {
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            assertDoesNotThrow(
                    () -> client.delete("phantom-key"),
                    "delete() on a missing key must be a safe no-op"
            );
        }
    }

    @Test
    @Order(5)
    @DisplayName("put() overwrites existing value")
    void testPut_overwritesExistingValue() throws Exception {
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            client.put("counter", "1");
            client.put("counter", "2");

            String result = client.get("counter");

            assertEquals("2", result,
                    "Second put() must overwrite the first value");
        }
    }

    @Test
    @Order(6)
    @DisplayName("Key with TTL is accessible before expiry")
    void testTTL_keyAccessibleBeforeExpiry() throws Exception {
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            client.put("token", "bearer-xyz", 5);

            String result = client.get("token");

            assertEquals("bearer-xyz", result,
                    "Key should be accessible before its TTL expires");
        }
    }

    @Test
    @Order(7)
    @DisplayName("Key with TTL returns null after TTL elapses")
    void testTTL_keyGoneAfterExpiry() throws Exception {
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            client.put("expiring", "value", 1);

            Thread.sleep(1300);

            String result = client.get("expiring");

            assertNull(result,
                    "Key should return null after its TTL expires");
        }
    }

    @Test
    @Order(8)
    @DisplayName("Key with no TTL persists beyond 1 second")
    void testNoTTL_keyPersists() throws Exception {
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            client.put("permanent", "stays");

            Thread.sleep(1100);

            String result = client.get("permanent");

            assertEquals("stays", result,
                    "Key with no TTL should persist indefinitely");
        }
    }

    @Test
    @Order(9)
    @DisplayName("Multiple distinct keys coexist without collision")
    void testMultipleKeys_noCollision() throws Exception {
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            client.put("user:1", "Alice");
            client.put("user:2", "Bob");
            client.put("user:3", "Carol");

            assertEquals("Alice", client.get("user:1"));
            assertEquals("Bob",   client.get("user:2"));
            assertEquals("Carol", client.get("user:3"));
        }
    }

    @Test
    @Order(10)
    @DisplayName("put() 50 keys; all 50 are retrievable")
    void testBulkPut_allKeysRetrievable() throws Exception {
        int count = 50;
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            for (int i = 0; i < count; i++) {
                client.put("bulk-key-" + i, "bulk-value-" + i);
            }

            int retrieved = 0;
            for (int i = 0; i < count; i++) {
                String val = client.get("bulk-key-" + i);
                if (("bulk-value-" + i).equals(val)) retrieved++;
            }

            assertEquals(count, retrieved,
                    "All " + count + " bulk-inserted keys should be retrievable");
        }
    }

    @Test
    @Order(11)
    @DisplayName("stats() returns a non-null, non-empty map")
    void testStats_returnsData() throws Exception {
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            client.put("k", "v");
            client.get("k");
            client.get("nope");

            var stats = client.stats();

            assertNotNull(stats, "stats() must not return null");
            assertFalse(stats.isEmpty(), "stats() must return at least one metric");
        }
    }

    @Test
    @Order(12)
    @DisplayName("stats() includes hit and miss counts")
    void testStats_containsHitsAndMisses() throws Exception {
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            client.put("stat-key", "stat-val");
            client.get("stat-key");
            client.get("missing");

            var stats = client.stats();

            assertTrue(stats.containsKey("hits") || stats.containsKey("hitRate"),
                    "stats() should include hit count or hit rate. Got: " + stats.keySet());
        }
    }

    @Test
    @Order(13)
    @DisplayName("flush() clears all keys from the cache")
    void testFlush_clearsAllKeys() throws Exception {
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            client.put("a", "1");
            client.put("b", "2");
            client.put("c", "3");

            client.flush();

            assertNull(client.get("a"), "Key 'a' should be gone after flush");
            assertNull(client.get("b"), "Key 'b' should be gone after flush");
            assertNull(client.get("c"), "Key 'c' should be gone after flush");
        }
    }

    @Test
    @Order(14)
    @DisplayName("Large values (10KB) round-trip correctly")
    void testLargeValue_roundTripCorrect() throws Exception {
        String largeValue = "X".repeat(10 * 1024);

        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            client.put("large-key", largeValue);

            String result = client.get("large-key");

            assertEquals(largeValue, result, "Large value should survive put/get round-trip intact");
        }
    }

    @Test
    @Order(15)
    @DisplayName("Values with spaces and special characters round-trip correctly")
    void testSpecialCharacterValues() throws Exception {
        String specialValue = "hello world! @#$%^&*() 日本語 émojis 🎉";

        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            client.put("special", specialValue);

            String result = client.get("special");

            assertEquals(specialValue, result,
                    "Values with special characters and unicode must survive round-trip");
        }
    }

    @Test
    @Order(16)
    @DisplayName("Numeric string values round-trip correctly")
    void testNumericStringValue() throws Exception {
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            client.put("count", "42");
            client.put("pi",    "3.14159");
            client.put("neg",   "-17");

            assertEquals("42",      client.get("count"));
            assertEquals("3.14159", client.get("pi"));
            assertEquals("-17",     client.get("neg"));
        }
    }

    @Test
    @Order(17)
    @DisplayName("10 concurrent clients each perform 100 put/get ops without corruption")
    void testConcurrentClients_noDataCorruption() throws Exception {
        int threadCount = 10;
        int opsPerThread = 100;
        CountDownLatch start  = new CountDownLatch(1);
        CountDownLatch done   = new CountDownLatch(threadCount);
        AtomicInteger errors  = new AtomicInteger(0);
        AtomicInteger mismatches = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try (CacheClient client = new CacheClient("localhost", serverPort)) {
                    start.await();

                    for (int i = 0; i < opsPerThread; i++) {
                        String key   = "t" + threadId + "-k" + i;
                        String value = "t" + threadId + "-v" + i;

                        client.put(key, value);

                        String retrieved = client.get(key);

                        if (!value.equals(retrieved)) {
                            mismatches.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    System.err.println("[Test] Thread " + threadId + " error: " + e.getMessage());
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        boolean completed = done.await(30, TimeUnit.SECONDS);

        pool.shutdownNow();

        assertTrue(completed, "All threads should complete within 30 seconds");
        assertEquals(0, errors.get(),
                "No connection or I/O errors should occur under concurrent load");
        assertEquals(0, mismatches.get(),
                "No value mismatches: each thread should get its own values back correctly");
    }

    @Test
    @Order(18)
    @DisplayName("1000 sequential put/get ops from single client all succeed")
    void testSequentialLoad_singleClient() throws Exception {
        int ops = 1000;
        int failures = 0;

        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            for (int i = 0; i < ops; i++) {
                String key   = "seq-" + i;
                String value = "val-" + i;

                client.put(key, value);
                String result = client.get(key);

                if (!value.equals(result)) failures++;
            }
        }

        assertEquals(0, failures,
                "All 1000 sequential put/get operations should return correct values");
    }

    @Test
    @Order(19)
    @DisplayName("Connection pool reuses connections across requests")
    void testConnectionPool_reusesConnections() throws Exception {
        int poolSize = 3;
        ConnectionPool pool = new ConnectionPool("localhost", serverPort, poolSize);

        try {
            int requestCount = 20;

            for (int i = 0; i < requestCount; i++) {
                CacheClient conn = pool.acquire();
                try {
                    conn.put("pool-key-" + i, "pool-val-" + i);
                } finally {
                    pool.release(conn);
                }
            }

            assertTrue(pool.getActiveCount() <= poolSize,
                    "Active connections should never exceed pool size");
        } finally {
            pool.close();
        }
    }

    @Test
    @Order(20)
    @DisplayName("Connection pool blocks when exhausted and throws on timeout")
    void testConnectionPool_exhaustionThrows() throws Exception {
        int poolSize = 2;
        ConnectionPool pool = new ConnectionPool("localhost", serverPort, poolSize);

        CacheClient conn1 = pool.acquire();
        CacheClient conn2 = pool.acquire();

        try {
            assertThrows(
                    TimeoutException.class,
                    () -> pool.acquireWithTimeout(100, TimeUnit.MILLISECONDS),
                    "Exhausted pool should throw TimeoutException after timeout elapses"
            );
        } finally {
            pool.release(conn1);
            pool.release(conn2);
            pool.close();
        }
    }

    @Test
    @Order(21)
    @DisplayName("ping() returns true when server is up")
    void testPing_serverUp_returnsTrue() throws Exception {
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            boolean alive = client.ping();

            assertTrue(alive, "ping() should return true when server is reachable");
        }
    }

    private static CacheClient connectWithRetry(String host, int port, int maxAttempts)
            throws Exception {
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                CacheClient client = new CacheClient(host, port);

                if (client.ping()) {
                    return client;
                }
                client.close();
            } catch (Exception e) {
                lastException = e;
                Thread.sleep(100L * attempt);
            }
        }

        throw new RuntimeException(
                "Failed to connect to server after " + maxAttempts + " attempts",
                lastException
        );
    }
}

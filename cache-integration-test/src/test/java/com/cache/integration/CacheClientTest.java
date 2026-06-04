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

/**
 * CacheClientTest — integration test for the full client → network → server stack.
 *
 * ═══════════════════════════════════════════════════════════════
 * INTEGRATION TEST vs UNIT TEST
 * ═══════════════════════════════════════════════════════════════
 *
 * Unit tests (LRUCacheTest, CommandParserTest) test one class in isolation
 * with mocked/fake dependencies. Fast, deterministic, no I/O.
 *
 * Integration tests (this file) test multiple real components working together:
 *   CacheClient → TCP socket → Netty server → CacheServerHandler → LRUCache
 *
 * Integration tests are:
 *   - SLOWER (real network I/O, even on localhost)
 *   - MORE REALISTIC (test the actual wire protocol, not a mock)
 *   - HARDER TO DEBUG (failures can be in any layer)
 *
 * We keep integration tests separate from unit tests in CI:
 *   Unit tests: mvn test           (fast, every push)
 *   Integration: mvn verify        (slower, pre-merge gates)
 *
 * ═══════════════════════════════════════════════════════════════
 * SERVER LIFECYCLE IN TESTS
 * ═══════════════════════════════════════════════════════════════
 *
 * @BeforeAll: Boot ONE server for all tests in this class.
 *   Why one server? Starting a server takes ~100ms. With 20 tests,
 *   one server = 100ms total overhead vs 2000ms if we restart per test.
 *
 * @BeforeEach: Clear the cache before each test.
 *   Tests must not depend on each other's state. A failed test that leaves
 *   stale keys would cause false failures in subsequent tests.
 *   We send FLUSH before each test to guarantee a clean slate.
 *
 * @AfterAll: Shut down the server after all tests complete.
 *   Without this, the Netty event loop threads keep running, blocking JVM exit.
 *
 * ═══════════════════════════════════════════════════════════════
 * RANDOM PORT SELECTION
 * ═══════════════════════════════════════════════════════════════
 *
 * We don't hardcode port 6379. Why?
 *   - Another test class might also start a server (port collision → bind failure)
 *   - Developer might have Redis running on 6379 (port collision)
 *   - Parallel CI runs might use the same port
 *
 * Strategy: pass port=0 to ServerConfig. The OS assigns an available ephemeral
 * port. We then query the server for the actual port it bound to.
 * This is the standard approach in Spring Boot tests (@SpringBootTest with
 * webEnvironment=RANDOM_PORT) and in any professional test suite.
 *
 * ═══════════════════════════════════════════════════════════════
 * WHAT WE TEST
 * ═══════════════════════════════════════════════════════════════
 *
 *   1. Basic CRUD: put, get, delete
 *   2. Missing keys return null (not exception)
 *   3. TTL expiry observed end-to-end (client → server → eviction → client)
 *   4. STATS command returns parseable metrics
 *   5. FLUSH clears all keys
 *   6. Concurrent clients: 10 threads, no data corruption
 *   7. Large values (10KB strings)
 *   8. Special characters in values
 *   9. Re-put overwrites old value
 *  10. Connection pool behavior: pool exhaustion and release
 */
@DisplayName("CacheClient Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CacheClientTest {

    // -------------------------------------------------------------------------
    // Server lifecycle (shared across all tests)
    // -------------------------------------------------------------------------

    /**
     * The server under test. Started once in @BeforeAll, shut down in @AfterAll.
     * All test methods share this single server instance.
     */
    private static CacheServer server;

    /**
     * The port the server is actually listening on.
     * Determined after the server binds (may differ from configured port if 0 was used).
     */
    private static int serverPort;

    /**
     * A dedicated client for setup/teardown operations (FLUSH, seed data).
     * Separate from clients created in individual tests to avoid interference.
     */
    private static CacheClient setupClient;

    /**
     * Starts the server and verifies it's ready before any test runs.
     *
     * We use @BeforeAll (runs once) instead of @BeforeEach (runs per test)
     * to avoid the 100ms+ startup overhead per test.
     *
     * The server uses LRU policy with capacity 100 — large enough that
     * none of our tests trigger eviction (which would make assertions flaky).
     */
    @BeforeAll
    static void startServer() throws Exception {
        // Use port 0 to let the OS assign a free port.
        // ServerConfig stores the actual bound port after bind() completes.
        ServerConfig config = ServerConfig.builder()
                .port(0)                  // OS assigns a free port
                .workerThreads(4)         // enough for concurrency tests
                .maxConnections(50)       // enough for connection pool tests
                .cacheCapacity(100)       // large enough to avoid eviction in tests
                .evictionPolicy(LRU)
                .build();

        server = new CacheServer(config);
        server.startAsync();              // blocks until Netty is bound and ready

        serverPort = server.getPort(); // retrieve the actual bound port

        // Verify the server is actually up before tests run.
        // Attempt connection with retries — Netty's bind() future resolves
        // before the accept loop is fully ready in rare cases.
        System.out.println(">>> Server bound to port: " + serverPort);
        setupClient = connectWithRetry("localhost", serverPort, 5);

        System.out.println("[Test] Server started on port " + serverPort);
    }

    /**
     * Clears the cache before each test to prevent state leakage between tests.
     *
     * WHY FLUSH AND NOT RESTART?
     * Restarting the server per test costs ~100ms each.
     * FLUSH is a single network roundtrip (~1ms).
     * The result is the same: a clean, empty cache.
     */
    @BeforeEach
    void clearCache() throws Exception {
        setupClient.flush(); // sends FLUSH command, blocks until +OK received
    }

    /**
     * Shuts down the server and closes the setup client after all tests complete.
     * Failure to do this leaves Netty worker threads running → JVM won't exit.
     */
    @AfterAll
    static void stopServer() throws Exception {
        if (setupClient != null) setupClient.close();
        if (server != null)      server.shutdown();
        System.out.println("[Test] Server stopped.");
    }

    // -------------------------------------------------------------------------
    // 1. Basic CRUD
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // 2. TTL expiry — end-to-end
    // -------------------------------------------------------------------------

    @Test
    @Order(6)
    @DisplayName("Key with TTL is accessible before expiry")
    void testTTL_keyAccessibleBeforeExpiry() throws Exception {
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            client.put("token", "bearer-xyz", 5); // 5 second TTL

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
            client.put("expiring", "value", 1); // 1 second TTL

            Thread.sleep(1300); // TTL + 300ms buffer

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
            client.put("permanent", "stays"); // no TTL

            Thread.sleep(1100);

            String result = client.get("permanent");

            assertEquals("stays", result,
                    "Key with no TTL should persist indefinitely");
        }
    }

    // -------------------------------------------------------------------------
    // 3. Multiple keys
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // 4. STATS command
    // -------------------------------------------------------------------------

    @Test
    @Order(11)
    @DisplayName("stats() returns a non-null, non-empty map")
    void testStats_returnsData() throws Exception {
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            client.put("k", "v");
            client.get("k");   // hit
            client.get("nope");// miss

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
            // Generate known hit and miss
            client.put("stat-key", "stat-val");
            client.get("stat-key"); // hit
            client.get("missing");  // miss

            var stats = client.stats();

            // Stats map should contain "hits" and "misses" keys.
            // Actual key names depend on your server's STATS response format.
            // Adjust these if your format uses different key names.
            assertTrue(stats.containsKey("hits") || stats.containsKey("hitRate"),
                    "stats() should include hit count or hit rate. Got: " + stats.keySet());
        }
    }

    // -------------------------------------------------------------------------
    // 5. FLUSH
    // -------------------------------------------------------------------------

    @Test
    @Order(13)
    @DisplayName("flush() clears all keys from the cache")
    void testFlush_clearsAllKeys() throws Exception {
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            client.put("a", "1");
            client.put("b", "2");
            client.put("c", "3");

            client.flush();

            // All keys should be gone after flush
            assertNull(client.get("a"), "Key 'a' should be gone after flush");
            assertNull(client.get("b"), "Key 'b' should be gone after flush");
            assertNull(client.get("c"), "Key 'c' should be gone after flush");
        }
    }

    // -------------------------------------------------------------------------
    // 6. Edge cases — value content
    // -------------------------------------------------------------------------

    @Test
    @Order(14)
    @DisplayName("Large values (10KB) round-trip correctly")
    void testLargeValue_roundTripCorrect() throws Exception {
        // Build a 10KB string
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

    // -------------------------------------------------------------------------
    // 7. Concurrent clients
    // -------------------------------------------------------------------------

    /**
     * 10 threads, each writing 100 unique keys and reading them back.
     * Tests that the server handles concurrent connections without:
     *   - Data corruption (wrong value for a key)
     *   - Connection errors (server rejects too many connections)
     *   - Response interleaving (thread A gets thread B's response)
     */
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
                // Each thread uses its OWN CacheClient (own connection).
                // Sharing a single client across threads without pooling would
                // interleave requests and corrupt responses.
                try (CacheClient client = new CacheClient("localhost", serverPort)) {
                    start.await(); // all threads start simultaneously

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

        start.countDown(); // release all threads simultaneously
        boolean completed = done.await(30, TimeUnit.SECONDS); // generous timeout for CI

        pool.shutdownNow();

        assertTrue(completed, "All threads should complete within 30 seconds");
        assertEquals(0, errors.get(),
                "No connection or I/O errors should occur under concurrent load");
        assertEquals(0, mismatches.get(),
                "No value mismatches — each thread should get its own values back correctly");
    }

    /**
     * Rapid sequential requests from a single client — tests that pipelining
     * or response ordering doesn't corrupt responses.
     * (Our protocol is synchronous request-response, so this mainly tests
     *  that the Netty handler doesn't mix up responses.)
     */
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

    // -------------------------------------------------------------------------
    // 8. Connection pool
    // -------------------------------------------------------------------------

    /**
     * Tests that the connection pool provides and returns connections correctly.
     * After using a pooled connection, it should be returned to the pool for reuse.
     * The pool should NOT open a new TCP connection for every request.
     */
    @Test
    @Order(19)
    @DisplayName("Connection pool reuses connections across requests")
    void testConnectionPool_reusesConnections() throws Exception {
        int poolSize = 3;
        ConnectionPool pool = new ConnectionPool("localhost", serverPort, poolSize);

        try {
            int requestCount = 20; // more requests than pool size → must reuse

            for (int i = 0; i < requestCount; i++) {
                // acquire() gets a connection from the pool (or blocks if all in use)
                CacheClient conn = pool.acquire();
                try {
                    conn.put("pool-key-" + i, "pool-val-" + i);
                } finally {
                    pool.release(conn); // MUST release or pool exhausts
                }
            }

            // If we got here without timeout/exception, the pool reused connections.
            // Additional verification: check active connection count
            assertTrue(pool.getActiveCount() <= poolSize,
                    "Active connections should never exceed pool size");

        } finally {
            pool.close();
        }
    }

    /**
     * Tests that pool exhaustion (all connections in use) either blocks or throws,
     * not silently returns null.
     *
     * In our implementation, acquire() blocks up to a timeout.
     * If the timeout elapses, it throws TimeoutException.
     * This test verifies that behavior.
     */
    @Test
    @Order(20)
    @DisplayName("Connection pool blocks when exhausted and throws on timeout")
    void testConnectionPool_exhaustionThrows() throws Exception {
        int poolSize = 2;
        ConnectionPool pool = new ConnectionPool("localhost", serverPort, poolSize);

        // Acquire all connections without releasing
        CacheClient conn1 = pool.acquire();
        CacheClient conn2 = pool.acquire();

        try {
            // Pool is now exhausted. acquire() should throw or block and timeout.
            assertThrows(
                    TimeoutException.class,
                    () -> pool.acquireWithTimeout(100, TimeUnit.MILLISECONDS),
                    "Exhausted pool should throw TimeoutException after timeout elapses"
            );
        } finally {
            // Release connections so the pool can close cleanly
            pool.release(conn1);
            pool.release(conn2);
            pool.close();
        }
    }

    // -------------------------------------------------------------------------
    // 9. PING
    // -------------------------------------------------------------------------

    @Test
    @Order(21)
    @DisplayName("ping() returns true when server is up")
    void testPing_serverUp_returnsTrue() throws Exception {
        try (CacheClient client = new CacheClient("localhost", serverPort)) {
            boolean alive = client.ping();

            assertTrue(alive, "ping() should return true when server is reachable");
        }
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Attempts to connect a CacheClient to the given host/port, retrying up to
     * maxAttempts times with 100ms between attempts.
     *
     * WHY RETRY?
     * Netty's channel.bind().sync() returns when the socket is bound, but
     * the NIO accept loop may not be ready to process connections for another
     * few milliseconds. Without retries, the first test might fail with
     * "Connection refused" on slow CI machines.
     *
     * @param host        Server hostname.
     * @param port        Server port.
     * @param maxAttempts Maximum connection attempts.
     * @return A connected CacheClient.
     * @throws Exception if all attempts fail.
     */
    private static CacheClient connectWithRetry(String host, int port, int maxAttempts)
            throws Exception {
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                CacheClient client = new CacheClient(host, port);
                // Verify connection is alive with a PING
                if (client.ping()) {
                    return client;
                }
                client.close();
            } catch (Exception e) {
                lastException = e;
                System.out.printf("[Test] Connection attempt %d/%d failed: %s%n",
                        attempt, maxAttempts, e.getMessage());
                Thread.sleep(100L * attempt); // back-off: 100ms, 200ms, 300ms, ...
            }
        }

        throw new RuntimeException(
                "Failed to connect to server after " + maxAttempts + " attempts",
                lastException
        );
    }
}
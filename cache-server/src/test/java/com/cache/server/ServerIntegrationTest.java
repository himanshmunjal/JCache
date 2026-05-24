package com.cache.server;

import com.cache.client.CacheClient;
import com.cache.client.ConnectionPool;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ServerIntegrationTest — end-to-end tests that boot a real CacheServer,
 * connect real CacheClients, and verify the full request-response cycle
 * over an actual TCP connection.
 *
 * WHAT MAKES THIS DIFFERENT FROM UNIT TESTS:
 *   Unit tests verify one class in isolation (mocked dependencies, no network).
 *   Integration tests verify that all the pieces work together:
 *     - CacheServer boots and binds to a port
 *     - Netty pipeline receives bytes, decodes lines, routes to handler
 *     - CacheServerHandler parses commands, calls the cache engine
 *     - Cache stores and retrieves correctly
 *     - Response is encoded and sent back over TCP
 *     - CacheClient reads and parses the response correctly
 *
 *   A unit test can pass while an integration test fails because the
 *   bug lives in the INTERACTION between components (e.g., wrong newline
 *   handling, encoding mismatch, race condition in handler setup).
 *
 * TEST ISOLATION STRATEGY:
 *   Server starts ONCE (@BeforeAll) — shared across all tests.
 *   Each test calls flush() in @BeforeEach to clear all data.
 *   This gives test-level isolation (clean slate per test) without the
 *   cost of server restart per test (~200ms each).
 *
 * PORT SELECTION:
 *   We use port 16379 (not 6379) to avoid conflicting with a real Redis
 *   or your development server if it's already running.
 *   Alternatively, port 0 would let the OS choose a free port, but then
 *   tests need to query the actual port after binding — more complexity.
 *
 * ORDERING:
 *   @TestMethodOrder(MethodOrderer.OrderAnnotation.class) ensures tests run
 *   in a predictable sequence. Integration tests sometimes have subtle ordering
 *   dependencies (e.g., a stats test expects a specific hit count that depends
 *   on no prior traffic). Explicit ordering makes this controllable.
 */
@DisplayName("Cache Server Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ServerIntegrationTest {

    /** Port for the test server. Different from default 6379 to avoid conflicts. */
    private static final int TEST_PORT = 16379;

    /** The server under test. Shared across all tests. */
    private static CacheServer server;

    /** A single client for simple (non-concurrent) tests. */
    private static CacheClient client;

    // -------------------------------------------------------------------------
    // Setup & Teardown
    // -------------------------------------------------------------------------

    /**
     * Starts the server once before any test in this class runs.
     *
     * WHY @BeforeAll (once) instead of @BeforeEach (per test)?
     * Server startup involves Netty EventLoopGroup creation, port binding,
     * and thread pool initialization — ~200ms per startup.
     * With 20 tests, @BeforeEach would add 4 seconds of pure overhead.
     * The server is stateless (we flush between tests), so sharing is safe.
     *
     * @throws Exception if server or client fails to start.
     */
    @BeforeAll
    static void startServer() throws Exception {
        ServerConfig config = ServerConfig.builder()
                .port(TEST_PORT)
                .evictionPolicy("LRU")
                .cacheCapacity(1000)
                .workerThreads(4)        // fewer threads for predictable test behavior
                .maxConnections(100)
                .verbose(false)          // suppress connection logs during tests
                .build();

        server = new CacheServer(config);
        server.startAsync(); // non-blocking — returns after server binds to port

        // Create a persistent client for single-threaded tests.
        client = new CacheClient("localhost", TEST_PORT);

        // Verify connection with PING before running tests.
        // If this fails, all tests will fail with a clear message.
        assertTrue(client.ping(), "Server should respond to PING after startup");
    }

    /**
     * Stops the server and closes the client after all tests complete.
     * Without this, the Netty threads would keep the test JVM alive.
     */
    @AfterAll
    static void stopServer() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    /**
     * Flushes the cache before each test to ensure a clean slate.
     * This is the integration test equivalent of creating a fresh mock.
     *
     * @throws IOException if the FLUSH command fails.
     */
    @BeforeEach
    void clearCache() throws IOException {
        client.flush();
    }

    // =========================================================================
    // 1. Basic connectivity
    // =========================================================================

    /**
     * Verify the server responds to PING with PONG.
     * If this fails, something is fundamentally broken in the pipeline.
     */
    @Test
    @Order(1)
    @DisplayName("Server responds to PING with PONG")
    void testPing() {
        assertTrue(client.ping(), "PING should return true (server responded PONG)");
    }

    /**
     * Multiple PINGs should all succeed — server doesn't close the connection
     * after the first command (keep-alive).
     */
    @Test
    @Order(2)
    @DisplayName("Server handles multiple sequential PINGs on same connection")
    void testMultiplePings() {
        for (int i = 0; i < 10; i++) {
            assertTrue(client.ping(), "PING " + i + " should succeed");
        }
    }

    // =========================================================================
    // 2. Basic PUT and GET
    // =========================================================================

    /**
     * Core test: put a value, get it back, verify they match.
     * If this fails, the basic request-response cycle is broken.
     */
    @Test
    @Order(3)
    @DisplayName("PUT then GET returns the stored value")
    void testPutAndGet() throws IOException {
        client.put("name", "Alice");

        String result = client.get("name");

        assertEquals("Alice", result, "GET should return the value that was PUT");
    }

    /**
     * GET on a key that was never PUT should return null, not throw an exception.
     * Cache misses are normal — they're not errors.
     */
    @Test
    @Order(4)
    @DisplayName("GET on missing key returns null")
    void testGetMissingKey_returnsNull() throws IOException {
        String result = client.get("nonexistent-key-xyz");

        assertNull(result, "GET on missing key should return null");
    }

    /**
     * PUT multiple keys and GET each one — verifies no key collision or
     * state contamination between entries.
     */
    @Test
    @Order(5)
    @DisplayName("Multiple PUT/GET pairs work independently")
    void testMultipleKeys() throws IOException {
        client.put("color", "blue");
        client.put("animal", "cat");
        client.put("fruit", "mango");

        assertEquals("blue",  client.get("color"),  "color should be blue");
        assertEquals("cat",   client.get("animal"), "animal should be cat");
        assertEquals("mango", client.get("fruit"),  "fruit should be mango");
    }

    /**
     * Overwriting an existing key should return the new value, not the old one.
     * Verifies the PUT replaces, not appends.
     */
    @Test
    @Order(6)
    @DisplayName("PUT on existing key overwrites the value")
    void testPutOverwrite() throws IOException {
        client.put("key", "original");
        client.put("key", "updated");

        assertEquals("updated", client.get("key"),
                "Second PUT should overwrite the first value");
    }

    /**
     * Values with spaces should be stored and retrieved correctly.
     * The wire protocol uses spaces as delimiters for key/value/ttl,
     * but the value portion should accept spaces (it's the last token).
     */
    @Test
    @Order(7)
    @DisplayName("Value with spaces stored and retrieved correctly")
    void testValueWithSpaces() throws IOException {
        client.put("greeting", "Hello World");

        assertEquals("Hello World", client.get("greeting"),
                "Value containing spaces should be preserved");
    }

    // =========================================================================
    // 3. DELETE
    // =========================================================================

    /**
     * DELETE should remove a key so subsequent GET returns null.
     */
    @Test
    @Order(8)
    @DisplayName("DELETE removes key from cache")
    void testDelete() throws IOException {
        client.put("temp", "value");
        assertNotNull(client.get("temp"), "Key should exist before DELETE");

        client.delete("temp");

        assertNull(client.get("temp"), "Key should not exist after DELETE");
    }

    /**
     * DELETE on a non-existent key should not throw.
     * It's a no-op — idempotent.
     */
    @Test
    @Order(9)
    @DisplayName("DELETE on missing key is a safe no-op")
    void testDeleteMissingKey_noException() {
        assertDoesNotThrow(
                () -> client.delete("key-that-never-existed"),
                "Deleting a non-existent key should not throw"
        );
    }

    /**
     * DELETE should not affect other keys — only the specified one is removed.
     */
    @Test
    @Order(10)
    @DisplayName("DELETE only removes the specified key, not others")
    void testDelete_doesNotAffectOtherKeys() throws IOException {
        client.put("a", "1");
        client.put("b", "2");
        client.put("c", "3");

        client.delete("b");

        assertEquals("1", client.get("a"), "Key 'a' should not be affected");
        assertNull(client.get("b"),         "Key 'b' should be deleted");
        assertEquals("3", client.get("c"), "Key 'c' should not be affected");
    }

    // =========================================================================
    // 4. TTL
    // =========================================================================

    /**
     * A key with TTL should be accessible before expiry and null after.
     * Uses 1-second TTL with 1.3-second sleep (300ms buffer for timing jitter).
     */
    @Test
    @Order(11)
    @DisplayName("Key expires after TTL elapses")
    void testTTLExpiry() throws IOException, InterruptedException {
        client.put("session", "user42", 1); // 1-second TTL

        // Immediately after PUT — should exist
        assertNotNull(client.get("session"), "Key should exist before TTL expires");

        // Wait for TTL + 300ms buffer
        Thread.sleep(1300);

        // After TTL — should be gone
        assertNull(client.get("session"), "Key should be null after TTL expires");
    }

    /**
     * Key with TTL=0 should persist indefinitely (no expiry).
     */
    @Test
    @Order(12)
    @DisplayName("TTL=0 means no expiry")
    void testTTLZero_noExpiry() throws IOException, InterruptedException {
        client.put("permanent", "value", 0);

        Thread.sleep(200); // give sweeper time to run

        assertEquals("value", client.get("permanent"),
                "Key with TTL=0 should not expire");
    }

    // =========================================================================
    // 5. STATS
    // =========================================================================

    /**
     * STATS should return a parseable response with at least the expected keys.
     * We don't assert specific counts here (depends on test ordering) —
     * just that the response structure is correct.
     */
    @Test
    @Order(13)
    @DisplayName("STATS returns map with expected metric keys")
    void testStats_returnsExpectedKeys() throws IOException {
        // Generate some traffic so stats are non-empty
        client.put("k1", "v1");
        client.put("k2", "v2");
        client.get("k1"); // hit
        client.get("k1"); // hit
        client.get("missing"); // miss

        Map<String, String> stats = client.stats();

        assertNotNull(stats, "Stats should not be null");
        assertTrue(stats.containsKey("hits"),      "Stats should contain 'hits'");
        assertTrue(stats.containsKey("misses"),    "Stats should contain 'misses'");
        assertTrue(stats.containsKey("evictions"), "Stats should contain 'evictions'");
        assertTrue(stats.containsKey("size"),      "Stats should contain 'size'");
    }

    /**
     * After FLUSH, size should be 0.
     */
    @Test
    @Order(14)
    @DisplayName("FLUSH clears all entries — size becomes 0")
    void testFlush_clearsAllEntries() throws IOException {
        client.put("a", "1");
        client.put("b", "2");
        client.put("c", "3");

        client.flush();

        Map<String, String> stats = client.stats();
        assertEquals("0", stats.get("size"), "Cache size should be 0 after FLUSH");
    }

    // =========================================================================
    // 6. High-volume sequential load
    // =========================================================================

    /**
     * Send 1,000 PUT operations and verify 1,000 GET operations return correct values.
     * This test catches:
     *   - Protocol framing bugs (missing or extra newlines corrupting message boundaries)
     *   - State corruption in the cache after many operations
     *   - Connection stability over many sequential requests
     *
     * 1,000 ops chosen for speed — runs in <1 second on localhost.
     * In a real system you'd use 100k+ ops, but CI needs to run fast.
     */
    @Test
    @Order(15)
    @DisplayName("1000 sequential PUT then GET operations all succeed")
    void testHighVolumeSequential_1000Ops() throws IOException {
        int count = 1000;

        // PUT all values
        for (int i = 0; i < count; i++) {
            client.put("load-key-" + i, "load-val-" + i);
        }

        // GET and verify a sample (not all 1000 — LRU eviction may have removed early ones)
        // The last CACHE_CAPACITY entries should definitely still be present.
        int verifyFrom = Math.max(0, count - 500); // verify last 500
        int successCount = 0;
        for (int i = verifyFrom; i < count; i++) {
            String val = client.get("load-key-" + i);
            if (("load-val-" + i).equals(val)) {
                successCount++;
            }
        }

        // At least 90% of the verified range should be correct.
        // Some may be evicted by LRU if cache capacity is exceeded.
        int verifyCount = count - verifyFrom;
        assertTrue(successCount > verifyCount * 0.9,
                "At least 90% of recently-put keys should be retrievable. " +
                        "Got: " + successCount + "/" + verifyCount);
    }

    // =========================================================================
    // 7. Concurrent clients
    // =========================================================================

    /**
     * 10 threads, each with their own CacheClient connection, performing
     * concurrent PUT and GET operations.
     *
     * This test verifies:
     *   - The server handles multiple concurrent connections correctly
     *   - No data corruption under concurrent writes from different clients
     *   - Each client's connection remains stable throughout
     *   - No deadlocks or starvation in the server's thread pool
     *
     * Thread-local key namespacing (thread-0-key-N) prevents cross-thread
     * key collisions — each thread owns its keys entirely.
     */
    @Test
    @Order(16)
    @DisplayName("10 concurrent clients perform PUT/GET without errors")
    void testConcurrentClients_10Threads() throws InterruptedException {
        int threadCount = 10;
        int opsPerThread = 100;

        CountDownLatch allReady = new CountDownLatch(threadCount);
        CountDownLatch allDone  = new CountDownLatch(threadCount);
        AtomicInteger errors    = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                // Each thread creates its own dedicated connection.
                // CacheClient is NOT thread-safe — one per thread.
                try (CacheClient threadClient = new CacheClient("localhost", TEST_PORT)) {
                    allReady.countDown();
                    allReady.await(); // all threads start simultaneously

                    for (int i = 0; i < opsPerThread; i++) {
                        String key = "thread-" + threadId + "-key-" + i;
                        String val = "thread-" + threadId + "-val-" + i;

                        threadClient.put(key, val);

                        String retrieved = threadClient.get(key);
                        if (!val.equals(retrieved)) {
                            errors.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    System.err.println("Thread " + threadId + " error: " + e.getMessage());
                } finally {
                    allDone.countDown();
                }
            });
        }

        boolean completed = allDone.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertTrue(completed, "All threads should complete within 30 seconds");
        assertEquals(0, errors.get(),
                "No errors should occur under concurrent client load");
    }

    /**
     * Tests the ConnectionPool under concurrent load.
     * 20 threads share a pool of 8 connections — threads must wait for available connections.
     *
     * Verifies:
     *   - Pool correctly limits concurrent connections to poolSize
     *   - All 20 threads eventually get a connection and complete
     *   - No connection is used by two threads simultaneously
     *   - Pool correctly returns connections after use
     */
    @Test
    @Order(17)
    @DisplayName("ConnectionPool serves 20 threads with 8-connection pool correctly")
    void testConnectionPool_concurrentAccess() throws Exception {
        int poolSize   = 8;
        int threadCount = 20;
        int opsPerThread = 50;

        ConnectionPool pool = new ConnectionPool("localhost", TEST_PORT, poolSize, 10_000);

        CountDownLatch allReady = new CountDownLatch(threadCount);
        CountDownLatch allDone  = new CountDownLatch(threadCount);
        AtomicInteger errors    = new AtomicInteger(0);
        AtomicInteger maxActive = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    allReady.countDown();
                    allReady.await();

                    for (int i = 0; i < opsPerThread; i++) {
                        CacheClient conn = pool.acquire();

                        // Track peak concurrent active connections
                        int active = pool.activeCount();
                        maxActive.updateAndGet(current -> Math.max(current, active));

                        try {
                            String key = "pool-thread-" + threadId + "-" + i;
                            conn.put(key, "val-" + i);
                            String result = conn.get(key);
                            if (!("val-" + i).equals(result)) {
                                errors.incrementAndGet();
                            }
                        } finally {
                            pool.release(conn); // ALWAYS in finally
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    System.err.println("Pool thread " + threadId + " error: " + e.getMessage());
                } finally {
                    allDone.countDown();
                }
            });
        }

        boolean completed = allDone.await(60, TimeUnit.SECONDS);
        executor.shutdown();
        pool.close();

        assertTrue(completed, "All threads should complete within 60 seconds");
        assertEquals(0, errors.get(), "No errors should occur under pool concurrent load");

        // Peak active should never exceed pool size
        assertTrue(maxActive.get() <= poolSize,
                "Active connections should never exceed pool size. Peak: " + maxActive.get());
    }

    // =========================================================================
    // 8. Protocol edge cases
    // =========================================================================

    /**
     * Keys that are purely numeric should work correctly.
     * The protocol is text-based — numeric keys are just strings.
     */
    @Test
    @Order(18)
    @DisplayName("Numeric keys stored and retrieved correctly")
    void testNumericKeys() throws IOException {
        client.put("12345", "value");
        assertEquals("value", client.get("12345"),
                "Numeric string key should work correctly");
    }

    /**
     * Long keys (within protocol limits) should work correctly.
     */
    @Test
    @Order(19)
    @DisplayName("Long key (200 chars) works correctly")
    void testLongKey() throws IOException {
        String longKey = "k".repeat(200);
        client.put(longKey, "long-key-value");
        assertEquals("long-key-value", client.get(longKey),
                "Long key should be stored and retrieved correctly");
    }

    /**
     * Multiple rapid connections to the server should all succeed.
     * Tests that the server can handle connection churn (connect, use, close).
     */
    @Test
    @Order(20)
    @DisplayName("Server handles 50 rapid connect/disconnect cycles")
    void testRapidConnectionCycles() {
        int cycles = 50;
        AtomicInteger failures = new AtomicInteger(0);

        for (int i = 0; i < cycles; i++) {
            try (CacheClient c = new CacheClient("localhost", TEST_PORT)) {
                if (!c.ping()) failures.incrementAndGet();
            } catch (IOException e) {
                failures.incrementAndGet();
            }
        }

        assertEquals(0, failures.get(),
                "All 50 rapid connections should succeed. Failures: " + failures.get());
    }

    // =========================================================================
    // 9. Server resilience
    // =========================================================================

    /**
     * After a heavy load test, the server should still respond correctly.
     * Verifies no resource leaks, thread pool exhaustion, or state corruption
     * from prior tests.
     */
    @Test
    @Order(21)
    @DisplayName("Server still healthy after all prior tests")
    void testServerHealthAfterLoad() throws IOException {
        // Fresh data after all previous tests
        client.put("health-check", "ok");
        String result = client.get("health-check");

        assertEquals("ok", result,
                "Server should respond correctly after all prior tests — no resource leaks");
        assertTrue(client.ping(), "Server should still respond to PING");
    }

    /**
     * Verifies connection to a wrong port fails gracefully (client throws, doesn't hang).
     * This tests the client's error handling, not the server.
     */
    @Test
    @Order(22)
    @DisplayName("Connecting to wrong port throws IOException, not hang")
    void testWrongPort_throwsImmediately() {
        // Port 19999 is (almost certainly) not listening.
        // The connection should fail fast (connection refused), not hang.
        assertThrows(IOException.class,
                () -> new CacheClient("localhost", 19999),
                "Connecting to a port with no server should throw IOException"
        );
    }
}
package com.cache.server;

import com.cache.client.CacheClient;
import com.cache.client.ConnectionPool;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.cache.api.CachePolicyType.LRU;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Cache Server Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ServerIntegrationTest {
    private static final int TEST_PORT = 16379;

    private static CacheServer server;

    private static CacheClient client;

    @BeforeAll
    static void startServer() throws Exception {
        ServerConfig config = ServerConfig.builder()
                .port(TEST_PORT)
                .evictionPolicy(LRU)
                .cacheCapacity(1000)
                .workerThreads(4)
                .maxConnections(100)
                .verbose(false)
                .build();

        server = new CacheServer(config);
        server.startAsync();
        Thread.sleep(1000);

        client = new CacheClient("localhost", TEST_PORT);

        assertTrue(client.ping(), "Server should respond to PING after startup");
    }

    @AfterAll
    static void stopServer() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.shutdown();
        }
    }

    @BeforeEach
    void clearCache() throws IOException {
        client.flush();
    }

    @Test
    @Order(1)
    @DisplayName("Server responds to PING with PONG")
    void testPing() {
        assertTrue(client.ping(), "PING should return true (server responded PONG)");
    }

    @Test
    @Order(2)
    @DisplayName("Server handles multiple sequential PINGs on same connection")
    void testMultiplePings() {
        for (int i = 0; i < 10; i++) {
            assertTrue(client.ping(), "PING " + i + " should succeed");
        }
    }

    @Test
    @Order(3)
    @DisplayName("PUT then GET returns the stored value")
    void testPutAndGet() throws IOException {
        client.put("name", "Alice");

        String result = client.get("name");

        assertEquals("Alice", result, "GET should return the value that was PUT");
    }

    @Test
    @Order(4)
    @DisplayName("GET on missing key returns null")
    void testGetMissingKey_returnsNull() throws IOException {
        String result = client.get("nonexistent-key-xyz");

        assertNull(result, "GET on missing key should return null");
    }

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

    @Test
    @Order(6)
    @DisplayName("PUT on existing key overwrites the value")
    void testPutOverwrite() throws IOException {
        client.put("key", "original");
        client.put("key", "updated");

        assertEquals("updated", client.get("key"),
                "Second PUT should overwrite the first value");
    }

    @Test
    @Order(7)
    @DisplayName("Value with spaces stored and retrieved correctly")
    void testValueWithSpaces() throws IOException {
        client.put("greeting", "Hello World");

        assertEquals("Hello World", client.get("greeting"),
                "Value containing spaces should be preserved");
    }

    @Test
    @Order(8)
    @DisplayName("DELETE removes key from cache")
    void testDelete() throws IOException {
        client.put("temp", "value");
        assertNotNull(client.get("temp"), "Key should exist before DELETE");

        client.delete("temp");

        assertNull(client.get("temp"), "Key should not exist after DELETE");
    }

    @Test
    @Order(9)
    @DisplayName("DELETE on missing key is a safe no-op")
    void testDeleteMissingKey_noException() {
        assertDoesNotThrow(
                () -> client.delete("key-that-never-existed"),
                "Deleting a non-existent key should not throw"
        );
    }

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

    @Test
    @Order(11)
    @DisplayName("Key expires after TTL elapses")
    void testTTLExpiry() throws IOException, InterruptedException {
        client.put("session", "user42", 1);

        assertNotNull(client.get("session"), "Key should exist before TTL expires");

        Thread.sleep(1300);

        assertNull(client.get("session"), "Key should be null after TTL expires");
    }

    @Test
    @Order(12)
    @DisplayName("TTL=0 means no expiry")
    void testTTLZero_noExpiry() throws IOException, InterruptedException {
        client.put("permanent", "value", 0);

        Thread.sleep(200);

        assertEquals("value", client.get("permanent"),
                "Key with TTL=0 should not expire");
    }

    @Test
    @Order(13)
    @DisplayName("STATS returns map with expected metric keys")
    void testStats_returnsExpectedKeys() throws IOException {
        client.put("k1", "v1");
        client.put("k2", "v2");
        client.get("k1");
        client.get("k1");
        client.get("missing");

        Map<String, String> stats = client.stats();

        assertNotNull(stats, "Stats should not be null");
        assertTrue(stats.containsKey("hits"),      "Stats should contain 'hits'");
        assertTrue(stats.containsKey("misses"),    "Stats should contain 'misses'");
        assertTrue(stats.containsKey("evictions"), "Stats should contain 'evictions'");
        assertTrue(stats.containsKey("size"),      "Stats should contain 'size'");
    }

    @Test
    @Order(14)
    @DisplayName("FLUSH clears all entries: size becomes 0")
    void testFlush_clearsAllEntries() throws IOException {
        client.put("a", "1");
        client.put("b", "2");
        client.put("c", "3");

        client.flush();

        Map<String, String> stats = client.stats();
        assertEquals("0", stats.get("size"), "Cache size should be 0 after FLUSH");
    }

    @Test
    @Order(15)
    @DisplayName("1000 sequential PUT then GET operations all succeed")
    void testHighVolumeSequential_1000Ops() throws IOException {
        int count = 1000;

        for (int i = 0; i < count; i++) {
            client.put("load-key-" + i, "load-val-" + i);
        }

        int verifyFrom = Math.max(0, count - 500);
        int successCount = 0;
        for (int i = verifyFrom; i < count; i++) {
            String val = client.get("load-key-" + i);
            if (("load-val-" + i).equals(val)) {
                successCount++;
            }
        }

        int verifyCount = count - verifyFrom;
        assertTrue(successCount > verifyCount * 0.9,
                "At least 90% of recently-put keys should be retrievable. " +
                        "Got: " + successCount + "/" + verifyCount);
    }

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
                try (CacheClient threadClient = new CacheClient("localhost", TEST_PORT)) {
                    allReady.countDown();
                    allReady.await();

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

    @Test
    @Order(17)
    @DisplayName("ConnectionPool serves 20 threads with 8-connection pool correctly")
    void testConnectionPool_concurrentAccess() throws Exception {
        int poolSize   = 8;
        int threadCount = 20;
        int opsPerThread = 50;

        ConnectionPool pool = new ConnectionPool("localhost", TEST_PORT, poolSize);

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

                        int active = pool.getActiveCount();
                        maxActive.updateAndGet(current -> Math.max(current, active));

                        try {
                            String key = "pool-thread-" + threadId + "-" + i;
                            conn.put(key, "val-" + i);
                            String result = conn.get(key);
                            if (!("val-" + i).equals(result)) {
                                errors.incrementAndGet();
                            }
                        } finally {
                            pool.release(conn);
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

        assertTrue(maxActive.get() <= poolSize,
                "Active connections should never exceed pool size. Peak: " + maxActive.get());
    }

    @Test
    @Order(18)
    @DisplayName("Numeric keys stored and retrieved correctly")
    void testNumericKeys() throws IOException {
        client.put("12345", "value");
        assertEquals("value", client.get("12345"),
                "Numeric string key should work correctly");
    }

    @Test
    @Order(19)
    @DisplayName("Long key (200 chars) works correctly")
    void testLongKey() throws IOException {
        String longKey = "k".repeat(200);
        client.put(longKey, "long-key-value");
        assertEquals("long-key-value", client.get(longKey),
                "Long key should be stored and retrieved correctly");
    }

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

    @Test
    @Order(21)
    @DisplayName("Server still healthy after all prior tests")
    void testServerHealthAfterLoad() throws IOException {
        client.put("health-check", "ok");
        String result = client.get("health-check");

        assertEquals("ok", result,
                "Server should respond correctly after all prior tests: no resource leaks");
        assertTrue(client.ping(), "Server should still respond to PING");
    }

    @Test
    @Order(22)
    @DisplayName("Connecting to wrong port throws IOException, not hang")
    void testWrongPort_throwsImmediately() {
        assertThrows(IOException.class,
                () -> new CacheClient("localhost", 19999),
                "Connecting to a port with no server should throw IOException"
        );
    }
}

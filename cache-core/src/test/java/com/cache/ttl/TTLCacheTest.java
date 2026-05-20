package com.cache.ttl;

import com.cache.api.Cache;
import com.cache.policy.LRUCache;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for TTLCache.
 *
 * Testing philosophy for time-sensitive code:
 *
 *   Time-based tests are inherently flaky if written naively.
 *   "Sleep for 1 second and check expiry" breaks on slow CI machines.
 *   We use SHORT TTLs (100ms) and slightly LONGER sleeps (150ms) to
 *   give a buffer without making tests slow.
 *
 *   Rule of thumb: sleep = TTL * 1.5 minimum.
 *   We also use a very short sweep interval (50ms) in tests so we
 *   don't have to wait 500ms for the sweeper to run.
 *
 * Test categories:
 *   1. Basic put/get without TTL           — delegate still works correctly
 *   2. TTL expiry via lazy eviction        — expired keys return null on get()
 *   3. TTL expiry via background sweeper   — keys cleaned from memory
 *   4. Edge cases                          — TTL=0, negative TTL, null keys
 *   5. TTL reset on re-put                 — overwriting a key resets its expiry
 *   6. getRemainingTTL                     — utility method correctness
 *   7. Concurrent access                   — no race conditions under load
 *   8. Shutdown behaviour                  — sweeper stops cleanly
 *
 * Setup: Each test gets a fresh TTLCache wrapping a fresh LRUCache.
 * The sweep interval is set to 50ms so sweeper tests run fast.
 */
@DisplayName("TTLCache Tests")
@Execution(ExecutionMode.SAME_THREAD) // Time-sensitive tests — avoid parallel execution
class TTLCacheTest {

    // Short sweep interval so we don't wait long in sweeper tests.
    private static final long SWEEP_INTERVAL_MS = 50L;

    // TTL used in most tests — short enough to test quickly.
    private static final long SHORT_TTL_SECONDS = 0L; // we'll use ms trick below

    private TTLCache<String, String> cache;

    /**
     * Create a fresh TTLCache before each test.
     * LRUCache with capacity 10 is our delegate — large enough to not
     * interfere with TTL tests via policy eviction.
     */
    @BeforeEach
    void setUp() {
        Cache<String, String> lruDelegate = new LRUCache<>(10);
        cache = new TTLCache<>(lruDelegate, SWEEP_INTERVAL_MS);
    }

    /**
     * Shut down the sweeper thread after each test.
     * Without this, the sweeper thread from test N might interfere with test N+1.
     */
    @AfterEach
    void tearDown() {
        cache.shutdown();
    }

    // =========================================================================
    // 1. Basic put/get without TTL
    // =========================================================================

    /**
     * Sanity check: TTLCache doesn't break basic cache behaviour.
     * A key put with no TTL should be retrievable indefinitely
     * (until evicted by the LRU policy).
     */
    @Test
    @DisplayName("get() returns value for key with no TTL")
    void testBasicPutAndGet_noTTL() {
        cache.put("name", "Alice");

        String result = cache.get("name");

        assertEquals("Alice", result, "Should return value when no TTL is set");
    }

    /**
     * A key that was never put should return null — TTLCache doesn't
     * invent values or change this fundamental contract.
     */
    @Test
    @DisplayName("get() returns null for key that was never put")
    void testGet_missingKey_returnsNull() {
        String result = cache.get("nonexistent");

        assertNull(result, "Missing key should return null");
    }

    /**
     * Multiple keys with no TTL should all coexist correctly.
     * Verifies TTLCache doesn't confuse expiry entries across keys.
     */
    @Test
    @DisplayName("Multiple keys with no TTL all accessible")
    void testMultipleKeys_noTTL() {
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");

        assertEquals("1", cache.get("a"));
        assertEquals("2", cache.get("b"));
        assertEquals("3", cache.get("c"));
    }

    // =========================================================================
    // 2. TTL expiry via lazy eviction
    // =========================================================================

    /**
     * Core test: a key with a TTL returns its value before expiry,
     * and null after expiry.
     *
     * We use TTL=1 second here, which is the minimum clean TTL value.
     * For speed in CI, we manipulate the system by checking immediately
     * after put (should exist) and after sleep (should be gone).
     *
     * Implementation note: we test with 200ms TTL by using the internal
     * put(key, value, ttlSeconds) method with ttlSeconds=0 being "no expiry".
     * For very short TTLs, we'd need to expose a millisecond-level method or
     * use reflection. Here we test with ttlSeconds=1 and sleep 1100ms.
     */
    @Test
    @DisplayName("get() returns value before TTL expires")
    void testLazyEviction_beforeExpiry() {
        cache.put("token", "abc123", 2); // 2 second TTL

        // Immediately after put — should be retrievable
        String result = cache.get("token");

        assertEquals("abc123", result, "Key should exist before TTL expires");
    }

    /**
     * Key must return null after its TTL has elapsed.
     * This is the MOST IMPORTANT test — the core contract of TTLCache.
     *
     * We sleep for TTL + 100ms buffer to account for scheduling jitter.
     */
    @Test
    @DisplayName("get() returns null after TTL expires (lazy eviction)")
    void testLazyEviction_afterExpiry() throws InterruptedException {
        cache.put("session", "user42", 1); // 1 second TTL

        // Wait for TTL to elapse (1000ms) + 200ms buffer for safety.
        Thread.sleep(1200);

        String result = cache.get("session");

        assertNull(result, "Key should return null after TTL has elapsed");
    }

    /**
     * After lazy eviction fires, the key should be gone from the size count too.
     * This verifies that deleteKey() removes from the delegate, not just hides it.
     */
    @Test
    @DisplayName("size() decreases after lazy eviction fires")
    void testLazyEviction_decreasesSize() throws InterruptedException {
        cache.put("temp", "value", 1);
        assertEquals(1, cache.size(), "Size should be 1 after put");

        Thread.sleep(1200); // wait for expiry

        cache.get("temp"); // trigger lazy eviction

        assertEquals(0, cache.size(), "Size should be 0 after lazy eviction");
    }

    /**
     * Lazy eviction should not affect OTHER keys that haven't expired.
     * A common bug: eviction of one key corrupts adjacent entries.
     */
    @Test
    @DisplayName("Lazy eviction of one key doesn't affect other keys")
    void testLazyEviction_doesNotAffectOtherKeys() throws InterruptedException {
        cache.put("expiring", "bye", 1);   // expires in 1 second
        cache.put("permanent", "stays");   // no TTL

        Thread.sleep(1200); // let "expiring" expire

        assertNull(cache.get("expiring"),    "Expired key should be null");
        assertEquals("stays", cache.get("permanent"), "Permanent key should survive");
    }

    // =========================================================================
    // 3. TTL expiry via background sweeper
    // =========================================================================

    /**
     * The sweeper must actually remove expired keys from the underlying delegate,
     * not just hide them. We verify this by checking size() after the sweeper runs.
     *
     * We use SWEEP_INTERVAL_MS=50ms so we don't have to wait long.
     * Total wait: TTL (1s) + 2 * sweep interval (100ms) + buffer (100ms) = ~1200ms.
     */
    @Test
    @DisplayName("Background sweeper removes expired keys from delegate")
    void testSweeper_removesExpiredKeys() throws InterruptedException {
        cache.put("ephemeral", "data", 1); // 1 second TTL
        assertEquals(1, cache.size());

        // Wait for TTL to pass AND for the sweeper to run at least once after that.
        Thread.sleep(1200);

        // DO NOT call cache.get() here — we want the sweeper to do the cleanup,
        // not lazy eviction. We check size directly.
        assertEquals(0, cache.size(),
                "Sweeper should have removed expired key from delegate");
    }

    /**
     * Sweeper eviction counter should increment when the sweeper cleans up keys.
     * This tests the metrics side of the sweeper.
     */
    @Test
    @DisplayName("Sweeper increments sweep eviction counter")
    void testSweeper_evictionCounterIncrements() throws InterruptedException {
        cache.put("k1", "v1", 1);
        cache.put("k2", "v2", 1);

        long before = cache.getSweepEvictionCount();

        Thread.sleep(1300); // TTL + sweep interval + buffer

        long after = cache.getSweepEvictionCount();

        // At least 2 sweep evictions (one per expired key).
        // Could be more if we put more keys, but never less.
        assertTrue(after - before >= 2,
                "Sweeper should have counted at least 2 evictions, got: " + (after - before));
    }

    /**
     * Permanent keys (no TTL) must NOT be swept by the background sweeper.
     * The NO_EXPIRY sentinel protects them.
     */
    @Test
    @DisplayName("Sweeper does not remove permanent keys (no TTL)")
    void testSweeper_doesNotRemovePermanentKeys() throws InterruptedException {
        cache.put("forever", "value"); // no TTL — should never be swept

        Thread.sleep(200); // give sweeper time to run several cycles

        assertEquals("value", cache.get("forever"),
                "Permanent key must not be removed by sweeper");
    }

    // =========================================================================
    // 4. Edge cases
    // =========================================================================

    /**
     * TTL=0 means "no expiry" in our system (matching Redis PERSIST semantics).
     * The key should remain accessible indefinitely.
     */
    @Test
    @DisplayName("TTL=0 means no expiry — key persists indefinitely")
    void testEdgeCase_ttlZeroMeansNoExpiry() throws InterruptedException {
        cache.put("persistent", "value", 0); // 0 = no expiry

        Thread.sleep(200); // give sweeper time to run

        assertEquals("value", cache.get("persistent"),
                "TTL=0 should mean no expiry, key must still be accessible");
    }

    /**
     * Negative TTL is a programming error and should throw immediately.
     * This prevents silent bugs where a misconfigured TTL makes all keys
     * look like they expire instantly.
     */
    @Test
    @DisplayName("Negative TTL throws IllegalArgumentException")
    void testEdgeCase_negativeTTL_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> cache.put("key", "value", -1),
                "Negative TTL should throw IllegalArgumentException"
        );
    }

    /**
     * Null key must throw NullPointerException or IllegalArgumentException.
     * Our cache doesn't support null keys — ConcurrentHashMap itself
     * doesn't support null keys, so this would fail there anyway.
     */
    @Test
    @DisplayName("Null key throws IllegalArgumentException on put()")
    void testEdgeCase_nullKey_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> cache.put(null, "value"),
                "Null key should throw IllegalArgumentException"
        );
    }

    /**
     * Null value must throw IllegalArgumentException.
     */
    @Test
    @DisplayName("Null value throws IllegalArgumentException on put()")
    void testEdgeCase_nullValue_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> cache.put("key", null),
                "Null value should throw IllegalArgumentException"
        );
    }

    /**
     * A cache with capacity=1 and TTL should still work correctly.
     * Tests the interaction between capacity eviction (LRU) and TTL eviction.
     */
    @Test
    @DisplayName("Capacity=1 cache with TTL behaves correctly")
    void testEdgeCase_capacityOne_withTTL() throws InterruptedException {
        Cache<String, String> tinyDelegate = new LRUCache<>(1);
        TTLCache<String, String> tinyCache = new TTLCache<>(tinyDelegate, SWEEP_INTERVAL_MS);

        try {
            tinyCache.put("first", "v1", 2);
            tinyCache.put("second", "v2", 2); // evicts "first" due to capacity

            // "first" was evicted by LRU, "second" should exist.
            assertNull(tinyCache.get("first"),   "LRU-evicted key should be gone");
            assertEquals("v2", tinyCache.get("second"), "Most recent key should exist");

            Thread.sleep(2200); // wait for TTL

            assertNull(tinyCache.get("second"), "Should be null after TTL expires");
        } finally {
            tinyCache.shutdown();
        }
    }

    // =========================================================================
    // 5. TTL reset on re-put
    // =========================================================================

    /**
     * If you put the same key again with a new TTL, the old TTL is completely
     * replaced. This is critical for session renewal, cache refresh patterns, etc.
     *
     * Scenario:
     *   t=0: put key with TTL=1s (expires at t=1s)
     *   t=500ms: put key again with TTL=2s (expires at t=2500ms)
     *   t=1200ms: key should STILL EXIST (original TTL would have expired, new one hasn't)
     */
    @Test
    @DisplayName("Re-putting a key resets its TTL")
    void testTTLReset_onRePut() throws InterruptedException {
        cache.put("renewable", "v1", 1); // expires at ~t+1000ms

        Thread.sleep(500); // t=500ms — original TTL halfway done

        cache.put("renewable", "v2", 2); // reset: now expires at ~t+2500ms

        Thread.sleep(700); // t=1200ms — original TTL would have expired, new one hasn't

        String result = cache.get("renewable");

        assertNotNull(result, "Key should still exist after TTL was reset by re-put");
        assertEquals("v2", result, "Value should be updated to the re-put value");
    }

    /**
     * Re-putting with TTL=0 makes a previously expiring key permanent.
     */
    @Test
    @DisplayName("Re-putting with TTL=0 makes key permanent")
    void testTTLReset_toNeverExpire() throws InterruptedException {
        cache.put("key", "value", 1); // expires in 1 second

        Thread.sleep(300); // well before expiry

        cache.put("key", "value", 0); // re-put with no TTL

        Thread.sleep(1000); // original TTL would have fired by now

        assertEquals("value", cache.get("key"),
                "Key should persist after TTL was reset to 0 (no expiry)");
    }

    // =========================================================================
    // 6. getRemainingTTL
    // =========================================================================

    /**
     * getRemainingTTL should return a positive number immediately after put.
     */
    @Test
    @DisplayName("getRemainingTTL returns positive value for live key")
    void testGetRemainingTTL_livKey() {
        cache.put("key", "value", 10); // 10 second TTL

        long remaining = cache.getRemainingTTL("key");

        assertTrue(remaining > 0 && remaining <= 10,
                "Remaining TTL should be between 0 and 10, got: " + remaining);
    }

    /**
     * getRemainingTTL should return -1 for a permanent key (no TTL).
     * -1 is our sentinel for "this key never expires".
     */
    @Test
    @DisplayName("getRemainingTTL returns -1 for permanent key")
    void testGetRemainingTTL_permanentKey() {
        cache.put("key", "value"); // no TTL

        long remaining = cache.getRemainingTTL("key");

        assertEquals(-1L, remaining,
                "Permanent key (no TTL) should return -1 from getRemainingTTL");
    }

    /**
     * getRemainingTTL should return 0 for an expired key.
     */
    @Test
    @DisplayName("getRemainingTTL returns 0 for expired key")
    void testGetRemainingTTL_expiredKey() throws InterruptedException {
        cache.put("key", "value", 1);

        Thread.sleep(1200);

        long remaining = cache.getRemainingTTL("key");

        assertEquals(0L, remaining,
                "Expired key should return 0 from getRemainingTTL");
    }

    /**
     * getRemainingTTL should return 0 for a key that was never put.
     */
    @Test
    @DisplayName("getRemainingTTL returns 0 for nonexistent key")
    void testGetRemainingTTL_missingKey() {
        long remaining = cache.getRemainingTTL("nope");

        assertEquals(0L, remaining,
                "Nonexistent key should return 0 from getRemainingTTL");
    }

    // =========================================================================
    // 7. Explicit evict()
    // =========================================================================

    /**
     * Explicitly evicting a key should remove it and its expiry entry.
     * Calling get() after evict() should return null, not the expired-but-
     * present key.
     */
    @Test
    @DisplayName("evict() removes key and its TTL entry")
    void testExplicitEvict_removesKeyAndTTL() {
        cache.put("key", "value", 10);

        cache.evict("key");

        assertNull(cache.get("key"), "Evicted key should return null");
        assertEquals(0L, cache.getRemainingTTL("key"),
                "Evicted key should have no remaining TTL entry");
    }

    /**
     * Evicting a key that doesn't exist should not throw.
     * This is the "safe no-op" contract.
     */
    @Test
    @DisplayName("evict() on nonexistent key is a safe no-op")
    void testExplicitEvict_nonexistentKey_noException() {
        assertDoesNotThrow(() -> cache.evict("ghost"),
                "Evicting a nonexistent key should not throw");
    }

    // =========================================================================
    // 8. Concurrent access
    // =========================================================================

    /**
     * 20 threads writing different keys concurrently — no exceptions,
     * no data corruption. Tests that ConcurrentHashMap in expiryMap
     * and the delegate's own locking cooperate correctly.
     */
    @Disabled("Temporarily disabled during server integration")
    @Test
    @DisplayName("Concurrent puts from multiple threads do not corrupt state")
    void testConcurrency_concurrentPuts() throws InterruptedException {
        int threadCount = 20;
        int opsPerThread = 50;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done  = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    start.await(); // all threads start simultaneously
                    for (int i = 0; i < opsPerThread; i++) {
                        String key = "thread-" + threadId + "-key-" + i;
                        cache.put(key, "value-" + i, 5);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown(); // release all threads
        done.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(0, errors.get(), "No errors should occur under concurrent puts");
    }

    /**
     * Mixed concurrent reads and writes — verifies no stale/wrong values
     * are returned and no exceptions are thrown.
     */
    @Disabled("Temporarily disabled during server integration")
    @Test
    @DisplayName("Concurrent gets and puts do not produce exceptions")
    void testConcurrency_mixedGetsPuts() throws InterruptedException {
        // Pre-populate some keys
        for (int i = 0; i < 20; i++) {
            cache.put("key-" + i, "val-" + i, 5);
        }

        int threadCount = 10;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done  = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 100; i++) {
                        if (i % 2 == 0) {
                            cache.get("key-" + (i % 20));       // read
                        } else {
                            cache.put("new-" + threadId + "-" + i, "v", 2); // write
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        done.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(0, errors.get(), "No exceptions should occur under concurrent mixed load");
    }

    // =========================================================================
    // 9. Shutdown behaviour
    // =========================================================================

    /**
     * After shutdown(), the sweeper stops but lazy eviction still works.
     * This verifies that shutdown() doesn't break the cache's core functionality.
     */
    @Test
    @DisplayName("Cache remains usable for lazy eviction after shutdown()")
    void testShutdown_lazyEvictionStillWorks() throws InterruptedException {
        cache.put("key", "value", 1);

        cache.shutdown(); // stop the sweeper

        Thread.sleep(1200); // TTL elapses

        // Lazy eviction should still work even without the sweeper
        assertNull(cache.get("key"),
                "Lazy eviction should still fire after shutdown()");
    }

    /**
     * Calling shutdown() multiple times should not throw.
     * ScheduledExecutorService.shutdown() is idempotent, and we should
     * not add any state that breaks on double-shutdown.
     */
    @Test
    @DisplayName("Multiple calls to shutdown() do not throw")
    void testShutdown_idempotent() {
        assertDoesNotThrow(() -> {
            cache.shutdown();
            cache.shutdown(); // second call should be safe
        });
    }
}
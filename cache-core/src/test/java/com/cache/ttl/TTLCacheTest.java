package com.cache.ttl;

import com.cache.api.Cache;
import com.cache.concurrent.CoarseGrainedCache;
import com.cache.concurrent.SegmentedCache;
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

@DisplayName("TTLCache Tests")
@Execution(ExecutionMode.SAME_THREAD)
class TTLCacheTest {
    private static final long SWEEP_INTERVAL_MS = 50L;

    private static final long SHORT_TTL_SECONDS = 0L;

    private TTLCache<String, String> cache;

    @BeforeEach
    void setUp() {
        Cache<String, String> lruDelegate = new SegmentedCache<>(100);
        cache = new TTLCache<>(new SegmentedCache<>(100), SWEEP_INTERVAL_MS);
    }

    @AfterEach
    void tearDown() {
        cache.shutdown();
    }

    @Test
    @DisplayName("get() returns value for key with no TTL")
    void testBasicPutAndGet_noTTL() {
        cache.put("name", "Alice");

        String result = cache.get("name");

        assertEquals("Alice", result, "Should return value when no TTL is set");
    }

    @Test
    @DisplayName("get() returns null for key that was never put")
    void testGet_missingKey_returnsNull() {
        String result = cache.get("nonexistent");

        assertNull(result, "Missing key should return null");
    }

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

    @Test
    @DisplayName("get() returns value before TTL expires")
    void testLazyEviction_beforeExpiry() {
        cache.put("token", "abc123", 2);

        String result = cache.get("token");

        assertEquals("abc123", result, "Key should exist before TTL expires");
    }

    @Test
    @DisplayName("get() returns null after TTL expires (lazy eviction)")
    void testLazyEviction_afterExpiry() throws InterruptedException {
        cache.put("session", "user42", 1);

        Thread.sleep(1200);

        String result = cache.get("session");

        assertNull(result, "Key should return null after TTL has elapsed");
    }

    @Test
    @DisplayName("size() decreases after lazy eviction fires")
    void testLazyEviction_decreasesSize() throws InterruptedException {
        cache.put("temp", "value", 1);
        assertEquals(1, cache.size(), "Size should be 1 after put");

        Thread.sleep(1200);

        cache.get("temp");

        assertEquals(0, cache.size(), "Size should be 0 after lazy eviction");
    }

    @Test
    @DisplayName("Lazy eviction of one key doesn't affect other keys")
    void testLazyEviction_doesNotAffectOtherKeys() throws InterruptedException {
        cache.put("expiring", "bye", 1);
        cache.put("permanent", "stays");

        Thread.sleep(1200);

        assertNull(cache.get("expiring"),    "Expired key should be null");
        assertEquals("stays", cache.get("permanent"), "Permanent key should survive");
    }

    @Test
    @DisplayName("Background sweeper removes expired keys from delegate")
    void testSweeper_removesExpiredKeys() throws InterruptedException {
        cache.put("ephemeral", "data", 1);
        assertEquals(1, cache.size());

        Thread.sleep(1200);

        assertEquals(0, cache.size(),
                "Sweeper should have removed expired key from delegate");
    }

    @Test
    @DisplayName("Sweeper increments sweep eviction counter")
    void testSweeper_evictionCounterIncrements() throws InterruptedException {
        cache.put("k1", "v1", 1);
        cache.put("k2", "v2", 1);

        long before = cache.getSweepEvictionCount();

        Thread.sleep(1300);

        long after = cache.getSweepEvictionCount();

        assertTrue(after - before >= 2,
                "Sweeper should have counted at least 2 evictions, got: " + (after - before));
    }

    @Test
    @DisplayName("Sweeper does not remove permanent keys (no TTL)")
    void testSweeper_doesNotRemovePermanentKeys() throws InterruptedException {
        cache.put("forever", "value");

        Thread.sleep(200);

        assertEquals("value", cache.get("forever"),
                "Permanent key must not be removed by sweeper");
    }

    @Test
    @DisplayName("TTL=0 means no expiry: key persists indefinitely")
    void testEdgeCase_ttlZeroMeansNoExpiry() throws InterruptedException {
        cache.put("persistent", "value", 0);

        Thread.sleep(200);

        assertEquals("value", cache.get("persistent"),
                "TTL=0 should mean no expiry, key must still be accessible");
    }

    @Test
    @DisplayName("Negative TTL throws IllegalArgumentException")
    void testEdgeCase_negativeTTL_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> cache.put("key", "value", -1),
                "Negative TTL should throw IllegalArgumentException"
        );
    }

    @Test
    @DisplayName("Null key throws IllegalArgumentException on put()")
    void testEdgeCase_nullKey_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> cache.put(null, "value"),
                "Null key should throw IllegalArgumentException"
        );
    }

    @Test
    @DisplayName("Null value throws IllegalArgumentException on put()")
    void testEdgeCase_nullValue_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> cache.put("key", null),
                "Null value should throw IllegalArgumentException"
        );
    }

    @Test
    @DisplayName("Capacity=1 cache with TTL behaves correctly")
    void testEdgeCase_capacityOne_withTTL() throws InterruptedException {
        Cache<String, String> tinyDelegate = new LRUCache<>(1);
        TTLCache<String, String> tinyCache = new TTLCache<>(tinyDelegate, SWEEP_INTERVAL_MS);

        try {
            tinyCache.put("first", "v1", 2);
            tinyCache.put("second", "v2", 2);

            assertNull(tinyCache.get("first"),   "LRU-evicted key should be gone");
            assertEquals("v2", tinyCache.get("second"), "Most recent key should exist");

            Thread.sleep(2200);

            assertNull(tinyCache.get("second"), "Should be null after TTL expires");
        } finally {
            tinyCache.shutdown();
        }
    }

    @Test
    @DisplayName("Re-putting a key resets its TTL")
    void testTTLReset_onRePut() throws InterruptedException {
        cache.put("renewable", "v1", 1);

        Thread.sleep(500);

        cache.put("renewable", "v2", 2);

        Thread.sleep(700);

        String result = cache.get("renewable");

        assertNotNull(result, "Key should still exist after TTL was reset by re-put");
        assertEquals("v2", result, "Value should be updated to the re-put value");
    }

    @Test
    @DisplayName("Re-putting with TTL=0 makes key permanent")
    void testTTLReset_toNeverExpire() throws InterruptedException {
        cache.put("key", "value", 1);

        Thread.sleep(300);

        cache.put("key", "value", 0);

        Thread.sleep(1000);

        assertEquals("value", cache.get("key"),
                "Key should persist after TTL was reset to 0 (no expiry)");
    }

    @Test
    @DisplayName("getRemainingTTL returns positive value for live key")
    void testGetRemainingTTL_livKey() {
        cache.put("key", "value", 10);

        long remaining = cache.getRemainingTTL("key");

        assertTrue(remaining > 0 && remaining <= 10,
                "Remaining TTL should be between 0 and 10, got: " + remaining);
    }

    @Test
    @DisplayName("getRemainingTTL returns -1 for permanent key")
    void testGetRemainingTTL_permanentKey() {
        cache.put("key", "value");

        long remaining = cache.getRemainingTTL("key");

        assertEquals(-1L, remaining,
                "Permanent key (no TTL) should return -1 from getRemainingTTL");
    }

    @Test
    @DisplayName("getRemainingTTL returns 0 for expired key")
    void testGetRemainingTTL_expiredKey() throws InterruptedException {
        cache.put("key", "value", 1);

        Thread.sleep(1200);

        long remaining = cache.getRemainingTTL("key");

        assertEquals(0L, remaining,
                "Expired key should return 0 from getRemainingTTL");
    }

    @Test
    @DisplayName("getRemainingTTL returns 0 for nonexistent key")
    void testGetRemainingTTL_missingKey() {
        long remaining = cache.getRemainingTTL("nope");

        assertEquals(0L, remaining,
                "Nonexistent key should return 0 from getRemainingTTL");
    }

    @Test
    @DisplayName("evict() removes key and its TTL entry")
    void testExplicitEvict_removesKeyAndTTL() {
        cache.put("key", "value", 10);

        cache.evict("key");

        assertNull(cache.get("key"), "Evicted key should return null");
        assertEquals(0L, cache.getRemainingTTL("key"),
                "Evicted key should have no remaining TTL entry");
    }

    @Test
    @DisplayName("evict() on nonexistent key is a safe no-op")
    void testExplicitEvict_nonexistentKey_noException() {
        assertDoesNotThrow(() -> cache.evict("ghost"),
                "Evicting a nonexistent key should not throw");
    }

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
                    start.await();
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
        start.countDown();
        done.await(10, TimeUnit.SECONDS);
        pool.shutdown();
        assertEquals(0, errors.get(), "No errors should occur under concurrent puts");
    }

    @Test
    @DisplayName("Concurrent gets and puts do not produce exceptions")
    void testConcurrency_mixedGetsPuts() throws InterruptedException {
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
                            cache.get("key-" + (i % 20));
                        } else {
                            cache.put("new-" + threadId + "-" + i, "v", 2);
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

    @Test
    @DisplayName("Cache remains usable for lazy eviction after shutdown()")
    void testShutdown_lazyEvictionStillWorks() throws InterruptedException {
        cache.put("key", "value", 1);

        cache.shutdown();

        Thread.sleep(1200);

        assertNull(cache.get("key"),
                "Lazy eviction should still fire after shutdown()");
    }

    @Test
    @DisplayName("Multiple calls to shutdown() do not throw")
    void testShutdown_idempotent() {
        assertDoesNotThrow(() -> {
            cache.shutdown();
            cache.shutdown();
        });
    }

    @Test
    @DisplayName("expire() on a key that has already expired returns false")
    void testExpire_onExpiredKey_returnsFalse() throws InterruptedException {
        cache.put("k", "v", 1);
        Thread.sleep(1_100);
        assertFalse(cache.expire("k", 60));
        assertNull(cache.get("k"));
    }

    @Test
    @DisplayName("expire() with 0 deletes the key")
    void testExpire_zero_deletesKey() {
        cache.put("k", "v");
        assertTrue(cache.expire("k", 0));
        assertNull(cache.get("k"));
    }

    @Test
    @DisplayName("persist() returns false for a key without a TTL, but the key stays")
    void testPersist_withoutTtl() {
        cache.put("k", "v");
        assertFalse(cache.persist("k"));
        assertEquals("v", cache.get("k"));
    }

    @Test
    @DisplayName("Reading an expired key counts as a miss")
    void testExpiredRead_countsAsMiss() throws InterruptedException {
        cache.put("k", "v", 1);
        Thread.sleep(1_100);
        long missesBefore = cache.getStats().misses();
        cache.get("k");
        assertEquals(missesBefore + 1, cache.getStats().misses());
    }

    @Test
    @DisplayName("Keys evicted by the delegate do not appear in snapshots")
    void testSnapshot_excludesKeysEvictedByDelegate() {
        TTLCache<String, String> small = new TTLCache<>(new CoarseGrainedCache<>(new LRUCache<>(2)));
        try {
            for (int i = 0; i < 100; i++) {
                small.put("k" + i, "v");
            }
            assertEquals(2, small.getSnapshotEntries().size());
            assertEquals(0, small.getRemainingTTL("k0"));
        } finally {
            small.shutdown();
        }
    }

    @Test
    @DisplayName("Default TTL applies to put(key, value)")
    void testDefaultTtl() throws InterruptedException {
        TTLCache<String, String> withDefault =
                new TTLCache<>(new SegmentedCache<>(10), 100, java.time.Duration.ofMillis(300));
        try {
            withDefault.put("k", "v");
            withDefault.put("forever", "v", 0);
            Thread.sleep(500);
            assertNull(withDefault.get("k"));
            assertEquals("v", withDefault.get("forever"));
        } finally {
            withDefault.shutdown();
        }
    }
}

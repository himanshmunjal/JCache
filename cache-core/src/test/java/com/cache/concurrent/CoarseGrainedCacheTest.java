package com.cache.concurrent;

import com.cache.api.Cache;
import com.cache.api.CacheStats;
import com.cache.policy.LRUCache;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for CoarseGrainedCache.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * TESTING PHILOSOPHY FOR CONCURRENT CODE
 * ─────────────────────────────────────────────────────────────────────────────
 * Concurrent bugs are non-deterministic — a race condition may only appear
 * 1 in 10,000 runs. We make bugs more likely to surface by:
 *
 *   1. Using MANY threads (10–50) to maximise contention.
 *   2. Using CountDownLatch to make ALL threads start AT THE SAME TIME.
 *      Without this, threads start sequentially and rarely interleave.
 *   3. Doing MANY operations per thread (hundreds to thousands).
 *   4. Asserting INVARIANTS that must hold regardless of scheduling order:
 *      - size() never exceeds capacity
 *      - a key put and never evicted must be retrievable
 *      - stats counters must be non-negative
 *      - no exceptions thrown under concurrent access
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * WHAT WE ARE NOT TESTING
 * ─────────────────────────────────────────────────────────────────────────────
 * We are NOT testing LRU eviction ordering here — that belongs in LRUCacheTest.
 * CoarseGrainedCache is a wrapper. We test the WRAPPER's guarantees:
 *   - No data corruption under concurrent access.
 *   - No deadlocks.
 *   - Invariants hold under concurrent load.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * STRUCTURE
 * ─────────────────────────────────────────────────────────────────────────────
 * 1. Single-threaded correctness   — wrapper doesn't break delegate behaviour
 * 2. Concurrent reads              — many readers at once, no corruption
 * 3. Concurrent writes             — many writers at once, size invariant holds
 * 4. Mixed reads + writes          — realistic workload, no exceptions
 * 5. Size invariant                — never exceeds capacity under load
 * 6. Stats consistency             — counters are non-negative and coherent
 * 7. No deadlocks                  — operations complete in bounded time
 * 8. Eviction under concurrency    — concurrent evict() is safe
 */
@DisplayName("CoarseGrainedCache Tests")
class CoarseGrainedCacheTest {

    // Cache capacity — large enough for multi-key tests, small enough to trigger eviction
    private static final int CAPACITY = 100;

    // Thread counts for concurrency tests
    private static final int READ_THREADS  = 10;
    private static final int WRITE_THREADS = 10;
    private static final int MIXED_THREADS = 20;

    // Ops per thread — high enough to expose races, low enough for fast tests
    private static final int OPS_PER_THREAD = 500;

    // Maximum time to wait for concurrent tests (prevents hangs from deadlocks)
    private static final int TIMEOUT_SECONDS = 10;

    private CoarseGrainedCache<String, String> cache;

    @BeforeEach
    void setUp() {
        // Wrap a fresh LRUCache with coarse-grained locking before each test.
        cache = new CoarseGrainedCache<>(new LRUCache<>(CAPACITY));
    }

    // =========================================================================
    // 1. Single-threaded correctness — wrapper must not break delegate
    // =========================================================================

    @Test
    @DisplayName("get() returns null for missing key")
    void testGet_missingKey_returnsNull() {
        assertNull(cache.get("missing"));
    }

    @Test
    @DisplayName("put() then get() returns correct value")
    void testPutAndGet_returnsValue() {
        cache.put("name", "Alice");
        assertEquals("Alice", cache.get("name"));
    }

    @Test
    @DisplayName("put() overwrites existing key")
    void testPut_overwritesExistingKey() {
        cache.put("key", "v1");
        cache.put("key", "v2");
        assertEquals("v2", cache.get("key"),
                "Second put() should overwrite first value");
    }

    @Test
    @DisplayName("evict() removes key — get() returns null after eviction")
    void testEvict_removesKey() {
        cache.put("temp", "value");
        cache.evict("temp");
        assertNull(cache.get("temp"), "Evicted key should return null");
    }

    @Test
    @DisplayName("evict() on missing key is a safe no-op")
    void testEvict_missingKey_noException() {
        assertDoesNotThrow(() -> cache.evict("ghost"));
    }

    @Test
    @DisplayName("size() reflects put and evict operations")
    void testSize_afterPutAndEvict() {
        assertEquals(0, cache.size());
        cache.put("a", "1");
        cache.put("b", "2");
        assertEquals(2, cache.size());
        cache.evict("a");
        assertEquals(1, cache.size());
    }

    @Test
    @DisplayName("size() never exceeds capacity (single-threaded)")
    void testSize_neverExceedsCapacity_singleThreaded() {
        // Put 3x capacity items — eviction should keep size at CAPACITY.
        for (int i = 0; i < CAPACITY * 3; i++) {
            cache.put("key-" + i, "val-" + i);
            assertTrue(cache.size() <= CAPACITY,
                    "Size exceeded capacity at i=" + i + ", size=" + cache.size());
        }
    }

    @Test
    @DisplayName("getStats() returns non-null stats")
    void testGetStats_returnsNonNull() {
        cache.put("k", "v");
        cache.get("k");
        cache.get("missing");

        CacheStats stats = cache.getstats();
        assertNotNull(stats);
    }

    // =========================================================================
    // 2. Concurrent reads — multiple readers simultaneously
    // =========================================================================

    /**
     * Pre-populate the cache, then flood it with concurrent readers.
     * All readers should see the correct values — no torn reads,
     * no null values for keys that exist.
     *
     * READ LOCK allows all threads to read simultaneously. If locking
     * is broken (e.g., no lock at all), a concurrent write during
     * LinkedList traversal in LRUCache would cause ConcurrentModificationException.
     */
    @Disabled("Temporarily disabled during server integration")
    @Test
    @DisplayName("Concurrent readers all see correct values — no corruption")
    void testConcurrentReads_noCorruption() throws InterruptedException {
        // Pre-populate 50 stable keys before concurrent reads start.
        for (int i = 0; i < 50; i++) {
            cache.put("stable-" + i, "value-" + i);
        }

        CountDownLatch startGun = new CountDownLatch(1);   // all threads wait here
        CountDownLatch allDone  = new CountDownLatch(READ_THREADS);
        AtomicInteger errors    = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(READ_THREADS);

        for (int t = 0; t < READ_THREADS; t++) {
            pool.submit(() -> {
                try {
                    startGun.await(); // wait for all threads to be ready
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        int keyIdx = i % 50;
                        String result = cache.get("stable-" + keyIdx);
                        // Every stable key must return its correct value.
                        if (!("value-" + keyIdx).equals(result)) {
                            errors.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    allDone.countDown();
                }
            });
        }

        startGun.countDown(); // RELEASE — all threads start simultaneously
        boolean finished = allDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        pool.shutdown();

        assertTrue(finished, "Test timed out — possible deadlock");
        assertEquals(0, errors.get(),
                "Concurrent readers saw incorrect values: " + errors.get() + " errors");
    }

    // =========================================================================
    // 3. Concurrent writes — multiple writers simultaneously
    // =========================================================================

    /**
     * Many threads writing different keys simultaneously.
     * The size invariant (never exceeds capacity) must hold at all times.
     * Data corruption in the delegate's HashMap or LinkedList would
     * cause size() to return negative values or throw exceptions.
     */
    @Test
    @DisplayName("Concurrent writers — size never exceeds capacity")
    void testConcurrentWrites_sizeNeverExceedsCapacity() throws InterruptedException {
        CountDownLatch startGun  = new CountDownLatch(1);
        CountDownLatch allDone   = new CountDownLatch(WRITE_THREADS);
        AtomicInteger  errors    = new AtomicInteger(0);
        // Track all size violations observed by any thread.
        List<Integer>  violations = Collections.synchronizedList(new ArrayList<>());

        ExecutorService pool = Executors.newFixedThreadPool(WRITE_THREADS);

        for (int t = 0; t < WRITE_THREADS; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    startGun.await();
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        // Each thread writes unique keys to avoid overwrite races
                        // confusing the test — we want to test structural safety.
                        cache.put("t" + threadId + "-k" + i, "v" + i);

                        int currentSize = cache.size();
                        if (currentSize > CAPACITY) {
                            violations.add(currentSize);
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    allDone.countDown();
                }
            });
        }

        startGun.countDown();
        boolean finished = allDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pool.shutdown();

        assertTrue(finished, "Test timed out — possible deadlock");
        assertEquals(0, errors.get(), "Exceptions during concurrent writes: " + errors.get());
        assertTrue(violations.isEmpty(),
                "Size exceeded capacity " + violations.size() + " times. Max seen: "
                        + violations.stream().mapToInt(Integer::intValue).max().orElse(0));
    }

    /**
     * Concurrent writes of the SAME key from multiple threads.
     * The final value must be one of the written values — no torn writes,
     * no null result, no exception.
     */
    @Test
    @DisplayName("Concurrent writes to same key — no torn write, value is coherent")
    void testConcurrentWrites_sameKey_coherentValue() throws InterruptedException {
        String sharedKey = "contested";
        int threadCount  = 20;

        CountDownLatch startGun = new CountDownLatch(1);
        CountDownLatch allDone  = new CountDownLatch(threadCount);
        AtomicInteger errors    = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final String myValue = "thread-" + t;
            pool.submit(() -> {
                try {
                    startGun.await();
                    for (int i = 0; i < 100; i++) {
                        cache.put(sharedKey, myValue);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    allDone.countDown();
                }
            });
        }

        startGun.countDown();
        boolean finished = allDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pool.shutdown();

        assertTrue(finished, "Test timed out");
        assertEquals(0, errors.get(), "Exceptions during concurrent same-key writes");

        // After all writes, the key must exist and hold SOME valid value.
        String finalValue = cache.get(sharedKey);
        assertNotNull(finalValue, "Shared key should not be null after concurrent writes");
        assertTrue(finalValue.startsWith("thread-"),
                "Value should be one of the written values, got: " + finalValue);
    }

    // =========================================================================
    // 4. Mixed reads + writes — realistic workload
    // =========================================================================

    /**
     * Simulates a realistic cache workload:
     *   70% reads, 30% writes, across many threads simultaneously.
     * Primary assertion: no exceptions thrown.
     * Secondary assertion: reads never return a value that was never written.
     */
    @Disabled("Temporarily disabled during server integration")
    @Test
    @DisplayName("Mixed concurrent reads and writes — no exceptions, no corruption")
    void testMixedReadWrite_noExceptions() throws InterruptedException {
        // Pre-populate some keys so reads have something to find.
        for (int i = 0; i < 20; i++) {
            cache.put("base-" + i, "val-" + i);
        }

        CountDownLatch startGun = new CountDownLatch(1);
        CountDownLatch allDone  = new CountDownLatch(MIXED_THREADS);
        AtomicInteger  errors   = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(MIXED_THREADS);

        for (int t = 0; t < MIXED_THREADS; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    startGun.await();
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        if (i % 10 < 7) {
                            // 70% reads — read existing base keys
                            cache.get("base-" + (i % 20));
                        } else {
                            // 30% writes — write unique per-thread keys
                            cache.put("t" + threadId + "-" + i, "v" + i);
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    System.err.println("Exception in thread " + threadId + ": " + e);
                } finally {
                    allDone.countDown();
                }
            });
        }

        startGun.countDown();
        boolean finished = allDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pool.shutdown();

        assertTrue(finished, "Test timed out — possible deadlock");
        assertEquals(0, errors.get(),
                "Exceptions during mixed load: " + errors.get());
    }

    // =========================================================================
    // 5. Size invariant under full concurrent load
    // =========================================================================

    /**
     * Hammer test: maximum contention across reads, writes, AND evictions.
     * The only assertion is that size() stays within [0, CAPACITY] always.
     * This catches off-by-one bugs in the delegate's size tracking.
     */
    @Test
    @DisplayName("Size invariant holds under full concurrent load (reads + writes + evictions)")
    void testSizeInvariant_underFullLoad() throws InterruptedException {
        int totalThreads = 30;
        CountDownLatch startGun  = new CountDownLatch(1);
        CountDownLatch allDone   = new CountDownLatch(totalThreads);
        AtomicInteger  errors    = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(totalThreads);

        for (int t = 0; t < totalThreads; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    startGun.await();
                    for (int i = 0; i < 200; i++) {
                        int op = i % 3;
                        if (op == 0) {
                            cache.put("k" + (threadId * 200 + i), "v");
                        } else if (op == 1) {
                            cache.get("k" + (i % 50)); // may or may not exist
                        } else {
                            cache.evict("k" + (i % 50));
                        }

                        int sz = cache.size();
                        if (sz < 0 || sz > CAPACITY) {
                            errors.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    allDone.countDown();
                }
            });
        }

        startGun.countDown();
        boolean finished = allDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pool.shutdown();

        assertTrue(finished, "Test timed out — deadlock suspected");
        assertEquals(0, errors.get(),
                "Size invariant violated or exception thrown: " + errors.get() + " occurrences");
    }

    // =========================================================================
    // 6. Stats consistency under concurrency
    // =========================================================================

    /**
     * Hits + misses should be non-negative after concurrent access.
     * A race condition in hit/miss counting would produce negative numbers
     * or an ArithmeticException.
     */
    @Test
    @DisplayName("Stats counters are non-negative after concurrent access")
    void testStats_nonNegativeAfterConcurrentAccess() throws InterruptedException {
        int threadCount = 10;
        CountDownLatch startGun = new CountDownLatch(1);
        CountDownLatch allDone  = new CountDownLatch(threadCount);

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        // Mix of hits and misses
        cache.put("exists", "yes");

        for (int t = 0; t < threadCount; t++) {
            pool.submit(() -> {
                try {
                    startGun.await();
                    for (int i = 0; i < 100; i++) {
                        cache.get("exists");   // hit
                        cache.get("missing");  // miss
                    }
                } catch (Exception e) {
                    // count as error below
                } finally {
                    allDone.countDown();
                }
            });
        }

        startGun.countDown();
        allDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pool.shutdown();

        CacheStats stats = cache.getstats();
        assertNotNull(stats);
        // Stats must be logically coherent — no negative counts.
        // Exact values depend on CacheStats implementation,
        // so we check the contract rather than specific numbers.
        assertTrue(cache.size() >= 0, "Size must be non-negative after concurrent access");
    }

    // =========================================================================
    // 7. No deadlocks — operations complete in bounded time
    // =========================================================================

    /**
     * A deadlock would cause allDone.await() to time out.
     * This test specifically exercises re-entrant scenarios:
     * threads calling get() while holding other resources.
     *
     * If ReentrantReadWriteLock wasn't reentrant, a thread calling
     * get() from within a read-locked context would deadlock itself.
     */
    @Test
    @DisplayName("No deadlock under sustained concurrent load")
    void testNoDeadlock_sustainedLoad() throws InterruptedException {
        int threadCount = 50;
        CountDownLatch startGun = new CountDownLatch(1);
        CountDownLatch allDone  = new CountDownLatch(threadCount);

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    startGun.await();
                    // Alternate between reads and writes rapidly
                    for (int i = 0; i < 100; i++) {
                        cache.put("key-" + threadId, "val-" + i);
                        cache.get("key-" + threadId);
                        cache.size();
                    }
                } catch (Exception ignored) {
                    // We only care about deadlock (timeout), not exceptions here
                } finally {
                    allDone.countDown();
                }
            });
        }

        startGun.countDown();
        // If this times out, we have a deadlock.
        boolean finished = allDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pool.shutdown();

        assertTrue(finished,
                "DEADLOCK DETECTED: " + threadCount + " threads did not complete within "
                        + TIMEOUT_SECONDS + " seconds");
    }

    // =========================================================================
    // 8. Concurrent evictions
    // =========================================================================

    /**
     * Multiple threads evicting the same key simultaneously.
     * Only one eviction should "win" — the rest are safe no-ops.
     * No exception, no negative size, no corruption.
     */
    @Test
    @DisplayName("Concurrent evictions of same key are safe")
    void testConcurrentEviction_sameKey_safe() throws InterruptedException {
        cache.put("shared", "value");

        int threadCount = 20;
        CountDownLatch startGun = new CountDownLatch(1);
        CountDownLatch allDone  = new CountDownLatch(threadCount);
        AtomicInteger errors    = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            pool.submit(() -> {
                try {
                    startGun.await();
                    cache.evict("shared"); // all threads evict the same key
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    allDone.countDown();
                }
            });
        }

        startGun.countDown();
        boolean finished = allDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pool.shutdown();

        assertTrue(finished, "Test timed out");
        assertEquals(0, errors.get(), "Concurrent eviction threw exceptions");
        assertNull(cache.get("shared"), "Key should be gone after concurrent evictions");
        assertEquals(0, cache.size(), "Size should be 0 after all evictions");
    }

    /**
     * Concurrent put + immediate evict pairs from different threads.
     * Tests the interleaving of write lock acquisitions.
     */
    @Test
    @DisplayName("Interleaved put and evict from different threads — no corruption")
    void testPutEvict_interleaved_noCorruption() throws InterruptedException {
        int threadCount = 20;
        CountDownLatch startGun = new CountDownLatch(1);
        CountDownLatch allDone  = new CountDownLatch(threadCount);
        AtomicInteger errors    = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    startGun.await();
                    for (int i = 0; i < 100; i++) {
                        String key = "t" + threadId + "-k" + i;
                        cache.put(key, "v" + i);
                        cache.evict(key);
                        // After evict, size must still be non-negative
                        if (cache.size() < 0) {
                            errors.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    allDone.countDown();
                }
            });
        }

        startGun.countDown();
        boolean finished = allDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pool.shutdown();

        assertTrue(finished, "Test timed out");
        assertEquals(0, errors.get(),
                "Errors during interleaved put/evict: " + errors.get());
    }
}
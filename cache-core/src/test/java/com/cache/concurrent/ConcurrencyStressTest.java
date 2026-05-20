package com.cache.concurrent;

import com.cache.api.Cache;
import com.cache.api.CacheStats;
import com.cache.policy.LRUCache;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ConcurrencyStressTest — multi-threaded correctness verification for all
 * three concurrency strategies: coarse-grained, segmented, and lock-free.
 *
 * -------------------------------------------------------------------------
 * WHAT WE'RE TESTING AND WHY
 * -------------------------------------------------------------------------
 * Unit tests run on a single thread. They verify logic but cannot find:
 *
 *   - Race conditions  : two threads interleave in a way that corrupts state.
 *   - Deadlocks        : threads waiting on each other forever.
 *   - Liveness issues  : one thread starves indefinitely.
 *   - Visibility bugs  : a write by thread A is not seen by thread B
 *                        because the JVM cached the value in a register.
 *   - ABA problems     : a CAS succeeds because value looks the same
 *                        but was changed and changed back in between.
 *
 * Stress tests use MANY threads and MANY operations to surface these bugs
 * probabilistically. They don't PROVE correctness (only formal verification
 * does that), but they make bugs extremely likely to manifest.
 *
 * -------------------------------------------------------------------------
 * TEST STRUCTURE
 * -------------------------------------------------------------------------
 * Each test follows the same pattern:
 *
 *   1. Create N threads. Give each a CountDownLatch to start simultaneously.
 *   2. All threads race to perform operations.
 *   3. Wait for all threads to finish (with timeout to catch deadlocks).
 *   4. Assert invariants on the final state.
 *
 * KEY INVARIANTS we verify:
 *   - size() never exceeds capacity.
 *   - size() never goes negative.
 *   - No exceptions thrown under concurrent load.
 *   - Stats are non-negative and internally consistent.
 *   - A key put by thread A is retrievable (eventually) by thread B.
 *
 * -------------------------------------------------------------------------
 * PARAMETERIZED APPROACH
 * -------------------------------------------------------------------------
 * We run the same stress tests against all three cache implementations
 * using @ParameterizedTest + @MethodSource. This ensures we don't test
 * each strategy in isolation and miss cross-strategy comparisons.
 *
 * If a test passes for CoarseGrainedCache but fails for LockFreeCache,
 * we've found a bug in the lock-free implementation specifically.
 */
@DisplayName("Concurrency Stress Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ConcurrencyStressTest {

    // -------------------------------------------------------------------------
    // Configuration constants
    // -------------------------------------------------------------------------

    /** Number of threads in heavy stress tests. Keep ≤ 50 for CI speed. */
    private static final int THREAD_COUNT = 30;

    /** Operations each thread performs. 1000 * 30 threads = 30k total ops. */
    private static final int OPS_PER_THREAD = 1000;

    /** Cache capacity — small enough to force evictions, large enough to test. */
    private static final int CACHE_CAPACITY = 100;

    /**
     * Maximum time to wait for all threads to finish.
     * If exceeded: test fails with a clear "deadlock suspected" message.
     * 30 seconds is generous — normal runs complete in < 2 seconds.
     */
    private static final int TIMEOUT_SECONDS = 30;

    // -------------------------------------------------------------------------
    // Test parameter provider
    // -------------------------------------------------------------------------

    /**
     * Provides all three cache implementations as test parameters.
     * Each test annotated with @ParameterizedTest + @MethodSource("cacheProvider")
     * runs three times — once per implementation.
     *
     * Named display: IntelliJ shows "CoarseGrainedCache", "SegmentedCache",
     * "LockFreeCache" as individual test entries.
     */
    static Stream<Cache<String, String>> cacheProvider() {
        return Stream.of(
                new CoarseGrainedCache<>(new LRUCache<>(CACHE_CAPACITY)),
                new SegmentedCache<>(CACHE_CAPACITY, 16),
                new LockFreeCache<>(CACHE_CAPACITY)
        );
    }

    // -------------------------------------------------------------------------
    // Helper: build a display-friendly name for a cache type
    // -------------------------------------------------------------------------

    private static String cacheName(Cache<?, ?> cache) {
        return cache.getClass().getSimpleName();
    }

    // -------------------------------------------------------------------------
    // Test 1: No exceptions under concurrent puts
    // -------------------------------------------------------------------------

    /**
     * 30 threads each insert 1000 unique keys concurrently.
     * Total = 30,000 puts with no reads or deletes.
     *
     * What can go wrong without proper synchronization:
     *   - ConcurrentModificationException during internal resize.
     *   - NullPointerException from partially-constructed nodes.
     *   - ArrayIndexOutOfBoundsException from unsynchronized array access.
     *
     * Invariants checked:
     *   - Zero exceptions thrown.
     *   - size() ≤ capacity (eviction must have kept up).
     *   - size() ≥ 0 (never corrupt to negative).
     */
    @Disabled("Temporarily disabled during server integration")
    @ParameterizedTest(name = "No exceptions under concurrent puts — {0}")
    @MethodSource("cacheProvider")
    @Order(1)
    @DisplayName("Concurrent puts: no exceptions, size within bounds")
    void testConcurrentPuts_noExceptions_sizeWithinBounds(Cache<String, String> cache)
            throws InterruptedException {

        AtomicInteger exceptionCount = new AtomicInteger(0);
        CountDownLatch startGate     = new CountDownLatch(1);
        CountDownLatch doneLatch     = new CountDownLatch(THREAD_COUNT);

        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    // All threads wait here until startGate opens.
                    // This maximises contention — all threads hit the cache
                    // simultaneously rather than staggered.
                    startGate.await();

                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        // Each thread uses unique keys to avoid key collisions
                        // (we test that separately).
                        String key   = "thread-" + threadId + "-key-" + i;
                        String value = "val-" + i;
                        cache.put(key, value);
                    }
                } catch (Exception e) {
                    // Count but don't rethrow — rethrow would kill only
                    // this thread, hiding the error from the main thread.
                    exceptionCount.incrementAndGet();
                    System.err.println("[" + cacheName(cache) + "] Exception in put: "
                            + e.getClass().getSimpleName() + ": " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startGate.countDown(); // release all threads simultaneously

        boolean finished = doneLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        pool.shutdown();

        // Deadlock detection: if threads didn't finish, we have a liveness problem.
        assertTrue(finished,
                "[" + cacheName(cache) + "] Threads did not finish within " +
                        TIMEOUT_SECONDS + "s — possible deadlock");

        assertEquals(0, exceptionCount.get(),
                "[" + cacheName(cache) + "] Exceptions occurred during concurrent puts");

        int finalSize = cache.size();
        assertTrue(finalSize >= 0,
                "[" + cacheName(cache) + "] size() went negative: " + finalSize);
        assertTrue(finalSize <= CACHE_CAPACITY,
                "[" + cacheName(cache) + "] size() exceeded capacity: " + finalSize);
    }

    // -------------------------------------------------------------------------
    // Test 2: Concurrent reads and writes mixed
    // -------------------------------------------------------------------------

    /**
     * 30 threads performing mixed reads (60%) and writes (40%) concurrently.
     *
     * Read-write mixing is where visibility bugs hide. Without volatile/atomic
     * fields, a write by thread A might not be visible to thread B reading
     * on a different CPU core (due to CPU cache lines and the JMM).
     *
     * What can go wrong:
     *   - Stale reads: thread sees old value because write wasn't published.
     *   - Lost writes: two threads write same key, one write disappears.
     *   - Torn reads: thread reads a partially-written object reference.
     *
     * We pre-populate the cache, then have threads mix reads and writes.
     * We verify no exceptions and final stats are internally consistent.
     */
    @Disabled("Temporarily disabled during server integration")
    @ParameterizedTest(name = "Mixed reads and writes — {0}")
    @MethodSource("cacheProvider")
    @Order(2)
    @DisplayName("Concurrent mixed reads/writes: no corruption")
    void testConcurrentMixedReadWrite_noCorruption(Cache<String, String> cache)
            throws InterruptedException {

        // Pre-populate with known keys so reads have something to hit.
        int preloadCount = CACHE_CAPACITY / 2;
        for (int i = 0; i < preloadCount; i++) {
            cache.put("preload-" + i, "initial-" + i);
        }

        AtomicInteger exceptionCount = new AtomicInteger(0);
        AtomicLong    totalReads     = new AtomicLong(0);
        AtomicLong    totalWrites    = new AtomicLong(0);
        CountDownLatch startGate     = new CountDownLatch(1);
        CountDownLatch doneLatch     = new CountDownLatch(THREAD_COUNT);
        Random         rng           = new Random(42); // fixed seed for reproducibility

        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    startGate.await();
                    Random localRng = new Random(threadId); // per-thread RNG, no contention

                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        if (localRng.nextDouble() < 0.6) {
                            // 60% reads — hit preloaded keys
                            int keyIdx = localRng.nextInt(preloadCount);
                            cache.get("preload-" + keyIdx);
                            totalReads.incrementAndGet();
                        } else {
                            // 40% writes — mix of new keys and updating preloaded keys
                            if (localRng.nextBoolean()) {
                                cache.put("new-" + threadId + "-" + i, "val");
                            } else {
                                int keyIdx = localRng.nextInt(preloadCount);
                                cache.put("preload-" + keyIdx, "updated-by-" + threadId);
                            }
                            totalWrites.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    exceptionCount.incrementAndGet();
                    System.err.println("[" + cacheName(cache) + "] Exception in mixed: "
                            + e.getClass().getSimpleName() + ": " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startGate.countDown();
        boolean finished = doneLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pool.shutdown();

        assertTrue(finished,
                "[" + cacheName(cache) + "] Mixed test timed out — possible deadlock");
        assertEquals(0, exceptionCount.get(),
                "[" + cacheName(cache) + "] Exceptions during mixed read/write");

        // Stats consistency check: hits + misses should equal totalReads approximately.
        // (approximate because stats are updated after the op, threads can interleave)
        CacheStats stats = cache.getstats();
        assertTrue(stats.hits() >= 0,    "Hit count must be non-negative");
        assertTrue(stats.misses() >= 0,  "Miss count must be non-negative");
        assertTrue(stats.evictions() >= 0, "Eviction count must be non-negative");

        System.out.printf("[%s] reads=%d writes=%d hits=%d misses=%d evictions=%d%n",
                cacheName(cache), totalReads.get(), totalWrites.get(),
                stats.hits(), stats.misses(), stats.evictions());
    }

    // -------------------------------------------------------------------------
    // Test 3: Size never exceeds capacity
    // -------------------------------------------------------------------------

    /**
     * Continuously samples size() while 30 threads hammer puts.
     *
     * This specifically catches the scenario where two threads both pass
     * the capacity check simultaneously and both insert, pushing size above
     * capacity before eviction catches up.
     *
     * LockFreeCache intentionally allows this by a small margin (see its
     * eviction CAS design). CoarseGrained and Segmented should be strict.
     * We use a tolerance of +5% for LockFreeCache.
     */
    @Disabled("Temporarily disabled during server integration")
    @ParameterizedTest(name = "Size never exceeds capacity — {0}")
    @MethodSource("cacheProvider")
    @Order(3)
    @DisplayName("size() stays within capacity bounds under concurrent puts")
    void testSizeNeverExceedsCapacity(Cache<String, String> cache)
            throws InterruptedException {

        AtomicInteger maxObservedSize = new AtomicInteger(0);
        CountDownLatch startGate      = new CountDownLatch(1);
        CountDownLatch doneLatch      = new CountDownLatch(THREAD_COUNT);

        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    startGate.await();
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        cache.put("k-" + threadId + "-" + i, "v");

                        // Sample size on every 10th operation.
                        // Sampling every op would slow the test significantly.
                        if (i % 10 == 0) {
                            int s = cache.size();
                            // CAS loop to update max atomically without a lock.
                            int current;
                            do {
                                current = maxObservedSize.get();
                                if (s <= current) break;
                            } while (!maxObservedSize.compareAndSet(current, s));
                        }
                    }
                } catch (Exception e) {
                    System.err.println(cacheName(cache) + " size test exception: " + e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startGate.countDown();
        doneLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pool.shutdown();

        int maxSeen = maxObservedSize.get();

        // LockFreeCache may exceed by a small margin due to its optimistic eviction.
        // We allow up to 5% over capacity for it specifically.
        int allowedMax = cache instanceof LockFreeCache
                ? (int) (CACHE_CAPACITY * 1.05)
                : CACHE_CAPACITY;

        assertTrue(maxSeen <= allowedMax,
                "[" + cacheName(cache) + "] Max observed size " + maxSeen +
                        " exceeded allowed max " + allowedMax);

        System.out.printf("[%s] Max observed size during stress: %d (capacity=%d)%n",
                cacheName(cache), maxSeen, CACHE_CAPACITY);
    }

    // -------------------------------------------------------------------------
    // Test 4: Put-then-get consistency within the same thread
    // -------------------------------------------------------------------------

    /**
     * After a thread puts a key, it should be able to get it back.
     * This sounds obvious but fails without proper memory visibility:
     * the JVM may reorder the put and get, or cache the result in a register.
     *
     * Note: This tests WITHIN-thread consistency. Cross-thread "I put it,
     * you get it" is harder to test deterministically (the key may have
     * been evicted by another thread between put and get).
     */
    @ParameterizedTest(name = "Put-then-get consistency — {0}")
    @MethodSource("cacheProvider")
    @Order(4)
    @DisplayName("Thread that puts a key can immediately get it back")
    void testPutThenGet_sameThread_consistent(Cache<String, String> cache)
            throws InterruptedException {

        AtomicInteger inconsistencies = new AtomicInteger(0);
        CountDownLatch startGate      = new CountDownLatch(1);
        CountDownLatch doneLatch      = new CountDownLatch(THREAD_COUNT);

        // Smaller capacity to force evictions, making the test harder.
        // Use separate cache here — we want a tight capacity.
        Cache<String, String> tightCache;
        if      (cache instanceof CoarseGrainedCache) tightCache = new CoarseGrainedCache<>(new LRUCache<>(10));
        else if (cache instanceof SegmentedCache)      tightCache = new SegmentedCache<>(10, 4);
        else                                           tightCache = new LockFreeCache<>(10);

        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    startGate.await();

                    // Each thread uses a key unique to itself.
                    // Since capacity=10 and threads=30, many keys will be evicted.
                    // We only check consistency when the key is definitely still ours.
                    String myKey = "exclusive-thread-" + threadId;

                    for (int i = 0; i < 50; i++) {
                        String expected = "v-" + i;
                        tightCache.put(myKey, expected);

                        // Immediately retrieve — if the key was evicted by another
                        // thread between put and get, result may be null or stale.
                        // We only flag as inconsistent if we got a WRONG value
                        // (null is acceptable — eviction is correct behaviour).
                        String actual = tightCache.get(myKey);
                        if (actual != null && !actual.equals(expected)) {
                            inconsistencies.incrementAndGet();
                            System.err.printf("[%s] Thread %d: put '%s' but got '%s'%n",
                                    cacheName(cache), threadId, expected, actual);
                        }
                    }
                } catch (Exception e) {
                    System.err.println(cacheName(cache) + " consistency exception: " + e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startGate.countDown();
        doneLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(0, inconsistencies.get(),
                "[" + cacheName(cache) + "] Value inconsistencies detected: " +
                        inconsistencies.get() + " times a get() returned wrong value after put()");
    }

    // -------------------------------------------------------------------------
    // Test 5: Concurrent evictions — no negative size
    // -------------------------------------------------------------------------

    /**
     * 30 threads each calling evict() on the same set of keys concurrently.
     * Multiple threads evicting the same key is a classic double-free scenario.
     *
     * What can go wrong:
     *   - size goes negative (decremented twice for one entry).
     *   - Exception from removing an already-removed key.
     *   - Deadlock if evict() and put() acquire locks in different orders.
     */
    @ParameterizedTest(name = "Concurrent evictions — {0}")
    @MethodSource("cacheProvider")
    @Order(5)
    @DisplayName("Concurrent evictions: size never negative, no exceptions")
    void testConcurrentEvictions_noNegativeSize(Cache<String, String> cache)
            throws InterruptedException {

        // Pre-populate 50 keys.
        int keyCount = 50;
        for (int i = 0; i < keyCount; i++) {
            cache.put("evict-key-" + i, "value");
        }

        AtomicInteger exceptionCount = new AtomicInteger(0);
        CountDownLatch startGate     = new CountDownLatch(1);
        CountDownLatch doneLatch     = new CountDownLatch(THREAD_COUNT);

        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);

        for (int t = 0; t < THREAD_COUNT; t++) {
            pool.submit(() -> {
                try {
                    startGate.await();
                    // All threads evict the same 50 keys — racing with each other.
                    for (int i = 0; i < keyCount; i++) {
                        cache.evict("evict-key-" + i);
                    }
                } catch (Exception e) {
                    exceptionCount.incrementAndGet();
                    System.err.println(cacheName(cache) + " evict exception: " + e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startGate.countDown();
        doneLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(0, exceptionCount.get(),
                "[" + cacheName(cache) + "] Exceptions during concurrent evictions");

        int finalSize = cache.size();
        assertTrue(finalSize >= 0,
                "[" + cacheName(cache) + "] size() went negative after concurrent evictions: "
                        + finalSize);
    }

    // -------------------------------------------------------------------------
    // Test 6: Deadlock detection under producer-consumer pattern
    // -------------------------------------------------------------------------

    /**
     * Classic deadlock scenario: producers put keys, consumers get+evict them.
     * If locking order is inconsistent (e.g., put() acquires lock A then B,
     * evict() acquires B then A), threads deadlock.
     *
     * We run this with a strict timeout. If threads don't finish in time,
     * we declare a deadlock and fail clearly.
     */
    @ParameterizedTest(name = "Producer-consumer deadlock detection — {0}")
    @MethodSource("cacheProvider")
    @Order(6)
    @DisplayName("Producer-consumer pattern completes without deadlock")
    void testProducerConsumer_noDeadlock(Cache<String, String> cache)
            throws InterruptedException {

        int producerCount = 10;
        int consumerCount = 10;
        int totalThreads  = producerCount + consumerCount;

        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(totalThreads);
        AtomicInteger  errors    = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(totalThreads);

        // Producers: put keys continuously
        for (int p = 0; p < producerCount; p++) {
            final int pid = p;
            pool.submit(() -> {
                try {
                    startGate.await();
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        cache.put("prod-" + pid + "-" + i, "data");
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Consumers: get and evict keys continuously
        for (int c = 0; c < consumerCount; c++) {
            final int cid = c;
            pool.submit(() -> {
                try {
                    startGate.await();
                    Random rng = new Random(cid);
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        int pid = rng.nextInt(producerCount);
                        int idx = rng.nextInt(OPS_PER_THREAD);
                        String key = "prod-" + pid + "-" + idx;

                        if (i % 3 == 0) {
                            cache.evict(key);    // evict every 3rd op
                        } else {
                            cache.get(key);      // read otherwise
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startGate.countDown();

        // Strict 10-second timeout — well below normal completion (~1s).
        // Deadlock will cause this to hit the timeout.
        boolean finished = doneLatch.await(10, TimeUnit.SECONDS);
        pool.shutdownNow(); // force-kill if deadlocked

        assertTrue(finished,
                "[" + cacheName(cache) + "] DEADLOCK DETECTED — threads did not finish in 10s");
        assertEquals(0, errors.get(),
                "[" + cacheName(cache) + "] Errors during producer-consumer test");
    }

    // -------------------------------------------------------------------------
    // Test 7: Stats consistency under concurrent load
    // -------------------------------------------------------------------------

    /**
     * Under concurrent load, stats counters must never be negative and must
     * roughly match the number of operations performed.
     *
     * "Roughly" because there's a race between the operation completing and
     * the stat being recorded — we allow 5% slack for this.
     *
     * This specifically catches AtomicLong vs non-atomic counter bugs:
     * a non-synchronized int++ would lose increments under concurrency.
     */
    @ParameterizedTest(name = "Stats consistency under load — {0}")
    @MethodSource("cacheProvider")
    @Order(7)
    @DisplayName("Stats counters are consistent under concurrent load")
    void testStats_consistentUnderConcurrentLoad(Cache<String, String> cache)
            throws InterruptedException {

        int readThreads  = 15;
        int writeThreads = 15;
        int totalThreads = readThreads + writeThreads;
        int opsEach      = 500;

        // Pre-populate so reads have hits.
        for (int i = 0; i < CACHE_CAPACITY / 2; i++) {
            cache.put("base-" + i, "val");
        }

        CountDownLatch startGate   = new CountDownLatch(1);
        CountDownLatch doneLatch   = new CountDownLatch(totalThreads);
        AtomicLong expectedReads   = new AtomicLong(0);
        AtomicLong expectedWrites  = new AtomicLong(0);

        ExecutorService pool = Executors.newFixedThreadPool(totalThreads);

        for (int t = 0; t < readThreads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    startGate.await();
                    for (int i = 0; i < opsEach; i++) {
                        cache.get("base-" + (i % (CACHE_CAPACITY / 2)));
                        expectedReads.incrementAndGet();
                    }
                } catch (Exception e) {
                    System.err.println("Stats read error: " + e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        for (int t = 0; t < writeThreads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    startGate.await();
                    for (int i = 0; i < opsEach; i++) {
                        cache.put("write-" + tid + "-" + i, "v");
                        expectedWrites.incrementAndGet();
                    }
                } catch (Exception e) {
                    System.err.println("Stats write error: " + e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startGate.countDown();
        doneLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pool.shutdown();

        CacheStats stats = cache.getstats();

        // Hard invariants — these must always hold.
        assertTrue(stats.hits() >= 0,      "Hits must be non-negative");
        assertTrue(stats.misses() >= 0,    "Misses must be non-negative");
        assertTrue(stats.evictions() >= 0, "Evictions must be non-negative");

        // Soft invariant: hits + misses should approximately equal reads performed.
        long recordedReads = stats.hits() + stats.misses();
        long actualReads   = expectedReads.get();
        long slack         = (long) (actualReads * 0.05); // 5% tolerance
        assertTrue(
                Math.abs(recordedReads - actualReads) <= slack + 10,
                String.format(
                        "[%s] Stats mismatch: recorded %d reads but expected ~%d (slack=%d)",
                        cacheName(cache), recordedReads, actualReads, slack
                )
        );

        System.out.printf("[%s] Stats: hits=%d misses=%d evictions=%d hitRate=%.2f%%%n",
                cacheName(cache), stats.hits(), stats.misses(),
                stats.evictions(), stats.hitRate() * 100);
    }

    // -------------------------------------------------------------------------
    // Test 8: High contention on single key
    // -------------------------------------------------------------------------

    /**
     * All threads read and write the SAME single key simultaneously.
     * This is maximum contention — the worst case for any synchronization strategy.
     *
     * CoarseGrained: every thread blocks every other — very slow.
     * Segmented: all threads go to same segment — degrades to coarse.
     * LockFree: ConcurrentHashMap handles it with internal striping.
     *
     * We verify correctness (no exceptions, size stays sane) not performance.
     * Performance comparison is the job of JMH benchmarks.
     */
    @Disabled("Temporarily disabled during server integration")
    @ParameterizedTest(name = "High contention on single key — {0}")
    @MethodSource("cacheProvider")
    @Order(8)
    @DisplayName("All threads hammering one key: no corruption")
    void testHighContention_singleKey(Cache<String, String> cache)
            throws InterruptedException {

        String hotKey            = "THE_HOT_KEY";
        AtomicInteger exceptions = new AtomicInteger(0);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);

        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    startGate.await();
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        // Alternate: read, write, read, evict, read, write...
                        switch (i % 4) {
                            case 0 -> cache.get(hotKey);
                            case 1 -> cache.put(hotKey, "value-" + tid + "-" + i);
                            case 2 -> cache.get(hotKey);
                            case 3 -> cache.evict(hotKey);
                        }
                    }
                } catch (Exception e) {
                    exceptions.incrementAndGet();
                    System.err.println(cacheName(cache) + " hot key exception: " + e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startGate.countDown();
        boolean finished = doneLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pool.shutdown();

        assertTrue(finished,
                "[" + cacheName(cache) + "] Hot key test timed out");
        assertEquals(0, exceptions.get(),
                "[" + cacheName(cache) + "] Exceptions under single-key contention");
    }
}
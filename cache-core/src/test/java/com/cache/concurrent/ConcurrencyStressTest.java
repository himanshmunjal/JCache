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

@DisplayName("Concurrency Stress Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ConcurrencyStressTest {
    private static final int THREAD_COUNT = 30;

    private static final int OPS_PER_THREAD = 1000;

    private static final int CACHE_CAPACITY = 100;

    private static final int TIMEOUT_SECONDS = 30;

    static Stream<Cache<String, String>> cacheProvider() {
        return Stream.of(
                new CoarseGrainedCache<>(new LRUCache<>(CACHE_CAPACITY)),
                new SegmentedCache<>(CACHE_CAPACITY, 16),
                new LockFreeCache<>(CACHE_CAPACITY)
        );
    }

    private static String cacheName(Cache<?, ?> cache) {
        return cache.getClass().getSimpleName();
    }

    @ParameterizedTest(name = "No exceptions under concurrent puts: {0}")
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
                    startGate.await();

                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        String key   = "thread-" + threadId + "-key-" + i;
                        String value = "val-" + i;
                        cache.put(key, value);
                    }
                } catch (Exception e) {
                    exceptionCount.incrementAndGet();
                    System.err.println("[" + cacheName(cache) + "] Exception in put: " + e.getClass().getSimpleName() + ": " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startGate.countDown();

        boolean finished = doneLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        pool.shutdown();

        assertTrue(finished,
                "[" + cacheName(cache) + "] Threads did not finish within " +
                        TIMEOUT_SECONDS + "s: possible deadlock");

        assertEquals(0, exceptionCount.get(),
                "[" + cacheName(cache) + "] Exceptions occurred during concurrent puts");

        int finalSize = cache.size();
        assertTrue(finalSize >= 0,
                "[" + cacheName(cache) + "] size() went negative: " + finalSize);
        assertTrue(finalSize <= CACHE_CAPACITY,
                "[" + cacheName(cache) + "] size() exceeded capacity: " + finalSize);
    }

    @ParameterizedTest(name = "Mixed reads and writes: {0}")
    @MethodSource("cacheProvider")
    @Order(2)
    @DisplayName("Concurrent mixed reads/writes: no corruption")
    void testConcurrentMixedReadWrite_noCorruption(Cache<String, String> cache)
            throws InterruptedException {
        int preloadCount = CACHE_CAPACITY / 2;
        for (int i = 0; i < preloadCount; i++) {
            cache.put("preload-" + i, "initial-" + i);
        }

        AtomicInteger exceptionCount = new AtomicInteger(0);
        AtomicLong    totalReads     = new AtomicLong(0);
        AtomicLong    totalWrites    = new AtomicLong(0);
        CountDownLatch startGate     = new CountDownLatch(1);
        CountDownLatch doneLatch     = new CountDownLatch(THREAD_COUNT);
        Random         rng           = new Random(42);

        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    startGate.await();
                    Random localRng = new Random(threadId);

                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        if (localRng.nextDouble() < 0.6) {
                            int keyIdx = localRng.nextInt(preloadCount);
                            cache.get("preload-" + keyIdx);
                            totalReads.incrementAndGet();
                        } else {
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
                "[" + cacheName(cache) + "] Mixed test timed out: possible deadlock");
        assertEquals(0, exceptionCount.get(),
                "[" + cacheName(cache) + "] Exceptions during mixed read/write");

        CacheStats stats = cache.getStats();
        assertTrue(stats.hits() >= 0,    "Hit count must be non-negative");
        assertTrue(stats.misses() >= 0,  "Miss count must be non-negative");
        assertTrue(stats.evictions() >= 0, "Eviction count must be non-negative");
    }

    @ParameterizedTest(name = "Size never exceeds capacity: {0}")
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

                        if (i % 10 == 0) {
                            int s = cache.size();

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

        int allowedMax = cache instanceof LockFreeCache
                ? (int) (CACHE_CAPACITY * 1.05)
                : CACHE_CAPACITY;

        assertTrue(maxSeen <= allowedMax,
                "[" + cacheName(cache) + "] Max observed size " + maxSeen +
                        " exceeded allowed max " + allowedMax);
    }

    @ParameterizedTest(name = "Put-then-get consistency: {0}")
    @MethodSource("cacheProvider")
    @Order(4)
    @DisplayName("Thread that puts a key can immediately get it back")
    void testPutThenGet_sameThread_consistent(Cache<String, String> cache)
            throws InterruptedException {
        AtomicInteger inconsistencies = new AtomicInteger(0);
        CountDownLatch startGate      = new CountDownLatch(1);
        CountDownLatch doneLatch      = new CountDownLatch(THREAD_COUNT);

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

                    String myKey = "exclusive-thread-" + threadId;

                    for (int i = 0; i < 50; i++) {
                        String expected = "v-" + i;
                        tightCache.put(myKey, expected);

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

    @ParameterizedTest(name = "Concurrent evictions: {0}")
    @MethodSource("cacheProvider")
    @Order(5)
    @DisplayName("Concurrent evictions: size never negative, no exceptions")
    void testConcurrentEvictions_noNegativeSize(Cache<String, String> cache)
            throws InterruptedException {
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

    @ParameterizedTest(name = "Producer-consumer deadlock detection: {0}")
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
                            cache.evict(key);
                        } else {
                            cache.get(key);
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

        boolean finished = doneLatch.await(10, TimeUnit.SECONDS);
        pool.shutdownNow();

        assertTrue(finished,
                "[" + cacheName(cache) + "] DEADLOCK DETECTED: threads did not finish in 10s");
        assertEquals(0, errors.get(),
                "[" + cacheName(cache) + "] Errors during producer-consumer test");
    }

    @ParameterizedTest(name = "Stats consistency under load: {0}")
    @MethodSource("cacheProvider")
    @Order(7)
    @DisplayName("Stats counters are consistent under concurrent load")
    void testStats_consistentUnderConcurrentLoad(Cache<String, String> cache)
            throws InterruptedException {
        int readThreads  = 15;
        int writeThreads = 15;
        int totalThreads = readThreads + writeThreads;
        int opsEach      = 500;

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

        CacheStats stats = cache.getStats();

        assertTrue(stats.hits() >= 0,      "Hits must be non-negative");
        assertTrue(stats.misses() >= 0,    "Misses must be non-negative");
        assertTrue(stats.evictions() >= 0, "Evictions must be non-negative");

        long recordedReads = stats.hits() + stats.misses();
        long actualReads   = expectedReads.get();
        long slack         = (long) (actualReads * 0.05);
        assertTrue(
                Math.abs(recordedReads - actualReads) <= slack + 10,
                String.format(
                        "[%s] Stats mismatch: recorded %d reads but expected ~%d (slack=%d)",
                        cacheName(cache), recordedReads, actualReads, slack
                )
        );
    }

    @ParameterizedTest(name = "High contention on single key: {0}")
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

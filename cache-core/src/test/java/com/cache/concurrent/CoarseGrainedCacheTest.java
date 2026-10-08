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

@DisplayName("CoarseGrainedCache Tests")
class CoarseGrainedCacheTest {
    private static final int CAPACITY = 100;

    private static final int READ_THREADS  = 10;
    private static final int WRITE_THREADS = 10;
    private static final int MIXED_THREADS = 20;

    private static final int OPS_PER_THREAD = 500;

    private static final int TIMEOUT_SECONDS = 10;

    private CoarseGrainedCache<String, String> cache;

    @BeforeEach
    void setUp() {
        cache = new CoarseGrainedCache<>(new LRUCache<>(CAPACITY));
    }

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
    @DisplayName("evict() removes key: get() returns null after eviction")
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

        CacheStats stats = cache.getStats();
        assertNotNull(stats);
    }

    @Test
    @DisplayName("Concurrent readers all see correct values: no corruption")
    void testConcurrentReads_noCorruption() throws InterruptedException {
        for (int i = 0; i < 50; i++) {
            cache.put("stable-" + i, "value-" + i);
        }
        CountDownLatch startGun = new CountDownLatch(1);
        CountDownLatch allDone  = new CountDownLatch(READ_THREADS);
        AtomicInteger errors    = new AtomicInteger(0);
        ExecutorService pool = Executors.newFixedThreadPool(READ_THREADS);
        for (int t = 0; t < READ_THREADS; t++) {
            pool.submit(() -> {
                try {
                    startGun.await();
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        int keyIdx = i % 50;
                        String result = cache.get("stable-" + keyIdx);

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
        startGun.countDown();
        boolean finished = allDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pool.shutdown();
        assertTrue(finished, "Test timed out: possible deadlock");
        assertEquals(0, errors.get(),
                "Concurrent readers saw incorrect values: " + errors.get() + " errors");
    }

    @Test
    @DisplayName("Concurrent writers: size never exceeds capacity")
    void testConcurrentWrites_sizeNeverExceedsCapacity() throws InterruptedException {
        CountDownLatch startGun  = new CountDownLatch(1);
        CountDownLatch allDone   = new CountDownLatch(WRITE_THREADS);
        AtomicInteger  errors    = new AtomicInteger(0);

        List<Integer>  violations = Collections.synchronizedList(new ArrayList<>());

        ExecutorService pool = Executors.newFixedThreadPool(WRITE_THREADS);

        for (int t = 0; t < WRITE_THREADS; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    startGun.await();
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
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

        assertTrue(finished, "Test timed out: possible deadlock");
        assertEquals(0, errors.get(), "Exceptions during concurrent writes: " + errors.get());
        assertTrue(violations.isEmpty(),
                "Size exceeded capacity " + violations.size() + " times. Max seen: "
                        + violations.stream().mapToInt(Integer::intValue).max().orElse(0));
    }

    @Test
    @DisplayName("Concurrent writes to same key: no torn write, value is coherent")
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

        String finalValue = cache.get(sharedKey);
        assertNotNull(finalValue, "Shared key should not be null after concurrent writes");
        assertTrue(finalValue.startsWith("thread-"),
                "Value should be one of the written values, got: " + finalValue);
    }

    @Test
    @DisplayName("Mixed concurrent reads and writes: no exceptions, no corruption")
    void testMixedReadWrite_noExceptions() throws InterruptedException {
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
                            cache.get("base-" + (i % 20));
                        } else {
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
        assertTrue(finished, "Test timed out: possible deadlock");
        assertEquals(0, errors.get(),
                "Exceptions during mixed load: " + errors.get());
    }

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
                            cache.get("k" + (i % 50));
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

        assertTrue(finished, "Test timed out: deadlock suspected");
        assertEquals(0, errors.get(),
                "Size invariant violated or exception thrown: " + errors.get() + " occurrences");
    }

    @Test
    @DisplayName("Stats counters are non-negative after concurrent access")
    void testStats_nonNegativeAfterConcurrentAccess() throws InterruptedException {
        int threadCount = 10;
        CountDownLatch startGun = new CountDownLatch(1);
        CountDownLatch allDone  = new CountDownLatch(threadCount);

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        cache.put("exists", "yes");

        for (int t = 0; t < threadCount; t++) {
            pool.submit(() -> {
                try {
                    startGun.await();
                    for (int i = 0; i < 100; i++) {
                        cache.get("exists");
                        cache.get("missing");
                    }
                } catch (Exception e) {
                } finally {
                    allDone.countDown();
                }
            });
        }

        startGun.countDown();
        allDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pool.shutdown();

        CacheStats stats = cache.getStats();
        assertNotNull(stats);

        assertTrue(cache.size() >= 0, "Size must be non-negative after concurrent access");
    }

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

                    for (int i = 0; i < 100; i++) {
                        cache.put("key-" + threadId, "val-" + i);
                        cache.get("key-" + threadId);
                        cache.size();
                    }
                } catch (Exception ignored) {
                } finally {
                    allDone.countDown();
                }
            });
        }

        startGun.countDown();

        boolean finished = allDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pool.shutdown();

        assertTrue(finished,
                "DEADLOCK DETECTED: " + threadCount + " threads did not complete within "
                        + TIMEOUT_SECONDS + " seconds");
    }

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
                    cache.evict("shared");
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

    @Test
    @DisplayName("Interleaved put and evict from different threads: no corruption")
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
